// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/TableScan.h"

#include "access/expressions/ExampleExpression.h"
#include "access/expressions/pred_SimpleExpression.h"
#include "access/expressions/ExpressionRegistration.h"
#include "storage/AbstractTable.h"
#include "storage/PointerCalculator.h"
#include "storage/PointerCalculatorFactory.h"
#include "storage/TableRangeView.h"
#include "helper/types.h"
#include "helper/make_unique.h"


#include "access/UnionAll.h"
#include "access/system/ResponseTask.h"

namespace hyrise { namespace access {

namespace { auto _ = QueryParser::registerPlanOperation<TableScan>("TableScan"); }

TableScan::TableScan(std::unique_ptr<AbstractExpression> expr) : _expr(std::move(expr)) {}

void TableScan::setupPlanOperation() {
  const auto& table = getInputTable();
  auto tablerange = std::dynamic_pointer_cast<const TableRangeView>(table);
  if(tablerange)
    _expr->walk({tablerange->getActualTable()});
  else
    _expr->walk({table});
}

void TableScan::executePlanOperation() {
  size_t start, stop;
  const auto& tablerange = std::dynamic_pointer_cast<const TableRangeView>(getInputTable());
  if(tablerange){
    start = tablerange->getStart();
    stop = start + tablerange->size();
  }
  else{
    start = 0;
    stop = getInputTable()->size();
  }

  pos_list_t* positions = _expr->match(start, stop);
  addResult(PointerCalculatorFactory::createPointerCalculatorNonRef(getInputTable(), nullptr, positions));
}

std::shared_ptr<PlanOperation> TableScan::parse(Json::Value& data) {
  return std::make_shared<TableScan>(Expressions::parse(data["expression"].asString(), data));
}

uint TableScan::determineDynamicCount(size_t maxTaskRunTime) {
  if (maxTaskRunTime == 0) {
    return 1;
  }
  const auto& tablerange = std::dynamic_pointer_cast<const TableRangeView>(getInputTable());
  size_t tbl_size = tablerange->size();
  auto rows_per_time_unit = 63104; // rows per ms. TODO this needs to be a configurable value
  auto num_tasks = (tbl_size / (rows_per_time_unit * maxTaskRunTime)) + 1;
  return num_tasks;
}

std::vector<std::shared_ptr<Task> > TableScan::applyDynamicParallelization(size_t maxTaskRunTime){

  std::vector<std::shared_ptr<Task> > tasks;

  auto dynamicCount = this->determineDynamicCount(maxTaskRunTime);

  // if no parallelization is necessary, just return this task again as is
  if (dynamicCount <= 1) {
    tasks.push_back(shared_from_this());
    return tasks;
  }

  // get successors of current task
  std::vector<Task*> successors;
  for (auto doneObserver : _doneObservers) {
    Task* const task = dynamic_cast<Task*>(doneObserver);
    successors.push_back(task);
  }

  // remove task from dependencies of successors
  for (auto successor : successors){
    successor->removeDependency(shared_from_this());
  }

  // remove done observers from current task
  _doneObservers.clear();

  // set part and count for this task as first task
  this->setPart(0);
  this->setCount(dynamicCount);
  tasks.push_back(shared_from_this());
  std::string opIdBase = _operatorId;
  std::ostringstream os;
  os << 0;
  _operatorId = opIdBase + "_" + os.str();
 
  // create other TableScans 
  for(uint i = 1; i < dynamicCount; i++){
    auto t = std::make_shared<TableScan>(std::unique_ptr<AbstractExpression>(_expr->clone()));

    std::ostringstream s;
    s << i;
    t->setOperatorId(opIdBase + "_" + s.str());

    // build tabletask
    t->setProducesPositions(producesPositions);
    t->setPart(i);
    t->setCount(dynamicCount);
    t->setPriority(_priority);
    t->setSessionId(_sessionId);
    t->setPlanId(_planId);
    t->setTXContext(_txContext);
    t->setId(_txContext.tid);

    // set dependencies equal to current task
    for(auto d : _dependencies)
      t->addDoneDependency(d);

    t->setPlanOperationName("TableScan");
    getResponseTask()->registerPlanOperation(t);
    tasks.push_back(t);
  }

  // create union and set dependencies
  auto unionall = std::make_shared<UnionAll>();
  unionall->setPlanOperationName("UnionAll");
  
  for(auto t: tasks)
    unionall->addDependency(t);

  // set union as dependency to all successors
  for (auto successor : successors)
      successor->addDependency(unionall);

  getResponseTask()->registerPlanOperation(unionall);
  tasks.push_back(unionall);
  
  return tasks;
}


}}
