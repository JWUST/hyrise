// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/SimpleTableScan.h"

#include "access/expressions/pred_buildExpression.h"
#include "access/expressions/pred_EqualsExpression.h"

#include "storage/PointerCalculator.h"
#include "storage/PointerCalculatorFactory.h"
#include "access/system/ResponseTask.h"
#include "access/UnionAll.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<SimpleTableScan>("SimpleTableScan");
}

SimpleTableScan::SimpleTableScan(): _comparator(nullptr) {
}

SimpleTableScan::~SimpleTableScan() {
  if (_comparator)
    delete _comparator;
}

void SimpleTableScan::setupPlanOperation() {
  _comparator->walk(input.getTables());
}

void SimpleTableScan::executePositional() {
  auto tbl = input.getTable(0);
  storage::pos_list_t *pos_list = new pos_list_t();
  for (size_t row=0, input_size=tbl->size();
       row < input_size;
       ++row) {
    if ((*_comparator)(row)) {
      pos_list->push_back(row);
    }
  }
  addResult(PointerCalculatorFactory::createPointerCalculatorNonRef(tbl, nullptr, pos_list));
}

void SimpleTableScan::executeMaterialized() {
  auto tbl = input.getTable(0);
  auto result_table = tbl->copy_structure_modifiable();
  size_t target_row = 0;
  for (size_t row=0, input_size=tbl->size();
       row < input_size;
       ++row) {
    if ((*_comparator)(row)) {
        // TODO materializing result set will make the allocation the boundary
      result_table->resize(target_row + 1);
      result_table->copyRowFrom(input.getTable(0),
                                row,
                                target_row++,
                                true /* Copy Value*/,
                                false /* Use Memcpy */);
    }
  }
  addResult(result_table);
}

void SimpleTableScan::executePlanOperation() {

  if (producesPositions) {
    executePositional();
  } else {
    executeMaterialized();
  }
}

std::shared_ptr<PlanOperation> SimpleTableScan::parse(Json::Value &data) {
  std::shared_ptr<SimpleTableScan> pop = std::make_shared<SimpleTableScan>();

  if (data.isMember("materializing"))
    pop->setProducesPositions(!data["materializing"].asBool());

  if (!data.isMember("predicates")) {
    throw std::runtime_error("There is no reason for a Selection without predicates");
  }
  pop->setPredicate(buildExpression(data["predicates"]));

  return pop;
}

const std::string SimpleTableScan::vname() {
  return "SimpleTableScan";
}

void SimpleTableScan::setPredicate(SimpleExpression *c) {
  _comparator = c;
}

uint SimpleTableScan::determineDynamicCount(size_t maxTaskRunTime) {
  if (maxTaskRunTime == 0) {
    return 1;
  }
  const auto& dep = std::dynamic_pointer_cast<PlanOperation>(_dependencies[0]);
  auto& inputTable = dep->getResultTable();
  auto tbl_size = inputTable->size();
  auto rows_per_time_unit = 5; // TODO this needs to be a configurable value
  auto num_tasks = (tbl_size / (rows_per_time_unit * maxTaskRunTime)) + 1;
  return num_tasks;
}

std::vector<std::shared_ptr<Task> > SimpleTableScan::applyDynamicParallelization(size_t maxTaskRunTime){

  std::vector<std::shared_ptr<Task> > tasks;

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

  auto dynamicCount = this->determineDynamicCount(maxTaskRunTime);

  // set part and count for this task as first task
  this->setPart(0);
  this->setCount(dynamicCount);
  tasks.push_back(shared_from_this());
  std::string opIdBase = _operatorId;
  std::ostringstream os;
  os << 0;
  _operatorId = opIdBase + "_" + os.str();
 
  // create other simpleTableScans 
  for(uint i = 1; i < dynamicCount; i++){
    auto t = std::make_shared<SimpleTableScan>();

    std::ostringstream s;
    s << i;
    t->setOperatorId(opIdBase + "_" + s.str());

    // build simpletabletask
    t->setPredicate(_comparator->clone());
    t->setProducesPositions(producesPositions);
    t->setPart(i);
    t->setCount(dynamicCount);
    t->setPriority(_priority);
    t->setSessionId(_sessionId);
    t->setPlanId(_planId);
    t->setTXContext(_txContext);
    t->setId(_txContext.tid);

    // set depencies equal to current task
    for(auto d : _dependencies)
      t->addDoneDependency(d);

    t->setPlanOperationName("SimpleTableScan");
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


}
}
