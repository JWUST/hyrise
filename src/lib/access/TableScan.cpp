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

}}
