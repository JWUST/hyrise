// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/CCTableScan.h"

#include <cassert>

#include "access/system/QueryParser.h"
#include "storage/PointerCalculator.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<CCTableScan>("CCTableScan");
}

CCTableScan::CCTableScan() {
}

CCTableScan::~CCTableScan() {
}

void CCTableScan::setupPlanOperation() {
  setEvent("PAPI_L3_TCM");

  const auto tab = getInputTable(0);
  const auto &avs = tab->getAttributeVectors(0);
  _attribute_vector = std::dynamic_pointer_cast<hyrise::storage::FixedLengthVector<value_id_t> >(avs.at(0).attribute_vector);
  assert(_attribute_vector != nullptr);
  _attribute_vector_offset = avs.at(0).attribute_offset;

  typedef hyrise::storage::BaseDictionary<hyrise_int_t> dict_t;
  _dictionary = std::static_pointer_cast<dict_t>(tab->dictionaryAt(0));
}

void CCTableScan::executePlanOperation() {
  storage::pos_list_t *posList = new pos_list_t;
  pl->reserve(_attribute_vector->size());
  for (size_t i = 0; i < _attribute_vector->size(); i++) {
    auto val = _attribute_vector->getRef(0, i);
    if (_dictionary->getValueForValueId(val) >= 10000) {
      posList->push_back(i);
    }
  }

  addResult(hyrise::storage::PointerCalculator::create(getInputTable(0), posList));
}

std::shared_ptr<PlanOperation> CCTableScan::parse(const Json::Value &data) {
  return std::make_shared<CCTableScan>();
}

size_t CCTableScan::getFootprint() {
  return 0;
}

const std::string CCTableScan::vname() {
  return "CCTableScan";
}

}
}
