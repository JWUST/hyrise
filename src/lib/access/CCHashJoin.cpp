// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/CCHashJoin.h"

#include <cassert>
#include <unordered_map>

#include "access/system/QueryParser.h"
#include "storage/PointerCalculator.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<CCHashJoin>("CCHashJoin");
}

CCHashJoin::CCHashJoin() {
}

CCHashJoin::~CCHashJoin() {
}

void CCHashJoin::setupPlanOperation() {
  setEvent("PAPI_L3_TCM");
  // const auto tab = getInputTable(0);
  // // const auto &avs = tab->getAttributeVectors(0);
  // // _attribute_vector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t> >(avs.at(0).attribute_vector);
  // // assert(_attribute_vector != nullptr);
  // // _attribute_vector_offset = avs.at(0).attribute_offset;
  // // _dictionary = tab->dictionaryAt(0);

  // auto p = std::dynamic_pointer_cast<const PointerCalculator>(tab);
  // auto ipair = getDataVector(p->getActualTable(), p->getTableColumnForColumn(field));
  // const auto &ivec = ipair.first;
  // const auto &dict = std::dynamic_pointer_cast<OrderPreservingDictionary<T>>(tab->dictionaryAt(p->getTableColumnForColumn(field)));
  // const auto &offset = ipair.second;

  // dict->getValueForValueId(ivec->get(offset, p->getTableRowForRow(row)));

  // std::

  // // auto hasher = std::hash<T>();
  // // for(size_t row = start; row < stop; ++row) {
  // //   auto hash_value  = hasher(dict->getValueForValueId(ivec->get(offset, p->getTableRowForRow(row))));
  // //   pair.first->inc(0, (hash_value & mask) >> significantOffset
  // // }
}

void CCHashJoin::executePlanOperation() {
  typedef std::unordered_multimap<hyrise_int_t, size_t> hashmap_t;

  // auto small_tbl = getInputTable(0)->size() <= getInputTable(1)->size() ? getInputTable(0) : getInputTable(1);
  // auto large_tbl = getInputTable(0)->size() > getInputTable(1)->size() ? getInputTable(0) : getInputTable(1);
  auto small_tbl = getInputTable(0);
  auto large_tbl = getInputTable(1);
  // probe
  hashmap_t hashmap;
  hashmap.reserve(10000);
  size_t small_table_size = small_tbl->size();
  for (size_t i = 0; i < small_table_size; i++) {
    hyrise_int_t val = small_tbl->getValue<hyrise_int_t>(_indexed_field_definition[0], i);
    hashmap.insert(hashmap_t::value_type(val, i));
    //printf("%ld\n", val);
  }

  // join
  storage::pos_list_t *leftResults = new pos_list_t;
  storage::pos_list_t *rightResults = new pos_list_t;
  
  size_t large_table_size = large_tbl->size();
  for (size_t i = 0; i < large_table_size; i++) {
    hyrise_int_t val = large_tbl->getValue<hyrise_int_t>(_indexed_field_definition[1], i);
    const auto range = hashmap.equal_range(val);
    for (auto it = range.first; it != range.second; it++) {
      leftResults->push_back(it->second);
      rightResults->push_back(i);
      //printf("%d -> %d\n", val, it->second);
    }
  }

  // build result table
  std::vector<storage::atable_ptr_t> parts;
  parts.push_back(hyrise::storage::PointerCalculator::create(small_tbl, leftResults));
  parts.push_back(hyrise::storage::PointerCalculator::create(large_tbl, rightResults));

  storage::atable_ptr_t result = std::make_shared<hyrise::storage::MutableVerticalTable>(parts);
  addResult(result);
}

std::shared_ptr<PlanOperation> CCHashJoin::parse(const Json::Value &data) {
  return BasicParser<CCHashJoin>::parse(data); //std::make_shared<CCHashJoin>();
}

size_t CCHashJoin::getFootprint() {
  // distinct values, hardcoded for the sake of the experiment
  return 10000;
}

const std::string CCHashJoin::vname() {
  return "CCHashJoin";
}

}
}
