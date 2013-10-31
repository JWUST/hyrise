
// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/expressions/StringEqualSpecialExpression.h"

#include "storage/AbstractTable.h"

#include "helper/make_unique.h"
#include "access/expressions/ExpressionRegistration.h"

namespace hyrise { namespace access {

namespace {
auto _ = Expressions::add<StringEqualSpecialExpression>("hyrise::stringequal");
}

StringEqualSpecialExpression::StringEqualSpecialExpression(const size_t& column, const hyrise_string_t& value) : _column(column), _value(value) {}

inline bool StringEqualSpecialExpression::operator()(const size_t& row) {  return _vector->getRef(_column, row) == _valueid;  }

pos_list_t* StringEqualSpecialExpression::match(const size_t start, const size_t stop) {
  auto pl = new pos_list_t;
  for(size_t row=start; row < stop; ++row) {
    if (this->StringEqualSpecialExpression::operator()(row)) {
      pl->push_back(row);
    }
  }
  return pl;
}

void StringEqualSpecialExpression::walk(const std::vector<hyrise::storage::c_atable_ptr_t> &tables) {
  _table = tables.at(0);
  const auto& avs = _table->getAttributeVectors(_column);
  _vector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(avs.at(0).attribute_vector);
  _dict = std::dynamic_pointer_cast<OrderPreservingDictionary<hyrise_string_t>>(_table->dictionaryAt(_column));
  if (!(_vector && _dict)) throw std::runtime_error("Could not extract proper structures");
  _valueid = _dict->getValueIdForValue(_value);

}
    
std::unique_ptr<StringEqualSpecialExpression> StringEqualSpecialExpression::parse(const Json::Value& data) {
  return make_unique<StringEqualSpecialExpression>(data["column"].asUInt(), data["value"].asString());
}

StringEqualSpecialExpression * StringEqualSpecialExpression::clone() {
  return new StringEqualSpecialExpression(_column, _value);
}

uint StringEqualSpecialExpression::determineDynamicCount(size_t maxTaskRunTime, size_t input_table_size) {
  auto total_tbl_size_in_100k = (input_table_size)/ 100000.0;

  // this is the b of the mts = a/instances+b model
  auto min_mts = 0.4;

  if (maxTaskRunTime < min_mts) {
    // std::cerr << "Could not honor mts request. Too small." << std::endl;
    return 1024;
  }

  auto a = 1.75162831259492 * total_tbl_size_in_100k - 10.7798799886654;
  int num_tasks = std::max(1,static_cast<int>(round(a/(maxTaskRunTime - min_mts))));

  // std::cout << "StringEqualsExpression: tts(100k): " << total_tbl_size_in_100k << ", num_tasks: " << num_tasks << std::endl;

  return num_tasks;
}

}}
