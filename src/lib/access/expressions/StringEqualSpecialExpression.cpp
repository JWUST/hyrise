
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

uint StringEqualSpecialExpression::determineDynamicCount(size_t max_task_size, size_t input_table_size) {
  //find closest table size in lookup

  int difference, size_index;
  size_index = 0;
  difference = abs(input_table_size - table_size[0]);

  // TODO constant values
  for(int i = 0; i < table_size_size; i++) {
    if (difference > abs(input_table_size - table_size[i])) {
      difference = abs(input_table_size - table_size[i]);
      size_index = i;
    }
  }

  int lookup_index;
  lookup_index = 0;
  difference = abs(max_task_size - lookup[size_index][0][0]);

  // TODO constant values
  // TOOD maybe sort the values the other way around.
  for(int i = 0; i < lookup_size; i++) {
    if (difference > abs(max_task_size - lookup[size_index][i][0])) {
      difference = abs(max_task_size - lookup[size_index][i][0]);
      lookup_index = i;
    }
  }
  std::cout << "TablScan: Size is: " << input_table_size << " lookup_index=" << lookup_index << ", size_index=" << size_index << std::endl;
  std::cout << "TableScan determineDynamicCount: " << lookup[size_index][lookup_index][1] << std::endl;
  return lookup[size_index][lookup_index][1];
}

}}
