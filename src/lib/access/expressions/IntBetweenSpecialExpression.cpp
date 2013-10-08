
// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/expressions/IntBetweenSpecialExpression.h"

#include "storage/AbstractTable.h"

#include "helper/make_unique.h"
#include "access/expressions/ExpressionRegistration.h"

namespace hyrise { namespace access {

namespace {
auto _ = Expressions::add<IntBetweenSpecialExpression>("hyrise::intbetween");
}

IntBetweenSpecialExpression::IntBetweenSpecialExpression(
  const size_t& column,
  const hyrise_int_t& fromValue,
  const hyrise_int_t& toValue) : 
    _column(column),
    _fromValue(fromValue),
    _toValue(toValue) {}

// TODO change to between value ids. Assuming order preserving dictionary.
inline bool IntBetweenSpecialExpression::operator()(const size_t& row) {  \
  auto curValueid = _vector->getRef(_column, row);
  return  _fromValueid <= curValueid && curValueid <= _toValueid;
}

pos_list_t* IntBetweenSpecialExpression::match(const size_t start, const size_t stop) {
  // if from-to-range is out of bounds for dictionary
  if (_fromValue > _dict->getGreatestValue() || _toValue < _dict->getSmallestValue()) { 
    return new pos_list_t();
  } // select everything
  else if (_fromValue < _dict->getSmallestValue() && _toValue > _dict->getGreatestValue()) {
    return nullptr;
  }

  // find an inclusive range
  // if border values do not exist, chose the next "more inner" ones
  _fromValueid = _dict->getValueIdForValueGreaterOrEqualTo(_fromValue);
  _toValueid = _dict->getValueIdForValueSmallerOrEqualTo(_toValue);

  auto pl = new pos_list_t;
  for(size_t row=start; row < stop; ++row) {
    if (this->IntBetweenSpecialExpression::operator()(row)) {
      pl->push_back(row);
    }
  }
  return pl;
}

void IntBetweenSpecialExpression::walk(const std::vector<hyrise::storage::c_atable_ptr_t> &tables) {
  _table = tables.at(0);
  const auto& avs = _table->getAttributeVectors(_column);
  _vector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(avs.at(0).attribute_vector);
  _dict = std::dynamic_pointer_cast<OrderPreservingDictionary<hyrise_int_t>>(_table->dictionaryAt(_column));
  if (!(_vector && _dict)) throw std::runtime_error("Could not extract proper structures");
}
    
std::unique_ptr<IntBetweenSpecialExpression> IntBetweenSpecialExpression::parse(const Json::Value& data) {
  return make_unique<IntBetweenSpecialExpression>(
    data["column"].asUInt(),
    data["fromValue"].asInt(),
    data["toValue"].asInt());
}

IntBetweenSpecialExpression * IntBetweenSpecialExpression::clone() {
  return new IntBetweenSpecialExpression(_column, _fromValue, _toValue);
}

}}
