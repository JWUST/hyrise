// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_INTBETWEENSPECIALEXPRESSION_H_
#define SRC_LIB_ACCESS_INTBETWEENSPECIALEXPRESSION_H_

#include "json.h"

#include "access/expressions/AbstractExpression.h"
#include "helper/types.h"
#include "storage/FixedLengthVector.h"
#include "storage/OrderPreservingDictionary.h"
#include "storage/storage_types.h"

namespace hyrise { namespace access {

class IntBetweenSpecialExpression : public AbstractExpression {
  storage::c_atable_ptr_t _table;
  std::shared_ptr<FixedLengthVector<value_id_t>> _vector;
  std::shared_ptr<OrderPreservingDictionary<hyrise_int_t>> _dict;
  const size_t _column;
  const hyrise_int_t _fromValue;
  const hyrise_int_t _toValue;
  value_id_t _fromValueid;
  value_id_t _toValueid;
 public:
  IntBetweenSpecialExpression(
    const size_t& column, 
    const hyrise_int_t& fromValue, 
    const hyrise_int_t& toValue);
  bool operator()(const size_t& row);
  virtual pos_list_t* match(const size_t start, const size_t stop);
  virtual void walk(const std::vector<hyrise::storage::c_atable_ptr_t> &l);
  static std::unique_ptr<IntBetweenSpecialExpression> parse(const Json::Value& data);
  virtual IntBetweenSpecialExpression * clone();
  
};

}}

#endif
