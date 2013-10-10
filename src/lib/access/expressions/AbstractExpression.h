#ifndef SRC_LIB_ACCESS_ABSTRACTEXPRESSION_H_
#define SRC_LIB_ACCESS_ABSTRACTEXPRESSION_H_

#include <vector>
#include "helper/types.h"

namespace hyrise { namespace access {

/// Abstract expression interface
class AbstractExpression {
 public:
  virtual ~AbstractExpression() {}
  virtual void walk(const std::vector<storage::c_atable_ptr_t> &l) = 0;
  virtual storage::pos_list_t* match(const size_t start, const size_t stop) = 0;
  virtual AbstractExpression * clone(){
    throw std::runtime_error("Cannot clone base class; implement in derived");
  }
  // delegated from TableScan
  virtual uint determineDynamicCount(size_t table_size) {
    throw std::runtime_error("Expression has not implemented determineDynamicCount. \
      Cannot proceed.");
  }
};

}}

#endif
