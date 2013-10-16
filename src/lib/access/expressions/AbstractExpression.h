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
  virtual uint determineDynamicCount(size_t max_task_time, size_t input_table_size) {
    throw std::runtime_error("determineDynamicCount needs to be implemented for this expression for dynamic parallelization.");
  }

private:
  // needs to be filled in in concrete expressions
  // for expression where this is not overridden, 
  // determineDynamicCount results in 1
  // these are the table sizes for the first dimension in lookup
  int table_size_size;
  int table_size[1];
  // for each table size store an array of 2-arrays with the meanTaskRunTime and
  // the degree of parallelism.
  int lookup_size;
  int lookup[1][1][2];


};

}}

#endif
