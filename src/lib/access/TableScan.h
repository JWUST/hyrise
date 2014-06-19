// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_TABLESCAN_H_
#define SRC_LIB_ACCESS_TABLESCAN_H_

#include <memory>
#include "access/system/ParallelizablePlanOperation.h"
#include "helper/types.h"

namespace hyrise {
namespace access {

class AbstractExpression;

/// Implements registration based expression scan
class TableScan : public ParallelizablePlanOperation {
 public:
  /// Construct TableScan for a specific expression, take
  /// ownership of passed in expression
  explicit TableScan(std::unique_ptr<AbstractExpression> expr);
  /// Parse TableScan from
  const std::string vname() { return "TableScan"; }
  virtual taskscheduler::DynamicCount determineDynamicCount(size_t maxTaskRunTime);
  virtual std::vector<taskscheduler::task_ptr_t> applyDynamicParallelization(taskscheduler::DynamicCount dynamicCount) override;
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

 protected:
  void setupPlanOperation();
  void executePlanOperation();

 private:
  std::unique_ptr<AbstractExpression> _expr;

  // parameters for MTS model
  double _a = 4.01879860e-01;
  double _b = 3.21264964e-05;
  double _c = 2.14619448e-01;
};
}
}

#endif
