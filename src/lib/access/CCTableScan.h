// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_CCTABLESCAN_H_
#define SRC_LIB_ACCESS_CCTABLESCAN_H_

#include "access/system/PlanOperation.h"
#include "storage/AbstractTable.h"
#include "storage/BaseDictionary.h"
#include "storage/FixedLengthVector.h"

namespace hyrise {
namespace access {

// used to test protected methods
class ParallelExecutionTest_data_distribution_Test;

class CCTableScan : public PlanOperation {
  friend class ParallelExecutionTest_data_distribution_Test;

public:
  CCTableScan();
  virtual ~CCTableScan();

  void setupPlanOperation();
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value &data);
  const std::string vname();

  size_t getFootprint();
private:
  std::shared_ptr<hyrise::storage::FixedLengthVector<value_id_t> > _attribute_vector;
  size_t _attribute_vector_offset;
  std::shared_ptr<hyrise::storage::BaseDictionary<hyrise_int_t> > _dictionary;
};

}
}

#endif  // SRC_LIB_ACCESS_CCTABLESCAN_H_
