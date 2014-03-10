// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_CCHASHJOIN_H_
#define SRC_LIB_ACCESS_CCHASHJOIN_H_

#include "access/system/PlanOperation.h"
#include "storage/AbstractTable.h"
#include "storage/FixedLengthVector.h"

namespace hyrise {
namespace access {

// used to test protected methods
class ParallelExecutionTest_data_distribution_Test;

class CCHashJoin : public PlanOperation {
  friend class ParallelExecutionTest_data_distribution_Test;

public:
  CCHashJoin();
  virtual ~CCHashJoin();

  void setupPlanOperation();
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value &data);
  const std::string vname();

  size_t getFootprint();
private:
  std::shared_ptr<hyrise::storage::FixedLengthVector<value_id_t> > _attribute_vector;
  size_t _attribute_vector_offset;
  hyrise::storage::AbstractTable::SharedDictionaryPtr _dictionary;
};

}
}

#endif  // SRC_LIB_ACCESS_CCHASHJOIN_H_
