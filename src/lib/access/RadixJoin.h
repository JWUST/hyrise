// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_RADIXJOIN_H_
#define SRC_LIB_ACCESS_RADIXJOIN_H_

#include "access/system/ParallelizablePlanOperation.h"

namespace hyrise {
namespace access {

class RadixJoin : public ParallelizablePlanOperation {
public:
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(Json::Value &data);
  const std::string vname();
  void setBits1(const uint32_t b);
  void setBits2(const uint32_t b);
  uint32_t bits1() const;
  uint32_t bits2() const;

  virtual std::vector<std::shared_ptr<Task> > applyDynamicParallelization(size_t maxTaskRunTime);
protected:
  uint determineDynamicCount(size_t maxTaskRunTime);

private:
  uint32_t _bits1;
  uint32_t _bits2;

void distributePartitions(
          const int partitions,
          const int join_count,
          const int current_join,
          int &first,
          int &last) const;

void copyTaskAttributesFromThis(std::shared_ptr<PlanOperation> to);

std::vector<std::shared_ptr<Task> > build_probe_side(std::string prefix,
                                                          field_t &fields,
                                                          uint probe_par,
                                                          uint32_t bits1,
                                                          uint32_t bits2,
                                                          std::shared_ptr<Task> input);

std::vector<std::shared_ptr<Task> > build_hash_side(std::string prefix,
                                                          field_t &fields,
                                                          uint probe_par,
                                                          uint32_t bits1,
                                                          uint32_t bits2,
                                                          std::shared_ptr<Task> input);

};

}
}


#endif  // SRC_LIB_ACCESS_RADIXJOIN_H_
