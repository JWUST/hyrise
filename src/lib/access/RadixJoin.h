// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_RADIXJOIN_H_
#define SRC_LIB_ACCESS_RADIXJOIN_H_

#include "access/system/PlanOperation.h"
#include "helper/types.h"

namespace hyrise {
namespace access {

class RadixJoin : public PlanOperation {
 public:
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  const std::string vname();
  void setBits1(const uint32_t b);
  void setBits2(const uint32_t b);
  uint32_t bits1() const;
  uint32_t bits2() const;

  virtual taskscheduler::DynamicCount determineDynamicCount(size_t maxTaskRunTime) override;
  virtual std::vector<taskscheduler::task_ptr_t> applyDynamicParallelization(taskscheduler::DynamicCount dynamicCount) override;

 protected:
  // for determineDynamicCount
  // overridden from PlanOperation
  size_t getTableSize(size_t dep_index);
  size_t getHashTableSize();
  size_t getProbeTableSize();

 private:
  // Used for fitting model
  double _cluster_a = 3.59397494e+01;
  double _cluster_b = -3.78947920e-03;
  double _cluster_c = 5.05182902e-02;
  double _join_a = 0.31581988;
  double _join_b = -0.01299909;
  double _join_c = 8.30758765;

  uint32_t _bits1;
  uint32_t _bits2;
  static const size_t MaxParallelizationDegree = 400;

  void distributePartitions(const int partitions, const int join_count, const int current_join, int& first, int& last)
      const;

  void copyTaskAttributesFromThis(std::shared_ptr<PlanOperation> to);

  std::vector<taskscheduler::task_ptr_t> build_probe_side(std::string prefix,
                                                          field_t& fields,
                                                          uint probe_par,
                                                          uint32_t bits1,
                                                          uint32_t bits2,
                                                          taskscheduler::task_ptr_t input);

  std::vector<taskscheduler::task_ptr_t> build_hash_side(std::string prefix,
                                                         field_t& fields,
                                                         uint probe_par,
                                                         uint32_t bits1,
                                                         uint32_t bits2,
                                                         taskscheduler::task_ptr_t input);
};
}
}


#endif  // SRC_LIB_ACCESS_RADIXJOIN_H_
