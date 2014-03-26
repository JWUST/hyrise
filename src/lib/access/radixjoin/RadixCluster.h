// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_RADIXCLUSTER_H_
#define SRC_LIB_ACCESS_RADIXCLUSTER_H_

#include "access/system/ParallelizablePlanOperation.h"

#include "Histogram.h"

namespace hyrise {
namespace access {

class CreateRadixTable : public PlanOperation {
 public:
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  const std::string vname();
};

/// This class provides a straight-forward implementation of a radix-clustering
/// algorithm. The input to this operation are the required number of bits for
/// the clustering, the prefix sum table from the histogram, the cluster field
/// and the input table to cluster
class RadixCluster : public ParallelizablePlanOperation {
 public:
  RadixCluster();

  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  const std::string vname();
  template <typename T>
  void executeClustering();
  void setPart(const size_t p);
  void setCount(const size_t c);
  void setPartInfo(const int32_t p, const int32_t n);
  void setBits(const uint32_t b, const uint32_t sig = 0);
  uint32_t bits() const;
  uint32_t significantOffset() const;

 private:
  uint32_t _bits;
  uint32_t _significantOffset;
  size_t _start;
  size_t _stop;
  size_t _part;
  size_t _count;
};

template <typename T>
void RadixCluster::executeClustering() {
  const auto& tab = getInputTable();
  auto tableSize = tab->size();
  auto field = _field_definition[0];

  // Result Vector
  const auto& result = getInputTable(1);

  // Get the prefix sum from the input
  const auto& prefix_sum = getInputTable(2);
  const auto& data_prefix_sum =
      std::dynamic_pointer_cast<storage::FixedLengthVector<value_id_t>>(getFixedDataVector(prefix_sum).first->copy());

  const auto& data_hash = getFixedDataVector(result).first;
  const auto& data_pos = getFixedDataVector(result, 1).first;

  // Calculate start stop
  _start = 0;
  _stop = tableSize;
  if (_count > 0) {
    _start = (tableSize / _count) * _part;
    _stop = (_count - 1) == _part ? tableSize : (tableSize / _count) * (_part + 1);
  }

  // check if tab is PointerCalculator; if yes, get underlying table and actual rows and columns
  auto p = std::dynamic_pointer_cast<const storage::PointerCalculator>(tab);
  if (p) {
    _executeHistogram<T>(p->getActualTable(), p->getTableColumnForColumn(field), _start, _stop, bits(), significantOffset(), data_prefix_sum, p->getPositions(), data_hash, data_pos);
  } else {
    // output of radix join is MutableVerticalTable of PointerCalculators
    auto mvt = std::dynamic_pointer_cast<const storage::MutableVerticalTable>(tab);
    if (mvt) {
      auto pc = mvt->containerAt(field);
      auto fieldInContainer = mvt->getOffsetInContainer(field);
      auto p = std::dynamic_pointer_cast<const storage::PointerCalculator>(pc);
      if (p) {
        _executeHistogram<T>(p->getActualTable(), p->getTableColumnForColumn(fieldInContainer), _start, _stop, bits(), significantOffset(), data_prefix_sum, p->getPositions(), data_hash, data_pos);
      } else {
        throw std::runtime_error(
            "Histogram only supports MutableVerticalTable of PointerCalculators; found other AbstractTable than "
            "PointerCalculator inside od MutableVerticalTable.");
      }
    } else {
      _executeHistogram<T>(tab, field, _start, _stop, bits(), significantOffset(), data_prefix_sum, nullptr, data_hash, data_pos);
    }
  }
  addResult(result);
}

/// Second pass on the previously radix clustered table to create the final output
/// table. Input to this operation is the result of the first pass and the second
/// pass of the prefix sum calculation
class RadixCluster2ndPass : public ParallelizablePlanOperation {
 public:
  RadixCluster2ndPass();

  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  const std::string vname();
  void setBits1(const uint32_t b, const uint32_t sig = 0);
  void setBits2(const uint32_t b, const uint32_t sig = 0);
  void setPart(const size_t p);
  void setCount(const size_t c);

 private:
  uint32_t _bits1;
  uint32_t _significantOffset1;
  uint32_t _bits2;
  uint32_t _significantOffset2;
  size_t _part;
  size_t _count;
};
}
}

#endif  // SRC_LIB_ACCESS_RADIXCLUSTER_H_
