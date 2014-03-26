// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_HISTOGRAM_H_
#define SRC_LIB_ACCESS_HISTOGRAM_H_

#include "access/system/ParallelizablePlanOperation.h"

#include "storage/FixedLengthVector.h"
#include "storage/BaseAttributeVector.h"
#include "storage/OrderPreservingDictionary.h"
#include "storage/BaseDictionary.h"
#include "storage/PointerCalculator.h"

namespace hyrise {
namespace access {

// Extracts the AV from the table at given column
template <typename VectorType, typename Table>
inline std::pair<std::shared_ptr<VectorType>, size_t> _getDataVector(const Table& tab, const size_t column = 0, const bool delta = false) {
  const auto& avs = tab->getAttributeVectors(column);
  auto index = delta ? 1 : 0;
  const auto data = std::dynamic_pointer_cast<VectorType>(avs.at(index).attribute_vector);
  assert(data != nullptr);
  return {data, avs.at(index).attribute_offset};
}

template <typename VectorType = storage::BaseAttributeVector<value_id_t>>
inline std::pair<std::shared_ptr<VectorType>, size_t> getBaseDataVector(
    const storage::c_atable_ptr_t& tab, const size_t column = 0, const bool delta = false) {
  return _getDataVector<VectorType>(tab, column, delta);
}
  
template <typename VectorType = storage::FixedLengthVector<value_id_t>>
inline std::pair<std::shared_ptr<VectorType>, size_t> getFixedDataVector(
   const storage::c_atable_ptr_t& tab, const size_t column = 0) {
  return _getDataVector<VectorType>(tab, column, false); 
}

// Execute the main work of the histogram
// If this is not working on a store, you have to supply the pos_list of the PointerCalculator
// TODO needs a better name and less parameters
template <typename T, typename ResultType = storage::FixedLengthVector<value_id_t>>
void _executeHistogram(storage::c_atable_ptr_t tab, size_t column, size_t start, size_t stop, uint32_t bits, uint32_t significantOffset, std::shared_ptr<ResultType> result_av, const pos_list_t *pc_pos_list = nullptr, std::shared_ptr<ResultType> data_hash = nullptr, std::shared_ptr<ResultType> data_pos = nullptr) {
  // TODO use std::tie
  auto ipair_main = getBaseDataVector(tab, column, false);
  auto ipair_delta = getBaseDataVector(tab, column, true);

  const auto& ivec_main = ipair_main.first;
  const auto& main_dict = std::dynamic_pointer_cast<storage::OrderPreservingDictionary<T>>(tab->dictionaryAt(column));
  const auto& offset_main = ipair_main.second;

  size_t main_size = ivec_main->size();

  const auto& ivec_delta = ipair_delta.first;
  // Detla dict or if delta is empty, main dict which will not be used afterwards.
  // TODO does not work for empty table!!
  // TODO investigate if cast to store is possible.
  const auto& delta_dict = std::dynamic_pointer_cast<storage::BaseDictionary<T>>(tab->dictionaryAt(column, tab->size()-1));
  const auto& offset_delta = ipair_delta.second;

  auto hasher = std::hash<T>();
  auto mask = ((1 << bits) - 1) << significantOffset;
  size_t hash_value;
  for (size_t row = start; row < stop; ++row) {
    size_t actual_row = pc_pos_list ? pc_pos_list->at(row) : row;
    if (actual_row < main_size) {
      hash_value = hasher(main_dict->getValueForValueId(ivec_main->get(offset_main, actual_row)));
    } else {
      hash_value = hasher(delta_dict->getValueForValueId(ivec_delta->get(offset_delta, actual_row - main_size)));
    }
    // happens for histogram
    auto pos_to_write = result_av->inc(0, (hash_value & mask) >> significantOffset);
    // happens for cluster
    if (data_hash) {
      data_hash->set(0, pos_to_write, hash_value);
    }
    if (data_pos) {
      data_pos->set(0, pos_to_write, row);
    }
  }
}

/// This is a Histogram Plan Operation that calculates the number
/// occurences of a single value based on a hash function and a number
/// of significant bits. This Operation is used for the Radix Join
/// implementation
class Histogram : public ParallelizablePlanOperation {
 public:
  Histogram();

  void executePlanOperation();
  /// Parses the JSON string to create the plan operation, parameters
  /// to the json are:
  ///  - bits: to set the number of used bits for the histogram
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  const std::string vname();
  template <typename T>
  void executeHistogram();
  void setPart(const size_t p);
  void setCount(const size_t c);
  void setBits(const uint32_t b, const uint32_t sig = 0);
  void setBits2(const uint32_t b, const uint32_t sig = 0);
  uint32_t bits() const;
  uint32_t significantOffset() const;

 protected:
  std::shared_ptr<storage::Table> createOutputTable(const size_t size) const;

  uint32_t _bits;
  uint32_t _significantOffset;
  uint32_t _bits2;
  uint32_t _significantOffset2;
  size_t _part;
  size_t _count;
};

template <typename T>
void Histogram::executeHistogram() {
  const auto& tab = getInputTable();
  const auto tableSize = getInputTable()->size();
  const auto field = _field_definition[0];

  // Prepare Output Table
  auto result = createOutputTable(1 << bits());
  auto pair = getFixedDataVector(result);

  // Iterate and hash based on the part description
  size_t start = 0, stop = tableSize;
  if (_count > 0) {
    start = (tableSize / _count) * _part;
    stop = (_count - 1) == _part ? tableSize : (tableSize / _count) * (_part + 1);
  }

  // check if tab is PointerCalculator; if yes, get underlying table and actual rows and columns
  auto p = std::dynamic_pointer_cast<const storage::PointerCalculator>(tab);
  if (p) {
    _executeHistogram<T>(p->getActualTable(), p->getTableColumnForColumn(field), start, stop, bits(), significantOffset(), pair.first, p->getPositions()); 
  } else {
    // output of radix join is MutableVerticalTable of PointerCalculators
    auto mvt = std::dynamic_pointer_cast<const storage::MutableVerticalTable>(tab);
    if (mvt) {
      auto pc = mvt->containerAt(field);
      auto fieldInContainer = mvt->getOffsetInContainer(field);
      auto p = std::dynamic_pointer_cast<const storage::PointerCalculator>(pc);
      if (p) {
        _executeHistogram<T>(p->getActualTable(), p->getTableColumnForColumn(fieldInContainer), start, stop, bits(), significantOffset(), pair.first, p->getPositions());
      } else {
        throw std::runtime_error(
            "Histogram only supports MutableVerticalTable of PointerCalculators; found other AbstractTable than "
            "PointerCalculator inside MutableVerticalTable.");
      }
    } else {
      // else; we expect a raw table
      _executeHistogram<T>(tab, field, start, stop, bits(), significantOffset(), pair.first);
    }
  }
  addResult(result);
}

class Histogram2ndPass : public Histogram {
 public:
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  const std::string vname();
};
}
}

#endif  // SRC_LIB_ACCESS_HISTOGRAM_H_
