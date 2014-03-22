// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_HISTOGRAM_H_
#define SRC_LIB_ACCESS_HISTOGRAM_H_

#include "access/system/ParallelizablePlanOperation.h"

#include "storage/FixedLengthVector.h"
#include "storage/OrderPreservingDictionary.h"
#include "storage/PointerCalculator.h"

namespace hyrise {
namespace access {


// Extracts the AV from the table at given column
template <typename Table, typename VectorType = storage::FixedLengthVector<value_id_t>>
inline std::pair<std::shared_ptr<VectorType>, size_t> _getDataVector(const Table& tab, const size_t column = 0) {
  const auto& avs = tab->getAttributeVectors(column);
  const auto data = std::dynamic_pointer_cast<VectorType>(avs.at(0).attribute_vector);
  assert(data != nullptr);
  return {data, avs.at(0).attribute_offset};
}

template <typename VectorType = storage::FixedLengthVector<value_id_t>>
inline std::pair<std::shared_ptr<VectorType>, size_t> getDataVector(const storage::c_atable_ptr_t& tab,
                                                                    const size_t column = 0) {
  return _getDataVector<decltype(tab), VectorType>(tab, column);
}


// Extracts the AV from the table at given column
template <typename Table, typename VectorType = storage::FixedLengthVector<value_id_t>>
inline std::pair<std::shared_ptr<VectorType>, size_t> _getDeltaDataVector(const Table& tab, const size_t column = 0) {
  const auto& avs = tab->getAttributeVectors(column);
  const auto data = std::dynamic_pointer_cast<VectorType>(avs.at(1).attribute_vector);
  assert(data != nullptr);
  return {data, avs.at(0).attribute_offset};
}

template <typename VectorType = storage::FixedLengthVector<value_id_t>>
inline std::pair<std::shared_ptr<VectorType>, size_t> getDeltaDataVector(const storage::c_atable_ptr_t& tab,
                                                                    const size_t column = 0) {
  return _getDataVector<decltype(tab), VectorType>(tab, column);
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

  // Prepare mask
  auto mask = ((1 << bits()) - 1) << significantOffset();

  // Prepare Output Table
  auto result = createOutputTable(1 << bits());
  auto pair = getDataVector(result);

  // Iterate and hash based on the part description
  size_t start = 0, stop = tableSize;
  if (_count > 0) {
    start = (tableSize / _count) * _part;
    stop = (_count - 1) == _part ? tableSize : (tableSize / _count) * (_part + 1);
  }

  // check if tab is PointerCalculator; if yes, get underlying table and actual rows and columns
  auto p = std::dynamic_pointer_cast<const storage::PointerCalculator>(tab);
  if (p) {

    //auto ipair = getDataVector(p->getActualTable(), p->getTableColumnForColumn(field));
    //const auto& ivec = ipair.first;
    //const auto& dict = std::dynamic_pointer_cast<storage::OrderPreservingDictionary<T>>(
    //    tab->dictionaryAt(p->getTableColumnForColumn(field)));
    //const auto& offset = ipair.second;

    auto ipair_main = getDataVector(p->getActualTable(), p->getTableColumnForColumn(field));
    auto ipair_delta = getDeltaDataVector(p->getActualTable(), p->getTableColumnForColumn(field));

    const auto& ivec_main = ipair_main.first;
    const auto& dict = std::dynamic_pointer_cast<storage::OrderPreservingDictionary<T>>(tab->dictionaryAt(p->getTableColumnForColumn(field)));
    const auto& offset_main = ipair_main.second;

    const auto& ivec_delta = ipair_delta.first;
    const auto& offset_delta = ipair_delta.second;

    size_t main_size = ivec_main->size();
    auto hasher = std::hash<T>();
    size_t hash_value;
    for (size_t row = start; row < stop; ++row) {
      if(row < main_size)
        hash_value = hasher(dict->getValueForValueId(ivec_main->get(offset_main, p->getTableRowForRow(row))));
      else
        hash_value = hasher(dict->getValueForValueId(ivec_delta->get(offset_delta, p->getTableRowForRow(row)-main_size)));
      pair.first->inc(0, (hash_value & mask) >> significantOffset());
    }
  } else {

    // output of radix join is MutableVerticalTable of PointerCalculators
    auto mvt = std::dynamic_pointer_cast<const storage::MutableVerticalTable>(tab);
    if (mvt) {
      auto pc = mvt->containerAt(field);
      auto p = std::dynamic_pointer_cast<const storage::PointerCalculator>(pc);
      if (p) {
        
        auto ipair_main = getDataVector(p->getActualTable(), p->getTableColumnForColumn(field));
        auto ipair_delta = getDeltaDataVector(p->getActualTable(), p->getTableColumnForColumn(field));

        const auto& ivec_main = ipair_main.first;
        const auto& dict = std::dynamic_pointer_cast<storage::OrderPreservingDictionary<T>>(tab->dictionaryAt(p->getTableColumnForColumn(field)));
        const auto& offset_main = ipair_main.second;

        const auto& ivec_delta = ipair_delta.first;
        const auto& offset_delta = ipair_delta.second;

        size_t main_size = ivec_main->size();
        auto hasher = std::hash<T>();
        size_t hash_value;
        for (size_t row = start; row < stop; ++row) {
          if(row < main_size)
            hash_value = hasher(dict->getValueForValueId(ivec_main->get(offset_main, p->getTableRowForRow(row))));
          else
            hash_value = hasher(dict->getValueForValueId(ivec_delta->get(offset_delta, p->getTableRowForRow(row)-main_size)));
          pair.first->inc(0, (hash_value & mask) >> significantOffset());
        }

      } else {
        throw std::runtime_error(
            "Histogram only supports MutableVerticalTable of PointerCalculators; found other AbstractTable than "
            "PointerCalculator inside od MutableVerticalTable.");
      }
    } else {
      // else; we expect a raw table
      auto ipair_main = getDataVector(tab, field);
      auto ipair_delta = getDeltaDataVector(tab, field);

      const auto& ivec_main = ipair_main.first;
      const auto& dict = std::dynamic_pointer_cast<storage::OrderPreservingDictionary<T>>(tab->dictionaryAt(field));
      const auto& offset_main = ipair_main.second;

      const auto& ivec_delta = ipair_delta.first;
      const auto& offset_delta = ipair_delta.second;

      size_t main_size = ivec_main->size();
      auto hasher = std::hash<T>();
      size_t hash_value;
      for (size_t row = start; row < stop; ++row) {
        if(row < main_size)
          hash_value = hasher(dict->getValueForValueId(ivec_main->get(offset_main, row)));
        else
          hash_value = hasher(dict->getValueForValueId(ivec_delta->get(offset_delta, row-main_size)));
        pair.first->inc(0, (hash_value & mask) >> significantOffset());
      }
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
