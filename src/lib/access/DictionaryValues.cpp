#include "access/DictionaryValues.h"
#include "access/system/QueryParser.h"

#include "storage/BaseDictionary.h"
#include "storage/Store.h"
#include "storage/storage_types.h"
#include "storage/meta_storage.h"
#include "storage/Table.h"

namespace hyrise {
  namespace access {
namespace {
  auto _ = QueryParser::registerTrivialPlanOperation<DictionaryValues>("DictionaryValues");
}

struct DictionaryValuesFunctor {
  typedef const storage::c_atable_ptr_t value_type;
  const storage::c_store_ptr_t& store;
  size_t column;
  DictionaryValuesFunctor(const storage::c_store_ptr_t& s, size_t col) : store(s), column(col) {}

  template <typename R>
  value_type operator()() {
      const auto& main_dict = store->getMainTable()->dictionaryAt(column);
      const auto& delta_dict = store->getDeltaTable()->dictionaryAt(column);
      const auto& main_base_dict = std::dynamic_pointer_cast<storage::BaseDictionary<R>>(main_dict);
      const auto& delta_base_dict = std::dynamic_pointer_cast<storage::BaseDictionary<R>>(delta_dict);

      assert(main_base_dict != nullptr);
      assert(delta_base_dict != nullptr);

      std::vector<R> partResult;
      for (auto it = main_base_dict->begin(); it != main_base_dict->end(); ++it) {
        partResult.push_back(*it);
      }
      for (auto it = delta_base_dict->begin(); it != delta_base_dict->end(); ++it) {
        partResult.push_back(*it);
      }

      // create output table
      field_list_t subfields({column});
      auto result = store->copy_structure_modifiable(&subfields);
      result->resize(partResult.size());
      for (size_t i = 0; i < partResult.size(); ++i) {
        result->setValue<R>(0, i, partResult[i]);
      }
      
      return result;
  }
};

    void DictionaryValues::executePlanOperation() {
      const auto& inputTable = input.getTable(0);

      const auto& store = std::dynamic_pointer_cast<const storage::Store>(inputTable);
      if (!store) {
        throw std::runtime_error("Expected a Store (e.g.) TableLoad as input.");
      }

      const auto field = _field_definition[0];
      DictionaryValuesFunctor fun(store, field);
      storage::type_switch<hyrise_basic_types> ts;
      addResult(ts(inputTable->typeOfColumn(field), fun));
    }
    }}
