#pragma once

#include "access/system/PlanOperation.h"
#include "storage/BaseDictionary.h"

namespace hyrise {
  namespace access {
    class DictionaryValues : public PlanOperation {
      public: 
        virtual void executePlanOperation();
    };
  }}
