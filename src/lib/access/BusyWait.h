// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_BUSYWAIT_H_
#define SRC_LIB_ACCESS_BUSYWAIT_H_

#include "access/system/PlanOperation.h"

namespace hyrise {
namespace access {

class BusyWait : public PlanOperation {
public:
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(Json::Value &data);
  const std::string vname();

  size_t getMicroseconds() const {
    return _microseconds;
  }

  void setMicroseconds(size_t microseconds) {
    _microseconds = microseconds;
  }

private:
  size_t _microseconds;
};
}
}

#endif // SRC_LIB_ACCESS_BUSYWAIT_H_
