// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/BusyWait.h"
#include <time.h>
#include <sys/time.h>
#include "access/system/QueryParser.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<BusyWait>("BusyWait");
}

void BusyWait::executePlanOperation() {
  uint64_t istart, iend;
  struct timeval time;

  gettimeofday(&time, 0);
  istart = time.tv_usec + time.tv_sec * 1000000;
  iend = istart;

  while ((iend - istart) < _microseconds){
    gettimeofday(&time, 0);
    iend = time.tv_usec + time.tv_sec * 1000000;
  }
}

std::shared_ptr<PlanOperation> BusyWait::parse(Json::Value &data) {
  std::shared_ptr<BusyWait> waiter = std::make_shared<BusyWait>();
  waiter->setMicroseconds(data["microseconds"].asUInt());
  return waiter;
}

const std::string BusyWait::vname() {
  return "BusyWait";
}

}
}
