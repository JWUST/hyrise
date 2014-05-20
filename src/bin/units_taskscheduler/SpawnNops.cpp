// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "SpawnNops.h"

#include "taskscheduler/SharedScheduler.h"

namespace hyrise {
namespace access {

// Executing this on a store with delta results in undefined behavior
// Execution with horizontal tables results in undefined behavior
void SpawnNops::executePlanOperation() {
  std::vector<std::shared_ptr<PlanOperation>> children;

  auto scheduler = taskscheduler::SharedScheduler::getInstance().getScheduler();
  for (auto nop : _nops) {
    scheduler->schedule(nop);
  }
}

void SpawnNops::createNops(const size_t number) { 
  m_numberOfNops = number; 
  std::vector<std::shared_ptr<Task>> successors;
  {
    std::lock_guard<decltype(_observerMutex)> lk(_observerMutex);
    for (const auto& weakDoneObserver : _doneObservers) {
      if (auto doneObserver = weakDoneObserver.lock()) {
        if (const auto task = std::dynamic_pointer_cast<Task>(doneObserver)) {
          successors.push_back(std::move(task));
        }
      }
    }
  }

  _nops.reserve(m_numberOfNops);
  for (size_t i = 0; i < m_numberOfNops; ++i) {
    _nops.push_back(std::make_shared<access::NoOp>());
    for (auto successor : successors)
      successor->addDependency(_nops[i]);
  }

}

std::shared_ptr<PlanOperation> SpawnNops::parse(const Json::Value& data) {
  return std::make_shared<access::NoOp>();
}

const std::string SpawnNops::vname() { return "SpawnNops"; }
}
}
