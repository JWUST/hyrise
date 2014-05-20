// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "SpawnNops.h"

#include "access/NoOp.h"
#include "taskscheduler/SharedScheduler.h"

namespace hyrise {
namespace access {

// Executing this on a store with delta results in undefined behavior
// Execution with horizontal tables results in undefined behavior
void SpawnNops::executePlanOperation() {
  std::vector<std::shared_ptr<PlanOperation>> children;
  std::vector<std::shared_ptr<Task>> successors;
  auto scheduler = taskscheduler::SharedScheduler::getInstance().getScheduler();

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

  for (size_t i = 0; i < m_numberOfNops; ++i) {
    children.push_back(std::make_shared<access::NoOp>());
    for (auto successor : successors)
      successor->addDependency(children[i]);
  }

  for (auto child : children) {
    scheduler->schedule(child);
  }
}

void SpawnNops::setNumberOfNops(const size_t number) { m_numberOfNops = number; }

std::shared_ptr<PlanOperation> SpawnNops::parse(const Json::Value& data) {
  return std::make_shared<access::NoOp>();
}

const std::string SpawnNops::vname() { return "SpawnNops"; }
}
}
