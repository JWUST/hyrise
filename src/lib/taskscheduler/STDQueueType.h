// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#pragma once

#include <queue>
#include "AbstractQueueType.h"

namespace hyrise {
namespace taskscheduler {

class STDQueueType : public AbstractQueueType {
  std::queue<std::shared_ptr<Task>> _runQueue;

 public:
  void push(const std::shared_ptr<Task>& task);
  bool try_pop(std::shared_ptr<Task>& task);
  size_t unsafe_size();
  size_t size();
};
}
}
