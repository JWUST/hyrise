// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "FairShareQueue.h"
#include "SharedScheduler.h"

namespace hyrise {
namespace taskscheduler {

namespace {
bool registered1 = SharedScheduler::registerScheduler<FairSharePriorityQueue>("FairShareScheduler");
}
}
}