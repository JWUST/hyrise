/*
 * CacheConsciousPriorityScheduler.cpp
 *
 *  Created on: Sep 9, 2013
 *      Author: mkilling
 */

#include "CacheConsciousPriorityScheduler.h"
#include "SharedScheduler.h"

namespace hyrise {
namespace taskscheduler {

class Scheduler;

// register Scheduler at SharedScheduler
namespace {
bool registered  =
    SharedScheduler::registerScheduler<CacheConsciousPriorityScheduler<PriorityQueueType>>("CacheConsciousPriorityScheduler");
}

}}
