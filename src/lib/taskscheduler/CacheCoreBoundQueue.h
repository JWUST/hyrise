/*
 * CacheCoreBoundQueue.h
 *
 *  Created on: Febr 24, 2014
 *      Author: jwust
 */

#pragma once

#include "taskscheduler/CoreBoundQueue.h"
#include "taskscheduler/RunObserver.h"

namespace hyrise {
namespace taskscheduler {

/*
 * A queue with a dedicated worker thread; used by SimpleTaskScheduler to run tasks
 */
class CacheCoreBoundQueue : public CoreBoundQueue {
  RunObserver * _cache;

 public:
  /*
   * intializes a task queue and pins created thread to given core
   */
  CacheCoreBoundQueue(int core): CoreBoundQueue(core), _cache(NULL){}
  CacheCoreBoundQueue(int core, RunObserver * observer): CoreBoundQueue(core), _cache(observer){}
  ~CacheCoreBoundQueue(){};
  /*
   * Is executed by dedicated thread to work the queue
   */
  void setRunObserver(RunObserver * observer);
  /*
   * Is executed by dedicated thread to work the queue
   */
  void executeTask();
};

} } // namespace hyrise::taskscheduler

