// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#pragma once

#include "ThreadLevelQueuesScheduler.h"
#include "CacheConsciousQueue.h"

namespace hyrise {
namespace taskscheduler {

/*
* 2-Level-Scheduler: This scheduler dispatches tasks to queues,
* each running a provided number of threads on a dedicated node.
* I considers the last level data cache filling of each core
*
* Note that the "ownership" of queries is transferred, e.g., a
* dispatched RequestParseTask will submit generated tasks to the node
* level scheduler
*/
template <class QUEUE>
class CacheConsciousScheduler : public NodeBoundQueuesScheduler<QUEUE> {
    using ThreadLevelQueuesScheduler<QUEUE>::_queues;
    using ThreadLevelQueuesScheduler<QUEUE>::getNextQueue;
    using ThreadLevelQueuesScheduler<QUEUE>::_queueCount;
    using ThreadLevelQueuesScheduler<QUEUE>::_status;

protected:
  std::queue<std::shared_ptr<Task> > _waitQueue;

  virtual std::shared_ptr<typename ThreadLevelQueuesScheduler<QUEUE>::task_queue_t> createTaskQueue(int node){
    return std::make_shared<Cache<QUEUE>>(node, 1);
  }

public:
  CacheConsciousScheduler(int queues): ThreadLevelQueuesScheduler<QUEUE>(queues), _totalThreads(queues){};
  ~CacheConsciousScheduler(){};

  virtual std::shared_ptr<typename ThreadLevelQueuesScheduler<QUEUE>::task_queue_t> createTaskQueue(int node, int threads){
    return std::make_shared<CacheConsciousQueue<QUEUE>>(node, threads);
  }

    /**
   * get number of worker
   */
  virtual size_t getNumberOfWorker() const {
    return _totalThreads;
  }
  
  void CacheConsciousScheduler::schedule(std::shared_ptr<Task> task) {
    LOG4CXX_WARN(_logger, "Task scheduled");
    task->lockForNotifications();
    if (!task->isReady()) {
      task->unlockForNotifications();
      task->addReadyObserver(my_enable_shared_from_this<TaskReadyObserver>::shared_from_this());
    } else {
      LOG4CXX_INFO(_logger, "Task is ready, putting int onto wait queue.");
      _waitQueue.push(task);
      task->unlockForNotifications();
      tryExecuteFirstTaskInQueue();
    }
  }
  
  void CacheConsciousScheduler::tryExecuteFirstTaskInQueue() {  
    if (_waitQueue.empty()) return;
    std::shared_ptr<Task> task = _waitQueue.front();
  
    std::shared_ptr<Cache>  cacheWithLowestPerformanceImpact = NULL;
    for (auto cache : _caches) {
      LOG4CXX_INFO(_logger, "Cache filling of " << cache->id_ << " is " << cache->getFilling());
      if (cache->is_free()) {
        if (cacheWithLowestPerformanceImpact == NULL ||
            cache->calculate_performance_with_op(task) < cacheWithLowestPerformanceImpact->calculate_performance_with_op(task)) {
          cacheWithLowestPerformanceImpact = cache;
        }
      }
  
    }
  
    // if we found one, add the task to that cache
    if (cacheWithLowestPerformanceImpact) {
      LOG4CXX_INFO(_logger, "Push task " << task->vname() << " to cache " << cacheWithLowestPerformanceImpact->id_ );
      _waitQueue.pop();
      task->addDoneObserver(my_enable_shared_from_this<TaskDoneObserver>::shared_from_this());
      cacheWithLowestPerformanceImpact->add_op(task);
    } else {
      LOG4CXX_INFO(_logger, "No free cache, leaving task in WaitQueue");
    }
  }
  
  void notifyDone(std::shared_ptr<Task> task) {
    // execute the next waiting task
    //LOG4CXX_INFO(_logger, "NotifyDone " << task->vname());
    //std::cout << "Scheduler::NotifyDone " << task->vname()  << std::endl;
    tryExecuteFirstTaskInQueue();
  }
  
  void notifyReady(std::shared_ptr<Task> task) {
    LOG4CXX_INFO(_logger, "NotifyReady");
  
    // remove task from wait set
    _setMutex.lock();
    int tmp = _waitSet.erase(task);
    _setMutex.unlock();
  
    // if task was found in wait set. try to schedule it
    if (tmp == 1) {
      schedule(task);
      LOG4CXX_INFO(_logger, "Task is ready, scheduling it");
    } else {
      // should never happen, but check to identify potential race conditions
      LOG4CXX_ERROR(_logger, "Task that notified to be ready to run was not found / found more than once in waitSet! " << std::to_string(tmp));
    }
  }

};

}}



/*
 * CacheConsciousScheduler.h
 *
 *  Created on: Sep 9, 2013
 *      Author: mkilling
 */

//#ifndef CACHECONSCIOUSSCHEDULER_H_
//#define CACHECONSCIOUSSCHEDULER_H_
//
//#include "taskscheduler/Task.h"
//#include "taskscheduler/CoreBoundQueue.h"
//#include "taskscheduler/AbstractCoreBoundQueuesScheduler.h"
//#include "taskscheduler/CoreBoundQueuesScheduler.h"
//#include "helper/HwlocHelper.h"
//#include "taskscheduler/Cache.h"
//#include <memory>
//#include <unordered_set>
//#include <vector>
//#include <queue>
//#include <mutex>
//#include <log4cxx/logger.h>
//
//
//namespace hyrise {
//namespace taskscheduler {
//
///**
// * a task scheduler with thread specific queues
// */
//class CacheConsciousScheduler : public CoreBoundQueuesScheduler, TaskDoneObserver {
//
//   /**
//    * push ready task to the next queue
//    */
//   //virtual void pushToQueue(std::shared_ptr<Task> task);
//
//   /*
//    * create a new task queue
//    */
//   //virtual task_queue_t *createTaskQueue(int core);
//
//public:
//  CacheConsciousScheduler(int queues = getNumberOfCoresOnSystem()-1);
//  
//  virtual ~CacheConsciousScheduler();
//  virtual void init();
//  void schedule(std::shared_ptr<Task> task);
//  void notifyDone(std::shared_ptr<Task> task);
//  void notifyReady(std::shared_ptr<Task> task);
//protected:
//  virtual CacheCoreBoundQueue *createTaskQueue(int core);
//private:
//  void assignQueuesToCaches();
//  std::shared_ptr<Cache> findOrCreateCacheWithObj(hwloc_obj_t cache_obj);
//  void tryExecuteFirstTaskInQueue();
//
//  std::vector<std::shared_ptr<Cache> > _caches;
//  std::queue<std::shared_ptr<Task> > _waitQueue;
//
//  std::recursive_mutex _cachesMutex;
//
//};
//}}
//
//#endif /* CACHECONSCIOUSSCHEDULER_H_ */
//