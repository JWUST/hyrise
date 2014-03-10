// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#pragma once

#include <queue>
#include <algorithm>
#include "NodeBoundQueuesScheduler.h"
#include "CacheConsciousPriorityQueue.h"

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
class CacheConsciousPriorityScheduler :
  virtual public NodeBoundQueuesScheduler<QUEUE>,
  virtual public ThreadLevelQueuesScheduler<QUEUE>, 
  public TaskDoneObserver {
    using ThreadLevelQueuesScheduler<QUEUE>::_logger;
protected:
  std::queue<std::shared_ptr<Task> > _waitQueue;
  AbstractTaskScheduler::lock_t _cachesMutex;
public:
  CacheConsciousPriorityScheduler(int queues): ThreadLevelQueuesScheduler<QUEUE>(queues), NodeBoundQueuesScheduler<QUEUE>(queues){};
  ~CacheConsciousPriorityScheduler(){};

  virtual std::shared_ptr<typename ThreadLevelQueuesScheduler<QUEUE>::task_queue_t> createTaskQueue(int node, int threads){
    return std::make_shared<CacheConsciousPriorityQueue<QUEUE>>(node, threads);
  }
  
  void schedule(std::shared_ptr<Task> task) {
    LOG4CXX_INFO(_logger, "Task " << task->vname() << " scheduled");
    task->lockForNotifications();
    if (!task->isReady()) {
      task->addReadyObserver(my_enable_shared_from_this<TaskReadyObserver>::shared_from_this());
      task->unlockForNotifications();
    } else {
      task->unlockForNotifications();
      LOG4CXX_INFO(_logger, "Task is ready, putting int onto wait queue.");
      {
        std::lock_guard<AbstractTaskScheduler::lock_t> lk(_cachesMutex);
        _waitQueue.push(task);
      }
      tryExecuteFirstTaskInQueue();
    }
  }

  void tryExecuteFirstTaskInQueue() {  
    std::lock_guard<AbstractTaskScheduler::lock_t> lk(_cachesMutex);
    if (_waitQueue.empty()) return;
    std::shared_ptr<Task> task = _waitQueue.front();

    LOG4CXX_INFO(_logger, "first task in queue is " << task->vname());

    std::shared_ptr<CacheConsciousPriorityQueue<QUEUE>> cacheWithLowestPerformanceImpact = NULL;
    for (auto c : ThreadLevelQueuesScheduler<QUEUE>::_queues) {
      auto cache = std::dynamic_pointer_cast<CacheConsciousPriorityQueue<QUEUE>>(c);
      LOG4CXX_INFO(_logger, "Cache filling of " << cache->getNode() << " is " << cache->getFilling());
      if (cache->is_free(task)) {
        if (cacheWithLowestPerformanceImpact == NULL ||
            cache->calculate_performance_with_op(task) < cacheWithLowestPerformanceImpact->calculate_performance_with_op(task)) {
          cacheWithLowestPerformanceImpact = cache;
        }
      }
    }
  
    // if we found one, add the task to that cache
    if (cacheWithLowestPerformanceImpact) {
      LOG4CXX_INFO(_logger, "Push task " << task->vname() << " to cache " << cacheWithLowestPerformanceImpact->getNode());
      _waitQueue.pop();
      task->addDoneObserver(my_enable_shared_from_this<TaskDoneObserver>::shared_from_this());
      cacheWithLowestPerformanceImpact->schedule(task);
    } else {
      LOG4CXX_INFO(_logger, "No free cache, leaving task in WaitQueue");
    }

  }
  
  void notifyDone(std::shared_ptr<Task> task) {
    // execute the next waiting task
    LOG4CXX_INFO(_logger, "NotifyDone " << task->vname());
    tryExecuteFirstTaskInQueue();
  }
  
  void notifyReady(std::shared_ptr<Task> task) {
    LOG4CXX_INFO(_logger, "NotifyReady" << task->vname());
    schedule(task);
  }

};

}}