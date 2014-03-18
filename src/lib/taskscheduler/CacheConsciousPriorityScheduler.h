// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#pragma once

#include <queue>
#include <algorithm>
#include <tbb/concurrent_priority_queue.h>
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
  tbb::concurrent_priority_queue<std::shared_ptr<Task> , CompareTaskPtr> _waitQueue;
  // holds the next task to be pushed in case all queues are busy;
  // workaround as tbb prio queue has no "front" method
  std::shared_ptr<Task> _nextTask;
  AbstractTaskScheduler::lock_t _cachesMutex;
public:
  CacheConsciousPriorityScheduler(int queues): ThreadLevelQueuesScheduler<QUEUE>(queues), NodeBoundQueuesScheduler<QUEUE>(queues){};
  ~CacheConsciousPriorityScheduler(){};

  virtual std::shared_ptr<typename ThreadLevelQueuesScheduler<QUEUE>::task_queue_t> createTaskQueue(int node, int threads){
    return std::make_shared<CacheConsciousPriorityQueue<QUEUE>>(node, threads);
  }
  
  void schedule(const std::shared_ptr<Task> &  task) {
    LOG4CXX_INFO(_logger, "Task " << task->vname() << " scheduled");
    task->lockForNotifications();
    if (!task->isReady()) {
      task->addReadyObserver(my_enable_shared_from_this<TaskReadyObserver>::shared_from_this());
      task->unlockForNotifications();
    } else {
      //if(task->getFootprint() > 0){
      task->unlockForNotifications();
      
      //std::lock_guard<AbstractTaskScheduler::lock_t> lk(_cachesMutex);
      _waitQueue.push(task);
      tryExecuteFirstTaskInQueue();
    }
  }

  void tryExecuteFirstTaskInQueue() {
    LOG4CXX_INFO(_logger, "entering tryexecute next");
    std::lock_guard<AbstractTaskScheduler::lock_t> lk(_cachesMutex);

    if(!_nextTask){
      LOG4CXX_INFO(_logger, "_nextTask not set");
      if(!_waitQueue.try_pop(_nextTask)){
        LOG4CXX_INFO(_logger, "No task in wait queue - return ");
        // no task waiting, no task in queue -> return
        return;
      } 
    }
    
    //if (_waitQueue.empty()) return;
    //std::shared_ptr<Task> task = _waitQueue.front();

    LOG4CXX_INFO(_logger, "first task in queue is " << _nextTask->vname());

    std::shared_ptr<CacheConsciousPriorityQueue<QUEUE>> cacheWithLowestPerformanceImpact = NULL;
    size_t lowestPerformance = 0;
    for (auto c : ThreadLevelQueuesScheduler<QUEUE>::_queues) {
      auto cache = std::dynamic_pointer_cast<CacheConsciousPriorityQueue<QUEUE>>(c);
      LOG4CXX_INFO(_logger, "Cache filling of " << cache->getNode() << " is " << cache->getFilling());
      if (cache->is_free(_nextTask)) {
        if (cacheWithLowestPerformanceImpact == NULL) {
          cacheWithLowestPerformanceImpact = cache;
          lowestPerformance = cache->calculate_performance_with_op(_nextTask);
        }
        else if(cache->calculate_performance_with_op(_nextTask) < lowestPerformance){
          cacheWithLowestPerformanceImpact = cache;
          lowestPerformance = cache->calculate_performance_with_op(_nextTask);
        }
      }
    }
    // if we found one, add the task to that cache
    if (cacheWithLowestPerformanceImpact) {
      LOG4CXX_INFO(_logger, "Push task " << _nextTask->vname() << " to cache " << cacheWithLowestPerformanceImpact->getNode());
      _nextTask->addDoneObserver(my_enable_shared_from_this<TaskDoneObserver>::shared_from_this());
      cacheWithLowestPerformanceImpact->schedule(_nextTask);
      _nextTask.reset();
    } else {
      LOG4CXX_INFO(_logger, "No free cache, leaving task in WaitQueue");
    }

  }
  
  void notifyDone(const std::shared_ptr<Task>& task) {
    // execute the next waiting task
    LOG4CXX_INFO(_logger, "NotifyDone " << task->vname());
    if(ThreadLevelQueuesScheduler<QUEUE>::_status == AbstractTaskScheduler::RUN)
      tryExecuteFirstTaskInQueue();
  }
  
  void notifyReady(const std::shared_ptr<Task>& task) {
    LOG4CXX_INFO(_logger, "notifyReady" << task->vname());
    if(ThreadLevelQueuesScheduler<QUEUE>::_status == AbstractTaskScheduler::RUN)
      schedule(task);
  }

};

}}