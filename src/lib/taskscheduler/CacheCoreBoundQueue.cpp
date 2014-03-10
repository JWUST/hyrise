/*
 * CacheCoreBoundQueue.cpp
 *
 *  Created on: Febr 24, 2014
 *      Author: jwust
 */

#include "CacheCoreBoundQueue.h"

namespace hyrise {
namespace taskscheduler {

void CacheCoreBoundQueue::executeTask() {
  size_t retries = 0;
  //infinite thread loop
  while (1) {
    //block protected by _threadStatusMutex
    {
      std::lock_guard<lock_t> lk1(_threadStatusMutex);
      if (_status == TO_STOP)
        break;
    }
    // lock queue to get task
    std::unique_lock<lock_t> ul(_queueMutex);
    // get task and execute
    if (_runQueue.size() > 0) {
      std::shared_ptr<Task> task = _runQueue.front();
      // get first task
      _runQueue.pop();
      ul.unlock();
      if (task) {
        LOG4CXX_INFO(logger, "Task is not null");
        // set queue to _blocked as we run task; this is a simple mechanism to avoid that further tasks are pushed to this queue if a long running task is executed; check WSSimpleTaskScheduler for task stealing queue
        _blocked = true;
        //LOG4CXX_DEBUG(logger, "Started executing task" << std::hex << &task << std::dec << " on core " << _core);
        // run task
        //std::cout << "Executed task " << task->vname() << "; hex " << std::hex << &task << std::dec << " on core " << _core<< std::endl;
        _cache->notifyStart(task);
        (*task)();
        _cache->notifyStop(task);
        LOG4CXX_INFO(logger, "Executed task " << task->vname() << "; hex " << std::hex << &task << std::dec << " on core " << _core);
        // notify done observers that task is done
        task->notifyDoneObservers();
        _blocked = false;
      }
    }
    // no task in runQueue -> sleep and wait for new tasks
    else {
      //if queue still empty go to sleep and wait until new tasks have been arrived
      if (_runQueue.size() < 1) {
        
        if (retries++ < 1000) {
          ul.unlock();
          if (retries > 300) 
            std::this_thread::yield();
        } else {
          // if thread is about to stop, break execution loop
          if(_status != RUN)
            break;
          _condition.wait(ul);      
        }
      }
    }
  }
}

void CacheCoreBoundQueue::setRunObserver(RunObserver * observer){
	_cache = observer;
}

}}