// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

/*
 * CacheConsciousQueue.h
 *
 *  Created on: Sep 10, 2013
 *      Author: jwust/mkilling
 */

#pragma once

#include <hwloc.h>
#include <vector>
#include <cmath>
#include <iostream>
#include <atomic>

#include "taskscheduler/Task.h"
#include "taskscheduler/RunObserver.h"
#include "taskscheduler/NodeBoundQueue.h"

// hardcoded for begram!!
#define N_EMPTY 1400000
#define N 500000

namespace hyrise {
namespace taskscheduler {

template<class QUEUE>
class CacheConsciousQueue : 
    virtual public NodeBoundQueue<QUEUE>{

        using ThreadLevelQueue<QUEUE>::_runQueue;
        using ThreadLevelQueue<QUEUE>::_threadCount;
        using ThreadLevelQueue<QUEUE>::_status;
        using ThreadLevelQueue<QUEUE>::_logger;
        using ThreadLevelQueue<QUEUE>::_queuecheck;
        using ThreadLevelQueue<QUEUE>::_lockqueue;

protected:
    std::atomic_size_t _cache_filling;
    std::atomic_size_t _running_ops;

public:

    CacheConsciousQueue(int node, size_t threads): ThreadLevelQueue<QUEUE>(threads), NodeBoundQueue<QUEUE>(node, threads), _cache_filling(0) {}
    ~CacheConsciousQueue(){}

    virtual void executeTasks(){
      size_t retries = 0;
      //infinite thread loop
      while (true) {
        if (_status == AbstractTaskScheduler::TO_STOP) { break; } // break out when asked
        std::shared_ptr<Task> task = nullptr;
        if (_runQueue.try_pop(task)) {
          retries = 0;
          startTask(task);
          (*task)();
          stopTask(task);
          LOG4CXX_DEBUG(_logger,  "Executed task " << task->vname());
          task->notifyDoneObservers();
        } else {
          if (retries++ < 10000) {
            if (retries > 300) 
              std::this_thread::yield();
          } 
          else {
            std::unique_lock<AbstractTaskScheduler::lock_t> locker(_lockqueue);
            if(_status != AbstractTaskScheduler::RUN)
                break;
            _queuecheck.wait(locker);
            retries = 0;
          }
        }
      }
    }

    size_t calculate_performance_with_op(const std::shared_ptr<Task> & new_op) {
        //size_t total_footprint = 0;
        /*
        auto ops = operations();
        for (auto op : ops) {
            total_footprint += op->getFootprint();
        }
        */
        if (_running_ops == 0) {
            return single_op_performance(new_op->getFootprint());
        } else {
            return multiple_op_performance(_cache_filling + new_op->getFootprint());
        }
    }

    bool is_free() {
        return (_running_ops < _threadCount);
    }


    size_t getFilling(){
        return _cache_filling;
    }

    size_t getNode(){
        return NodeBoundQueue<QUEUE>::_node;
    }


protected:


    void startTask(const std::shared_ptr<Task> & task){
        _cache_filling += task->getFootprint();
        LOG4CXX_INFO(_logger, "Cache " << NodeBoundQueue<QUEUE>::_node << "; update filling " <<  _cache_filling);
        _running_ops++;
    }

    void stopTask(const std::shared_ptr<Task> & task){
        _cache_filling -= task->getFootprint();
        _running_ops--;
    }

private:
    size_t single_op_performance(size_t footprint) {
        return (size_t)(1000000000 * std::max(0.0, log(footprint)/log(N_EMPTY)));
    }

    size_t multiple_op_performance(size_t footprint) {
        return (size_t)(1000000000 * std::max(0.0, log(footprint)/log(N)));
    }
/*
    std::vector<std::shared_ptr<Task> > operations() {
        std::vector<std::shared_ptr<Task> > ret;
        for (auto q : queues_) {
            auto t = q->getFirstTask();
            if (t) {
                ret.push_back(t);
            }
        }
        return ret;
    }
*/
};
}}
