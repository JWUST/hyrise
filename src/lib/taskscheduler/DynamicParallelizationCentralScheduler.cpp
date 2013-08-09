/*
 * DynamicParallelizationCentralScheduler.cpp
 *
 *  Created on: Mar 20, 2013
 *      Author: jwust
 */

#include "DynamicParallelizationCentralScheduler.h"
#include "taskscheduler/SharedScheduler.h"
#include "access/system/ParallelizablePlanOperation.h"

/*
log4cxx::LoggerPtr DynamicParallelizationCentralScheduler::_logger = log4cxx::Logger::getLogger("taskscheduler.DynamicParallelizationCentralScheduler");
*/
// register Scheduler at SharedScheduler
namespace {
bool registered  =
    SharedScheduler::registerScheduler<DynamicParallelizationCentralScheduler>("DynamicParallelizationCentralScheduler");
}

DynamicParallelizationCentralScheduler::DynamicParallelizationCentralScheduler(int threads):CentralScheduler(threads){
	// do nothing
}

DynamicParallelizationCentralScheduler::~DynamicParallelizationCentralScheduler(){}


void DynamicParallelizationCentralScheduler::schedule(std::shared_ptr<Task> task){
	//std::cout << "DynamicParallelizationCentralScheduler schedule " << task->vname() << std::endl;

	if (auto para = std::dynamic_pointer_cast<hyrise::access::ParallelizablePlanOperation>(task)) {
		if(para->hasDynamicCount() && para->isReady()){
			auto tasks = para->applyDynamicParallelization();
			for (const auto& i: tasks) {
      			CentralScheduler::schedule(i);
    	}			
    }
    else
      CentralScheduler::schedule(task);
	} else
		CentralScheduler::schedule(task);
}

void DynamicParallelizationCentralScheduler::notifyReady(std::shared_ptr<Task> task){
	// remove task from wait set
  _setMutex.lock();
  int tmp = _waitSet.erase(task);
  _setMutex.unlock();

  // if task was found in wait set, schedule task to next queue
  if (tmp == 1) {
    LOG4CXX_DEBUG(_logger, "Task " << std::hex << (void *)task.get() << std::dec << " ready to run");
    if (auto para = std::dynamic_pointer_cast<hyrise::access::ParallelizablePlanOperation>(task)) {
		  if(para->hasDynamicCount()){
			 auto tasks = para->applyDynamicParallelization();
			 for (const auto& i: tasks) {
        if (i->isReady()){
          std::lock_guard<std::mutex> lk(_queueMutex);
          _runQueue.push(i);
          _condition.notify_one();
        }
        else {
          i->addReadyObserver(this);
          std::lock_guard<std::mutex> lk(_setMutex);
          _waitSet.insert(i);
          LOG4CXX_DEBUG(_logger,  "Task " << std::hex << (void *)i.get() << std::dec << " inserted in wait queue");
        }
      }			
    }
    else{
     // std::cout << "schedule ParallelizablePlanOperation but wo dynamic; task " << task->vname() << std::hex << (void *)task.get() << std::dec << std::endl;

      std::lock_guard<std::mutex> lk(_queueMutex);
      _runQueue.push(task);
      _condition.notify_one();
    }
	} else{
     // std::cout << "schedule non ParallelizablePlanOperation; task " << task->vname() << std::hex << (void *)task.get() << std::dec << std::endl;

		  std::lock_guard<std::mutex> lk(_queueMutex);
    	_runQueue.push(task);
    	_condition.notify_one();
  }

  } else
    // should never happen, but check to identify potential race conditions
    LOG4CXX_ERROR(_logger, "Task that notified to be ready to run was not found / found more than once in waitSet! " << std::to_string(tmp));
}
