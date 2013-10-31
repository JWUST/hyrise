// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
/*
 * Task.cpp
 *
 *  Created on: Feb 15, 2012
 *      Author: jwust
 */

#include "taskscheduler/Task.h"
#include "taskscheduler/DynamicParallelizationCentralScheduler.h"

#include <iostream>
#include <thread>

void Task::lockForNotifications() {
  _notifyMutex.lock();
}

void Task::unlockForNotifications() {
  _notifyMutex.unlock();
}

void Task::notifyReadyObservers() {
  //TODO see notifyDoneObservers
	std::lock_guard<std::mutex> lk(_readyObserverMutex);
	// std::vector<std::shared_ptr<TaskReadyObserver> >::iterator itr;

  if (_readyObservers.size() != 1 && _priority != HIGH_PRIORITY) {
    std::cout << "Size _readyObservers: " << _readyObservers.size() << std::endl; 
    std::cout << "Task is: " << vname() << std::endl;
  }

	for (auto itr = _readyObservers.begin(); itr != _readyObservers.end(); ++itr) {
    auto observer = itr->lock();
		observer->notifyReady(std::dynamic_pointer_cast<Task>(shared_from_this()));
	}
}

void Task::notifyDoneObservers() {
  //TODO copy _doneObservers and then notify.
  // You shouldn't leave your code while holding a lock.
  // Confer http://stackoverflow.com/questions/14381588/observer-pattern-using-weak-ptr
	std::lock_guard<std::mutex> lk(_doneObserverMutex);
	// std::vector<std::shared_ptr<TaskDoneObserver> >::iterator itr;
  
	for (auto itr = _doneObservers.begin(); itr != _doneObservers.end(); ++itr) {
    auto observer = itr->lock();
		observer->notifyDone(std::dynamic_pointer_cast<Task>(shared_from_this()));
	}
}

Task::Task(): _dependencyWaitCount(0), _preferredCore(NO_PREFERRED_CORE), _preferredNode(NO_PREFERRED_NODE), _priority(DEFAULT_PRIORITY), _sessionId(SESSION_ID_NOT_SET), _id(0) {
}

void Task::addDependency(std::shared_ptr<Task> dependency) {
  {
    std::lock_guard<std::mutex> lk(_depMutex);
    _dependencies.push_back(dependency);
    ++_dependencyWaitCount;
    //std::cout << "add: Task " << _id << " count:" << _dependencyWaitCount << std::endl;
  }
  dependency->addDoneObserver(std::dynamic_pointer_cast<Task>(shared_from_this()));
}

void Task::addDoneDependency(std::shared_ptr<Task> dependency) {
  {
    std::lock_guard<std::mutex> lk(_depMutex);
    _dependencies.push_back(dependency);
  }
}

void Task::removeDependency(std::shared_ptr<Task> dependency) {
    
    std::lock_guard<std::mutex> lk(_depMutex);
    // remove from dependencies
    std::vector<std::shared_ptr<Task>>::iterator it = _dependencies.begin();
    while (it != _dependencies.end()) {
      if (*it == dependency){
        it = _dependencies.erase(it);
        --_dependencyWaitCount;
	//std::cout << "remove: Task " << _id << " count:" << _dependencyWaitCount << std::endl;
      }
      else 
        ++it;
    }
}

void Task::changeDependency(std::shared_ptr<Task> from, std::shared_ptr<Task> to) {
    
    std::lock_guard<std::mutex> lk(_depMutex);
    // find from dependencies
    for(size_t i = 0, size = _dependencies.size(); i < size; i++){
      if(_dependencies[i] == from){
	       _dependencies[i] = to;
        // std::cout << "changedDep" << std::endl;
       }
    }
    // add new done observer
    to->addDoneObserver(std::dynamic_pointer_cast<Task>(shared_from_this()));
}

void Task::setDependencies(std::vector<std::shared_ptr<Task> > dependencies, int count) {
    std::lock_guard<std::mutex> lk(_depMutex);
    _dependencies = dependencies;
    _dependencyWaitCount = 0;
}

void Task::addReadyObserver(const std::weak_ptr<TaskReadyObserver>& observer) {
  std::lock_guard<std::mutex> lk(_readyObserverMutex);
  _readyObservers.push_back(observer);
}

void Task::addDoneObserver(const std::weak_ptr<TaskDoneObserver>& observer) {
  std::lock_guard<std::mutex> lk(_doneObserverMutex);
  _doneObservers.push_back(observer);
}

void Task::notifyDone(std::shared_ptr<Task> task) {
  _depMutex.lock();
  int t = --_dependencyWaitCount;
  _depMutex.unlock();

  if (t == 0) {
    if(_preferredCore == NO_PREFERRED_CORE && _preferredNode == NO_PREFERRED_NODE)
      _preferredNode = task->getActualNode();
    std::lock_guard<std::mutex> lk(_notifyMutex);
    notifyReadyObservers();
  }
}

bool Task::isReady() {
  std::lock_guard<std::mutex> lk(_depMutex);
  return (_dependencyWaitCount == 0);
}

int Task::getDependencyCount() {
  return _dependencies.size();
}

// TODO make nicer; method needed to identify result task of a query
// in the query tree, we have no successor if we have no doneObserver
bool Task::hasSuccessors() {
  return (_doneObservers.size() > 0);
}

void Task::setPreferredCore(int core) {
  _preferredCore = core;
}

int Task::getPreferredCore() {
  return _preferredCore;
}

WaitTask::WaitTask() {
  _finished = false;
}

void WaitTask::operator()() {
  {
    std::lock_guard<std::mutex> lock(_mut);
    _finished = true;
  }
  _cond.notify_one();
}

void WaitTask::wait() {
  std::unique_lock<std::mutex> ul(_mut);
  while (!_finished) {
    _cond.wait(ul);
  }
}

SleepTask::SleepTask(int microseconds) {
  _microseconds = microseconds;
}

void SleepTask::operator()() {
  std::this_thread::sleep_for(std::chrono::microseconds(_microseconds));
}

void SyncTask::operator()() {
  //do nothing
}
