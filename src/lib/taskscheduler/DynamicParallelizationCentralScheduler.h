/*
 * DynamicParallelizationCentralScheduler.h
 *
 *  Created on: Mar 20, 2013
 *      Author: jwust
 */

#ifndef DYNAMICPARALLELIZATIONCENTRALSCHEDULER_H_
#define DYNAMICPARALLELIZATIONCENTRALSCHEDULER_H_

#include "CentralPriorityScheduler.h"

class DynamicParallelizationCentralScheduler : public CentralPriorityScheduler{

public:
  DynamicParallelizationCentralScheduler(int threads = getNumberOfCoresOnSystem());
  virtual ~DynamicParallelizationCentralScheduler();

  virtual void notifyReady(std::shared_ptr<Task> task);

  virtual void schedule(std::shared_ptr<Task> task);
};

#endif /* DYNAMICPARALLELIZATIONCENTRALSCHEDULER_H_ */
