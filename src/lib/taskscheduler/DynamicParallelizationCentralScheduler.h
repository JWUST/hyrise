/*
 * DynamicParallelizationCentralScheduler.h
 *
 *  Created on: Mar 20, 2013
 *      Author: jwust
 */

#ifndef DYNAMICPARALLELIZATIONCENTRALSCHEDULER_H_
#define DYNAMICPARALLELIZATIONCENTRALSCHEDULER_H_

#include "CentralScheduler.h"

class DynamicParallelizationCentralScheduler : public CentralScheduler{

public:
  DynamicParallelizationCentralScheduler(int threads = getNumberOfCoresOnSystem());
  virtual ~DynamicParallelizationCentralScheduler();

  void virtual notifyReady(std::shared_ptr<Task> task);

};

#endif /* DYNAMICPARALLELIZATIONCENTRALSCHEDULER_H_ */
