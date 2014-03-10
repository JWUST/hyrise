/*
 * RunObserver.h
 *
 *  Created on: Sep 10, 2013
 *      Author: mkilling
 */

 #pragma once

#include "helper/types.h"

namespace hyrise {
namespace taskscheduler {

class RunObserver {
  /*
   * notify that task has changed state
   */
public:
  virtual void notifyStart(task_ptr_t task) = 0;
  virtual void notifyStop(task_ptr_t task) = 0;
};

}}