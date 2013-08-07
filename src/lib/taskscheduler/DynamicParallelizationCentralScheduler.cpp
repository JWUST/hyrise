/*
 * DynamicParallelizationCentralScheduler.cpp
 *
 *  Created on: Mar 20, 2013
 *      Author: jwust
 */

#include "DynamicParallelizationCentralScheduler.h"
 #include <taskscheduler/SharedScheduler.h>

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

//void DynamicParallelizationCentralScheduler::notifyReady(std::shared_ptr<Task> task){}
