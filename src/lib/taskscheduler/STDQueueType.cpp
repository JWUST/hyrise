#include "STDQueueType.h"
namespace hyrise {
namespace taskscheduler {
void STDQueueType::push(const std::shared_ptr<Task>& task) { _runQueue.push(task); }
bool STDQueueType::try_pop(std::shared_ptr<Task>& task) { 
	bool retVal = false;
	if(!_runQueue.empty()){
		task = _runQueue.front();
		_runQueue.pop();
		retVal = true;
	}
	return retVal;
}
size_t STDQueueType::unsafe_size() { return _runQueue.size(); }
size_t STDQueueType::size() { return _runQueue.size(); }
}
}
