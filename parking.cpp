#include "internal/parking.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

void CrossingWaker::Wake() const {
	if (parking) {
		parking->Reschedule();
	}
}

shared_ptr<CrossingParking> CrossingParking::Of(const InterruptState &resume) {
	return shared_ptr<CrossingParking>(new CrossingParking(resume));
}

CrossingWaker CrossingParking::Waker() {
	return CrossingWaker(shared_ptr_cast<Task, CrossingParking>(shared_from_this()));
}

bool CrossingParking::Park(StateWithBlockableTasks &state, const unique_lock<mutex> &guard) {
	lock_guard<mutex> mine(lock);
	if (woken) {
		return false;
	}
	weak_ptr<Task> self = shared_from_this();
	if (!state.BlockTask(guard, InterruptState(self))) {
		return false;
	}
	parked = true;
	return true;
}

bool CrossingParking::Park() {
	lock_guard<mutex> mine(lock);
	if (woken) {
		return false;
	}
	parked = true;
	return true;
}

void CrossingParking::Reschedule() {
	Fire();
}

TaskExecutionResult CrossingParking::Execute(TaskExecutionMode) {
	throw InternalException("crossing: a parking is never scheduled");
}

void CrossingParking::Fire() {
	bool fire;
	{
		lock_guard<mutex> mine(lock);
		if (woken) {
			return;
		}
		woken = true;
		fire = parked;
	}
	if (fire) {
		resume.Callback();
	}
}

} // namespace duckdb
