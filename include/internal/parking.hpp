#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task.hpp"

#include "crossing.hpp"

namespace duckdb {

struct CrossingParking : public Task {
	static shared_ptr<CrossingParking> Of(const InterruptState &resume);

	CrossingWaker Waker();

	bool Park(StateWithBlockableTasks &state, const unique_lock<mutex> &guard);
	bool Park();

	void Reschedule() override;
	TaskExecutionResult Execute(TaskExecutionMode mode) override;
	string TaskType() const override {
		return "CrossingParking";
	}

private:
	explicit CrossingParking(const InterruptState &resume_p) : resume(resume_p) {
	}

	void Fire();

	mutex lock;
	bool parked = false;
	bool woken = false;
	InterruptState resume;
};

} // namespace duckdb
