#include "internal/seam_inbox.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

CrossingSeamInbox::CrossingSeamInbox(ClientContext &context, vector<LogicalType> types_p, bool ordered_p, idx_t streams)
    : buffer_manager(BufferManager::GetBufferManager(context)), types(std::move(types_p)), ordered(ordered_p),
      stream_batches(streams) {
}

unique_ptr<ColumnDataCollection> CrossingSeamInbox::NewCollection() const {
	return make_uniq<ColumnDataCollection>(buffer_manager, types);
}

void CrossingSeamInbox::WakeConsumers() {
	vector<InterruptState> blocked;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		blocked = TakeBlockedTasks();
	}
	for (auto &task : blocked) {
		task.Callback();
	}
}

void CrossingSeamInbox::ReleaseWholeBatches() {
	optional_idx lowest;
	for (auto &stream : stream_batches) {
		if (stream.state == StreamState::NOT_STARTED) {
			return;
		}
		if (stream.state == StreamState::AT_BATCH && (!lowest.IsValid() || stream.batch < lowest.GetIndex())) {
			lowest = stream.batch;
		}
	}
	while (!batches.empty() && (!lowest.IsValid() || batches.begin()->first < lowest.GetIndex())) {
		ready.push_back(std::move(batches.begin()->second));
		batches.erase(batches.begin());
	}
}

void CrossingSeamInbox::Append(idx_t stream, DataChunk &rows, idx_t batch) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (error.HasError()) {
			return;
		}
		auto &progress = stream_batches[stream];
		progress.state = StreamState::AT_BATCH;
		progress.batch = batch;
		if (rows.size() > 0) {
			if (!ordered) {
				if (!tail) {
					tail = NewCollection();
				}
				tail->Append(rows);
			} else {
				auto &collection = batches[batch];
				if (!collection) {
					collection = NewCollection();
				}
				collection->Append(rows);
			}
		}
		if (ordered) {
			ReleaseWholeBatches();
		}
	}
	WakeConsumers();
}

void CrossingSeamInbox::Idle(idx_t stream) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		stream_batches[stream].state = StreamState::IDLE;
		ReleaseWholeBatches();
	}
	WakeConsumers();
}

void CrossingSeamInbox::StreamDone(idx_t stream) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		stream_batches[stream].state = StreamState::DONE;
		ReleaseWholeBatches();
	}
	WakeConsumers();
}

void CrossingSeamInbox::Cancel(const ErrorData &error_p) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!error.HasError()) {
			error = error_p;
		}
	}
	WakeConsumers();
}

bool CrossingSeamInbox::TryTakeLocked(unique_ptr<ColumnDataCollection> &rows, idx_t &rank) {
	if (error.HasError()) {
		error.Throw();
	}
	if (tail) {
		ready.push_back(std::move(tail));
		tail = nullptr;
	}
	if (ready.empty()) {
		return false;
	}
	rows = std::move(ready.front());
	ready.pop_front();
	rank = next_rank++;
	return true;
}

bool CrossingSeamInbox::TryTake(unique_ptr<ColumnDataCollection> &rows, idx_t &rank) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return TryTakeLocked(rows, rank);
}

CrossingSeamInbox::Took CrossingSeamInbox::Take(unique_ptr<ColumnDataCollection> &rows, idx_t &rank,
                                                const InterruptState &resume) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (TryTakeLocked(rows, rank)) {
		return Took::ROWS;
	}
	for (auto &stream : stream_batches) {
		if (stream.state != StreamState::DONE) {
			BlockTask(resume);
			return Took::PARK;
		}
	}
	return Took::DONE;
}

} // namespace duckdb
