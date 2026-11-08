#include "internal/seam_inbox.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

string CrossingRowHeader::Encode() const {
	return to_string(sequence) + " " + to_string(batch) + " " + to_string(min_batch) + " " + to_string(bytes);
}

CrossingRowHeader CrossingRowHeader::Decode(const string &text) {
	auto parts = StringUtil::Split(text, ' ');
	CrossingRowHeader header;
	header.sequence = std::stoull(parts[0]);
	header.batch = std::stoull(parts[1]);
	header.min_batch = std::stoull(parts[2]);
	header.bytes = std::stoull(parts[3]);
	return header;
}

CrossingSeamInbox::CrossingSeamInbox(ClientContext &context, vector<LogicalType> types_p, bool ordered_p,
                                     idx_t streams_p)
    : buffer_manager(BufferManager::GetBufferManager(context)), types(std::move(types_p)), ordered(ordered_p),
      streams(streams_p), streams_open(streams_p) {
}

CrossingSeamInbox::Held &CrossingSeamInbox::HeldFor(Held &held) {
	if (!held.rows) {
		held.rows = make_uniq<ColumnDataCollection>(buffer_manager, types);
	}
	return held;
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
	while (!arrived.empty() && arrived.begin()->first == next_sequence) {
		whole_below = MaxValue(whole_below, arrived.begin()->second);
		arrived.erase(arrived.begin());
		next_sequence++;
	}
	while (!batches.empty() && (streams_open == 0 || batches.begin()->first < whole_below)) {
		ready.push_back(std::move(batches.begin()->second));
		batches.erase(batches.begin());
	}
}

void CrossingSeamInbox::Append(DataChunk &rows, const CrossingRowHeader &header) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (error.HasError()) {
			return;
		}
		if (rows.size() > 0) {
			auto &held = HeldFor(ordered ? batches[header.batch] : tail);
			held.rows->Append(rows);
			held.bytes += header.bytes;
		}
		if (ordered) {
			arrived[header.sequence] = header.min_batch;
			ReleaseWholeBatches();
		}
	}
	WakeConsumers();
}

void CrossingSeamInbox::StreamDone() {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		streams_open--;
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

CrossingSeamInbox::Took CrossingSeamInbox::Take(unique_ptr<ColumnDataCollection> &rows, idx_t &rank, idx_t &bytes,
                                                const InterruptState &resume) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (error.HasError()) {
		error.Throw();
	}
	if (tail.rows) {
		ready.push_back(std::move(tail));
		tail = Held();
	}
	if (ready.empty()) {
		if (streams_open == 0 && batches.empty()) {
			return Took::DONE;
		}
		BlockTask(resume);
		return Took::PARK;
	}
	rows = std::move(ready.front().rows);
	bytes = ready.front().bytes;
	ready.pop_front();
	rank = next_rank++;
	return Took::ROWS;
}

} // namespace duckdb
