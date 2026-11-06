#include "internal/stream_sink.hpp"

#include "internal/parking.hpp"

namespace duckdb {

const char *const CROSSING_IDLE_HEADER = "I";

idx_t CrossingSliceStreamId(idx_t slice, bool into_far, idx_t stream) {
	return 1 + (((slice << 1) | (into_far ? 1 : 0)) << 16 | stream);
}

string CrossingBatchHeader(idx_t batch) {
	return to_string(batch);
}

void CrossingFailure::Hold(vector<unique_ptr<CrossingStream>> streams) {
	lock_guard<mutex> guard(lock);
	for (auto &stream : streams) {
		held.push_back(std::move(stream));
	}
}

void CrossingFailure::CloseHeld(const ErrorData &error) {
	lock_guard<mutex> guard(lock);
	for (auto &stream : held) {
		stream->Close(error.HasError() ? &error : nullptr);
	}
	held.clear();
}

namespace {

enum class Sent : uint8_t { TAKEN, BLOCKED, STOPPED };

Sent SendFrame(CrossingStream &stream, CrossingFrame &frame, const InterruptState &resume) {
	while (true) {
		auto parking = CrossingParking::Of(resume);
		switch (stream.Send(frame, parking->Waker())) {
		case CrossingIo::TAKEN:
			return Sent::TAKEN;
		case CrossingIo::DONE:
			return Sent::STOPPED;
		default:
			if (parking->Park()) {
				return Sent::BLOCKED;
			}
		}
	}
}

struct StreamSinkGlobal : public GlobalSinkState {
	vector<unique_ptr<CrossingStream>> streams;
	vector<idx_t> free_streams;
	idx_t next_sequential_batch = 0;
	bool closed = false;
	shared_ptr<CrossingFailure> failure;

	void CloseAll(optional_ptr<const ErrorData> error) {
		if (closed) {
			return;
		}
		closed = true;
		for (auto &stream : streams) {
			stream->Close(error);
		}
	}

	~StreamSinkGlobal() override {
		if (closed) {
			return;
		}
		if (failure) {
			failure->Hold(std::move(streams));
			return;
		}
		ErrorData ended(ExceptionType::INTERRUPT, "crossing: the statement ended before this slice finished");
		CloseAll(&ended);
	}

	idx_t MaxThreads(idx_t source_max_threads) override {
		return MinValue<idx_t>(source_max_threads, streams.size());
	}
};

struct StreamSinkLocal : public LocalSinkState {
	optional_idx stream;
	DataChunk frame_rows;
	bool stopped = false;
};

bool ClaimStream(StreamSinkGlobal &global, StreamSinkLocal &local, const InterruptState &resume) {
	annotated_lock_guard<annotated_mutex> guard(global.lock);
	if (global.free_streams.empty()) {
		global.BlockTask(resume);
		return false;
	}
	local.stream = global.free_streams.back();
	global.free_streams.pop_back();
	return true;
}

} // namespace

CrossingStreamSink::CrossingStreamSink(CrossingLink &link_p, idx_t slice_p, bool into_far_p, vector<idx_t> columns_p,
                                       vector<LogicalType> types_p, bool ordered_p, bool parallel_p,
                                       shared_ptr<CrossingFailure> failure_p)
    : link(&link_p), slice(slice_p), into_far(into_far_p), columns(std::move(columns_p)), types(std::move(types_p)),
      ordered(ordered_p), parallel(parallel_p), failure(std::move(failure_p)) {
}

unique_ptr<GlobalSinkState> CrossingStreamSink::GlobalState() const {
	auto state = make_uniq<StreamSinkGlobal>();
	state->failure = failure;
	auto count = parallel ? link->Streams() : idx_t(1);
	for (idx_t k = 0; k < link->Streams(); k++) {
		auto stream = link->Open(CrossingSliceStreamId(slice, into_far, k));
		if (k < count) {
			state->streams.push_back(std::move(stream));
			state->free_streams.push_back(count - 1 - k);
		} else {
			stream->Close(nullptr);
		}
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> CrossingStreamSink::LocalState() const {
	auto state = make_uniq<StreamSinkLocal>();
	state->frame_rows.InitializeEmpty(types);
	return std::move(state);
}

SinkResultType CrossingStreamSink::Sink(DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global = input.global_state.Cast<StreamSinkGlobal>();
	auto &local = input.local_state.Cast<StreamSinkLocal>();
	if (local.stopped) {
		return SinkResultType::FINISHED;
	}
	if (!local.stream.IsValid() && !ClaimStream(global, local, input.interrupt_state)) {
		return SinkResultType::BLOCKED;
	}
	idx_t batch = 0;
	if (ordered) {
		batch = parallel ? local.partition_info.batch_index.GetIndex() : global.next_sequential_batch;
	}
	for (idx_t c = 0; c < columns.size(); c++) {
		local.frame_rows.data[c].Reference(chunk.data[columns[c]]);
	}
	local.frame_rows.SetChildCardinality(chunk.size());
	CrossingFrame frame;
	frame.header = CrossingBatchHeader(batch);
	frame.rows.InitializeEmpty(types);
	frame.rows.Reference(local.frame_rows);
	switch (SendFrame(*global.streams[local.stream.GetIndex()], frame, input.interrupt_state)) {
	case Sent::BLOCKED:
		return SinkResultType::BLOCKED;
	case Sent::STOPPED:
		local.stopped = true;
		return SinkResultType::FINISHED;
	default:
		break;
	}
	if (ordered && !parallel) {
		global.next_sequential_batch++;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType CrossingStreamSink::Combine(OperatorSinkCombineInput &input) const {
	auto &global = input.global_state.Cast<StreamSinkGlobal>();
	auto &local = input.local_state.Cast<StreamSinkLocal>();
	if (!local.stream.IsValid()) {
		return SinkCombineResultType::FINISHED;
	}
	if (ordered && !local.stopped) {
		CrossingFrame idle;
		idle.header = CROSSING_IDLE_HEADER;
		if (SendFrame(*global.streams[local.stream.GetIndex()], idle, input.interrupt_state) == Sent::BLOCKED) {
			return SinkCombineResultType::BLOCKED;
		}
	}
	vector<InterruptState> blocked;
	{
		annotated_lock_guard<annotated_mutex> guard(global.lock);
		global.free_streams.push_back(local.stream.GetIndex());
		blocked = global.TakeBlockedTasks();
	}
	local.stream = optional_idx();
	for (auto &task : blocked) {
		task.Callback();
	}
	return SinkCombineResultType::FINISHED;
}

SinkNextBatchType CrossingStreamSink::NextBatch(OperatorSinkNextBatchInput &input) const {
	auto &global = input.global_state.Cast<StreamSinkGlobal>();
	auto &local = input.local_state.Cast<StreamSinkLocal>();
	if (!ordered || !parallel || !local.stream.IsValid() || local.stopped) {
		return SinkNextBatchType::READY;
	}
	CrossingFrame reached;
	reached.header = CrossingBatchHeader(local.partition_info.batch_index.GetIndex());
	switch (SendFrame(*global.streams[local.stream.GetIndex()], reached, input.interrupt_state)) {
	case Sent::BLOCKED:
		return SinkNextBatchType::BLOCKED;
	case Sent::STOPPED:
		local.stopped = true;
		return SinkNextBatchType::READY;
	default:
		return SinkNextBatchType::READY;
	}
}

void CrossingStreamSink::Finish(GlobalSinkState &state) const {
	state.Cast<StreamSinkGlobal>().CloseAll(nullptr);
}

} // namespace duckdb
