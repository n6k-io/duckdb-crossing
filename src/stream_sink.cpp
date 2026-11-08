#include "internal/stream_sink.hpp"

#include "internal/parking.hpp"
#include "internal/seam_inbox.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

idx_t CrossingSliceStreamId(idx_t slice, bool into_far, idx_t stream) {
	return 1 + (((slice << 1) | (into_far ? 1 : 0)) << 16 | stream);
}

void CrossingFailure::Hold(vector<unique_ptr<CrossingStream>> streams, shared_ptr<CrossingReceiver> receiver) {
	lock_guard<mutex> guard(lock);
	if (receiver) {
		held_receivers.push_back(std::move(receiver));
	}
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
	held_receivers.clear();
}

void CrossingFailure::Cancel(const ErrorData &error) {
	vector<CrossingWaker> wake;
	{
		lock_guard<mutex> guard(lock);
		cancelled = error;
		wake.swap(waiting);
	}
	for (auto &waker : wake) {
		waker.Wake();
	}
}

void CrossingFailure::WakeOnCancel(CrossingWaker waker) {
	{
		lock_guard<mutex> guard(lock);
		if (!cancelled.HasError()) {
			waiting.push_back(std::move(waker));
			return;
		}
	}
	waker.Wake();
}

void CrossingFailure::ThrowIfCancelled() {
	lock_guard<mutex> guard(lock);
	if (cancelled.HasError()) {
		cancelled.Throw();
	}
}

namespace {

enum class Sent : uint8_t { TAKEN, BLOCKED, STOPPED };

Sent SendFrame(CrossingStream &stream, CrossingFrame &frame, const InterruptState &resume,
               const shared_ptr<CrossingFailure> &failure) {
	while (true) {
		if (failure) {
			failure->ThrowIfCancelled();
		}
		auto parking = CrossingParking::Of(resume);
		switch (stream.Send(frame, parking->Waker())) {
		case CrossingIo::TAKEN:
			return Sent::TAKEN;
		case CrossingIo::DONE:
			return Sent::STOPPED;
		default:
			if (failure) {
				failure->WakeOnCancel(parking->Waker());
			}
			if (parking->Park()) {
				return Sent::BLOCKED;
			}
		}
	}
}

struct StreamSinkGlobal;

//! Frames from the end reading this slice's rows: how many bytes of them it has read.
struct CreditReceiver : public CrossingReceiver {
	explicit CreditReceiver(StreamSinkGlobal &global_p) : global(&global_p) {
	}
	const vector<LogicalType> &Types() const override {
		static const vector<LogicalType> none;
		return none;
	}
	void Deliver(CrossingFrame &frame) override;
	void Closed(optional_ptr<const ErrorData> error) override;

	StreamSinkGlobal *global;
};

struct StreamSinkGlobal : public GlobalSinkState {
	vector<unique_ptr<CrossingStream>> streams;
	shared_ptr<CreditReceiver> credit;
	vector<idx_t> free_streams;
	atomic<idx_t> next_sequence {0};
	idx_t next_sequential_batch = 0;
	bool closed = false;
	shared_ptr<CrossingFailure> failure;

	mutex credit_lock;
	idx_t sent_bytes = 0;
	idx_t read_bytes = 0;
	idx_t min_batch = 0;
	idx_t largest_frame = 0;
	map<idx_t, idx_t> sent_by_batch;
	idx_t released_bytes = 0;
	bool reader_gone = false;
	vector<CrossingWaker> waiting;

	void WakeWaiting() {
		vector<CrossingWaker> wake;
		{
			lock_guard<mutex> guard(credit_lock);
			wake.swap(waiting);
		}
		for (auto &waker : wake) {
			waker.Wake();
		}
	}

	void AdvanceMinBatch(idx_t min_batch_p) {
		{
			lock_guard<mutex> guard(credit_lock);
			if (min_batch_p <= min_batch) {
				return;
			}
			min_batch = min_batch_p;
		}
		WakeWaiting();
	}

	//! A frame reporting `reported` was taken: the reader can release what is below it. Caller holds credit_lock.
	void Reported(idx_t reported) {
		while (!sent_by_batch.empty() && sent_by_batch.begin()->first < reported) {
			released_bytes += sent_by_batch.begin()->second;
			sent_by_batch.erase(sent_by_batch.begin());
		}
	}

	//! BatchedBufferedData::ShouldBlockBatch, or SimpleBufferedData's rule when no order is kept.
	bool ShouldBlock(bool ordered, idx_t budget, idx_t batch, idx_t incoming) {
		if (budget == 0 || reader_gone) {
			return false;
		}
		largest_frame = MaxValue(largest_frame, incoming);
		auto total = sent_bytes - read_bytes;
		if (!ordered) {
			return total > 0 && total + incoming > budget;
		}
		if (batch <= min_batch) {
			return total + incoming > budget && released_bytes > read_bytes;
		}
		if (total == 0) {
			return false;
		}
		auto reserve = MaxValue<idx_t>(MinValue<idx_t>(largest_frame, budget / 2), MaxValue<idx_t>(budget / 8, 1));
		return total + incoming > MaxValue<idx_t>(budget - reserve, 1);
	}

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
			if (credit) {
				credit->global = nullptr;
			}
			failure->Hold(std::move(streams), credit);
			return;
		}
		ErrorData ended(ExceptionType::INTERRUPT, "crossing: the statement ended before this slice finished");
		CloseAll(&ended);
	}

	idx_t MaxThreads(idx_t source_max_threads) override {
		return MinValue<idx_t>(source_max_threads, streams.size());
	}
};

void CreditReceiver::Deliver(CrossingFrame &frame) {
	if (!global) {
		return;
	}
	{
		lock_guard<mutex> guard(global->credit_lock);
		global->read_bytes += std::stoull(frame.header);
	}
	global->WakeWaiting();
}

void CreditReceiver::Closed(optional_ptr<const ErrorData>) {
	if (!global) {
		return;
	}
	{
		lock_guard<mutex> guard(global->credit_lock);
		global->reader_gone = true;
	}
	global->WakeWaiting();
}

struct StreamSinkLocal : public LocalSinkState {
	optional_idx stream;
	DataChunk frame_rows;
	bool stopped = false;
	//! A frame given to Send that has not been taken yet keeps its place in the slice's order.
	bool sending = false;
	CrossingRowHeader header;
	bool reporting = false;
	CrossingRowHeader report;
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

CrossingStreamSink::CrossingStreamSink(CrossingLink &link_p, idx_t streams_p, idx_t slice_p, bool into_far_p,
                                       vector<idx_t> columns_p, vector<LogicalType> types_p, bool ordered_p,
                                       bool parallel_p, shared_ptr<CrossingFailure> failure_p, idx_t budget_p)
    : link(&link_p), streams(streams_p), slice(slice_p), into_far(into_far_p), columns(std::move(columns_p)),
      types(std::move(types_p)), ordered(ordered_p), parallel(parallel_p), failure(std::move(failure_p)),
      budget(budget_p) {
}

unique_ptr<GlobalSinkState> CrossingStreamSink::GlobalState() const {
	auto state = make_uniq<StreamSinkGlobal>();
	state->failure = failure;
	if (!into_far) {
		state->credit = make_shared_ptr<CreditReceiver>(*state);
	}
	auto count = parallel ? streams : idx_t(1);
	for (idx_t k = 0; k < streams; k++) {
		auto stream = link->Open(CrossingSliceStreamId(slice, into_far, k), state->credit.get());
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
	idx_t min_batch = 0;
	if (ordered) {
		batch = parallel ? local.partition_info.batch_index.GetIndex() : global.next_sequential_batch;
		min_batch = parallel ? local.partition_info.min_batch_index.GetIndex() : batch;
		global.AdvanceMinBatch(min_batch);
	}
	for (idx_t c = 0; c < columns.size(); c++) {
		local.frame_rows.data[c].Reference(chunk.data[columns[c]]);
	}
	local.frame_rows.SetChildCardinality(chunk.size());
	CrossingFrame frame;
	frame.rows.InitializeEmpty(types);
	frame.rows.Reference(local.frame_rows);
	if (!local.sending) {
		auto bytes = frame.rows.GetDataSize();
		while (true) {
			auto parking = CrossingParking::Of(input.interrupt_state);
			{
				lock_guard<mutex> guard(global.credit_lock);
				if (!global.ShouldBlock(ordered, budget, batch, bytes)) {
					break;
				}
				global.waiting.push_back(parking->Waker());
			}
			if (failure) {
				failure->WakeOnCancel(parking->Waker());
				failure->ThrowIfCancelled();
			}
			if (parking->Park()) {
				return SinkResultType::BLOCKED;
			}
		}
		local.header.sequence = global.next_sequence++;
		local.header.batch = batch;
		local.header.min_batch = min_batch;
		local.header.bytes = bytes;
		local.sending = true;
	}
	frame.header = local.header.Encode();
	switch (SendFrame(*global.streams[local.stream.GetIndex()], frame, input.interrupt_state, failure)) {
	case Sent::BLOCKED:
		return SinkResultType::BLOCKED;
	case Sent::STOPPED:
		local.stopped = true;
		return SinkResultType::FINISHED;
	default:
		break;
	}
	local.sending = false;
	{
		lock_guard<mutex> guard(global.credit_lock);
		global.sent_bytes += local.header.bytes;
		global.sent_by_batch[local.header.batch] += local.header.bytes;
		global.Reported(local.header.min_batch);
	}
	if (ordered && !parallel) {
		global.next_sequential_batch++;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

namespace {

//! Tells the reader how far the slice has got. False when the sender is to report BLOCKED and be called again.
bool ReportProgress(StreamSinkGlobal &global, StreamSinkLocal &local, bool parallel, const InterruptState &resume,
                    const shared_ptr<CrossingFailure> &failure) {
	if (!parallel) {
		return true;
	}
	auto min_batch = local.partition_info.min_batch_index.GetIndex();
	global.AdvanceMinBatch(min_batch);
	if (!local.stream.IsValid() || local.stopped) {
		return true;
	}
	if (!local.reporting) {
		local.report.sequence = global.next_sequence++;
		local.report.batch = local.partition_info.batch_index.GetIndex();
		local.report.min_batch = min_batch;
		local.report.bytes = 0;
		local.reporting = true;
	}
	CrossingFrame frame;
	frame.header = local.report.Encode();
	auto sent = SendFrame(*global.streams[local.stream.GetIndex()], frame, resume, failure);
	if (sent == Sent::BLOCKED) {
		return false;
	}
	local.reporting = false;
	local.stopped |= sent == Sent::STOPPED;
	{
		lock_guard<mutex> guard(global.credit_lock);
		global.Reported(local.report.min_batch);
	}
	global.WakeWaiting();
	return true;
}

} // namespace

SinkCombineResultType CrossingStreamSink::Combine(OperatorSinkCombineInput &input) const {
	auto &global = input.global_state.Cast<StreamSinkGlobal>();
	auto &local = input.local_state.Cast<StreamSinkLocal>();
	if (!local.stream.IsValid()) {
		return SinkCombineResultType::FINISHED;
	}
	if (ordered && !ReportProgress(global, local, parallel, input.interrupt_state, failure)) {
		return SinkCombineResultType::BLOCKED;
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
	if (!ordered) {
		return SinkNextBatchType::READY;
	}
	return ReportProgress(global, local, parallel, input.interrupt_state, failure) ? SinkNextBatchType::READY
	                                                                               : SinkNextBatchType::BLOCKED;
}

SinkNextBatchType CrossingStreamSink::UpdateMinBatchIndex(OperatorSinkNextBatchInput &input) const {
	auto &global = input.global_state.Cast<StreamSinkGlobal>();
	auto &local = input.local_state.Cast<StreamSinkLocal>();
	if (ordered && parallel) {
		global.AdvanceMinBatch(local.partition_info.min_batch_index.GetIndex());
	}
	return SinkNextBatchType::READY;
}

void CrossingStreamSink::Finish(GlobalSinkState &state) const {
	state.Cast<StreamSinkGlobal>().CloseAll(nullptr);
}

} // namespace duckdb
