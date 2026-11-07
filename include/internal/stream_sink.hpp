#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/execution/partition_info.hpp"

#include "crossing_link.hpp"

namespace duckdb {

//! The id both ends open a slice's k-th stream by. Stream 0 is the control stream.
idx_t CrossingSliceStreamId(idx_t slice, bool into_far, idx_t stream);

//! Holds the streams of slices a failing statement tears down, until it is known why it failed.
class CrossingFailure {
public:
	void Hold(vector<unique_ptr<CrossingStream>> streams, shared_ptr<CrossingReceiver> receiver);
	void CloseHeld(const ErrorData &error);
	//! Wakes every sink waiting to send; each throws `error` when it goes on.
	void Cancel(const ErrorData &error);
	void WakeOnCancel(CrossingWaker waker);
	void ThrowIfCancelled();

private:
	mutex lock;
	vector<unique_ptr<CrossingStream>> held;
	vector<shared_ptr<CrossingReceiver>> held_receivers;
	ErrorData cancelled;
	vector<CrossingWaker> waiting;
};

//! What sending rows into a slice's streams takes, on either side of the wire. With a budget it holds back senders as
//! BatchedBufferedData and SimpleBufferedData hold back a result's producers, by what the other end has not yet read.
class CrossingStreamSink {
public:
	CrossingStreamSink(CrossingLink &link, idx_t streams, idx_t slice, bool into_far, vector<idx_t> columns,
	                   vector<LogicalType> types, bool ordered, bool parallel, shared_ptr<CrossingFailure> failure,
	                   idx_t budget = 0);

	bool OrderDependent() const {
		return ordered && !parallel;
	}
	OperatorPartitionInfo PartitionInfo() const {
		return ordered && parallel ? OperatorPartitionInfo::BatchIndex() : OperatorPartitionInfo::NoPartitionInfo();
	}
	unique_ptr<GlobalSinkState> GlobalState() const;
	unique_ptr<LocalSinkState> LocalState() const;
	SinkResultType Sink(DataChunk &chunk, OperatorSinkInput &input) const;
	SinkCombineResultType Combine(OperatorSinkCombineInput &input) const;
	SinkNextBatchType NextBatch(OperatorSinkNextBatchInput &input) const;
	SinkNextBatchType UpdateMinBatchIndex(OperatorSinkNextBatchInput &input) const;
	void Finish(GlobalSinkState &state) const;

	CrossingLink *link;
	idx_t streams;
	idx_t slice;
	bool into_far;
	vector<idx_t> columns;
	vector<LogicalType> types;
	bool ordered;
	bool parallel;
	shared_ptr<CrossingFailure> failure;
	idx_t budget;
};

} // namespace duckdb
