#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

//! The rows that crossed a seam, held until a consumer takes them. Taking every row and spilling is what
//! lets a sender finish while the consumer cannot run yet (PipelineBroadcastExchangeBufferMode::BUFFER_ALL).
//!
//! Ordered: each stream's batches rise. A batch below every open stream's latest is whole, and is released
//! in batch order, under ranks that rise by one, so a thread's ranks never go down.
class CrossingSeamInbox : public StateWithBlockableTasks {
public:
	enum class Took : uint8_t { ROWS, PARK, DONE };

	CrossingSeamInbox(ClientContext &context, vector<LogicalType> types, bool ordered, idx_t streams);

	const vector<LogicalType> &Types() const {
		return types;
	}
	bool Ordered() const {
		return ordered;
	}
	idx_t Streams() const {
		return stream_batches.size();
	}

	//! `rows` empty only says the stream reached `batch`.
	void Append(idx_t stream, DataChunk &rows, idx_t batch);
	//! The stream has nothing open now, though it may later go on with a higher batch.
	void Idle(idx_t stream);
	void StreamDone(idx_t stream);
	void Cancel(const ErrorData &error);

	//! Near's estimate of the rows coming, for far's optimizer.
	optional_idx estimate;

	//! Throws what Cancel was given. False when nothing is ready now.
	bool TryTake(unique_ptr<ColumnDataCollection> &rows, idx_t &rank);
	//! Throws what Cancel was given. PARK registers `resume`, which is called when there may be more.
	Took Take(unique_ptr<ColumnDataCollection> &rows, idx_t &rank, const InterruptState &resume);

private:
	unique_ptr<ColumnDataCollection> NewCollection() const;
	void ReleaseWholeBatches() DUCKDB_REQUIRES(lock);
	void WakeConsumers() DUCKDB_EXCLUDES(lock);
	bool TryTakeLocked(unique_ptr<ColumnDataCollection> &rows, idx_t &rank) DUCKDB_REQUIRES(lock);

	BufferManager &buffer_manager;
	vector<LogicalType> types;
	bool ordered;

	enum class StreamState : uint8_t { NOT_STARTED, AT_BATCH, IDLE, DONE };
	struct StreamProgress {
		StreamState state = StreamState::NOT_STARTED;
		idx_t batch = 0;
	};
	vector<StreamProgress> stream_batches DUCKDB_GUARDED_BY(lock);

	unique_ptr<ColumnDataCollection> tail DUCKDB_GUARDED_BY(lock);
	map<idx_t, unique_ptr<ColumnDataCollection>> batches DUCKDB_GUARDED_BY(lock);
	deque<unique_ptr<ColumnDataCollection>> ready DUCKDB_GUARDED_BY(lock);
	idx_t next_rank DUCKDB_GUARDED_BY(lock) = 0;
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
