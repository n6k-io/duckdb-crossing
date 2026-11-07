#pragma once

#include "duckdb/common/deque.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"

#include "crossing_link.hpp"

namespace duckdb {

//! Where a frame of a slice's rows sits, carried in the frame's header.
struct CrossingRowHeader {
	//! Ordered across the slice's streams. Every frame gets one.
	idx_t sequence = 0;
	idx_t batch = 0;
	//! No batch below this was still being sent when this frame took its sequence.
	idx_t min_batch = 0;
	//! What the receiver gives back as credit once the rows are read.
	idx_t bytes = 0;

	string Encode() const;
	static CrossingRowHeader Decode(const string &header);
};

//! The rows that crossed a seam, until a consumer takes them. It takes every frame and spills, so the transport never
//! waits on a consumer (PipelineBroadcastExchangeBufferMode::BUFFER_ALL); a sender that has to be held back is held
//! by CrossingStreamSink.
//!
//! Ordered: frames may overtake each other across streams, so a batch is released once every frame sequenced before a
//! frame reporting it finished has arrived; then in batch order, under ranks that rise by one, so a thread's ranks
//! never go down.
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
		return streams;
	}

	//! `rows` empty only reports progress.
	void Append(DataChunk &rows, const CrossingRowHeader &header);
	void StreamDone();
	void Cancel(const ErrorData &error);

	//! Near's estimate of the rows coming, for far's optimizer.
	optional_idx estimate;

	//! Throws what Cancel was given. PARK registers `resume`, which is called when there may be more. `bytes` is the
	//! credit `rows` earns once read.
	Took Take(unique_ptr<ColumnDataCollection> &rows, idx_t &rank, idx_t &bytes, const InterruptState &resume);

private:
	struct Held {
		unique_ptr<ColumnDataCollection> rows;
		idx_t bytes = 0;
	};

	Held &HeldFor(Held &held);
	void ReleaseWholeBatches() DUCKDB_REQUIRES(lock);
	void WakeConsumers() DUCKDB_EXCLUDES(lock);

	BufferManager &buffer_manager;
	vector<LogicalType> types;
	bool ordered;
	idx_t streams;

	idx_t streams_open DUCKDB_GUARDED_BY(lock);
	Held tail DUCKDB_GUARDED_BY(lock);
	map<idx_t, Held> batches DUCKDB_GUARDED_BY(lock);
	deque<Held> ready DUCKDB_GUARDED_BY(lock);
	//! Sequences arrived past `next_sequence`, with the min_batch each reported.
	map<idx_t, idx_t> arrived DUCKDB_GUARDED_BY(lock);
	idx_t next_sequence DUCKDB_GUARDED_BY(lock) = 0;
	idx_t whole_below DUCKDB_GUARDED_BY(lock) = 0;
	idx_t next_rank DUCKDB_GUARDED_BY(lock) = 0;
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

//! Takes a stream's frames into an inbox.
class CrossingInboxReceiver : public CrossingReceiver {
public:
	explicit CrossingInboxReceiver(shared_ptr<CrossingSeamInbox> inbox_p) : inbox(std::move(inbox_p)) {
	}

	const vector<LogicalType> &Types() const override {
		return inbox->Types();
	}
	void Deliver(CrossingFrame &frame) override {
		inbox->Append(frame.rows, CrossingRowHeader::Decode(frame.header));
	}
	void Closed(optional_ptr<const ErrorData> closed_with) override {
		if (closed_with) {
			inbox->Cancel(*closed_with);
		}
		inbox->StreamDone();
	}

private:
	shared_ptr<CrossingSeamInbox> inbox;
};

} // namespace duckdb
