#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include "crossing.hpp"

namespace duckdb {

//! `header` is crossing's: carry it unread. `rows` may be empty, and has no columns on a frame that
//! carries only a header. Encode the rows however the transport likes.
struct CrossingFrame {
	string header;
	DataChunk rows;
};

//! TAKEN: Send has the frame and needs nothing from it after returning. DONE: the other end closed with no error, so
//! it wants no more. WAIT: not yet; call the waker's Wake() when it may go on, and Send is asked again with the frame.
enum class CrossingIo : uint8_t { TAKEN, DONE, WAIT };

//! Where the transport hands what arrives on a stream. One call at a time for each stream, from any thread, in the
//! order sent. It never waits on the transport.
class CrossingReceiver {
public:
	virtual ~CrossingReceiver() = default;

	//! What a frame's rows are, to decode them into. Empty when frames carry only a header.
	virtual const vector<LogicalType> &Types() const = 0;
	virtual void Deliver(CrossingFrame &frame) = 0;
	//! The other end closed. Non-null is the error it closed with. Called once, after the last Deliver.
	virtual void Closed(optional_ptr<const ErrorData> error) = 0;
};

//! Send throws the error the other end closed with. One thread at a time at each end.
class CrossingStream {
public:
	virtual ~CrossingStream() = default;

	virtual CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) = 0;
	//! No more frames from this end, and none delivered to it once this returns. Null when it ended cleanly. Called
	//! once, and never throws.
	virtual void Close(optional_ptr<const ErrorData> error) = 0;
};

//! One far transaction. Both ends open a stream by the same id, in either order, from any thread. What arrives before
//! this end opens is held for it.
class CrossingLink {
public:
	virtual ~CrossingLink() = default;

	//! How many streams a slice may use in each direction. Only near's end is asked.
	virtual idx_t Streams() {
		return 1;
	}
	//! `receiver` is null at an end that only sends. It outlives the stream.
	virtual unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) = 0;
};

} // namespace duckdb
