#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/string.hpp"

#include "crossing.hpp"

namespace duckdb {

//! `header` is crossing's: carry it unread. `rows` may be empty, and has no columns on a frame that
//! carries only a header. Encode the rows however the transport likes.
struct CrossingFrame {
	string header;
	DataChunk rows;
};

//! TAKEN: Send has the frame and needs nothing from it after returning. ROWS: Recv filled the frame.
//! DONE: the other end closed with no error, so it wants no more (Send) or has sent all (Recv). WAIT: not
//! yet; call the waker's Wake() when it may go on.
enum class CrossingIo : uint8_t { TAKEN, ROWS, DONE, WAIT };

//! Frames in order, one thread at a time at each end. Send and Recv throw the error the other end closed with.
class CrossingStream {
public:
	virtual ~CrossingStream() = default;

	virtual CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) = 0;
	virtual CrossingIo Recv(CrossingFrame &frame, CrossingWaker waker) = 0;
	//! No more frames from this end. Null when it ended cleanly. Called once, and never throws.
	virtual void Close(optional_ptr<const ErrorData> error) = 0;
};

//! One far transaction. Both ends open a stream by the same id, in either order, from any thread.
class CrossingLink {
public:
	virtual ~CrossingLink() = default;

	//! How many streams a slice may use in each direction. Only near's end is asked.
	virtual idx_t Streams() {
		return 1;
	}
	virtual unique_ptr<CrossingStream> Open(idx_t id) = 0;
};

} // namespace duckdb
