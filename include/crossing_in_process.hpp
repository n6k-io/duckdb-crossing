#pragma once

#include "crossing_far.hpp"
#include "crossing_link.hpp"

#include "duckdb/main/database.hpp"

#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <thread>

namespace duckdb {

//! Two ends of one link in one process, with one thread delivering everything. A pipe holds `capacity` frames, then
//! Send returns WAIT.
class CrossingLinkPair {
public:
	explicit CrossingLinkPair(idx_t streams = 1, idx_t capacity = 4, idx_t far_streams = 0);
	virtual ~CrossingLinkPair();

	CrossingLink &Near();
	CrossingLink &Far();

protected:
	//! Side 0 sends from near, side 1 from far. The hooks below run with `lock` held, except Sending.
	virtual void Sending(idx_t id, int side, CrossingFrame &frame) {
	}
	virtual void Queued(idx_t id, int side, const CrossingFrame &frame) {
	}
	virtual void Waiting(idx_t id, int side) {
	}
	virtual void Closing(idx_t id, int side) {
	}
	virtual void Delivering(idx_t id, int side, bool rows) {
	}
	//! Whether frames from far on stream `id` are held back for now.
	virtual bool Holds(idx_t id) {
		return false;
	}
	//! Lifts the frame limit and wakes every waiting sender.
	void Unblock();
	//! Ends delivery. A subclass overriding a hook calls this from its destructor.
	void Stop();
	//! Frames far sent that near has not been handed yet. `lock` must be held.
	idx_t HeldForNear() const;

	std::mutex lock;

private:
	struct Stored {
		string header;
		unique_ptr<DataChunk> rows;
	};

	//! Frames travelling one way on one stream, keyed by the side that sends them.
	struct Pipe {
		std::deque<Stored> frames;
		bool closed = false;
		bool close_delivered = false;
		ErrorData error;
		vector<CrossingWaker> senders;
		optional_ptr<CrossingReceiver> receiver;
	};

	class Stream;
	class End;

	void Run();

	unique_ptr<End> near_end;
	unique_ptr<End> far_end;
	idx_t streams;
	idx_t far_streams;
	idx_t capacity;
	std::condition_variable changed;
	std::map<std::pair<idx_t, int>, Pipe> pipes;
	bool stopping = false;
	bool delivering = false;
	std::thread delivery;
};

//! Near's end of a link to a far transaction this process serves: `catalog` of `db`, on a connection of its own and a
//! thread running CrossingFar::Serve. Hand it to crossing from CrossingSource::Connect.
class CrossingInProcessLink : public CrossingLink {
public:
	CrossingInProcessLink(DatabaseInstance &db, string catalog, CrossingFarOptions options = CrossingFarOptions(),
	                      unique_ptr<CrossingLinkPair> pair = nullptr);
	~CrossingInProcessLink() override;

	idx_t Streams() override;
	unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) override;

	//! Waits for Serve to return, which it does once near ends the transaction or closes the link.
	void Wait();
	CrossingLinkPair &Pair();

private:
	unique_ptr<CrossingLinkPair> pair;
	Connection connection;
	CrossingFar far;
	std::thread serving;
};

} // namespace duckdb
