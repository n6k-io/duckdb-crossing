#pragma once

#include "crossing_link.hpp"

#include "duckdb/common/deque.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"

#include <map>
#include <set>
#include <thread>

namespace duckdb {

//! Two ends of one link in one process. A pipe holds `capacity` frames, then Send returns WAIT.
class TestLinkPair {
public:
	explicit TestLinkPair(idx_t streams_p = 1, idx_t capacity_p = 4)
	    : near_end(make_uniq<End>(*this, 0)), far_end(make_uniq<End>(*this, 1)), streams(streams_p),
	      capacity(capacity_p) {
	}

	CrossingLink &Near() {
		return *near_end;
	}
	CrossingLink &Far() {
		return *far_end;
	}

	std::set<idx_t> UsedStreams() {
		lock_guard<mutex> guard(lock);
		return used_streams;
	}
	std::set<std::thread::id> SendingThreads() {
		lock_guard<mutex> guard(lock);
		return sending_threads;
	}
	bool FarSentBeforeSeamEnded() {
		lock_guard<mutex> guard(lock);
		return far_sent_before_seam_ended;
	}
	idx_t Waits() {
		lock_guard<mutex> guard(lock);
		return waits;
	}

private:
	struct Stored {
		string header;
		bool has_rows = false;
		unique_ptr<DataChunk> rows;
	};

	struct Pipe {
		deque<Stored> frames;
		bool closed = false;
		ErrorData error;
		vector<CrossingWaker> senders;
		vector<CrossingWaker> receivers;
	};

	class Stream : public CrossingStream {
	public:
		Stream(TestLinkPair &pair_p, idx_t id_p, int side_p) : pair(pair_p), id(id_p), side(side_p) {
		}

		CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) override {
			vector<CrossingWaker> wake;
			{
				lock_guard<mutex> guard(pair.lock);
				auto &reverse = pair.pipes[{id, 1 - side}];
				if (reverse.closed) {
					if (reverse.error.HasError()) {
						reverse.error.Throw();
					}
					return CrossingIo::DONE;
				}
				auto &pipe = pair.pipes[{id, side}];
				if (pipe.frames.size() >= pair.capacity) {
					pipe.senders.push_back(std::move(waker));
					pair.waits++;
					return CrossingIo::WAIT;
				}
				Stored stored;
				stored.header = frame.header;
				if (frame.rows.ColumnCount() > 0) {
					stored.has_rows = true;
					stored.rows = make_uniq<DataChunk>();
					stored.rows->Initialize(Allocator::DefaultAllocator(), frame.rows.GetTypes());
					frame.rows.Copy(*stored.rows);
					pair.used_streams.insert(id);
					if (side == 1 && !pair.seam_ended) {
						pair.far_sent_before_seam_ended = true;
					}
					pair.sending_threads.insert(std::this_thread::get_id());
				}
				pipe.frames.push_back(std::move(stored));
				wake.swap(pipe.receivers);
			}
			for (auto &sleeper : wake) {
				sleeper.Wake();
			}
			return CrossingIo::TAKEN;
		}

		CrossingIo Recv(CrossingFrame &frame, CrossingWaker waker) override {
			vector<CrossingWaker> wake;
			CrossingIo result;
			{
				lock_guard<mutex> guard(pair.lock);
				auto &pipe = pair.pipes[{id, 1 - side}];
				if (!pipe.frames.empty()) {
					auto stored = std::move(pipe.frames.front());
					pipe.frames.pop_front();
					frame.header = stored.header;
					if (stored.has_rows) {
						frame.rows.Reset();
						stored.rows->Copy(frame.rows);
					}
					wake.swap(pipe.senders);
					result = CrossingIo::ROWS;
				} else if (pipe.closed) {
					if (pipe.error.HasError()) {
						pipe.error.Throw();
					}
					return CrossingIo::DONE;
				} else {
					pipe.receivers.push_back(std::move(waker));
					return CrossingIo::WAIT;
				}
			}
			for (auto &sleeper : wake) {
				sleeper.Wake();
			}
			return result;
		}

		void Close(optional_ptr<const ErrorData> error) override {
			vector<CrossingWaker> wake;
			{
				lock_guard<mutex> guard(pair.lock);
				auto &pipe = pair.pipes[{id, side}];
				pipe.closed = true;
				if (side == 0 && id > 0 && ((id - 1) >> 16) & 1) {
					pair.seam_ended = true;
				}
				if (error) {
					pipe.error = *error;
				}
				wake.swap(pipe.receivers);
				auto &reverse = pair.pipes[{id, 1 - side}];
				wake.insert(wake.end(), reverse.senders.begin(), reverse.senders.end());
				reverse.senders.clear();
			}
			for (auto &sleeper : wake) {
				sleeper.Wake();
			}
		}

	private:
		TestLinkPair &pair;
		idx_t id;
		int side;
	};

	class End : public CrossingLink {
	public:
		End(TestLinkPair &pair_p, int side_p) : pair(pair_p), side(side_p) {
		}
		idx_t Streams() override {
			return pair.streams;
		}
		unique_ptr<CrossingStream> Open(idx_t id) override {
			return make_uniq<Stream>(pair, id, side);
		}

	private:
		TestLinkPair &pair;
		int side;
	};

	unique_ptr<End> near_end;
	unique_ptr<End> far_end;
	idx_t streams;
	idx_t capacity;
	mutex lock;
	std::map<std::pair<idx_t, int>, Pipe> pipes;
	std::set<idx_t> used_streams;
	std::set<std::thread::id> sending_threads;
	idx_t waits = 0;
	bool seam_ended = false;
	bool far_sent_before_seam_ended = false;
};

} // namespace duckdb
