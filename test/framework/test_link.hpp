#pragma once

#include "crossing_link.hpp"

#include "duckdb/common/deque.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <map>
#include <set>
#include <thread>

namespace duckdb {

//! Two ends of one link in one process, with one thread delivering everything. A pipe holds `capacity` frames, then
//! Send returns WAIT.
class TestLinkPair {
public:
	explicit TestLinkPair(idx_t streams_p = 1, idx_t capacity_p = 4, idx_t far_streams_p = 0)
	    : near_end(make_uniq<End>(*this, 0)), far_end(make_uniq<End>(*this, 1)), streams(streams_p),
	      far_streams(far_streams_p ? far_streams_p : streams_p), capacity(capacity_p), delivery([this]() { Run(); }) {
	}

	~TestLinkPair() {
		{
			std::lock_guard<std::mutex> guard(lock);
			stopping = true;
		}
		changed.notify_all();
		delivery.join();
	}

	CrossingLink &Near() {
		return *near_end;
	}
	CrossingLink &Far() {
		return *far_end;
	}

	//! Called with each frame of rows sent, and each control frame near sends.
	std::function<void(idx_t id, int side, DataChunk &rows)> on_rows;
	std::function<void(const string &header)> on_control;
	//! Whether delivery to near on stream `id` waits, as if far were slow. Release ends the wait.
	std::function<bool(idx_t id)> stall;

	//! Lifts every limit and wakes everything waiting, so whatever a test left stuck can end.
	void Release() {
		vector<CrossingWaker> wake;
		{
			std::lock_guard<std::mutex> guard(lock);
			released = true;
			capacity = NumericLimits<idx_t>::Maximum();
			for (auto &entry : pipes) {
				wake.insert(wake.end(), entry.second.senders.begin(), entry.second.senders.end());
				entry.second.senders.clear();
			}
		}
		changed.notify_all();
		for (auto &sleeper : wake) {
			sleeper.Wake();
		}
	}
	std::set<idx_t> NearRecvStreams() {
		std::lock_guard<std::mutex> guard(lock);
		return recv_streams;
	}
	idx_t NearControlReplies() {
		std::lock_guard<std::mutex> guard(lock);
		return near_control_replies;
	}
	idx_t SeamRows() {
		std::lock_guard<std::mutex> guard(lock);
		return seam_rows;
	}
	idx_t MostHeldForNear() {
		std::lock_guard<std::mutex> guard(lock);
		return most_held_for_near;
	}
	bool FarSentBeforeSeamEnded() {
		std::lock_guard<std::mutex> guard(lock);
		return far_sent_before_seam_ended;
	}
	std::set<idx_t> UsedStreams() {
		std::lock_guard<std::mutex> guard(lock);
		return used_streams;
	}
	std::set<std::thread::id> SendingThreads() {
		std::lock_guard<std::mutex> guard(lock);
		return sending_threads;
	}
	idx_t Waits() {
		std::lock_guard<std::mutex> guard(lock);
		return waits;
	}

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

	static bool IntoFar(idx_t id) {
		return id > 0 && (((id - 1) >> 16) & 1);
	}

	void Run() {
		std::unique_lock<std::mutex> guard(lock);
		while (!stopping) {
			bool delivered = false;
			for (auto &entry : pipes) {
				auto id = entry.first.first;
				auto side = entry.first.second;
				auto &pipe = entry.second;
				if (!pipe.receiver || (side == 1 && !released && stall && stall(id))) {
					continue;
				}
				if (!pipe.frames.empty()) {
					auto stored = std::move(pipe.frames.front());
					pipe.frames.pop_front();
					if (side == 1 && id == 0) {
						near_control_replies++;
					}
					if (side == 1 && id != 0 && stored.rows) {
						recv_streams.insert(id);
					}
					vector<CrossingWaker> wake;
					wake.swap(pipe.senders);
					auto receiver = pipe.receiver;
					delivering = true;
					guard.unlock();
					for (auto &sleeper : wake) {
						sleeper.Wake();
					}
					CrossingFrame frame;
					frame.header = std::move(stored.header);
					if (stored.rows) {
						frame.rows.Initialize(Allocator::DefaultAllocator(), receiver->Types());
						stored.rows->Copy(frame.rows);
					}
					receiver->Deliver(frame);
					guard.lock();
					delivering = false;
					changed.notify_all();
					delivered = true;
					break;
				}
				if (pipe.closed && !pipe.close_delivered) {
					pipe.close_delivered = true;
					auto receiver = pipe.receiver;
					auto error = pipe.error;
					delivering = true;
					guard.unlock();
					receiver->Closed(error.HasError() ? &error : nullptr);
					guard.lock();
					delivering = false;
					changed.notify_all();
					delivered = true;
					break;
				}
			}
			if (!delivered) {
				changed.wait_for(guard, std::chrono::milliseconds(5));
			}
		}
	}

	class Stream : public CrossingStream {
	public:
		Stream(TestLinkPair &pair_p, idx_t id_p, int side_p) : pair(pair_p), id(id_p), side(side_p) {
		}

		CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) override {
			if (pair.on_control && id == 0 && side == 0) {
				pair.on_control(frame.header);
			}
			if (pair.on_rows && frame.rows.ColumnCount() > 0) {
				pair.on_rows(id, side, frame.rows);
			}
			{
				std::lock_guard<std::mutex> guard(pair.lock);
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
					stored.rows = make_uniq<DataChunk>();
					stored.rows->Initialize(Allocator::DefaultAllocator(), frame.rows.GetTypes());
					frame.rows.Copy(*stored.rows);
					pair.used_streams.insert(id);
					pair.sending_threads.insert(std::this_thread::get_id());
					if (side == 0) {
						pair.seam_rows += frame.rows.size();
					}
					if (side == 1 && !pair.seam_ended) {
						pair.far_sent_before_seam_ended = true;
					}
				}
				pipe.frames.push_back(std::move(stored));
				if (side == 1) {
					idx_t held = 0;
					for (auto &entry : pair.pipes) {
						if (entry.first.second == 1) {
							held += entry.second.frames.size();
						}
					}
					pair.most_held_for_near = MaxValue(pair.most_held_for_near, held);
				}
			}
			pair.changed.notify_all();
			return CrossingIo::TAKEN;
		}

		void Close(optional_ptr<const ErrorData> error) override {
			vector<CrossingWaker> wake;
			{
				std::unique_lock<std::mutex> guard(pair.lock);
				auto &pipe = pair.pipes[{id, side}];
				pipe.closed = true;
				if (error) {
					pipe.error = *error;
				}
				if (side == 0 && IntoFar(id)) {
					pair.seam_ended = true;
				}
				auto &reverse = pair.pipes[{id, 1 - side}];
				wake.swap(reverse.senders);
				reverse.receiver = nullptr;
				while (pair.delivering) {
					pair.changed.wait(guard);
				}
			}
			pair.changed.notify_all();
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
			return side == 0 ? pair.streams : pair.far_streams;
		}
		unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) override {
			{
				std::lock_guard<std::mutex> guard(pair.lock);
				pair.pipes[{id, 1 - side}].receiver = receiver;
			}
			pair.changed.notify_all();
			return make_uniq<Stream>(pair, id, side);
		}

	private:
		TestLinkPair &pair;
		int side;
	};

	unique_ptr<End> near_end;
	unique_ptr<End> far_end;
	idx_t streams;
	idx_t far_streams;
	idx_t capacity;
	std::mutex lock;
	std::condition_variable changed;
	std::map<std::pair<idx_t, int>, Pipe> pipes;
	bool stopping = false;
	bool delivering = false;
	bool released = false;
	idx_t near_control_replies = 0;
	std::set<idx_t> recv_streams;
	std::set<idx_t> used_streams;
	std::set<std::thread::id> sending_threads;
	idx_t waits = 0;
	idx_t seam_rows = 0;
	idx_t most_held_for_near = 0;
	bool seam_ended = false;
	bool far_sent_before_seam_ended = false;
	std::thread delivery;
};

} // namespace duckdb
