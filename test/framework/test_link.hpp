#pragma once

#include "crossing_in_process.hpp"

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

//! The in-process pair, instrumented: what it carried, and knobs to stall and release it.
class TestLinkPair : public CrossingLinkPair {
public:
	explicit TestLinkPair(idx_t streams = 1, idx_t capacity = 4, idx_t far_streams = 0)
	    : CrossingLinkPair(streams, capacity, far_streams) {
	}

	~TestLinkPair() override {
		Stop();
	}

	//! Called with each frame of rows sent, and each control frame near sends.
	std::function<void(idx_t id, int side, DataChunk &rows)> on_rows;
	std::function<void(const string &header)> on_control;
	//! Whether delivery to near on stream `id` waits, as if far were slow. Release ends the wait.
	std::function<bool(idx_t id)> stall;

	//! Lifts every limit and wakes everything waiting, so whatever a test left stuck can end.
	void Release() {
		{
			std::lock_guard<std::mutex> guard(lock);
			released = true;
		}
		Unblock();
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

protected:
	void Sending(idx_t id, int side, CrossingFrame &frame) override {
		if (on_control && id == 0 && side == 0) {
			on_control(frame.header);
		}
		if (on_rows && frame.rows.ColumnCount() > 0) {
			on_rows(id, side, frame.rows);
		}
	}
	void Queued(idx_t id, int side, const CrossingFrame &frame) override {
		if (frame.rows.ColumnCount() > 0) {
			used_streams.insert(id);
			sending_threads.insert(std::this_thread::get_id());
			if (side == 0) {
				seam_rows += frame.rows.size();
			}
			if (side == 1 && !seam_ended) {
				far_sent_before_seam_ended = true;
			}
		}
		if (side == 1) {
			most_held_for_near = MaxValue(most_held_for_near, HeldForNear());
		}
	}
	void Waiting(idx_t, int) override {
		waits++;
	}
	void Closing(idx_t id, int side) override {
		if (side == 0 && id > 0 && (((id - 1) >> 16) & 1)) {
			seam_ended = true;
		}
	}
	void Delivering(idx_t id, int side, bool rows) override {
		if (side == 1 && id == 0) {
			near_control_replies++;
		}
		if (side == 1 && id != 0 && rows) {
			recv_streams.insert(id);
		}
	}
	bool Holds(idx_t id) override {
		return !released && stall && stall(id);
	}

private:
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
};

} // namespace duckdb
