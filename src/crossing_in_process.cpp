#include "crossing_in_process.hpp"

#include "duckdb/common/limits.hpp"

#include <chrono>

namespace duckdb {

class CrossingLinkPair::Stream : public CrossingStream {
public:
	Stream(CrossingLinkPair &pair_p, idx_t id_p, int side_p) : pair(pair_p), id(id_p), side(side_p) {
	}

	CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) override {
		pair.Sending(id, side, frame);
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
				pair.Waiting(id, side);
				return CrossingIo::WAIT;
			}
			Stored stored;
			stored.header = frame.header;
			if (frame.rows.ColumnCount() > 0) {
				stored.rows = make_uniq<DataChunk>();
				stored.rows->Initialize(Allocator::DefaultAllocator(), frame.rows.GetTypes());
				frame.rows.Copy(*stored.rows);
			}
			pipe.frames.push_back(std::move(stored));
			pair.Queued(id, side, frame);
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
			pair.Closing(id, side);
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
	CrossingLinkPair &pair;
	idx_t id;
	int side;
};

class CrossingLinkPair::End : public CrossingLink {
public:
	End(CrossingLinkPair &pair_p, int side_p) : pair(pair_p), side(side_p) {
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
	CrossingLinkPair &pair;
	int side;
};

CrossingLinkPair::CrossingLinkPair(idx_t streams_p, idx_t capacity_p, idx_t far_streams_p)
    : near_end(make_uniq<End>(*this, 0)), far_end(make_uniq<End>(*this, 1)), streams(streams_p),
      far_streams(far_streams_p ? far_streams_p : streams_p), capacity(capacity_p), delivery([this]() { Run(); }) {
}

CrossingLinkPair::~CrossingLinkPair() {
	Stop();
}

void CrossingLinkPair::Stop() {
	{
		std::lock_guard<std::mutex> guard(lock);
		stopping = true;
	}
	changed.notify_all();
	if (delivery.joinable()) {
		delivery.join();
	}
}

CrossingLink &CrossingLinkPair::Near() {
	return *near_end;
}

CrossingLink &CrossingLinkPair::Far() {
	return *far_end;
}

void CrossingLinkPair::Unblock() {
	vector<CrossingWaker> wake;
	{
		std::lock_guard<std::mutex> guard(lock);
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

idx_t CrossingLinkPair::HeldForNear() const {
	idx_t held = 0;
	for (auto &entry : pipes) {
		if (entry.first.second == 1) {
			held += entry.second.frames.size();
		}
	}
	return held;
}

void CrossingLinkPair::Run() {
	std::unique_lock<std::mutex> guard(lock);
	while (!stopping) {
		bool delivered = false;
		for (auto &entry : pipes) {
			auto id = entry.first.first;
			auto side = entry.first.second;
			auto &pipe = entry.second;
			if (!pipe.receiver || (side == 1 && Holds(id))) {
				continue;
			}
			if (!pipe.frames.empty()) {
				auto stored = std::move(pipe.frames.front());
				pipe.frames.pop_front();
				Delivering(id, side, stored.rows != nullptr);
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

CrossingInProcessLink::CrossingInProcessLink(DatabaseInstance &db, string catalog, CrossingFarOptions options,
                                             unique_ptr<CrossingLinkPair> pair_p)
    : pair(pair_p ? std::move(pair_p) : make_uniq<CrossingLinkPair>()), connection(db),
      far(connection, std::move(catalog), std::move(options)) {
	serving = std::thread([this]() {
		try {
			far.Serve(pair->Far());
		} catch (...) {
		}
	});
}

CrossingInProcessLink::~CrossingInProcessLink() {
	Wait();
}

idx_t CrossingInProcessLink::Streams() {
	return pair->Near().Streams();
}

unique_ptr<CrossingStream> CrossingInProcessLink::Open(idx_t id, optional_ptr<CrossingReceiver> receiver) {
	return pair->Near().Open(id, receiver);
}

void CrossingInProcessLink::Wait() {
	if (serving.joinable()) {
		serving.join();
	}
}

CrossingLinkPair &CrossingInProcessLink::Pair() {
	return *pair;
}

} // namespace duckdb
