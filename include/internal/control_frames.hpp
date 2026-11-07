#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/interrupt.hpp"

#include "crossing_link.hpp"
#include "internal/parking.hpp"

namespace duckdb {

static constexpr const char *CROSSING_CONTROL_PLAN = "PLAN\n";
static constexpr const char *CROSSING_CONTROL_DDL = "DDL\n";
static constexpr const char *CROSSING_CONTROL_VIEW = "VIEW\n";
static constexpr const char *CROSSING_CONTROL_COMMIT = "COMMIT";
static constexpr const char *CROSSING_CONTROL_ROLLBACK = "ROLLBACK";
static constexpr const char *CROSSING_CONTROL_TABLES = "TABLES\n";
static constexpr const char *CROSSING_CONTROL_DESCRIBE = "DESCRIBE\n";
static constexpr const char *CROSSING_CONTROL_DONE = "DONE";
static constexpr const char *CROSSING_CONTROL_ERROR = "ERROR\n";

inline void SendControl(CrossingStream &stream, const string &header) {
	CrossingFrame frame;
	frame.header = header;
	while (true) {
		auto signal = make_shared_ptr<InterruptDoneSignalState>();
		auto parking = CrossingParking::Of(InterruptState(signal));
		switch (stream.Send(frame, parking->Waker())) {
		case CrossingIo::TAKEN:
			return;
		case CrossingIo::DONE:
			throw IOException("crossing: the link closed");
		default:
			if (parking->Park()) {
				signal->Await();
			}
		}
	}
}

//! False when the other end closed.
inline bool RecvControl(CrossingStream &stream, string &header) {
	while (true) {
		CrossingFrame frame;
		auto signal = make_shared_ptr<InterruptDoneSignalState>();
		auto parking = CrossingParking::Of(InterruptState(signal));
		switch (stream.Recv(frame, parking->Waker())) {
		case CrossingIo::ROWS:
			header = std::move(frame.header);
			return true;
		case CrossingIo::DONE:
			return false;
		default:
			if (parking->Park()) {
				signal->Await();
			}
		}
	}
}

//! What far must know of a statement before it can read its plan. A statement that ends the transaction itself is
//! the whole of a read-only autocommit transaction: far commits it and sends no reply.
struct CrossingPlanFrame {
	bool ends_transaction = false;
	idx_t streams = 1;
	vector<idx_t> slices;
	string plan;

	string Encode() const {
		string frame = string(CROSSING_CONTROL_PLAN) + (ends_transaction ? "1\n" : "0\n") + to_string(streams) + "\n";
		for (idx_t i = 0; i < slices.size(); i++) {
			frame += (i ? "," : "") + to_string(slices[i]);
		}
		return frame + "\n" + plan;
	}
	//! `body` is the frame past CROSSING_CONTROL_PLAN.
	static CrossingPlanFrame Decode(const string &body) {
		CrossingPlanFrame frame;
		auto streams_at = body.find('\n') + 1;
		auto slices_at = body.find('\n', streams_at) + 1;
		auto plan_at = body.find('\n', slices_at) + 1;
		frame.ends_transaction = body[0] == '1';
		frame.streams = std::stoull(body.substr(streams_at, slices_at - streams_at - 1));
		for (auto &slice : StringUtil::Split(body.substr(slices_at, plan_at - slices_at - 1), ',')) {
			frame.slices.push_back(std::stoull(slice));
		}
		frame.plan = body.substr(plan_at);
		return frame;
	}
};

inline string ControlError(const ErrorData &error) {
	return string(CROSSING_CONTROL_ERROR) + Exception::ExceptionTypeToString(error.Type()) + "\n" + error.RawMessage();
}

//! Throws what the other end reported, with its type.
inline void ThrowIfControlError(const string &header) {
	const string prefix(CROSSING_CONTROL_ERROR);
	if (header.compare(0, prefix.size(), prefix) != 0) {
		return;
	}
	auto type_end = header.find('\n', prefix.size());
	auto type = Exception::StringToExceptionType(header.substr(prefix.size(), type_end - prefix.size()));
	ErrorData(type, header.substr(type_end + 1)).Throw();
}

} // namespace duckdb
