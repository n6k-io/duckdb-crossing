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
static constexpr const char *CROSSING_CONTROL_BOUND = "BOUND";
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
