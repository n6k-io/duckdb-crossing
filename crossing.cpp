#include "crossing.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

const char *CrossingVerbName(CrossingVerb verb) {
	switch (verb) {
	case CrossingVerb::SELECT:
		return "select";
	case CrossingVerb::INSERT:
		return "insert";
	case CrossingVerb::UPDATE:
		return "update";
	case CrossingVerb::DELETE_:
		return "delete";
	default:
		throw InternalException("crossing: unknown verb %d", static_cast<int>(verb));
	}
}

vector<string> CrossingSource::Schemas() {
	return {"main"};
}

CrossingVerdict CrossingSource::AcceptsCall(const Expression &) {
	return CrossingVerdict::No("this source computes nothing");
}

CrossingVerdict CrossingSource::AcceptsType(const LogicalType &) {
	return CrossingVerdict::No("this source holds only what it was described with");
}

void CrossingSource::Detach(ClientContext &) {
}

CrossingWriter CrossingSession::Write(ClientContext &, const CrossingQuery &) {
	throw NotImplementedException("crossing: this source is read-only");
}

void CrossingSession::Commit() {
}

void CrossingSession::Rollback() {
}

} // namespace duckdb
