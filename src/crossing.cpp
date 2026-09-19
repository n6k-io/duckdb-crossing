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

const vector<CrossingVerb> &CrossingVerbs() {
	static const vector<CrossingVerb> verbs {CrossingVerb::SELECT, CrossingVerb::INSERT, CrossingVerb::UPDATE,
	                                         CrossingVerb::DELETE_};
	return verbs;
}

CrossingWriter CrossingSession::Write(ClientContext &, const CrossingQuery &) {
	throw NotImplementedException("crossing: this source's session has no Write");
}

} // namespace duckdb
