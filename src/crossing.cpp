#include "crossing.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
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
	case CrossingVerb::CREATE:
		return "create";
	case CrossingVerb::ALTER:
		return "alter";
	case CrossingVerb::DROP:
		return "drop";
	default:
		throw InternalException("crossing: unknown verb %d", static_cast<int>(verb));
	}
}

const vector<CrossingVerb> &CrossingVerbs() {
	static const vector<CrossingVerb> verbs {CrossingVerb::SELECT,  CrossingVerb::INSERT, CrossingVerb::UPDATE,
	                                         CrossingVerb::DELETE_, CrossingVerb::CREATE, CrossingVerb::ALTER,
	                                         CrossingVerb::DROP};
	return verbs;
}

bool IsDdlVerb(CrossingVerb verb) {
	return verb == CrossingVerb::CREATE || verb == CrossingVerb::ALTER || verb == CrossingVerb::DROP;
}

vector<string> CrossingViewColumnNames(const CreateViewInfo &view) {
	auto names = view.names;
	for (idx_t i = 0; i < view.aliases.size() && i < names.size(); i++) {
		names[i] = view.aliases[i];
	}
	return names;
}

CrossingWriter CrossingSession::Write(ClientContext &, const CrossingQuery &) {
	throw NotImplementedException("crossing: this source's session has no Write");
}

void CrossingSession::Ddl(ClientContext &, const CrossingDdl &) {
	throw NotImplementedException("crossing: this source's session has no Ddl");
}

} // namespace duckdb
