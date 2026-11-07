#include "crossing.hpp"
#include "internal/names.hpp"

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
	auto names = NamesOf(view.names);
	for (idx_t i = 0; i < view.aliases.size() && i < names.size(); i++) {
		names[i] = view.aliases[i].GetIdentifierName();
	}
	return names;
}

void CrossingSource::DescribeChanged(const string &, CrossingTable &table) {
	table.returning = true;
	table.verbs = {CrossingVerb::SELECT, CrossingVerb::DROP};
	if (table.row_ids) {
		table.verbs.insert(table.verbs.end(),
		                   {CrossingVerb::INSERT, CrossingVerb::UPDATE, CrossingVerb::DELETE_, CrossingVerb::ALTER});
	}
}

const CrossingIdentity &CrossingDefaultIdentity() {
	static const CrossingIdentity identity;
	return identity;
}

} // namespace duckdb
