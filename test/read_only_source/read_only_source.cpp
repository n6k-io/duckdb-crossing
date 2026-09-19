#include "read_only_source/read_only_source.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace readonly {

CrossingScan ReadOnlySource::Session::Read(ClientContext &, const CrossingQuery &) {
	CrossingScan scan;
	scan.open = [](ClientContext &, idx_t) -> CrossingReader {
		return [](ClientContext &, DataChunk &, CrossingWaker) {
			return CrossingReadResult::Done();
		};
	};
	return scan;
}

vector<string> ReadOnlySource::Tables(const string &) {
	return {"t", "w", "a"};
}

CrossingTable ReadOnlySource::Describe(const string &, const string &name) {
	CrossingTable table;
	table.name = name;
	table.Column("id", LogicalType::INTEGER);
	table.verbs = {CrossingVerb::SELECT};
	if (name == "w") {
		table.verbs.push_back(CrossingVerb::INSERT);
	}
	if (name == "a") {
		table.verbs.push_back(CrossingVerb::ALTER);
	}
	return table;
}

CrossingPlan ReadOnlySource::Plan(const CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		return CrossingPlan::Of(MakeFloorNode(0, request.schema, request.table, {"id"}, {LogicalType::INTEGER}));
	}
	return CrossingPlan::Of(MakeSeamNode(0, request.seam.types));
}

unique_ptr<ReadOnlySource::Session> ReadOnlySource::Begin(ClientContext &) {
	return make_uniq<Session>();
}

void ReadOnlySource::Register(ExtensionLoader &loader) {
	Crossing<ReadOnlySource>::Register(loader, "rodb",
	                                   [](ClientContext &, AttachInfo &) { return make_uniq<ReadOnlySource>(); });
}

} // namespace readonly
