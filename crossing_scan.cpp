#include "internal/crossing_scan.hpp"

#include "internal/crossing_read.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

namespace {

void CrossingScanFunc(ClientContext &, TableFunctionInput &data_p, DataChunk &) {
	throw InternalException("crossing: a scan of '%s' reached execution without the crossing pass",
	                        data_p.bind_data->Cast<CrossingScanBindData>().source_table);
}

} // namespace

BindInfo CrossingScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<CrossingScanBindData>();
	if (!data.table) {
		throw InternalException("crossing: a scan with no table entry behind it");
	}
	return BindInfo(*data.table.get_mutable());
}

InsertionOrderPreservingMap<string> CrossingScanToString(TableFunctionToStringInput &input) {
	if (!input.bind_data) {
		return InsertionOrderPreservingMap<string>();
	}
	return CrossingScanParams(input.bind_data->Cast<CrossingScanBindData>());
}

TableFunction CrossingScanFunction() {
	TableFunction function(CROSSING_SCAN_FUNCTION, {}, CrossingScanFunc);
	function.to_string = CrossingScanToString;
	function.get_bind_info = CrossingScanGetBindInfo;
	// Without this, column_ids is not the output-slot-to-source-column map the scan reads it as.
	// Filters stay off: crossing wants them as operators to fold, not as a TableFilterSet.
	function.projection_pushdown = true;
	return function;
}

unique_ptr<FunctionData> MakeCrossingScanBindData(CrossingTableCatalogEntry &entry, CrossingSource &source) {
	auto bind_data = make_uniq<CrossingScanBindData>();
	bind_data->source = &source;
	bind_data->source_schema = entry.source_schema;
	bind_data->source_table = entry.described.name;
	bind_data->column_names = entry.described.column_names;
	bind_data->column_types = entry.described.column_types;
	bind_data->table = &entry;

	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->column_names = bind_data->column_names;
	fragment->column_types = bind_data->column_types;

	CrossingPlanRequest request;
	request.verb = CrossingVerb::SELECT;
	request.schema = bind_data->source_schema;
	request.table = bind_data->source_table;
	auto planned = source.Plan(request);
	if (!planned.plan) {
		throw NotImplementedException("crossing: the source has no scan of '%s': %s", bind_data->source_table,
		                              planned.declined.empty() ? "declined" : planned.declined);
	}
	auto floor = std::move(planned.plan);
	idx_t next_index = 0;
	FreshTableIndex local = [&next_index]() {
		return next_index++;
	};
	RemapTableIndices(*floor, local);
	fragment->table_index = local();
	floor->ResolveOperatorTypes();
	fragment->floor_bindings = floor->GetColumnBindings();
	if (fragment->floor_bindings.size() != fragment->column_names.size()) {
		throw CatalogException("crossing: '%s' changed on the source: its scan produces %llu columns, the "
		                       "description taken at attach names %llu; refresh the attach",
		                       bind_data->source_table, fragment->floor_bindings.size(), fragment->column_names.size());
	}
	fragment->SealFloor(*floor);
	fragment->floor = std::move(floor);

	vector<column_t> all_columns;
	for (idx_t i = 0; i < fragment->column_names.size(); i++) {
		all_columns.push_back(i);
	}
	fragment->RebuildPlanForColumns(all_columns);

	bind_data->fragment = std::move(fragment);
	return std::move(bind_data);
}

} // namespace duckdb
