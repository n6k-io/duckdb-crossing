#include "internal/crossing_read.hpp"

#include "crossing_attach.hpp"
#include "internal/crossing_remote.hpp"
#include "internal/floor.hpp"
#include "internal/function_tag.hpp"
#include "internal/seam.hpp"
#include "internal/table_indices.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

namespace {

string TypeList(const vector<LogicalType> &types) {
	vector<string> names;
	for (auto &type : types) {
		names.push_back(type.ToString());
	}
	return StringUtil::Join(names, ", ");
}

bool PlanIsOrdered(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
		return true;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
		return !op.children.empty() && PlanIsOrdered(*op.children[0]);
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return PlanIsOrdered(*op.children[1]);
	default:
		return false;
	}
}

void CrossingReadFunc(ClientContext &, TableFunctionInput &data, DataChunk &) {
	throw InternalException("crossing: the scan of '%s' reached execution without the crossing pass",
	                        data.bind_data->Cast<CrossingBindData>().SourceTable());
}

optional_ptr<TableCatalogEntry> CrossingReadGetTableEntry(optional_ptr<const FunctionData> bind_data) {
	return &bind_data->Cast<CrossingBindData>().table;
}

InsertionOrderPreservingMap<string> CrossingReadToString(TableFunctionToStringInput &input) {
	if (!input.bind_data) {
		return InsertionOrderPreservingMap<string>();
	}
	return input.bind_data->Cast<CrossingBindData>().Params();
}

//! The host's catalog, unless this transaction wrote to another attach whose scans were folded in:
//! only that attach's session sees the writes.
Catalog &ReadingCatalog(ClientContext &context, CrossingBindData &bind_data) {
	auto &host = bind_data.table.ParentCatalog();
	auto modified = MetaTransaction::Get(context).ModifiedDatabase();
	if (!modified) {
		return host;
	}
	auto &modified_catalog = modified->GetCatalog();
	auto attach = CrossingAttach::TryOf(modified_catalog);
	if (!attach) {
		return host;
	}
	auto &written = attach->Source();
	for (auto &source : bind_data.fragment->folded_sources) {
		if (&source.get() == &written) {
			return modified_catalog;
		}
	}
	return host;
}

} // namespace

CrossingBindData::CrossingBindData(CrossingTableCatalogEntry &table_p, CrossingVerb verb_p, CrossingSeam seam_p,
                                   shared_ptr<CrossingFragment> fragment_p)
    : table(table_p), verb(verb_p), seam(std::move(seam_p)), fragment(std::move(fragment_p)) {
}

const CrossingIdentity &CrossingBindData::Identity() const {
	return table.source.Identity();
}

string CrossingBindData::QualifiedTable() const {
	return table.source_schema + "." + table.described.name;
}

InsertionOrderPreservingMap<string> CrossingBindData::Params() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = QualifiedTable();
	if (verb != CrossingVerb::SELECT) {
		result["Crossing"] = string(CrossingVerbName(verb)) + " on source";
	}
	if (fragment) {
		if (verb == CrossingVerb::SELECT) {
			result["Table Index"] = to_string(fragment->table_index.index);
			result["Plan Columns"] = to_string(fragment->output_types.size());
		}
		result["Plan"] = fragment->plan_text.empty() ? "(none)" : fragment->plan_text;
	}
	return result;
}

optional_ptr<CrossingBindData> CrossingBindDataOf(LogicalGet &get, const CrossingIdentity &identity) {
	if (!get.bind_data || !IsOwnCrossingFunction(get)) {
		return nullptr;
	}
	auto &data = get.bind_data->Cast<CrossingBindData>();
	if (&data.Identity() != &identity) {
		return nullptr;
	}
	return &data;
}

TableFunction CrossingReadFunction() {
	TableFunction function(CROSSING_READ_FUNCTION, {}, CrossingReadFunc);
	function.to_string = CrossingReadToString;
	function.get_table_entry = CrossingReadGetTableEntry;
	// Without this, column_ids is not the output-slot-to-source-column map the scan reads it as.
	// Filters stay off: crossing wants them as operators to fold, not as a TableFilterSet.
	function.projection_pushdown = true;
	return TagCrossingFunction(function);
}

shared_ptr<CrossingFragment> BuildScanFragment(unique_ptr<LogicalOperator> floor, const string &source_table,
                                               vector<string> column_names, vector<LogicalType> column_types) {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->column_names = std::move(column_names);
	fragment->column_types = std::move(column_types);

	idx_t next_index = 0;
	FreshTableIndex local = [&next_index]() {
		return TableIndex(next_index++);
	};
	RemapTableIndices(*floor, local);
	fragment->table_index = local();
	floor->ResolveOperatorTypes();
	fragment->floor_bindings = floor->GetColumnBindings();
	if (fragment->floor_bindings.size() != fragment->column_names.size()) {
		throw CatalogException("crossing: '%s' changed on the source: its scan produces %llu columns, the "
		                       "description taken at attach names %llu; refresh the attach",
		                       source_table, fragment->floor_bindings.size(), fragment->column_names.size());
	}
	if (floor->types != fragment->column_types) {
		throw CatalogException("crossing: '%s' changed on the source: its scan produces [%s], the description "
		                       "taken at attach says [%s]; refresh the attach",
		                       source_table, TypeList(floor->types), TypeList(fragment->column_types));
	}
	fragment->SealFloor(*floor);
	if (floor->type == LogicalOperatorType::LOGICAL_GET) {
		fragment->floor_get = floor->Cast<LogicalGet>();
	}
	fragment->floor = std::move(floor);

	vector<column_t> all_columns;
	for (idx_t i = 0; i < fragment->column_names.size(); i++) {
		all_columns.push_back(i);
	}
	fragment->RebuildPlanForColumns(all_columns);
	return fragment;
}

unique_ptr<FunctionData> MakeReadBindData(CrossingTableCatalogEntry &entry) {
	auto floor = MakeFloorNode(0, entry.source_schema, entry.described.name, entry.described.column_names,
	                           entry.described.column_types, entry.described.catalog);
	auto fragment = BuildScanFragment(std::move(floor), entry.described.name, entry.described.column_names,
	                                  entry.described.column_types);
	return make_uniq<CrossingBindData>(entry, CrossingVerb::SELECT, CrossingSeam(), std::move(fragment));
}

LogicalCrossingRead::LogicalCrossingRead(TableIndex table_index_p, vector<ColumnIndex> column_ids_p,
                                         vector<LogicalType> output_types_p, unique_ptr<CrossingBindData> bind_data_p)
    : table_index(table_index_p), column_ids(std::move(column_ids_p)), output_types(std::move(output_types_p)),
      bind_data(std::move(bind_data_p)) {
}

PhysicalOperator &LogicalCrossingRead::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &fragment = *bind_data->fragment;
	auto &source_table = bind_data->SourceTable();
	if (!fragment.plan) {
		throw InternalException("crossing: the scan of '%s' carries no plan", source_table);
	}
	CrossingRemoteColumns columns;
	columns.received = fragment.output_types;
	for (auto &column : column_ids) {
		auto id = column.GetPrimaryIndex();
		auto position = CrossingRemoteColumns::NULL_COLUMN;
		for (idx_t p = 0; p < fragment.projected_columns.size(); p++) {
			if (fragment.projected_columns[p] == id) {
				position = p;
				break;
			}
		}
		columns.positions.push_back(position);
	}
	auto &catalog = ReadingCatalog(context, *bind_data);
	auto &attach = CrossingAttach::Of(catalog);
	auto statement = attach.RemoteStatement(context, Transaction::Get(context, catalog));
	bind_data->table.source.Planned(*fragment.plan);

	CrossingRemoteSlice slice;
	slice.id = statement->Remote().NextSliceId();
	slice.ordered = PlanIsOrdered(*fragment.plan) || Settings::Get<PreserveInsertionOrderSetting>(context);
	slice.borrowed = fragment.plan.get();
	slice.keep = bind_data->fragment;
	optional_ptr<PhysicalOperator> feed;
	unique_ptr<CrossingRemoteSeamSink> seam;
	optional_idx estimate;
	if (!children.empty()) {
		feed = planner.CreatePlan(*children[0]);
		seam = CrossingSeamSinkOf(context, *feed);
		estimate = feed->estimated_cardinality;
	}
	return PlanCrossingRemote(context, planner, types, std::move(statement), std::move(slice), std::move(columns),
	                          std::move(seam), feed, estimate, estimated_cardinality);
}

vector<ColumnBinding> LogicalCrossingRead::GetColumnBindings() {
	return GenerateColumnBindings(table_index, column_ids.size());
}

vector<TableIndex> LogicalCrossingRead::GetTableIndex() const {
	return {table_index};
}

string LogicalCrossingRead::GetName() const {
	return "CROSSING_READ";
}

InsertionOrderPreservingMap<string> LogicalCrossingRead::ParamsToString() const {
	return bind_data ? bind_data->Params() : InsertionOrderPreservingMap<string>();
}

string LogicalCrossingRead::GetExtensionName() const {
	return "crossing";
}

void LogicalCrossingRead::Serialize(Serializer &) const {
	throw NotImplementedException("crossing: a read node is planned, never serialized");
}

void LogicalCrossingRead::ResolveTypes() {
	types = output_types;
}

} // namespace duckdb
