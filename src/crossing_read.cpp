#include "internal/crossing_read.hpp"

#include "crossing_attach.hpp"
#include "internal/parking.hpp"
#include "internal/table_indices.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
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

void EmitRowIds(Vector &rowid_vec, DataChunk &source_chunk, CrossingReadGlobalState &state) {
	auto row_ids = FlatVector::GetData<row_t>(rowid_vec);
	auto first = state.rows_emitted.fetch_add(source_chunk.size());
	for (idx_t i = 0; i < source_chunk.size(); i++) {
		row_ids[i] = static_cast<row_t>(first + i);
	}
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
	default:
		return false;
	}
}

void EmitChunk(CrossingReadGlobalState &state, DataChunk &source_chunk, DataChunk &output) {
	output.SetCardinality(source_chunk.size());
	for (idx_t idx = 0; idx < state.column_ids.size(); idx++) {
		if (state.column_ids[idx] == COLUMN_IDENTIFIER_ROW_ID) {
			EmitRowIds(output.data[idx], source_chunk, state);
			continue;
		}
		auto position = state.source_position[idx];
		if (position >= state.source_column_count) {
			output.data[idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(output.data[idx], true);
			continue;
		}
		output.data[idx].Reference(source_chunk.data[position]);
	}
}

void CrossingReadFunc(ClientContext &, TableFunctionInput &data, DataChunk &) {
	throw InternalException("crossing: the scan of '%s' reached execution without the crossing pass",
	                        data.bind_data->Cast<CrossingBindData>().SourceTable());
}

BindInfo CrossingReadGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	return BindInfo(bind_data->Cast<CrossingBindData>().table);
}

InsertionOrderPreservingMap<string> CrossingReadToString(TableFunctionToStringInput &input) {
	if (!input.bind_data) {
		return InsertionOrderPreservingMap<string>();
	}
	return input.bind_data->Cast<CrossingBindData>().Params();
}

unique_ptr<CrossingReadGlobalState> PrepareReadState(const CrossingFragment &fragment, const string &source_table,
                                                     const vector<column_t> &column_ids) {
	auto &emitted = fragment.output_types;

	auto state = make_uniq<CrossingReadGlobalState>();
	state->column_ids = column_ids;
	state->source_column_count = emitted.size();

	auto &projected = fragment.projected_columns;
	for (auto column : state->column_ids) {
		if (column != COLUMN_IDENTIFIER_ROW_ID && ColumnIndex(column).IsVirtualColumn()) {
			throw InternalException("crossing: the scan of '%s' was asked for a key alias the pass did not resolve",
			                        source_table);
		}
		idx_t position = state->source_column_count;
		for (idx_t p = 0; p < projected.size(); p++) {
			if (projected[p] == column) {
				position = p;
				break;
			}
		}
		state->source_position.push_back(position);
	}
	if (!fragment.plan) {
		throw InternalException("crossing: the scan of '%s' carries no plan", source_table);
	}
	state->query = make_uniq<CrossingQuery>(CrossingVerb::SELECT, *fragment.plan);
	state->query->types = emitted;
	state->query->ordered = PlanIsOrdered(*fragment.plan);
	state->query->tables = CrossingTablesOf(*fragment.plan);
	return state;
}

void CheckScanPartitions(const CrossingReadGlobalState &state, const string &source_table) {
	if (!state.scan.open) {
		throw InternalException("crossing: the source returned no scan of '%s'", source_table);
	}
	if (state.scan.partitions == 0) {
		throw InvalidInputException("crossing: the scan of '%s' offers no partitions", source_table);
	}
	if (state.query->ordered && state.scan.partitions > 1) {
		throw InvalidInputException("crossing: the scan of '%s' is ordered but the source offered %llu partitions; "
		                            "an ordered scan must come back as one",
		                            source_table, state.scan.partitions);
	}
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
			result["Table Index"] = to_string(fragment->table_index);
			result["Plan Columns"] = to_string(fragment->output_types.size());
		}
		result["Plan"] = fragment->plan_text.empty() ? "(none)" : fragment->plan_text;
	}
	return result;
}

optional_ptr<CrossingBindData> CrossingBindDataOf(LogicalGet &get, const CrossingIdentity &identity) {
	auto data = dynamic_cast<CrossingBindData *>(get.bind_data.get());
	if (!data || &data->Identity() != &identity) {
		return nullptr;
	}
	return data;
}

TableFunction CrossingReadFunction() {
	TableFunction function(CROSSING_READ_FUNCTION, {}, CrossingReadFunc);
	function.to_string = CrossingReadToString;
	function.get_bind_info = CrossingReadGetBindInfo;
	// Without this, column_ids is not the output-slot-to-source-column map the scan reads it as.
	// Filters stay off: crossing wants them as operators to fold, not as a TableFilterSet.
	function.projection_pushdown = true;
	return function;
}

shared_ptr<CrossingFragment> BuildScanFragment(unique_ptr<LogicalOperator> floor, const string &source_table,
                                               vector<string> column_names, vector<LogicalType> column_types) {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->column_names = std::move(column_names);
	fragment->column_types = std::move(column_types);

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
		                       source_table, fragment->floor_bindings.size(), fragment->column_names.size());
	}
	if (floor->types != fragment->column_types) {
		throw CatalogException("crossing: '%s' changed on the source: its scan produces [%s], the description "
		                       "taken at attach says [%s]; refresh the attach",
		                       source_table, TypeList(floor->types), TypeList(fragment->column_types));
	}
	fragment->SealFloor(*floor);
	fragment->floor = std::move(floor);

	vector<column_t> all_columns;
	for (idx_t i = 0; i < fragment->column_names.size(); i++) {
		all_columns.push_back(i);
	}
	fragment->RebuildPlanForColumns(all_columns);
	return fragment;
}

unique_ptr<LogicalOperator> RequirePlan(CrossingPlan planned, CrossingVerb verb, const string &table) {
	if (planned.plan) {
		return std::move(planned.plan);
	}
	auto &reason = planned.verdict.reason;
	throw NotImplementedException("crossing: the source has no %s of '%s': %s",
	                              verb == CrossingVerb::SELECT ? "scan" : CrossingVerbName(verb), table,
	                              reason.empty() ? "declined" : reason);
}

unique_ptr<FunctionData> MakeReadBindData(CrossingTableCatalogEntry &entry) {
	CrossingPlanRequest request;
	request.verb = CrossingVerb::SELECT;
	request.schema = entry.source_schema;
	request.table = entry.described.name;
	auto floor = RequirePlan(entry.source.Plan(request), CrossingVerb::SELECT, entry.described.name);
	auto fragment = BuildScanFragment(std::move(floor), entry.described.name, entry.described.column_names,
	                                  entry.described.column_types);
	return make_uniq<CrossingBindData>(entry, CrossingVerb::SELECT, CrossingSeam(), std::move(fragment));
}

LogicalCrossingRead::LogicalCrossingRead(idx_t table_index_p, vector<ColumnIndex> column_ids_p,
                                         vector<LogicalType> output_types_p, unique_ptr<CrossingBindData> bind_data_p)
    : table_index(table_index_p), column_ids(std::move(column_ids_p)), output_types(std::move(output_types_p)),
      bind_data(std::move(bind_data_p)) {
}

PhysicalOperator &LogicalCrossingRead::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	vector<column_t> ids;
	for (auto &column : column_ids) {
		ids.push_back(column.GetPrimaryIndex());
	}
	return planner.Make<CrossingRead>(types, std::move(bind_data), std::move(ids), estimated_cardinality);
}

vector<ColumnBinding> LogicalCrossingRead::GetColumnBindings() {
	return GenerateColumnBindings(table_index, column_ids.size());
}

vector<idx_t> LogicalCrossingRead::GetTableIndex() const {
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

CrossingRead::CrossingRead(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                           unique_ptr<CrossingBindData> bind_data_p, vector<column_t> column_ids_p,
                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)) {
}

unique_ptr<GlobalSourceState> CrossingRead::GetGlobalSourceState(ClientContext &context) const {
	if (!bind_data->fragment) {
		throw InternalException("crossing: the scan of '%s' has no fragment", bind_data->SourceTable());
	}
	auto state = PrepareReadState(*bind_data->fragment, bind_data->SourceTable(), column_ids);

	auto &catalog = bind_data->table.ParentCatalog();
	auto &session = CrossingAttach::Of(catalog).Session(context, Transaction::Get(context, catalog));
	state->scan = session.Read(context, *state->query);
	CheckScanPartitions(*state, bind_data->SourceTable());
	return std::move(state);
}

unique_ptr<LocalSourceState> CrossingRead::GetLocalSourceState(ExecutionContext &, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<CrossingReadGlobalState>();
	auto local = make_uniq<CrossingReadLocalState>();
	local->source_chunk.Initialize(Allocator::DefaultAllocator(), state.query->types);
	return std::move(local);
}

SourceResultType CrossingRead::GetDataInternal(ExecutionContext &context, DataChunk &output,
                                               OperatorSourceInput &input) const {
	auto &source_table = bind_data->SourceTable();
	auto &state = input.global_state.Cast<CrossingReadGlobalState>();
	auto &local = input.local_state.Cast<CrossingReadLocalState>();
	while (true) {
		if (!local.reader) {
			auto claimed = state.next_partition.fetch_add(1);
			if (claimed >= state.scan.partitions) {
				return SourceResultType::FINISHED;
			}
			local.reader = state.scan.open(context.client, claimed);
			if (!local.reader) {
				throw InternalException("crossing: the scan of '%s' opened no reader for partition %llu", source_table,
				                        claimed);
			}
		}
		auto parking = CrossingParking::Of(input.interrupt_state);
		local.source_chunk.Reset();
		auto pulled = local.reader(context.client, local.source_chunk, parking->Waker());
		if (pulled.outcome == CrossingReadResult::Outcome::WAIT) {
			SourceResultType result;
			if (ParkSource(*parking, state, result)) {
				if (result == SourceResultType::FINISHED) {
					local.reader = nullptr;
				}
				return result;
			}
			continue;
		}
		if (pulled.outcome == CrossingReadResult::Outcome::DONE || local.source_chunk.size() == 0) {
			local.reader = nullptr;
			state.finished_partitions++;
			continue;
		}
		auto &expected = state.query->types;
		auto produced = local.source_chunk.GetTypes();
		if (produced != expected) {
			throw InvalidInputException("crossing: the reader for '%s' filled a chunk of types [%s], the "
			                            "query wants [%s]",
			                            source_table, TypeList(produced), TypeList(expected));
		}
		EmitChunk(state, local.source_chunk, output);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
}

ProgressData CrossingRead::GetProgress(ClientContext &, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<CrossingReadGlobalState>();
	ProgressData progress;
	progress.done = static_cast<double>(state.finished_partitions.load());
	progress.total = static_cast<double>(state.scan.partitions);
	return progress;
}

string CrossingRead::GetName() const {
	return "CROSSING_READ";
}

InsertionOrderPreservingMap<string> CrossingRead::ParamsToString() const {
	return bind_data->Params();
}

bool ParkSource(CrossingParking &parking, GlobalSourceState &state, SourceResultType &result) {
	auto guard = state.Lock();
	if (parking.Park(state, guard)) {
		result = SourceResultType::BLOCKED;
		return true;
	}
	if (!state.CanBlock(guard)) {
		result = SourceResultType::FINISHED;
		return true;
	}
	return false;
}

} // namespace duckdb
