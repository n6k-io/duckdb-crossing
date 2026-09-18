#include "internal/crossing_read.hpp"

#include "crossing_attach.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/parking.hpp"

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

} // namespace

InsertionOrderPreservingMap<string> CrossingScanParams(const CrossingScanBindData &bind_data) {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = bind_data.source_schema + "." + bind_data.source_table;
	if (bind_data.fragment) {
		result["Table Index"] = to_string(bind_data.fragment->table_index);
		result["Plan Columns"] = to_string(bind_data.fragment->output_types.size());
		result["Plan"] = bind_data.fragment->plan_text.empty() ? "(none)" : bind_data.fragment->plan_text;
	}
	return result;
}

LogicalCrossingRead::LogicalCrossingRead(idx_t table_index_p, vector<ColumnIndex> column_ids_p,
                                         vector<LogicalType> output_types_p,
                                         unique_ptr<CrossingScanBindData> bind_data_p)
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
	return "CROSSING_TABLE_SCAN";
}

InsertionOrderPreservingMap<string> LogicalCrossingRead::ParamsToString() const {
	return bind_data ? CrossingScanParams(*bind_data) : InsertionOrderPreservingMap<string>();
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
                           unique_ptr<CrossingScanBindData> bind_data_p, vector<column_t> column_ids_p,
                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)) {
}

unique_ptr<GlobalSourceState> CrossingRead::GetGlobalSourceState(ClientContext &context) const {
	if (!bind_data->source) {
		throw InternalException("crossing: scan of '%s' has no source", bind_data->source_table);
	}
	if (!bind_data->fragment) {
		throw InternalException("crossing: scan of '%s' has no fragment", bind_data->source_table);
	}
	auto &emitted = bind_data->fragment->output_types;

	auto state = make_uniq<CrossingReadGlobalState>();
	state->column_ids = column_ids;
	state->source_column_count = emitted.size();

	auto &projected = bind_data->fragment->projected_columns;
	for (auto column : state->column_ids) {
		if (column != COLUMN_IDENTIFIER_ROW_ID && ColumnIndex(column).IsVirtualColumn()) {
			throw InternalException("crossing: scan of '%s' was asked for a key alias the pass did not "
			                        "resolve",
			                        bind_data->source_table);
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
	if (!bind_data->fragment->plan) {
		throw InternalException("crossing: scan of '%s' carries no plan", bind_data->source_table);
	}
	state->query = make_uniq<CrossingQuery>(CrossingVerb::SELECT, *bind_data->fragment->plan);
	state->query->types = emitted;
	state->query->ordered = PlanIsOrdered(*bind_data->fragment->plan);
	state->query->tables = CrossingTablesOf(*bind_data->fragment->plan);

	auto &write_catalog = bind_data->table.get_mutable()->ParentCatalog();
	auto &session = CrossingAttach::Of(write_catalog).Session(context, Transaction::Get(context, write_catalog));
	state->scan = session.Read(context, *state->query);
	if (!state->scan.open) {
		throw InternalException("crossing: the source returned no scan for '%s'", bind_data->source_table);
	}
	state->partitions = state->scan.partitions;
	if (state->partitions == 0) {
		throw InvalidInputException("crossing: the scan of '%s' offers no partitions", bind_data->source_table);
	}
	if (state->query->ordered && state->partitions > 1) {
		throw InvalidInputException("crossing: the scan of '%s' is ordered but the source offered %llu partitions; "
		                            "an ordered scan must come back as one",
		                            bind_data->source_table, state->partitions);
	}
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
	auto &state = input.global_state.Cast<CrossingReadGlobalState>();
	auto &local = input.local_state.Cast<CrossingReadLocalState>();
	while (true) {
		if (!local.reader) {
			auto claimed = state.next_partition.fetch_add(1);
			if (claimed >= state.partitions) {
				return SourceResultType::FINISHED;
			}
			local.reader = state.scan.open(context.client, claimed);
			if (!local.reader) {
				throw InternalException("crossing: the scan of '%s' opened no reader for partition %llu",
				                        bind_data->source_table, claimed);
			}
		}
		auto parking = CrossingParking::Of(input.interrupt_state);
		local.source_chunk.Reset();
		auto pull = local.reader(context.client, local.source_chunk, parking->Waker());
		if (pull.outcome == CrossingPull::Outcome::WAIT) {
			auto guard = state.Lock();
			if (parking->Park(state, guard)) {
				return SourceResultType::BLOCKED;
			}
			if (!state.CanBlock(guard)) {
				local.reader = nullptr;
				return SourceResultType::FINISHED;
			}
			continue;
		}
		if (pull.outcome == CrossingPull::Outcome::DONE || local.source_chunk.size() == 0) {
			local.reader = nullptr;
			state.finished_partitions++;
			continue;
		}
		auto &expected = state.query->types;
		auto produced = local.source_chunk.GetTypes();
		if (produced != expected) {
			throw InvalidInputException("crossing: the reader for '%s' filled a chunk of types [%s], the "
			                            "query wants [%s]",
			                            bind_data->source_table, TypeList(produced), TypeList(expected));
		}
		EmitChunk(state, local.source_chunk, output);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
}

ProgressData CrossingRead::GetProgress(ClientContext &, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<CrossingReadGlobalState>();
	ProgressData progress;
	progress.done = static_cast<double>(state.finished_partitions.load());
	progress.total = static_cast<double>(state.partitions);
	return progress;
}

string CrossingRead::GetName() const {
	return "CROSSING_TABLE_SCAN";
}

InsertionOrderPreservingMap<string> CrossingRead::ParamsToString() const {
	return CrossingScanParams(*bind_data);
}

} // namespace duckdb
