#include "internal/crossing_write.hpp"

#include "internal/crossing_remote.hpp"
#include "internal/fill.hpp"
#include "internal/key_check.hpp"
#include "internal/parking.hpp"
#include "internal/seam.hpp"
#include "internal/shaped_write.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

namespace {

InternalException NoSchemasHere() {
	return InternalException("crossing: the write catalog holds no schemas");
}

NotImplementedException PassDidNotRun(TableCatalogEntry &table) {
	return NotImplementedException("crossing: the write to '%s' needs the crossing optimizer pass",
	                               table.name.GetIdentifierName());
}

void CrossingWriteFunc(ClientContext &, TableFunctionInput &data, DataChunk &) {
	throw InternalException("crossing: the write to '%s' reached execution without the crossing pass",
	                        data.bind_data->Cast<CrossingBindData>().SourceTable());
}

InsertionOrderPreservingMap<string> CrossingWriteToString(TableFunctionToStringInput &input) {
	if (!input.bind_data) {
		return InsertionOrderPreservingMap<string>();
	}
	return input.bind_data->Cast<CrossingBindData>().Params();
}

CrossingWriteNode WriteNodeOf(CrossingTableCatalogEntry &table, CrossingVerb verb, const CrossingSeam &seam) {
	CrossingWriteNode node;
	node.verb = verb;
	node.schema = table.source_schema;
	node.table = table.described.name;
	node.key_columns = seam.key_columns;
	node.set_columns = seam.set_columns;
	node.returning_columns = seam.returning_columns;
	node.returning_types = seam.returning_types;
	return node;
}

bool FeedIsBareSeam(const LogicalOperator &write_node) {
	optional_ptr<const LogicalOperator> node = &write_node;
	while (true) {
		if (node->children.size() != 1) {
			return false;
		}
		node = node->children[0].get();
		if (node->type == LogicalOperatorType::LOGICAL_GET) {
			return node->Cast<LogicalGet>().function.GetName() == CROSSING_SEAM_FUNCTION;
		}
		if (node->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return false;
		}
	}
}

struct CrossingNearResult {
	unique_ptr<CrossingSeamFilledWrite> write;
	bool done = false;
	idx_t affected_rows = 0;
	unique_ptr<ColumnDataCollection> returned;

	void Finish(const CrossingWritePull &pull) {
		affected_rows = pull.affected_rows;
		returned = write->TakeReturned();
		write.reset();
		done = true;
	}
};

struct CrossingNearSinkState : public GlobalSinkState {
	unique_ptr<ColumnDataCollection> rows;
	value_set_t seen_keys;
	idx_t keys_sent = 0;
	CrossingNearResult result;
};

struct CrossingNearSourceState : public GlobalSourceState {
	CrossingNearResult result;
	bool emitted = false;
	bool scanning = false;
	ColumnDataScanState scan;
	DataChunk scanned;
};

void SeeKey(value_set_t &seen_keys, DataChunk &chunk, const vector<idx_t> &key_positions, idx_t index) {
	child_list_t<Value> key;
	for (idx_t k = 0; k < key_positions.size(); k++) {
		key.emplace_back(to_string(k), chunk.GetValue(key_positions[k], index));
	}
	seen_keys.insert(Value::STRUCT(std::move(key)));
}

struct CrossingWriteSourceState : public GlobalSourceState {
	bool done = false;
};

constexpr idx_t REMOTE_TARGET_INDEX = idx_t(1) << 40;

//! A write as far's statement: the rows come from the fragment's feed, and keys match its first columns.
CrossingRemoteSlice RemoteWriteSlice(CrossingRemoteStatement &statement, CrossingBindData &bind_data) {
	auto &fragment = bind_data.fragment;
	auto &plan = *fragment->plan;
	auto &node = *WriteOf(plan);
	auto write = make_uniq<SubstraitWrite>();
	write->verb = node.verb;
	write->catalog = node.catalog;
	write->schema = node.schema;
	write->table = node.table;
	write->target_columns = TableColumnNames(bind_data.table);
	write->target_index = REMOTE_TARGET_INDEX;
	write->set_columns = node.set_columns;
	write->returning = node.returning_columns;
	write->returning_types = node.returning_types;

	CrossingRemoteSlice slice;
	slice.id = statement.Remote().NextSliceId();
	slice.keep = fragment;
	vector<ColumnBinding> bindings;
	vector<LogicalType> feed_types;
	if (!plan.children.empty()) {
		auto &feed = *plan.children[0];
		slice.borrowed = &feed;
		feed.ResolveOperatorTypes();
		bindings = feed.GetColumnBindings();
		feed_types = feed.types;
	}
	if (bind_data.shaped) {
		auto &shaped = *bind_data.shaped;
		write->target_index = SHAPED_TARGET_INDEX;
		write->source_index = SHAPED_SOURCE_INDEX;
		if (shaped.condition) {
			slice.owned.push_back(shaped.condition->Copy());
			write->condition = slice.owned.back().get();
		}
		for (auto &value : shaped.values) {
			slice.owned.push_back(value->Copy());
			write->values.push_back(*slice.owned.back());
		}
		slice.write = std::move(write);
		return slice;
	}
	unique_ptr<Expression> condition;
	if (node.verb != CrossingVerb::INSERT) {
		auto key_types = ColumnTypesByName(bind_data.table, node.key_columns);
		for (idx_t k = 0; k < node.key_columns.size(); k++) {
			auto column = bind_data.table.GetColumns().GetColumn(Identifier(node.key_columns[k])).Logical().index;
			auto left = make_uniq<BoundColumnRefExpression>(
			    key_types[k], ColumnBinding(TableIndex(REMOTE_TARGET_INDEX), ProjectionIndex(column)));
			auto right = make_uniq<BoundColumnRefExpression>(feed_types[k], bindings[k]);
			auto same = BoundComparisonExpression::Create(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(left),
			                                              std::move(right));
			condition = condition ? make_uniq_base<Expression, BoundConjunctionExpression>(
			                            ExpressionType::CONJUNCTION_AND, std::move(condition), std::move(same))
			                      : std::move(same);
		}
		for (idx_t i = 0; i < node.set_columns.size(); i++) {
			auto position = node.key_columns.size() + i;
			slice.owned.push_back(make_uniq<BoundColumnRefExpression>(feed_types[position], bindings[position]));
			write->values.push_back(*slice.owned.back());
		}
	}
	if (condition) {
		slice.owned.push_back(std::move(condition));
		write->condition = slice.owned.back().get();
	}
	slice.write = std::move(write);
	return slice;
}

CrossingRemoteKeyCheck RemoteKeyCheck(const CrossingBindData &bind_data) {
	CrossingRemoteKeyCheck keys;
	keys.key_positions = bind_data.key_positions;
	keys.returning = !bind_data.seam.returning_columns.empty();
	keys.table = bind_data.SourceTable();
	keys.key_columns = bind_data.seam.key_columns;
	keys.verb = CrossingVerbName(bind_data.verb);
	return keys;
}

void TellPlanned(ClientContext &context, const CrossingBindData &bind_data) {
	auto &catalog = bind_data.table.ParentCatalog();
	auto &fragment = *bind_data.fragment;
	CrossingQuery query(*fragment.plan);
	query.types = bind_data.seam.returning_columns.empty() ? vector<LogicalType> {LogicalType::BIGINT}
	                                                       : bind_data.seam.returning_types;
	query.tables = CrossingTablesOf(*fragment.plan);
	CrossingAttach::Of(catalog).Session(context, Transaction::Get(context, catalog)).Planned(query);
}

//! `emit` says, for each output column, which of the columns far returns it is.
CrossingRemoteColumns RemoteWriteColumns(const CrossingBindData &bind_data, const vector<optional_idx> &emit) {
	CrossingRemoteColumns columns;
	columns.received = bind_data.seam.returning_columns.empty() ? vector<LogicalType> {LogicalType::BIGINT}
	                                                            : bind_data.seam.returning_types;
	for (auto &position : emit) {
		columns.positions.push_back(position.IsValid() ? position.GetIndex() : CrossingRemoteColumns::NULL_COLUMN);
	}
	return columns;
}

PhysicalOperator &PlanMergeWrite(PhysicalPlanGenerator &planner, LogicalMergeInto &op, ClientContext &context,
                                 CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
                                 vector<idx_t> key_positions) {
	auto types = op.types;
	vector<optional_idx> emit;
	if (op.return_chunk) {
		types.pop_back();
		seam.returning_columns = TableColumnNames(table);
		seam.returning_types = types;
		for (idx_t c = 0; c < types.size(); c++) {
			emit.emplace_back(c);
		}
	} else {
		emit.emplace_back(0);
	}
	auto fragment = PlanWriteFragment(table, verb, seam);
	auto bind_data = MakeWriteBindData(table, verb, std::move(seam), std::move(fragment));
	bind_data->key_positions = std::move(key_positions);
	auto &catalog = table.ParentCatalog();
	auto statement = CrossingAttach::Of(catalog).RemoteStatement(context, Transaction::Get(context, catalog));
	if (statement) {
		TellPlanned(context, *bind_data);
		auto columns = RemoteWriteColumns(*bind_data, emit);
		auto seam_sink = make_uniq<CrossingRemoteSeamSink>();
		seam_sink->types = bind_data->fragment->seam_types;
		seam_sink->keys = RemoteKeyCheck(*bind_data);
		seam_sink->awaits_result = !op.return_chunk;
		auto slice = RemoteWriteSlice(*statement, *bind_data);
		return PlanCrossingRemote(context, planner, std::move(types), std::move(statement), std::move(slice),
		                          std::move(columns), std::move(seam_sink), nullptr, optional_idx(),
		                          op.EstimateCardinality(context));
	}
	return planner.Make<CrossingNear>(std::move(types), std::move(emit), std::move(bind_data),
	                                  op.EstimateCardinality(context));
}

unique_ptr<MergeIntoOperator> PlanCrossingMergeAction(ClientContext &context, LogicalMergeInto &op,
                                                      PhysicalPlanGenerator &planner, BoundMergeIntoAction &action,
                                                      CrossingTableCatalogEntry &table) {
	auto result = make_uniq<MergeIntoOperator>();
	result->action_type = action.action_type;
	result->condition = std::move(action.condition);

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE:
	case MergeActionType::MERGE_DELETE: {
		auto verb = action.action_type == MergeActionType::MERGE_UPDATE ? CrossingVerb::UPDATE : CrossingVerb::DELETE_;
		RequireVerb(table, verb);
		auto seam = SeamOf(table, table.described, verb, SetColumnNames(table, action.columns));
		auto key_types = ColumnTypesByName(table, seam.key_columns);
		vector<idx_t> key_positions;
		for (idx_t k = 0; k < key_types.size(); k++) {
			result->expressions.push_back(RefAt(key_types[k], op.row_id_start + k));
			if (!table.described.key_unique) {
				key_positions.push_back(k);
			}
		}
		for (idx_t i = 0; i < action.expressions.size(); i++) {
			if (action.expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
				result->expressions.push_back(op.bound_defaults[action.columns[i].index]->Copy());
			} else {
				result->expressions.push_back(std::move(action.expressions[i]));
			}
		}
		result->op = PlanMergeWrite(planner, op, context, table, verb, std::move(seam), std::move(key_positions));
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		RequireVerb(table, CrossingVerb::INSERT);
		auto names = TableColumnNames(table);
		vector<string> named;
		for (idx_t c = 0; c < action.expressions.size(); c++) {
			if (!IsSourceDefault(*action.expressions[c])) {
				named.push_back(names[c]);
				result->expressions.push_back(std::move(action.expressions[c]));
			}
		}
		if (named.empty()) {
			throw BinderException("crossing: a MERGE into '%s' cannot insert DEFAULT VALUES; name at least one column",
			                      table.name.GetIdentifierName());
		}
		auto seam = SeamOf(table, table.described, CrossingVerb::INSERT, std::move(named));
		result->op = PlanMergeWrite(planner, op, context, table, CrossingVerb::INSERT, std::move(seam), {});
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw NotImplementedException("crossing: unsupported MERGE INTO action");
	}
	return result;
}

} // namespace

vector<LogicalType> ColumnTypesByName(TableCatalogEntry &table, const vector<string> &wanted) {
	vector<LogicalType> out;
	for (auto &want : wanted) {
		out.push_back(table.GetColumns().GetColumn(Identifier(want)).GetType());
	}
	return out;
}

vector<LogicalType> TableRowTypes(TableCatalogEntry &table) {
	vector<LogicalType> out;
	for (auto &column : table.GetColumns().Logical()) {
		out.push_back(column.GetType());
	}
	return out;
}

vector<string> TableColumnNames(TableCatalogEntry &table) {
	vector<string> out;
	for (auto &column : table.GetColumns().Logical()) {
		out.push_back(column.GetName().GetIdentifierName());
	}
	return out;
}

vector<string> SetColumnNames(TableCatalogEntry &table, const vector<PhysicalIndex> &columns) {
	vector<string> out;
	for (auto &column_index : columns) {
		out.push_back(table.GetColumns().GetColumn(column_index).GetName().GetIdentifierName());
	}
	return out;
}

unique_ptr<Expression> RefAt(const LogicalType &type, idx_t position) {
	return make_uniq<BoundReferenceExpression>(type, position);
}

optional_ptr<LogicalGet> FindGetWithIndex(LogicalOperator &op, TableIndex table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_GET && op.Cast<LogicalGet>().table_index == table_index) {
		return &op.Cast<LogicalGet>();
	}
	for (auto &child : op.children) {
		if (auto found = FindGetWithIndex(*child, table_index)) {
			return found;
		}
	}
	return nullptr;
}

CrossingSeam SeamOf(TableCatalogEntry &table, const CrossingTable &described, CrossingVerb verb,
                    vector<string> set_columns) {
	CrossingSeam seam;
	if (verb == CrossingVerb::INSERT) {
		seam.types = ColumnTypesByName(table, set_columns);
		seam.set_columns = std::move(set_columns);
		return seam;
	}
	seam.key_columns = described.key;
	seam.set_columns = std::move(set_columns);
	seam.types = ColumnTypesByName(table, seam.key_columns);
	for (auto &type : ColumnTypesByName(table, seam.set_columns)) {
		seam.types.push_back(type);
	}
	return seam;
}

void RequireVerb(CrossingTableCatalogEntry &table, CrossingVerb verb) {
	if (!table.described.Allows(verb)) {
		throw PermissionException("crossing: '%s' does not have '%s' permission", table.name.GetIdentifierName(),
		                          CrossingVerbName(verb));
	}
}

void RequireKey(TableCatalogEntry &table, const CrossingTable &described, const char *what) {
	if (described.key.empty()) {
		throw BinderException("crossing: '%s' has no key, so %s cannot address its rows",
		                      table.name.GetIdentifierName(), what);
	}
}

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam) {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->seam_types = seam.types;
	fragment->plan = MakeWriteNode(1, WriteNodeOf(table, verb, seam), MakeSeamNode(0, seam.types));
	fragment->ResolveTypesAndText();
	fragment->VerifyInvariants();
	return fragment;
}

shared_ptr<CrossingFragment> PlanBareWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                                   const CrossingSeam &seam, TableIndex table_index) {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->plan = MakeWriteNode(table_index.index, WriteNodeOf(table, verb, seam), nullptr);
	fragment->ResolveTypesAndText();
	return fragment;
}

unique_ptr<CrossingBindData> MakeWriteBindData(CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
                                               shared_ptr<CrossingFragment> fragment) {
	return make_uniq<CrossingBindData>(table, verb, std::move(seam), std::move(fragment));
}

TableFunction CrossingWriteFunction() {
	TableFunction function(CROSSING_WRITE_FUNCTION, {}, CrossingWriteFunc);
	function.to_string = CrossingWriteToString;
	function.projection_pushdown = false;
	return function;
}

CrossingSeamFilledWrite::CrossingSeamFilledWrite(const CrossingBindData &write, unique_ptr<ColumnDataCollection> rows)
    : table(write.table), fragment(write.fragment) {
	auto slot = fragment->SeamSlot();
	if (!slot && rows) {
		throw InternalException("crossing: rows were gathered for a seam a plan already fills");
	}
	// Over a bare seam no rows means no write. A join or an aggregate over one still has an answer.
	if (slot && (!rows || (rows->Count() == 0 && FeedIsBareSeam(*fragment->plan)))) {
		empty = true;
		return;
	}
	if (slot) {
		filling = make_uniq<CrossingSeamFilling>(fragment, std::move(rows));
	}
	query = make_uniq<CrossingQuery>(*fragment->plan);
	query->types = write.seam.returning_columns.empty() ? vector<LogicalType> {LogicalType::BIGINT}
	                                                    : write.seam.returning_types;
	returning = !write.seam.returning_columns.empty();
	query->tables = CrossingTablesOf(*fragment->plan);
}

CrossingSeamFilledWrite::~CrossingSeamFilledWrite() {
	reader = nullptr;
	scan = CrossingScan();
}

CrossingWritePull CrossingSeamFilledWrite::Pull(ClientContext &context, CrossingWaker waker) {
	CrossingWritePull pull;
	if (empty) {
		pull.done = true;
		return pull;
	}
	if (!opened) {
		auto &catalog = table.ParentCatalog();
		auto &session = CrossingAttach::Of(catalog).Session(context, Transaction::Get(context, catalog));
		scan = session.Run(context, *query);
		result_chunk.Initialize(Allocator::DefaultAllocator(), query->types);
		if (returning) {
			returned = make_uniq<ColumnDataCollection>(context, query->types);
		}
		opened = true;
	}
	while (true) {
		if (!reader) {
			if (next_partition >= scan.partitions) {
				pull.done = true;
				pull.affected_rows = affected_rows;
				return pull;
			}
			if (!scan.open) {
				throw InternalException("crossing: the source's write of '%s' opens no reader", table.described.name);
			}
			reader = scan.open(context, next_partition++);
			if (!reader) {
				throw InternalException("crossing: the source's write of '%s' opened no reader", table.described.name);
			}
		}
		result_chunk.Reset();
		auto result = reader(context, result_chunk, waker);
		if (result.outcome == CrossingReadResult::Outcome::WAIT) {
			return pull;
		}
		if (result.outcome == CrossingReadResult::Outcome::DONE || result_chunk.size() == 0) {
			reader = nullptr;
			continue;
		}
		if (result_chunk.GetTypes() != query->types) {
			throw InvalidInputException("crossing: the reader for the write of '%s' filled a chunk of other types "
			                            "than it was asked for",
			                            table.described.name);
		}
		result_chunk.SetChildCardinality(result_chunk.size());
		if (returning) {
			returned->Append(result_chunk);
			affected_rows += result_chunk.size();
			continue;
		}
		for (idx_t r = 0; r < result_chunk.size(); r++) {
			affected_rows += NumericCast<idx_t>(result_chunk.GetValue(0, r).GetValue<int64_t>());
		}
	}
}

LogicalCrossingNear::LogicalCrossingNear(TableIndex table_index_p, vector<ColumnBinding> bindings_p,
                                         vector<LogicalType> output_types_p, vector<optional_idx> emit_p,
                                         unique_ptr<CrossingBindData> bind_data_p)
    : table_index(table_index_p), bindings(std::move(bindings_p)), output_types(std::move(output_types_p)),
      emit(std::move(emit_p)), bind_data(std::move(bind_data_p)) {
}

PhysicalOperator &LogicalCrossingNear::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &catalog = bind_data->table.ParentCatalog();
	auto statement = CrossingAttach::Of(catalog).RemoteStatement(context, Transaction::Get(context, catalog));
	if (statement) {
		TellPlanned(context, *bind_data);
		optional_ptr<PhysicalOperator> feed;
		unique_ptr<CrossingRemoteSeamSink> seam;
		optional_idx estimate;
		if (!children.empty()) {
			feed = planner.CreatePlan(*children[0]);
			seam = CrossingSeamSinkOf(context, *feed, RemoteKeyCheck(*bind_data));
			estimate = feed->estimated_cardinality;
		}
		auto columns = RemoteWriteColumns(*bind_data, emit);
		auto slice = RemoteWriteSlice(*statement, *bind_data);
		return PlanCrossingRemote(context, planner, output_types, std::move(statement), std::move(slice),
		                          std::move(columns), std::move(seam), feed, estimate, estimated_cardinality);
	}
	auto &node = planner.Make<CrossingNear>(output_types, emit, std::move(bind_data), estimated_cardinality);
	if (!children.empty()) {
		node.children.push_back(planner.CreatePlan(*children[0]));
	}
	return node;
}

string LogicalCrossingNear::GetName() const {
	return "CROSSING_NEAR";
}

InsertionOrderPreservingMap<string> LogicalCrossingNear::ParamsToString() const {
	return bind_data ? bind_data->Params() : InsertionOrderPreservingMap<string>();
}

string LogicalCrossingNear::GetExtensionName() const {
	return "crossing";
}

void LogicalCrossingNear::Serialize(Serializer &) const {
	throw NotImplementedException("crossing: a near node is planned, never serialized");
}

CrossingNear::CrossingNear(PhysicalPlan &physical_plan, vector<LogicalType> types_p, vector<optional_idx> emit_p,
                           unique_ptr<CrossingBindData> bind_data_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      emit(std::move(emit_p)), bind_data(std::move(bind_data_p)) {
}

unique_ptr<GlobalSinkState> CrossingNear::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CrossingNearSinkState>();
	state->rows = make_uniq<ColumnDataCollection>(context, bind_data->fragment->seam_types);
	return std::move(state);
}

SinkResultType CrossingNear::Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<CrossingNearSinkState>();
	for (idx_t i = 0; i < chunk.size() && !bind_data->key_positions.empty(); i++) {
		SeeKey(state.seen_keys, chunk, bind_data->key_positions, i);
	}
	state.rows->Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType CrossingNear::Finalize(Pipeline &, Event &, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<CrossingNearSinkState>();
	auto &result = state.result;
	if (!result.write) {
		state.keys_sent = state.seen_keys.size();
		result.write = make_uniq<CrossingSeamFilledWrite>(*bind_data, std::move(state.rows));
	}
	while (true) {
		auto parking = CrossingParking::Of(input.interrupt_state);
		auto pull = result.write->Pull(context, parking->Waker());
		if (pull.done) {
			result.Finish(pull);
			break;
		}
		if (parking->Park()) {
			return SinkFinalizeType::BLOCKED;
		}
	}
	if (!bind_data->key_positions.empty()) {
		ThrowIfKeyNotUnique(result.affected_rows, state.keys_sent, bind_data->SourceTable(),
		                    bind_data->seam.key_columns, CrossingVerbName(bind_data->verb));
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> CrossingNear::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<CrossingNearSourceState>();
}

SourceResultType CrossingNear::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<CrossingNearSourceState>();
	if (!IsSink() && !state.result.done) {
		if (!state.result.write) {
			state.result.write = make_uniq<CrossingSeamFilledWrite>(*bind_data, nullptr);
		}
		while (!state.result.done) {
			auto parking = CrossingParking::Of(input.interrupt_state);
			auto pull = state.result.write->Pull(context.client, parking->Waker());
			if (pull.done) {
				state.result.Finish(pull);
				break;
			}
			SourceResultType parked;
			if (ParkSource(*parking, state, parked)) {
				if (parked == SourceResultType::FINISHED) {
					state.result.write.reset();
					state.result.done = true;
				}
				return parked;
			}
		}
	}
	auto &result = IsSink() ? sink_state->Cast<CrossingNearSinkState>().result : state.result;
	if (bind_data->seam.returning_columns.empty()) {
		if (state.emitted) {
			return SourceResultType::FINISHED;
		}
		state.emitted = true;
		chunk.SetChildCardinality(1);
		chunk.data[0].SetValue(0, Value::BIGINT(NumericCast<int64_t>(result.affected_rows)));
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	if (!result.returned) {
		return SourceResultType::FINISHED;
	}
	if (!state.scanning) {
		result.returned->InitializeScan(state.scan);
		result.returned->InitializeScanChunk(state.scanned);
		state.scanning = true;
	}
	state.scanned.Reset();
	result.returned->Scan(state.scan, state.scanned);
	if (state.scanned.size() == 0) {
		return SourceResultType::FINISHED;
	}
	chunk.SetChildCardinality(state.scanned.size());
	for (idx_t c = 0; c < emit.size(); c++) {
		if (emit[c].IsValid()) {
			chunk.data[c].Reference(state.scanned.data[emit[c].GetIndex()]);
			continue;
		}
		for (idx_t r = 0; r < state.scanned.size(); r++) {
			chunk.data[c].SetValue(r, Value(types[c]));
		}
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

string CrossingNear::GetName() const {
	return "CROSSING_NEAR";
}

InsertionOrderPreservingMap<string> CrossingNear::ParamsToString() const {
	auto result = bind_data->Params();
	if (!bind_data->obstacle.empty()) {
		result["Not whole because"] = bind_data->obstacle;
	}
	return result;
}

CrossingWriteCatalog::CrossingWriteCatalog(AttachedDatabase &db, CrossingAttach &attach_p)
    : Catalog(db), attach(attach_p) {
}

CrossingWriteCatalog::~CrossingWriteCatalog() = default;

void CrossingWriteCatalog::Initialize(bool) {
}

string CrossingWriteCatalog::GetCatalogType() {
	return "crossing_write";
}

optional_ptr<CatalogEntry> CrossingWriteCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw NoSchemasHere();
}

optional_ptr<SchemaCatalogEntry> CrossingWriteCatalog::LookupSchema(CatalogTransaction, const EntryLookupInfo &,
                                                                    OnEntryNotFound) {
	throw NoSchemasHere();
}

void CrossingWriteCatalog::ScanSchemas(ClientContext &, std::function<void(SchemaCatalogEntry &)>) {
}

void CrossingWriteCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw NoSchemasHere();
}

PhysicalOperator &CrossingWriteCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &,
                                                          LogicalCreateTable &, PhysicalOperator &) {
	throw NoSchemasHere();
}

DatabaseSize CrossingWriteCatalog::GetDatabaseSize(ClientContext &) {
	return DatabaseSize();
}

bool CrossingWriteCatalog::InMemory() {
	return true;
}

string CrossingWriteCatalog::GetDBPath() {
	return string();
}

PhysicalOperator &CrossingWriteCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &op,
                                                   optional_ptr<PhysicalOperator>) {
	throw PassDidNotRun(op.table);
}

PhysicalOperator &CrossingWriteCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &op,
                                                   PhysicalOperator &) {
	throw PassDidNotRun(op.table);
}

PhysicalOperator &CrossingWriteCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &op,
                                                   PhysicalOperator &) {
	throw PassDidNotRun(op.table);
}

namespace {

PhysicalOperator &PlanShapedMerge(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
                                  PhysicalOperator &plan, CrossingTableCatalogEntry &table, ShapedWrite &shaped,
                                  CrossingAttach &attach) {
	if (op.return_chunk && !table.described.returning) {
		throw BinderException("crossing: the source of '%s' cannot return rows, so RETURNING is refused",
		                      table.name.GetIdentifierName());
	}
	auto statement = attach.RemoteStatement(context, Transaction::Get(context, table.ParentCatalog()));
	auto write = make_uniq<SubstraitWrite>();
	write->merge = true;
	write->catalog = string();
	write->schema = table.source_schema;
	write->table = table.described.name;
	write->target_columns = TableColumnNames(table);
	write->target_index = SHAPED_TARGET_INDEX;
	write->source_index = SHAPED_SOURCE_INDEX;
	vector<unique_ptr<Expression>> owned;
	owned.push_back(shaped.condition->Copy());
	write->condition = owned.back().get();
	for (auto &action : shaped.actions) {
		SubstraitWriteAction rendered;
		rendered.when = action.when;
		rendered.type = action.type;
		if (action.condition) {
			owned.push_back(action.condition->Copy());
			rendered.condition = owned.back().get();
		}
		rendered.columns = action.columns;
		for (auto &value : action.values) {
			owned.push_back(value->Copy());
			rendered.values.push_back(*owned.back());
		}
		write->actions.push_back(std::move(rendered));
	}
	CrossingRemoteColumns columns;
	if (op.return_chunk) {
		write->returning = TableColumnNames(table);
		write->returning.push_back("merge_action");
		write->returning_types = op.types;
		columns = CrossingRemoteColumns::Identity(op.types);
	} else {
		columns = CrossingRemoteColumns::Identity({LogicalType::BIGINT});
	}
	if (TryPutWriteOnRemote(plan, *statement, write, owned, columns, op.types)) {
		return plan;
	}
	CrossingRemoteSlice slice;
	slice.id = statement->Remote().NextSliceId();
	slice.source = MakeSeamNode(0, shaped.source_types);
	slice.source->ResolveOperatorTypes();
	slice.write = std::move(write);
	slice.owned = std::move(owned);
	auto seam = CrossingSeamSinkOf(context, plan);
	return PlanCrossingRemote(context, planner, op.types, std::move(statement), std::move(slice), std::move(columns),
	                          std::move(seam), plan, plan.estimated_cardinality, op.EstimateCardinality(context));
}

} // namespace

PhysicalOperator &CrossingWriteCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalMergeInto &op, PhysicalOperator &plan) {
	auto table = CrossingTableOf(op.table, attach.Identity());
	if (!table) {
		throw PassDidNotRun(op.table);
	}
	if (auto shaped = ShapedWrites::Of(context).Take(op)) {
		return PlanShapedMerge(context, planner, op, plan, *table, *shaped, attach);
	}

	bool addresses_rows = false;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			addresses_rows |= action->action_type == MergeActionType::MERGE_UPDATE ||
			                  action->action_type == MergeActionType::MERGE_DELETE;
		}
	}
	if (addresses_rows) {
		RequireKey(*table, table->described, "a merge");
	}
	if (op.return_chunk && !table->described.returning) {
		throw BinderException("crossing: the source of '%s' cannot return rows, so RETURNING is refused",
		                      table->name.GetIdentifierName());
	}

	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			planned_actions.push_back(PlanCrossingMergeAction(context, op, planner, *action, *table));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto remote = attach.RemoteStatement(context, Transaction::Get(context, table->ParentCatalog())) != nullptr;
	auto &result = planner.Make<PhysicalMergeInto>(op.types, plan, std::move(actions), op.row_id_start,
	                                               op.source_marker, false, op.return_chunk, remote);
	return result;
}

// The base implementation casts the scan's bind data to TableScanBindData and writes through it.
unique_ptr<LogicalOperator> CrossingWriteCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &table,
                                                                  unique_ptr<LogicalOperator>) {
	throw BinderException("crossing: '%s' is served by a source; CREATE INDEX is not supported",
	                      table.name.GetIdentifierName());
}

CrossingCreateTableAs::CrossingCreateTableAs(PhysicalPlan &physical_plan, CrossingAttach &attach_p,
                                             SchemaCatalogEntry &owner_p, unique_ptr<BoundCreateTableInfo> info_p,
                                             vector<LogicalType> row_types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      attach(attach_p), owner(owner_p), info(std::move(info_p)), row_types(std::move(row_types_p)) {
}

unique_ptr<GlobalSinkState> CrossingCreateTableAs::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CrossingCreateTableAsState>();
	state->rows = make_uniq<ColumnDataCollection>(context, row_types);
	return std::move(state);
}

SinkResultType CrossingCreateTableAs::Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const {
	input.global_state.Cast<CrossingCreateTableAsState>().rows->Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType CrossingCreateTableAs::Finalize(Pipeline &, Event &, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<CrossingCreateTableAsState>();
	if (!state.created) {
		auto &create = info->Base();
		CrossingDdl ddl;
		ddl.verb = CrossingVerb::CREATE;
		ddl.schema = owner.name.GetIdentifierName();
		ddl.table = create.GetTableName().GetIdentifierName();
		ddl.create = info->base.get();
		auto &transaction = Transaction::Get(context, owner.ParentCatalog());
		attach.Ddl(context, transaction, owner, ddl);
		state.created = true;

		auto entry = attach.LookupTable(owner.name.GetIdentifierName(), owner,
		                                create.GetTableName().GetIdentifierName(), &transaction);
		if (!entry) {
			throw InternalException("crossing: '%s' was created but the source does not serve it",
			                        create.GetTableName().GetIdentifierName());
		}
		auto &table = entry->Cast<CrossingTableCatalogEntry>();
		RequireVerb(table, CrossingVerb::INSERT);
		auto seam = SeamOf(table, table.described, CrossingVerb::INSERT, TableColumnNames(table));
		if (seam.types != row_types) {
			throw BinderException("crossing: the source describes '%s' with columns other than the ones it was "
			                      "created with",
			                      create.GetTableName().GetIdentifierName());
		}
		auto fragment = PlanWriteFragment(table, CrossingVerb::INSERT, seam);
		state.bind_data = MakeWriteBindData(table, CrossingVerb::INSERT, std::move(seam), std::move(fragment));
		state.write = make_uniq<CrossingSeamFilledWrite>(*state.bind_data, std::move(state.rows));
	}
	while (true) {
		auto parking = CrossingParking::Of(input.interrupt_state);
		auto result = state.write->Pull(context, parking->Waker());
		if (result.done) {
			state.affected_rows = result.affected_rows;
			break;
		}
		if (parking->Park()) {
			return SinkFinalizeType::BLOCKED;
		}
	}
	state.write.reset();
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> CrossingCreateTableAs::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<CrossingWriteSourceState>();
}

SourceResultType CrossingCreateTableAs::GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<CrossingWriteSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;
	chunk.SetChildCardinality(1);
	chunk.data[0].SetValue(
	    0, Value::BIGINT(NumericCast<int64_t>(sink_state->Cast<CrossingCreateTableAsState>().affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

string CrossingCreateTableAs::GetName() const {
	return "CROSSING_CREATE_TABLE_AS";
}

PhysicalOperator &CrossingAttach::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &schema = op.schema;
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::CREATE;
	ddl.schema = schema.name.GetIdentifierName();
	ddl.table = op.info->Base().GetTableName().GetIdentifierName();
	ddl.create = op.info->base.get();
	auto &transaction = Transaction::Get(context, schema.ParentCatalog());
	Authorize(schema, ddl, &transaction);
	if (auto statement = RemoteStatement(context, transaction)) {
		auto write = make_uniq<SubstraitWrite>();
		write->verb = CrossingVerb::CREATE;
		write->schema = ddl.schema;
		write->table = ddl.table;
		for (auto &column : op.info->Base().columns.Logical()) {
			write->target_columns.push_back(column.GetName().GetIdentifierName());
		}
		write->on_conflict = static_cast<uint8_t>(op.info->Base().on_conflict);
		CrossingRemoteSlice slice;
		slice.id = statement->Remote().NextSliceId();
		slice.source = MakeSeamNode(0, plan.types);
		slice.source->ResolveOperatorTypes();
		slice.write = std::move(write);
		NoteAltered(transaction, ddl.schema);
		auto seam = CrossingSeamSinkOf(context, plan);
		return PlanCrossingRemote(context, planner, {LogicalType::BIGINT}, std::move(statement), std::move(slice),
		                          CrossingRemoteColumns::Identity({LogicalType::BIGINT}), std::move(seam), plan,
		                          plan.estimated_cardinality, op.estimated_cardinality);
	}
	auto &result =
	    planner.Make<CrossingCreateTableAs>(*this, schema, std::move(op.info), plan.types, op.estimated_cardinality);
	result.children.push_back(plan);
	return result;
}

} // namespace duckdb
