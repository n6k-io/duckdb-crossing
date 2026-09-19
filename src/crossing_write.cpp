#include "internal/crossing_write.hpp"

#include "internal/key_check.hpp"
#include "internal/parking.hpp"
#include "internal/seam.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
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
	return NotImplementedException("crossing: the write to '%s' needs the crossing optimizer pass", table.name);
}

void AddRowImage(LogicalOperator &op, const CrossingIdentity &identity) {
	if (op.type == LogicalOperatorType::LOGICAL_DELETE) {
		auto &del = op.Cast<LogicalDelete>();
		auto table = CrossingTableOf(del.table, identity);
		if (!table || !del.return_chunk || del.expressions.empty()) {
			return;
		}
		auto &key = del.expressions[0]->Cast<BoundColumnRefExpression>();
		auto get = FindGetWithIndex(*del.children[0], key.binding.table_index);
		if (!get) {
			return;
		}
		for (auto &ref : RowImageFrom(*get, *table)) {
			del.expressions.push_back(std::move(ref));
		}
		return;
	}
	if (op.type != LogicalOperatorType::LOGICAL_MERGE_INTO) {
		return;
	}
	auto &merge = op.Cast<LogicalMergeInto>();
	auto table = CrossingTableOf(merge.table, identity);
	if (!table || !merge.return_chunk || !MergeDeletes(merge) ||
	    merge.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = merge.children[0]->Cast<LogicalProjection>();
	if (merge.row_id_start >= projection.expressions.size()) {
		return;
	}
	auto &key = projection.expressions[merge.row_id_start]->Cast<BoundColumnRefExpression>();
	auto get = FindGetWithIndex(*projection.children[0], key.binding.table_index);
	if (!get) {
		return;
	}
	for (auto &ref : RowImageFrom(*get, *table)) {
		projection.expressions.push_back(std::move(ref));
	}
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

struct CrossingFenceState : public GlobalSourceState {
	unique_ptr<CrossingSeamFilledWrite> write;
	bool done = false;
};

struct CrossingWriteSourceState : public GlobalSourceState {
	bool done = false;
};

unique_ptr<MergeIntoOperator> PlanCrossingMergeAction(ClientContext &context, LogicalMergeInto &op,
                                                      PhysicalPlanGenerator &planner, BoundMergeIntoAction &action,
                                                      CrossingTableCatalogEntry &table, idx_t feed_width) {
	auto result = make_uniq<MergeIntoOperator>();
	result->action_type = action.action_type;
	result->condition = std::move(action.condition);

	auto return_types = op.types;
	if (op.return_chunk) {
		return_types.pop_back();
	}
	auto cardinality = op.EstimateCardinality(context);

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE:
	case MergeActionType::MERGE_DELETE: {
		auto verb = action.action_type == MergeActionType::MERGE_UPDATE ? CrossingVerb::UPDATE : CrossingVerb::DELETE_;
		RequireVerb(table, verb);
		auto seam = SeamOf(table, table.described, verb, SetColumnNames(table, action.columns));
		auto fragment = PlanWriteFragment(table, verb, seam);
		auto &keyed = planner
		                  .Make<CrossingWrite>(MakeWriteBindData(table, verb, seam, std::move(fragment)), return_types,
		                                       cardinality)
		                  .Cast<CrossingWrite>();
		keyed.return_chunk = op.return_chunk;
		auto key_types = ColumnTypesByName(table, seam.key_columns);
		for (idx_t k = 0; k < key_types.size(); k++) {
			keyed.seam_row.push_back(RefAt(key_types[k], op.row_id_start + k));
			if (!table.described.key_unique) {
				keyed.key_positions.push_back(op.row_id_start + k);
			}
		}
		vector<unique_ptr<Expression>> set_values;
		for (idx_t i = 0; i < action.expressions.size(); i++) {
			if (action.expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
				set_values.push_back(op.bound_defaults[action.columns[i].index]->Copy());
			} else {
				set_values.push_back(std::move(action.expressions[i]));
			}
		}
		if (op.return_chunk) {
			if (verb == CrossingVerb::DELETE_) {
				auto row_types = TableRowTypes(table);
				for (idx_t c = 0; c < row_types.size(); c++) {
					keyed.returned_row.push_back(RefAt(row_types[c], feed_width - row_types.size() + c));
				}
			} else {
				keyed.returned_row = ImageFromSetValues(table, seam, set_values);
			}
		}
		for (auto &value : set_values) {
			keyed.seam_row.push_back(std::move(value));
		}
		result->op = keyed;
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		RequireVerb(table, CrossingVerb::INSERT);
		auto seam = SeamOf(table, table.described, CrossingVerb::INSERT, {});
		auto fragment = PlanWriteFragment(table, CrossingVerb::INSERT, seam);
		auto &insert = planner.Make<CrossingWrite>(
		    MakeWriteBindData(table, CrossingVerb::INSERT, seam, std::move(fragment)), return_types, cardinality);
		insert.Cast<CrossingWrite>().return_chunk = op.return_chunk;
		result->op = insert;

		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &column : table.GetColumns().Physical()) {
				auto mapped_index = action.column_index_map[column.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					new_expressions.push_back(op.bound_defaults[column.StorageOid()]->Copy());
				} else {
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);
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
		out.push_back(table.GetColumns().GetColumn(want).GetType());
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
		out.push_back(column.GetName());
	}
	return out;
}

vector<string> SetColumnNames(TableCatalogEntry &table, const vector<PhysicalIndex> &columns) {
	vector<string> out;
	for (auto &column_index : columns) {
		out.push_back(table.GetColumns().GetColumn(column_index).GetName());
	}
	return out;
}

unique_ptr<Expression> RefAt(const LogicalType &type, idx_t position) {
	return make_uniq<BoundReferenceExpression>(type, position);
}

optional_ptr<LogicalGet> FindGetWithIndex(LogicalOperator &op, idx_t table_index) {
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

vector<unique_ptr<Expression>> RowImageFrom(LogicalGet &get, TableCatalogEntry &table) {
	vector<unique_ptr<Expression>> out;
	for (auto &column : table.GetColumns().Logical()) {
		auto wanted = column.Logical().index;
		auto &ids = get.GetColumnIds();
		idx_t position = ids.size();
		for (idx_t p = 0; p < ids.size(); p++) {
			if (!ids[p].IsVirtualColumn() && ids[p].GetPrimaryIndex() == wanted) {
				position = p;
				break;
			}
		}
		if (position == ids.size()) {
			get.AddColumnId(wanted);
		}
		out.push_back(make_uniq<BoundColumnRefExpression>(column.Name(), column.Type(),
		                                                  ColumnBinding(get.table_index, position)));
	}
	return out;
}

bool MergeDeletes(LogicalMergeInto &merge) {
	for (auto &entry : merge.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_DELETE) {
				return true;
			}
		}
	}
	return false;
}

vector<unique_ptr<Expression>> ImageFromSetValues(TableCatalogEntry &table, const CrossingSeam &seam,
                                                  const vector<unique_ptr<Expression>> &set_values) {
	vector<unique_ptr<Expression>> image;
	for (auto &name : TableColumnNames(table)) {
		idx_t set = seam.set_columns.size();
		for (idx_t s = 0; s < seam.set_columns.size(); s++) {
			if (StringUtil::CIEquals(seam.set_columns[s], name)) {
				set = s;
				break;
			}
		}
		if (set == seam.set_columns.size()) {
			throw BinderException("crossing: RETURNING on '%s' has no value for column '%s'", table.name, name);
		}
		image.push_back(set_values[set]->Copy());
	}
	return image;
}

CrossingTableUse WrittenTable(const string &source_schema, const CrossingTable &described, const CrossingSeam &seam) {
	CrossingTableUse use;
	use.schema = source_schema;
	use.table = described.name;
	auto &names = described.column_names;
	auto add = [&](const string &wanted) {
		for (idx_t c = 0; c < names.size(); c++) {
			if (StringUtil::CIEquals(names[c], wanted)) {
				use.columns.push_back(c);
				return;
			}
		}
	};
	for (auto &column : seam.key_columns) {
		add(column);
	}
	for (auto &column : seam.set_columns) {
		add(column);
	}
	return use;
}

CrossingSeam SeamOf(TableCatalogEntry &table, const CrossingTable &described, CrossingVerb verb,
                    vector<string> set_columns) {
	CrossingSeam seam;
	if (verb == CrossingVerb::INSERT) {
		seam.set_columns = TableColumnNames(table);
		seam.types = TableRowTypes(table);
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
		throw PermissionException("crossing: '%s' does not have '%s' permission", table.name, CrossingVerbName(verb));
	}
}

void RequireKey(TableCatalogEntry &table, const CrossingTable &described, const char *what) {
	if (described.key.empty()) {
		throw BinderException("crossing: '%s' has no key, so %s cannot address its rows", table.name, what);
	}
}

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam) {
	CrossingPlanRequest request;
	request.verb = verb;
	request.schema = table.source_schema;
	request.table = table.described.name;
	request.described = &table.described;
	request.seam = seam;
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->seam_types = seam.types;
	fragment->plan = RequirePlan(table.source.Plan(request), verb, table.name);
	if (!fragment->HasSeam()) {
		throw InternalException("crossing: the source's %s of '%s' holds no seam", CrossingVerbName(verb), table.name);
	}
	fragment->ResolveTypesAndText();
	fragment->VerifyInvariants();
	return fragment;
}

unique_ptr<CrossingBindData> MakeWriteBindData(CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
                                               shared_ptr<CrossingFragment> fragment) {
	return make_uniq<CrossingBindData>(table, verb, std::move(seam), std::move(fragment));
}

CrossingSeamEntry::CrossingSeamEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     unique_ptr<CrossingBindData> write_p, string obstacle_p)
    : TableCatalogEntry(catalog, schema, info), write(std::move(write_p)), obstacle(std::move(obstacle_p)) {
}

unique_ptr<BaseStatistics> CrossingSeamEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

TableFunction CrossingSeamEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &) {
	throw InternalException("crossing: a seam is written, never read");
}

TableStorageInfo CrossingSeamEntry::GetStorageInfo(ClientContext &) {
	return TableStorageInfo();
}

const string &CrossingSeamEntriesKey() {
	static const int anchor = 0;
	static const string key = "crossing/seam_entries/" + std::to_string(reinterpret_cast<uintptr_t>(&anchor));
	return key;
}

TableFunction CrossingWriteFunction() {
	TableFunction function(CROSSING_WRITE_FUNCTION, {}, CrossingWriteFunc);
	function.to_string = CrossingWriteToString;
	function.projection_pushdown = false;
	return function;
}

CrossingSeamFilledWrite::CrossingSeamFilledWrite(const CrossingBindData &write, unique_ptr<ColumnDataCollection> rows)
    : table(write.table), fragment(write.fragment), slot(fragment->SeamSlot()) {
	if (slot && (!rows || rows->Count() == 0)) {
		empty = true;
		return;
	}
	if (!slot && rows) {
		throw InternalException("crossing: rows were gathered for a seam a plan already fills");
	}
	if (slot) {
		seam_node = std::move(*slot);
		auto table_index = seam_node->Cast<LogicalGet>().table_index;
		*slot = make_uniq<LogicalColumnDataGet>(table_index, fragment->seam_types, std::move(rows));
		fragment->ResolveTypesAndText();
	}
	query = make_uniq<CrossingQuery>(write.verb, *fragment->plan);
	query->types = fragment->seam_types;
	query->tables = CrossingTablesOf(*fragment->plan);
	query->written = WrittenTable(table.source_schema, table.described, write.seam);
	query->key_columns = write.seam.key_columns;
	query->set_columns = write.seam.set_columns;
}

CrossingSeamFilledWrite::~CrossingSeamFilledWrite() {
	writer = nullptr;
	if (seam_node) {
		*slot = std::move(seam_node);
		fragment->ResolveTypesAndText();
	}
}

CrossingWriteResult CrossingSeamFilledWrite::Pull(ClientContext &context, CrossingWaker waker) {
	if (empty) {
		return CrossingWriteResult::Done(0);
	}
	if (!writer) {
		auto &catalog = table.ParentCatalog();
		auto &session = CrossingAttach::Of(catalog).Session(context, Transaction::Get(context, catalog));
		writer = session.Write(context, *query);
		if (!writer) {
			throw InternalException("crossing: the source returned no writer for '%s'", table.described.name);
		}
	}
	return writer(context, std::move(waker));
}

LogicalCrossingFence::LogicalCrossingFence(idx_t table_index_p, unique_ptr<CrossingBindData> bind_data_p)
    : table_index(table_index_p), bind_data(std::move(bind_data_p)) {
}

PhysicalOperator &LogicalCrossingFence::CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) {
	return planner.Make<CrossingFence>(std::move(bind_data), estimated_cardinality);
}

vector<ColumnBinding> LogicalCrossingFence::GetColumnBindings() {
	return {ColumnBinding(table_index, 0)};
}

vector<idx_t> LogicalCrossingFence::GetTableIndex() const {
	return {table_index};
}

string LogicalCrossingFence::GetName() const {
	return "CROSSING_FENCE";
}

InsertionOrderPreservingMap<string> LogicalCrossingFence::ParamsToString() const {
	return bind_data ? bind_data->Params() : InsertionOrderPreservingMap<string>();
}

string LogicalCrossingFence::GetExtensionName() const {
	return "crossing";
}

void LogicalCrossingFence::Serialize(Serializer &) const {
	throw NotImplementedException("crossing: a fence node is planned, never serialized");
}

void LogicalCrossingFence::ResolveTypes() {
	types = {LogicalType::BIGINT};
}

CrossingFence::CrossingFence(PhysicalPlan &physical_plan, unique_ptr<CrossingBindData> bind_data_p,
                             idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      bind_data(std::move(bind_data_p)) {
}

unique_ptr<GlobalSourceState> CrossingFence::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<CrossingFenceState>();
}

SourceResultType CrossingFence::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<CrossingFenceState>();
	if (state.done) {
		return SourceResultType::FINISHED;
	}
	if (!state.write) {
		state.write = make_uniq<CrossingSeamFilledWrite>(*bind_data, nullptr);
	}
	while (true) {
		auto parking = CrossingParking::Of(input.interrupt_state);
		auto result = state.write->Pull(context.client, parking->Waker());
		if (result.outcome == CrossingWriteResult::Outcome::DONE) {
			state.write.reset();
			state.done = true;
			chunk.SetCardinality(1);
			chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(result.affected_rows)));
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		SourceResultType parked;
		if (ParkSource(*parking, state, parked)) {
			if (parked == SourceResultType::FINISHED) {
				state.write.reset();
				state.done = true;
			}
			return parked;
		}
	}
}

string CrossingFence::GetName() const {
	return "CROSSING_FENCE";
}

InsertionOrderPreservingMap<string> CrossingFence::ParamsToString() const {
	return bind_data->Params();
}

void AddRowImageForReturning(LogicalOperator &plan, const CrossingIdentity &identity) {
	for (auto &child : plan.children) {
		AddRowImageForReturning(*child, identity);
	}
	AddRowImage(plan, identity);
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

PhysicalOperator &CrossingWriteCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	auto entry = dynamic_cast<CrossingSeamEntry *>(&op.table);
	if (!entry || &entry->write->Identity() != &attach.Identity()) {
		throw PassDidNotRun(op.table);
	}
	if (!plan) {
		throw InternalException("crossing: a seam insert without rows");
	}
	auto &write = *entry->write;
	auto &insert = planner.Make<CrossingWrite>(MakeWriteBindData(write.table, write.verb, write.seam, write.fragment),
	                                           op.types, op.estimated_cardinality);
	auto &crossing = insert.Cast<CrossingWrite>();
	crossing.entry = CrossingSeamEntries::Get(context)->Share(*entry);
	crossing.return_chunk = op.return_chunk;
	crossing.obstacle = entry->obstacle;
	crossing.key_positions = entry->key_positions;
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &CrossingWriteCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &op,
                                                   PhysicalOperator &) {
	throw PassDidNotRun(op.table);
}

PhysicalOperator &CrossingWriteCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &op,
                                                   PhysicalOperator &) {
	throw PassDidNotRun(op.table);
}

PhysicalOperator &CrossingWriteCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalMergeInto &op, PhysicalOperator &plan) {
	auto table = CrossingTableOf(op.table, attach.Identity());
	if (!table) {
		throw PassDidNotRun(op.table);
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

	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			planned_actions.push_back(
			    PlanCrossingMergeAction(context, op, planner, *action, *table, plan.types.size()));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               false, op.return_chunk);
	result.children.push_back(plan);
	return result;
}

void CrossingWriteState::SeeKey(DataChunk &chunk, const vector<idx_t> &key_positions, idx_t index) {
	child_list_t<Value> key;
	for (idx_t k = 0; k < key_positions.size(); k++) {
		key.emplace_back(to_string(k), chunk.GetValue(key_positions[k], index));
	}
	seen_keys.insert(Value::STRUCT(std::move(key)));
}

CrossingWrite::CrossingWrite(PhysicalPlan &physical_plan, unique_ptr<CrossingBindData> bind_data_p,
                             vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      bind_data(std::move(bind_data_p)) {
}

unique_ptr<GlobalSinkState> CrossingWrite::GetGlobalSinkState(ClientContext &) const {
	auto state = make_uniq<CrossingWriteState>();
	state->rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), bind_data->fragment->seam_types);
	if (return_chunk) {
		state->returned =
		    make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), TableRowTypes(bind_data->table));
	}
	return std::move(state);
}

SinkResultType CrossingWrite::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<CrossingWriteState>();
	for (idx_t i = 0; i < chunk.size() && !key_positions.empty(); i++) {
		state.SeeKey(chunk, key_positions, i);
	}

	if (seam_row.empty()) {
		state.rows->Append(chunk);
	} else {
		DataChunk row;
		row.Initialize(Allocator::DefaultAllocator(), bind_data->fragment->seam_types);
		ExpressionExecutor executor(context.client, seam_row);
		executor.Execute(chunk, row);
		state.rows->Append(row);
	}

	if (!return_chunk) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (returned_row.empty()) {
		state.returned->Append(chunk);
	} else {
		DataChunk image;
		image.Initialize(Allocator::DefaultAllocator(), TableRowTypes(bind_data->table));
		ExpressionExecutor executor(context.client, returned_row);
		executor.Execute(chunk, image);
		state.returned->Append(image);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType CrossingWrite::Finalize(Pipeline &, Event &, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<CrossingWriteState>();
	if (!state.write) {
		if (state.rows->Count() == 0) {
			return SinkFinalizeType::READY;
		}
		state.keys_sent = state.seen_keys.size();
		state.write = make_uniq<CrossingSeamFilledWrite>(*bind_data, std::move(state.rows));
	}
	while (true) {
		auto parking = CrossingParking::Of(input.interrupt_state);
		auto result = state.write->Pull(context, parking->Waker());
		if (result.outcome == CrossingWriteResult::Outcome::DONE) {
			state.affected_rows = result.affected_rows;
			break;
		}
		if (parking->Park()) {
			return SinkFinalizeType::BLOCKED;
		}
	}
	state.write.reset();
	if (!key_positions.empty()) {
		ThrowIfKeyNotUnique(state.affected_rows, state.keys_sent, bind_data->SourceTable(), bind_data->seam.key_columns,
		                    CrossingVerbName(bind_data->verb));
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> CrossingWrite::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<CrossingWriteSourceState>();
}

SourceResultType CrossingWrite::GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<CrossingWriteState>();
	auto &source_state = input.global_state.Cast<CrossingWriteSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	if (return_chunk) {
		if (!state.returned_scanning) {
			state.returned->InitializeScan(state.returned_scan);
			state.returned_scanning = true;
		}
		state.returned->Scan(state.returned_scan, chunk);
		if (chunk.size() == 0) {
			source_state.done = true;
			return SourceResultType::FINISHED;
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	source_state.done = true;
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

string CrossingWrite::GetName() const {
	return "CROSSING_WRITE";
}

InsertionOrderPreservingMap<string> CrossingWrite::ParamsToString() const {
	auto result = bind_data->Params();
	if (!obstacle.empty()) {
		result["Not whole because"] = obstacle;
	}
	return result;
}

CrossingCreateTableAs::CrossingCreateTableAs(PhysicalPlan &physical_plan, CrossingAttach &attach_p,
                                             SchemaCatalogEntry &owner_p, unique_ptr<BoundCreateTableInfo> info_p,
                                             vector<LogicalType> row_types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      attach(attach_p), owner(owner_p), info(std::move(info_p)), row_types(std::move(row_types_p)) {
}

unique_ptr<GlobalSinkState> CrossingCreateTableAs::GetGlobalSinkState(ClientContext &) const {
	auto state = make_uniq<CrossingCreateTableAsState>();
	state->rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), row_types);
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
		ddl.schema = owner.name;
		ddl.table = create.table;
		ddl.create = info->base.get();
		auto &transaction = Transaction::Get(context, owner.ParentCatalog());
		attach.Ddl(context, transaction, owner, ddl);
		state.created = true;

		auto entry = attach.LookupTable(owner.name, owner, create.table, &transaction);
		if (!entry) {
			throw InternalException("crossing: '%s' was created but the source does not serve it", create.table);
		}
		auto &table = entry->Cast<CrossingTableCatalogEntry>();
		RequireVerb(table, CrossingVerb::INSERT);
		auto seam = SeamOf(table, table.described, CrossingVerb::INSERT, {});
		if (seam.types != row_types) {
			throw BinderException("crossing: the source describes '%s' with columns other than the ones it was "
			                      "created with",
			                      create.table);
		}
		auto fragment = PlanWriteFragment(table, CrossingVerb::INSERT, seam);
		state.bind_data = MakeWriteBindData(table, CrossingVerb::INSERT, std::move(seam), std::move(fragment));
		state.write = make_uniq<CrossingSeamFilledWrite>(*state.bind_data, std::move(state.rows));
	}
	while (true) {
		auto parking = CrossingParking::Of(input.interrupt_state);
		auto result = state.write->Pull(context, parking->Waker());
		if (result.outcome == CrossingWriteResult::Outcome::DONE) {
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
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0,
	               Value::BIGINT(NumericCast<int64_t>(sink_state->Cast<CrossingCreateTableAsState>().affected_rows)));
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
	ddl.schema = schema.name;
	ddl.table = op.info->Base().table;
	ddl.create = op.info->base.get();
	Authorize(schema, ddl, &Transaction::Get(context, schema.ParentCatalog()));
	auto &result =
	    planner.Make<CrossingCreateTableAs>(*this, schema, std::move(op.info), plan.types, op.estimated_cardinality);
	result.children.push_back(plan);
	return result;
}

} // namespace duckdb
