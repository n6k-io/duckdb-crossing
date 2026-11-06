#include "internal/crossing_write.hpp"

#include "internal/crossing_remote.hpp"
#include "internal/fill.hpp"
#include "internal/seam.hpp"
#include "internal/shaped_write.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
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
		if (!node.key_columns.empty()) {
			write->target_columns.push_back(CROSSING_ROW_ID_COLUMN);
			auto left = make_uniq<BoundColumnRefExpression>(
			    LogicalType::ROW_TYPE,
			    ColumnBinding(TableIndex(REMOTE_TARGET_INDEX), ProjectionIndex(write->target_columns.size() - 1)));
			auto right = make_uniq<BoundColumnRefExpression>(feed_types[0], bindings[0]);
			condition =
			    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(left), std::move(right));
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
                                 CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam) {
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
	auto &catalog = table.ParentCatalog();
	auto statement = CrossingAttach::Of(catalog).RemoteStatement(context, Transaction::Get(context, catalog));
	table.source.Planned(*bind_data->fragment->plan);
	auto columns = RemoteWriteColumns(*bind_data, emit);
	auto seam_sink = make_uniq<CrossingRemoteSeamSink>();
	seam_sink->types = bind_data->fragment->seam_types;
	seam_sink->awaits_result = !op.return_chunk;
	auto slice = RemoteWriteSlice(*statement, *bind_data);
	return PlanCrossingRemote(context, planner, std::move(types), std::move(statement), std::move(slice),
	                          std::move(columns), std::move(seam_sink), nullptr, optional_idx(),
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
		result->expressions.push_back(RefAt(LogicalType::ROW_TYPE, op.row_id_start));
		for (idx_t i = 0; i < action.expressions.size(); i++) {
			if (action.expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
				result->expressions.push_back(op.bound_defaults[action.columns[i].index]->Copy());
			} else {
				result->expressions.push_back(std::move(action.expressions[i]));
			}
		}
		result->op = PlanMergeWrite(planner, op, context, table, verb, std::move(seam));
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
		result->op = PlanMergeWrite(planner, op, context, table, CrossingVerb::INSERT, std::move(seam));
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
	seam.key_columns = {CROSSING_ROW_ID_COLUMN};
	seam.set_columns = std::move(set_columns);
	seam.types = {LogicalType::ROW_TYPE};
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

LogicalCrossingNear::LogicalCrossingNear(TableIndex table_index_p, vector<ColumnBinding> bindings_p,
                                         vector<LogicalType> output_types_p, vector<optional_idx> emit_p,
                                         unique_ptr<CrossingBindData> bind_data_p)
    : table_index(table_index_p), bindings(std::move(bindings_p)), output_types(std::move(output_types_p)),
      emit(std::move(emit_p)), bind_data(std::move(bind_data_p)) {
}

PhysicalOperator &LogicalCrossingNear::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &catalog = bind_data->table.ParentCatalog();
	auto statement = CrossingAttach::Of(catalog).RemoteStatement(context, Transaction::Get(context, catalog));
	bind_data->table.source.Planned(*bind_data->fragment->plan);
	optional_ptr<PhysicalOperator> feed;
	unique_ptr<CrossingRemoteSeamSink> seam;
	optional_idx estimate;
	if (!children.empty()) {
		feed = planner.CreatePlan(*children[0]);
		seam = CrossingSeamSinkOf(context, *feed);
		estimate = feed->estimated_cardinality;
	}
	auto columns = RemoteWriteColumns(*bind_data, emit);
	auto slice = RemoteWriteSlice(*statement, *bind_data);
	return PlanCrossingRemote(context, planner, output_types, std::move(statement), std::move(slice),
	                          std::move(columns), std::move(seam), feed, estimate, estimated_cardinality);
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

	auto &result = planner.Make<PhysicalMergeInto>(op.types, plan, std::move(actions), op.row_id_start,
	                                               op.source_marker, false, op.return_chunk, true);
	return result;
}

// The base implementation casts the scan's bind data to TableScanBindData and writes through it.
unique_ptr<LogicalOperator> CrossingWriteCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &table,
                                                                  unique_ptr<LogicalOperator>) {
	throw BinderException("crossing: '%s' is served by a source; CREATE INDEX is not supported",
	                      table.name.GetIdentifierName());
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
	auto statement = RemoteStatement(context, transaction);
	source->Planned(*MakeSeamNode(0, plan.types));
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

} // namespace duckdb
