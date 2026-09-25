#include "crossing.hpp"
#include "internal/floor.hpp"
#include "internal/seam.hpp"
#include "internal/table_indices.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

namespace {

constexpr const char *SEAM_ALIAS = "crossing_seam";
constexpr const char *SEAM_SOURCE_ALIAS = "crossing_seam_source";

string TypeList(const vector<LogicalType> &types) {
	string out;
	for (auto &type : types) {
		out += (out.empty() ? "" : ", ") + type.ToString();
	}
	return out;
}

string NameList(const vector<string> &names) {
	string out;
	for (auto &name : names) {
		out += (out.empty() ? "" : ", ") + name;
	}
	return out;
}

idx_t MaxTableIndex(const LogicalOperator &op) {
	idx_t result = 0;
	for (auto &child : op.children) {
		result = MaxValue(result, MaxTableIndex(*child));
	}
	for (auto index : op.GetTableIndex()) {
		result = MaxValue(result, index);
	}
	return result;
}

void RequireTransaction(ClientContext &context, const char *what) {
	if (!context.transaction.HasActiveTransaction()) {
		throw InternalException("crossing: %s needs a transaction open on the context", what);
	}
}

void AdoptFloorIndex(LogicalOperator &root, const LogicalGet &floor_get, const CrossingFloor &floor) {
	switch (root.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = root.Cast<LogicalGet>();
		get.table_index = floor_get.table_index;
		vector<ColumnIndex> ids = floor_get.GetColumnIds();
		get.SetColumnIds(std::move(ids));
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION:
		root.Cast<LogicalProjection>().table_index = floor_get.table_index;
		return;
	default:
		throw InternalException("crossing: '%s.%s' bound to a %s, which cannot stand where a floor stood", floor.schema,
		                        floor.table, LogicalOperatorToString(root.type));
	}
}

void BindFloorsBelow(Binder &binder, unique_ptr<LogicalOperator> &op, const string &catalog,
                     const CrossingFloorResolver &resolver, idx_t &next_index) {
	for (auto &child : op->children) {
		BindFloorsBelow(binder, child, catalog, resolver, next_index);
	}
	auto floor = FloorOf(*op);
	if (!floor) {
		return;
	}
	auto &floor_get = op->Cast<LogicalGet>();
	unique_ptr<TableRef> ref = resolver ? resolver(*floor) : nullptr;
	if (!ref) {
		auto base = make_uniq<BaseTableRef>();
		base->catalog_name = catalog;
		base->schema_name = floor->schema;
		base->table_name = floor->table;
		ref = std::move(base);
	}
	auto bound = binder.Bind(*ref);
	if (!bound.plan) {
		throw InternalException("crossing: '%s.%s' bound to nothing", floor->schema, floor->table);
	}
	if (bound.names != floor_get.names || bound.types != floor_get.returned_types) {
		throw CatalogException("crossing: '%s.%s' changed on the source: it now has [%s] as [%s], the floor expects "
		                       "[%s] as [%s]; refresh the attach",
		                       floor->schema, floor->table, NameList(bound.names), TypeList(bound.types),
		                       NameList(floor_get.names), TypeList(floor_get.returned_types));
	}
	RemapTableIndices(*bound.plan, [&next_index]() { return next_index++; });
	AdoptFloorIndex(*bound.plan, floor_get, *floor);
	op = std::move(bound.plan);
}

vector<string> SeamColumnNames(const CrossingWriteTarget &target) {
	vector<string> names;
	for (idx_t i = 0; i < target.key_columns.size() + target.set_columns.size(); i++) {
		names.push_back("c" + to_string(i));
	}
	return names;
}

//! The seam's columns are what they are by position; this names them so the statement can refer
//! to them, whatever the TableRef underneath calls them.
unique_ptr<TableRef> SeamNamed(unique_ptr<TableRef> seam, const vector<string> &columns) {
	if (seam->alias.empty()) {
		seam->alias = SEAM_SOURCE_ALIAS;
	}
	auto node = make_uniq<SelectNode>();
	for (idx_t i = 0; i < columns.size(); i++) {
		auto column = make_uniq<PositionalReferenceExpression>(i + 1);
		column->alias = columns[i];
		node->select_list.push_back(std::move(column));
	}
	node->from_table = std::move(seam);
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);
	return make_uniq<SubqueryRef>(std::move(select), SEAM_ALIAS);
}

unique_ptr<BaseTableRef> TargetRef(const CrossingWriteTarget &target) {
	auto ref = make_uniq<BaseTableRef>();
	ref->catalog_name = target.catalog;
	ref->schema_name = target.schema;
	ref->table_name = target.table;
	return ref;
}

unique_ptr<ParsedExpression> KeysMatch(const CrossingWriteTarget &target, const vector<string> &seam_columns) {
	unique_ptr<ParsedExpression> condition;
	for (idx_t c = 0; c < target.key_columns.size(); c++) {
		auto target_column = make_uniq<ColumnRefExpression>(target.key_columns[c], target.table);
		auto seam_column = make_uniq<ColumnRefExpression>(seam_columns[c], SEAM_ALIAS);
		auto same = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(target_column),
		                                            std::move(seam_column));
		condition = condition ? make_uniq_base<ParsedExpression, ConjunctionExpression>(
		                            ExpressionType::CONJUNCTION_AND, std::move(condition), std::move(same))
		                      : unique_ptr<ParsedExpression>(std::move(same));
	}
	return condition;
}

unique_ptr<SQLStatement> InsertOf(const CrossingWriteTarget &target, unique_ptr<TableRef> seam,
                                  const vector<string> &seam_columns) {
	auto node = make_uniq<SelectNode>();
	for (idx_t c = 0; c < target.set_columns.size(); c++) {
		auto column = make_uniq<ColumnRefExpression>(seam_columns[c], SEAM_ALIAS);
		column->alias = target.set_columns[c];
		node->select_list.push_back(std::move(column));
	}
	node->from_table = std::move(seam);
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);

	auto statement = make_uniq<InsertStatement>();
	statement->catalog = target.catalog;
	statement->schema = target.schema;
	statement->table = target.table;
	statement->columns = target.set_columns;
	statement->select_statement = std::move(select);
	return std::move(statement);
}

unique_ptr<SQLStatement> UpdateOf(const CrossingWriteTarget &target, unique_ptr<TableRef> seam,
                                  const vector<string> &seam_columns) {
	auto set_info = make_uniq<UpdateSetInfo>();
	set_info->condition = KeysMatch(target, seam_columns);
	auto keys = target.key_columns.size();
	for (idx_t c = 0; c < target.set_columns.size(); c++) {
		set_info->columns.push_back(target.set_columns[c]);
		set_info->expressions.push_back(make_uniq<ColumnRefExpression>(seam_columns[keys + c], SEAM_ALIAS));
	}
	auto statement = make_uniq<UpdateStatement>();
	statement->table = TargetRef(target);
	statement->from_table = std::move(seam);
	statement->set_info = std::move(set_info);
	return std::move(statement);
}

unique_ptr<SQLStatement> DeleteOf(const CrossingWriteTarget &target, unique_ptr<TableRef> seam,
                                  const vector<string> &seam_columns) {
	auto statement = make_uniq<DeleteStatement>();
	statement->table = TargetRef(target);
	statement->using_clauses.push_back(std::move(seam));
	statement->condition = KeysMatch(target, seam_columns);
	return std::move(statement);
}

unique_ptr<TableRef> SeamRefOfPlan(Binder &top, unique_ptr<LogicalOperator> plan) {
	RemapTableIndices(*plan, [&top]() { return top.GenerateTableIndex(); });
	plan->ResolveOperatorTypes();
	auto bindings = plan->GetColumnBindings();
	if (bindings.empty()) {
		throw InternalException("crossing: a seam plan produces no columns");
	}
	auto index = bindings[0].table_index;
	vector<string> names;
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i].table_index != index) {
			throw InternalException("crossing: a seam plan answers to more than one table index");
		}
		names.push_back("c" + to_string(i));
	}
	auto binder = Binder::CreateBinder(top.context, &top);
	binder->bind_context.AddGenericBinding(index, SEAM_SOURCE_ALIAS, names, plan->types);
	BoundStatement bound;
	bound.types = plan->types;
	bound.names = std::move(names);
	bound.plan = std::move(plan);
	auto ref = make_uniq<BoundRefWrapper>(std::move(bound), std::move(binder));
	ref->alias = SEAM_SOURCE_ALIAS;
	return std::move(ref);
}

//! Copying a RelationStatement shares the relation, so the plan is never copied. Bound once: the
//! plan is handed over on the first Bind.
class PlanRelation : public Relation {
public:
	PlanRelation(const shared_ptr<ClientContext> &context, unique_ptr<LogicalOperator> plan_p)
	    : Relation(context, RelationType::QUERY_RELATION), plan(std::move(plan_p)) {
		plan->ResolveOperatorTypes();
		for (idx_t i = 0; i < plan->types.size(); i++) {
			columns.emplace_back("col" + to_string(i), plan->types[i]);
		}
	}

	const vector<ColumnDefinition> &Columns() override {
		return columns;
	}
	unique_ptr<QueryNode> GetQueryNode() override {
		throw InternalException("crossing: a plan relation has no query node");
	}
	string GetQuery() override {
		return ToString(0);
	}
	string ToString(idx_t) override {
		return plan ? plan->ToString() : "crossing plan (run)";
	}
	//! The optimizer draws table indices from this binder; what it draws must be past the plan's.
	BoundStatement Bind(Binder &binder) override {
		if (!plan) {
			throw InternalException("crossing: a plan can run once");
		}
		auto highest = MaxTableIndex(*plan);
		while (binder.GenerateTableIndex() < highest) {
		}
		BoundStatement result;
		result.types = plan->types;
		for (auto &column : columns) {
			result.names.push_back(column.Name());
		}
		result.plan = std::move(plan);
		return result;
	}

private:
	unique_ptr<LogicalOperator> plan;
	vector<ColumnDefinition> columns;
};

idx_t RunPlanned(ClientContext &context, const CrossingWriteTarget &target, unique_ptr<LogicalOperator> plan) {
	auto &attached = Catalog::GetCatalog(context, target.catalog).GetAttached();
	MetaTransaction::Get(context).ModifyDatabase(attached, DatabaseModificationType::UPDATE_DATA);

	auto pending = PendingCrossingPlan(context, std::move(plan), false);
	if (pending->HasError()) {
		pending->ThrowError();
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->ThrowError();
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0 || chunk->ColumnCount() == 0) {
		return 0;
	}
	return NumericCast<idx_t>(chunk->GetValue(0, 0).GetValue<int64_t>());
}

idx_t ExecuteBuilt(ClientContext &context, Planner &planner, const CrossingWriteTarget &target,
                   CrossingWriteStatement built, const CrossingWriteShaper &shape) {
	if (shape) {
		shape(built);
	}
	planner.CreatePlan(std::move(built.statement));
	return RunPlanned(context, target, std::move(planner.plan));
}

} // namespace

unique_ptr<PendingQueryResult> PendingCrossingPlan(ClientContext &context, unique_ptr<LogicalOperator> plan,
                                                   bool stream) {
	auto relation = make_shared_ptr<PlanRelation>(context.shared_from_this(), std::move(plan));
	return context.PendingQuery(relation, QueryParameters(stream));
}

void BindFloors(ClientContext &context, unique_ptr<LogicalOperator> &plan, const string &catalog,
                const CrossingFloorResolver &resolver) {
	RequireTransaction(context, "BindFloors");
	auto binder = Binder::CreateBinder(context);
	idx_t next_index = MaxTableIndex(*plan) + 1;
	BindFloorsBelow(*binder, plan, catalog, resolver, next_index);
	plan->ResolveOperatorTypes();
}

unique_ptr<TableRef> SeamRefOfRows(const ColumnDataCollection &rows) {
	// Scanning does not write the collection; ColumnDataRef's pointer type is just not const.
	auto &scanned = const_cast<ColumnDataCollection &>(rows); // NOLINT(cppcoreguidelines-pro-type-const-cast)
	auto ref = make_uniq<ColumnDataRef>(optionally_owned_ptr<ColumnDataCollection>(scanned));
	ref->alias = SEAM_SOURCE_ALIAS;
	return std::move(ref);
}

CrossingWriteStatement BuildWriteStatement(const CrossingWriteTarget &target, unique_ptr<TableRef> seam) {
	if (target.verb != CrossingVerb::INSERT && target.key_columns.empty()) {
		throw InvalidInputException("crossing: a %s of '%s.%s' names no key columns to address rows by",
		                            CrossingVerbName(target.verb), target.schema, target.table);
	}
	CrossingWriteStatement built;
	built.seam_alias = SEAM_ALIAS;
	built.seam_columns = SeamColumnNames(target);
	auto named = SeamNamed(std::move(seam), built.seam_columns);
	switch (target.verb) {
	case CrossingVerb::INSERT:
		built.statement = InsertOf(target, std::move(named), built.seam_columns);
		break;
	case CrossingVerb::UPDATE:
		built.statement = UpdateOf(target, std::move(named), built.seam_columns);
		break;
	case CrossingVerb::DELETE_:
		built.statement = DeleteOf(target, std::move(named), built.seam_columns);
		break;
	default:
		throw InvalidInputException("crossing: '%s' is not a write verb", CrossingVerbName(target.verb));
	}
	return built;
}

idx_t ExecuteWrite(ClientContext &context, const CrossingWriteTarget &target, unique_ptr<TableRef> seam,
                   const CrossingWriteShaper &shape) {
	RequireTransaction(context, "ExecuteWrite");
	Planner planner(context);
	return ExecuteBuilt(context, planner, target, BuildWriteStatement(target, std::move(seam)), shape);
}

idx_t ExecuteWrite(ClientContext &context, const CrossingWriteTarget &target, unique_ptr<LogicalOperator> seam,
                   const CrossingWriteShaper &shape) {
	RequireTransaction(context, "ExecuteWrite");
	Planner planner(context);
	auto ref = SeamRefOfPlan(*planner.binder, std::move(seam));
	return ExecuteBuilt(context, planner, target, BuildWriteStatement(target, std::move(ref)), shape);
}

} // namespace duckdb
