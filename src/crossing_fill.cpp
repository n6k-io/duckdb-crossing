#include "internal/crossing_fill.hpp"

#include "internal/crossing_read.hpp"
#include "internal/crossing_write.hpp"
#include "internal/fill.hpp"
#include "internal/names.hpp"
#include "internal/shaped_write.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

namespace {

optional_ptr<CrossingTableCatalogEntry> CrossingTableOfWrite(LogicalOperator &op, const CrossingIdentity &identity) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UPDATE:
		return CrossingTableOf(op.Cast<LogicalUpdate>().table, identity);
	case LogicalOperatorType::LOGICAL_DELETE:
		return CrossingTableOf(op.Cast<LogicalDelete>().table, identity);
	case LogicalOperatorType::LOGICAL_INSERT:
		return CrossingTableOf(op.Cast<LogicalInsert>().table, identity);
	default:
		return nullptr;
	}
}

unique_ptr<LogicalOperator> MakeNearNode(TableIndex table_index, unique_ptr<CrossingBindData> write,
                                         unique_ptr<LogicalOperator> rows, const FreshTableIndex &fresh) {
	vector<LogicalType> types {LogicalType::BIGINT};
	vector<Identifier> names {"Count"};
	if (!write->seam.returning_columns.empty()) {
		types = write->seam.returning_types;
		names = IdentifiersOf(write->seam.returning_columns);
	}
	vector<ColumnIndex> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		ids.push_back(ColumnIndex(i));
	}
	auto get = make_uniq<LogicalGet>(table_index, BoundTableFunction(CrossingWriteFunction()), std::move(write),
	                                 std::move(types), std::move(names));
	get->SetColumnIds(std::move(ids));
	if (rows) {
		get->children.push_back(MakeNearChild(std::move(rows), fresh));
	}
	return std::move(get);
}

void FillFromFeed(ClientContext &context, unique_ptr<LogicalOperator> &node, CrossingTableCatalogEntry &table,
                  CrossingVerb verb, CrossingSeam seam, shared_ptr<CrossingFragment> fragment,
                  unique_ptr<LogicalOperator> feed, const SeamObstacle &stop, const FreshTableIndex &fresh) {
	auto table_index = TableIndexOf(*node);
	auto fill = FillSeamFromFeed(std::move(feed), *fragment, table.source, stop, fresh,
	                             Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(context));
	auto write = MakeWriteBindData(table, verb, std::move(seam), std::move(fragment));
	write->obstacle = std::move(fill.obstacle);
	node = MakeNearNode(table_index, std::move(write), std::move(fill.remainder), fresh);
}

void FillShaped(ClientContext &context, unique_ptr<LogicalOperator> &node, CrossingTableCatalogEntry &table,
                shared_ptr<ShapedWrite> shaped, const FreshTableIndex &fresh) {
	auto verb = VerbOf(*node);
	if (shaped->reads_target || ReturnsRows(*node)) {
		RequireVerb(table, CrossingVerb::SELECT);
	}
	CrossingSeam seam;
	seam.types = shaped->source_types;
	seam.set_columns = shaped->set_columns;
	if (ReturnsRows(*node)) {
		if (!table.described.returning) {
			throw BinderException("crossing: the source of '%s' cannot return rows, so RETURNING is refused",
			                      table.name.GetIdentifierName());
		}
		seam.returning_columns = TableColumnNames(table);
		seam.returning_types = TableRowTypes(table);
	}
	auto table_index = TableIndexOf(*node);
	if (!shaped->has_source) {
		auto fragment = PlanBareWriteFragment(table, verb, seam, fresh());
		auto write = MakeWriteBindData(table, verb, std::move(seam), std::move(fragment));
		write->shaped = std::move(shaped);
		node = MakeNearNode(table_index, std::move(write), nullptr, fresh);
		return;
	}
	auto fragment = PlanWriteFragment(table, verb, seam);
	auto fill = FillSeamFromFeed(std::move(node->children[0]), *fragment, table.source, SeamObstacle(), fresh,
	                             Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(context));
	auto write = MakeWriteBindData(table, verb, std::move(seam), std::move(fragment));
	write->obstacle = std::move(fill.obstacle);
	write->shaped = std::move(shaped);
	node = MakeNearNode(table_index, std::move(write), std::move(fill.remainder), fresh);
}

void FillWrite(ClientContext &context, unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh,
               const CrossingIdentity &identity) {
	auto table = CrossingTableOfWrite(*node, identity);
	if (!table) {
		return;
	}
	auto verb = VerbOf(*node);
	RequireVerb(*table, verb);
	if (auto shaped = ShapedWrites::Of(context).Take(*node)) {
		FillShaped(context, node, *table, std::move(shaped), fresh);
		return;
	}
	if (node->children.size() != 1) {
		throw NotImplementedException("crossing: %s without a source of rows", CrossingVerbName(verb));
	}

	auto seam = SeamOf(*table, table->described, verb, SetColumnsOf(*node, *table));
	if (ReturnsRows(*node)) {
		if (!table->described.returning) {
			throw BinderException("crossing: the source of '%s' cannot return rows, so RETURNING is refused",
			                      table->name.GetIdentifierName());
		}
		seam.returning_columns = TableColumnNames(*table);
		seam.returning_types = TableRowTypes(*table);
	}
	auto fragment = PlanWriteFragment(*table, verb, seam);
	vector<column_t> keys {COLUMN_IDENTIFIER_ROW_ID};
	MaterialiseConstantRows(context, node->children[0]);
	auto feed = FeedOf(*node, keys, std::move(node->children[0]), fresh);

	FillFromFeed(context, node, *table, verb, std::move(seam), std::move(fragment), std::move(feed.plan),
	             SeamObstacle(), fresh);
}

optional_ptr<LogicalGet> OwnScan(LogicalOperator &op, CrossingTableCatalogEntry &table,
                                 const CrossingIdentity &identity) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	auto data = CrossingBindDataOf(get, identity);
	if (!data || &data->table != &table || get.table_filters.HasFilters()) {
		return nullptr;
	}
	return &get;
}

//! An update's values, in SET order, when none of them reads the table; empty otherwise.
vector<unique_ptr<Expression>> ValuesReadingNoRow(LogicalUpdate &update, CrossingTableCatalogEntry &table,
                                                  const CrossingIdentity &identity) {
	vector<unique_ptr<Expression>> values;
	if (update.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return values;
	}
	auto &projection = update.children[0]->Cast<LogicalProjection>();
	if (!OwnScan(*projection.children[0], table, identity)) {
		return values;
	}
	for (idx_t i = 0; i < update.expressions.size(); i++) {
		auto &expr = *update.expressions[i];
		unique_ptr<Expression> value;
		if (expr.GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			value = update.bound_defaults[update.columns[i].index]->Copy();
		} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &binding = expr.Cast<BoundColumnRefExpression>().Binding();
			if (binding.table_index != projection.table_index) {
				return {};
			}
			value = projection.expressions[binding.column_index]->Copy();
		} else {
			return {};
		}
		if (!value->IsFoldable() || !IsColumnFreeExpression(*value)) {
			return {};
		}
		values.push_back(std::move(value));
	}
	return values;
}

unique_ptr<LogicalOperator> OneRowOf(ClientContext &context, vector<unique_ptr<Expression>> values,
                                     const vector<LogicalType> &types, const FreshTableIndex &fresh) {
	for (idx_t i = 0; i < values.size(); i++) {
		if (values[i]->GetReturnType() != types[i]) {
			values[i] = BoundCastExpression::AddDefaultCastToType(std::move(values[i]), types[i]);
		}
	}
	vector<vector<unique_ptr<Expression>>> rows;
	rows.push_back(std::move(values));
	unique_ptr<LogicalOperator> row = make_uniq<LogicalExpressionGet>(fresh(), types, std::move(rows));
	MaterialiseConstantRows(context, row);
	row->ResolveOperatorTypes();
	return row;
}

bool HoldsCrossingWrite(LogicalOperator &op, const CrossingIdentity &identity) {
	if (CrossingTableOfWrite(op, identity)) {
		return true;
	}
	if (op.type == LogicalOperatorType::LOGICAL_MERGE_INTO &&
	    CrossingTableOf(op.Cast<LogicalMergeInto>().table, identity)) {
		return true;
	}
	for (auto &child : op.children) {
		if (HoldsCrossingWrite(*child, identity)) {
			return true;
		}
	}
	return false;
}

bool SetsItself(TableCatalogEntry &table, LogicalProjection &projection, PhysicalIndex set_column,
                const Expression &value) {
	if (value.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &set = value.Cast<BoundColumnRefExpression>().Binding();
	if (set.table_index != projection.table_index) {
		return false;
	}
	auto &projected = *projection.expressions[set.column_index];
	if (projected.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &read = projected.Cast<BoundColumnRefExpression>().Binding();
	auto get = FindGetWithIndex(*projection.children[0], read.table_index);
	if (!get || get->GetTable().get() != &table || read.column_index >= get->GetColumnIds().size()) {
		return false;
	}
	auto &id = get->GetColumnIds()[read.column_index];
	if (id.IsVirtualColumn() || id.GetPrimaryIndex() >= table.GetColumns().LogicalColumnCount()) {
		return false;
	}
	return table.GetColumns().GetColumn(LogicalIndex(id.GetPrimaryIndex())).Physical() == set_column;
}

//! `SET v = v` alone is left as it is: the statement still has to reach every row.
void NarrowSets(TableCatalogEntry &table, LogicalProjection &projection, vector<PhysicalIndex> &columns,
                vector<unique_ptr<Expression>> &expressions) {
	vector<idx_t> kept;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (!SetsItself(table, projection, columns[i], *expressions[i])) {
			kept.push_back(i);
		}
	}
	if (kept.empty() || kept.size() == columns.size()) {
		return;
	}
	vector<PhysicalIndex> kept_columns;
	vector<unique_ptr<Expression>> kept_expressions;
	for (auto i : kept) {
		kept_columns.push_back(columns[i]);
		kept_expressions.push_back(std::move(expressions[i]));
	}
	columns = std::move(kept_columns);
	expressions = std::move(kept_expressions);
}

bool HoldsSourceDefault(LogicalOperator &op) {
	bool found = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&found](unique_ptr<Expression> *expr) { found |= IsSourceDefault(**expr); });
	for (auto &child : op.children) {
		found |= HoldsSourceDefault(*child);
	}
	return found;
}

//! DuckDB binds ON CONFLICT as a merge whose source computes the columns left out, as `excluded` may read
//! them. A merge's own values sit in the projection directly under it, so only what is below that counts.
void RequireConflictClauseNamesEveryColumn(LogicalMergeInto &merge, const CrossingIdentity &identity) {
	if (!CrossingTableOf(merge.table, identity) || merge.children.size() != 1) {
		return;
	}
	for (auto &child : merge.children[0]->children) {
		if (HoldsSourceDefault(*child)) {
			throw BinderException("crossing: ON CONFLICT on '%s' must give every column a value: only the source "
			                      "knows the default of a column left out",
			                      merge.table.name.GetIdentifierName());
		}
	}
}

//! Builds the write CrossingWriteTarget::reads_rows describes. An update's values are worked out once, here.
void FillBareWrite(ClientContext &context, unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh,
                   const CrossingIdentity &identity) {
	if (node->type == LogicalOperatorType::LOGICAL_INSERT) {
		auto table = CrossingTableOfWrite(*node, identity);
		if (table && !node->Cast<LogicalInsert>().column_index_map.empty() && SetColumnsOf(*node, *table).empty()) {
			RequireVerb(*table, CrossingVerb::INSERT);
			auto fragment = PlanBareWriteFragment(*table, CrossingVerb::INSERT, CrossingSeam(), fresh());
			node = MakeNearNode(TableIndexOf(*node),
			                    MakeWriteBindData(*table, CrossingVerb::INSERT, CrossingSeam(), std::move(fragment)),
			                    nullptr, fresh);
		}
		return;
	}
	if (node->type != LogicalOperatorType::LOGICAL_UPDATE && node->type != LogicalOperatorType::LOGICAL_DELETE) {
		return;
	}
	auto table = CrossingTableOfWrite(*node, identity);
	if (!table || ReturnsRows(*node) || node->children.size() != 1 ||
	    ShapedWrites::Of(context).writes.count(node.get())) {
		return;
	}
	auto verb = VerbOf(*node);
	vector<unique_ptr<Expression>> values;
	if (verb == CrossingVerb::UPDATE) {
		values = ValuesReadingNoRow(node->Cast<LogicalUpdate>(), *table, identity);
		if (values.empty()) {
			return;
		}
	} else if (!OwnScan(*node->children[0], *table, identity)) {
		return;
	}
	RequireVerb(*table, verb);

	CrossingSeam seam;
	seam.set_columns = SetColumnsOf(*node, *table);
	seam.types = ColumnTypesByName(*table, seam.set_columns);
	if (verb == CrossingVerb::DELETE_) {
		auto fragment = PlanBareWriteFragment(*table, verb, seam, fresh());
		node = MakeNearNode(TableIndexOf(*node), MakeWriteBindData(*table, verb, std::move(seam), std::move(fragment)),
		                    nullptr, fresh);
		return;
	}
	auto fragment = PlanWriteFragment(*table, verb, seam);
	auto feed = OneRowOf(context, std::move(values), seam.types, fresh);
	FillFromFeed(context, node, *table, verb, std::move(seam), std::move(fragment), std::move(feed), SeamObstacle(),
	             fresh);
}

} // namespace

void RefuseWritesInCtes(LogicalOperator &plan, const CrossingIdentity &identity) {
	for (auto &child : plan.children) {
		RefuseWritesInCtes(*child, identity);
	}
	if (plan.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && HoldsCrossingWrite(*plan.children[0], identity)) {
		throw BinderException("crossing: a write to a source's table inside a WITH clause is not supported");
	}
}

void NarrowSetsToWhatChanges(LogicalOperator &plan, const CrossingIdentity &identity) {
	for (auto &child : plan.children) {
		NarrowSetsToWhatChanges(*child, identity);
	}
	if (plan.children.size() != 1 || plan.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = plan.children[0]->Cast<LogicalProjection>();
	if (plan.type == LogicalOperatorType::LOGICAL_UPDATE) {
		auto &update = plan.Cast<LogicalUpdate>();
		if (CrossingTableOf(update.table, identity)) {
			NarrowSets(update.table, projection, update.columns, update.expressions);
		}
		return;
	}
	if (plan.type != LogicalOperatorType::LOGICAL_MERGE_INTO) {
		return;
	}
	auto &merge = plan.Cast<LogicalMergeInto>();
	if (!CrossingTableOf(merge.table, identity)) {
		return;
	}
	for (auto &entry : merge.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE) {
				NarrowSets(merge.table, projection, action->columns, action->expressions);
			}
		}
	}
}

void LeaveInsertDefaultsToSource(LogicalOperator &plan, const CrossingIdentity &identity) {
	for (auto &child : plan.children) {
		LeaveInsertDefaultsToSource(*child, identity);
	}
	if (plan.type == LogicalOperatorType::LOGICAL_MERGE_INTO) {
		RequireConflictClauseNamesEveryColumn(plan.Cast<LogicalMergeInto>(), identity);
		return;
	}
	if (plan.type != LogicalOperatorType::LOGICAL_INSERT) {
		return;
	}
	auto &insert = plan.Cast<LogicalInsert>();
	if (!CrossingTableOf(insert.table, identity) || insert.children.size() != 1 ||
	    insert.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = insert.children[0]->Cast<LogicalProjection>();
	bool any = false;
	for (auto &expr : projection.expressions) {
		any |= IsSourceDefault(*expr);
	}
	if (!any) {
		return;
	}
	vector<unique_ptr<Expression>> named;
	for (auto &expr : projection.expressions) {
		if (IsSourceDefault(*expr)) {
			insert.column_index_map.push_back(DConstants::INVALID_INDEX);
		} else {
			insert.column_index_map.push_back(named.size());
			named.push_back(std::move(expr));
		}
	}
	projection.expressions = std::move(named);
}

void FillBareWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                    const CrossingIdentity &identity) {
	for (auto &child : plan->children) {
		FillBareWrites(context, child, fresh, identity);
	}
	FillBareWrite(context, plan, fresh, identity);
}

void FillWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                const CrossingIdentity &identity) {
	for (auto &child : plan->children) {
		FillWrites(context, child, fresh, identity);
	}
	FillWrite(context, plan, fresh, identity);
}

CrossingVerb VerbOf(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		return CrossingVerb::DELETE_;
	case LogicalOperatorType::LOGICAL_INSERT:
		return CrossingVerb::INSERT;
	default:
		return CrossingVerb::UPDATE;
	}
}

bool ReturnsRows(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		return op.Cast<LogicalDelete>().return_chunk;
	case LogicalOperatorType::LOGICAL_INSERT:
		return op.Cast<LogicalInsert>().return_chunk;
	default:
		return op.Cast<LogicalUpdate>().return_chunk;
	}
}

TableIndex TableIndexOf(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		return op.Cast<LogicalDelete>().table_index;
	case LogicalOperatorType::LOGICAL_INSERT:
		return op.Cast<LogicalInsert>().table_index;
	default:
		return op.Cast<LogicalUpdate>().table_index;
	}
}

vector<string> SetColumnsOf(LogicalOperator &op, TableCatalogEntry &table) {
	vector<string> out;
	if (op.type == LogicalOperatorType::LOGICAL_INSERT) {
		auto &named = op.Cast<LogicalInsert>().column_index_map;
		if (named.empty()) {
			return TableColumnNames(table);
		}
		for (auto &column : table.GetColumns().Physical()) {
			if (named[column.Physical()] != DConstants::INVALID_INDEX) {
				out.push_back(column.GetName().GetIdentifierName());
			}
		}
		return out;
	}
	if (op.type != LogicalOperatorType::LOGICAL_UPDATE) {
		return out;
	}
	for (auto &column_index : op.Cast<LogicalUpdate>().columns) {
		out.push_back(table.GetColumns().GetColumn(column_index).GetName().GetIdentifierName());
	}
	return out;
}

} // namespace duckdb
