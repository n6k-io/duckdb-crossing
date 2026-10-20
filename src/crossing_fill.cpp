#include "internal/crossing_fill.hpp"

#include "internal/crossing_read.hpp"
#include "internal/crossing_write.hpp"
#include "internal/fill.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
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

unique_ptr<LogicalOperator> MakeFence(idx_t table_index, unique_ptr<CrossingBindData> write) {
	auto get = make_uniq<LogicalGet>(table_index, CrossingWriteFunction(), std::move(write),
	                                 vector<LogicalType> {LogicalType::BIGINT}, vector<string> {"Count"});
	get->SetColumnIds({ColumnIndex(0)});
	return std::move(get);
}

//! The seam insert reads its child by position, but the optimizers that run after this pass reorder by
//! binding: a build/probe swap of a cross product flips its columns. A projection holds the order.
unique_ptr<LogicalOperator> InSeamOrder(unique_ptr<LogicalOperator> rows, const FreshTableIndex &fresh) {
	rows->ResolveOperatorTypes();
	auto bindings = rows->GetColumnBindings();
	vector<unique_ptr<Expression>> columns;
	for (idx_t i = 0; i < bindings.size(); i++) {
		columns.push_back(make_uniq<BoundColumnRefExpression>(rows->types[i], bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(fresh(), std::move(columns));
	projection->children.push_back(std::move(rows));
	projection->ResolveOperatorTypes();
	return std::move(projection);
}

unique_ptr<LogicalOperator> SeamInsert(ClientContext &context, idx_t table_index, unique_ptr<CrossingBindData> write,
                                       bool returning, vector<idx_t> key_positions, SeamFill fill,
                                       const FreshTableIndex &fresh) {
	auto &table = write->table;
	CreateTableInfo info(table.schema, table.name);
	if (returning) {
		info.columns = table.GetColumns().Copy();
	} else {
		for (idx_t c = 0; c < fill.seam_types.size(); c++) {
			info.columns.AddColumn(ColumnDefinition("c" + to_string(c), fill.seam_types[c]));
		}
	}
	auto entry = make_shared_ptr<CrossingSeamEntry>(table.catalog, table.schema, info, std::move(write),
	                                                std::move(fill.obstacle));
	entry->key_positions = std::move(key_positions);
	CrossingSeamEntries::Get(context)->Hold(entry);

	auto insert = make_uniq<LogicalInsert>(*entry, table_index);
	insert->return_chunk = returning;
	insert->expected_types = std::move(fill.seam_types);
	insert->children.push_back(InSeamOrder(std::move(fill.remainder), fresh));
	return std::move(insert);
}

void FillFromFeed(ClientContext &context, unique_ptr<LogicalOperator> &node, CrossingTableCatalogEntry &table,
                  CrossingVerb verb, CrossingSeam seam, shared_ptr<CrossingFragment> fragment,
                  unique_ptr<LogicalOperator> feed, const SeamObstacle &stop, bool returning,
                  vector<idx_t> key_positions, const FreshTableIndex &fresh) {
	auto table_index = TableIndexOf(*node);
	auto fill = FillSeamFromFeed(std::move(feed), *fragment, table.source, stop, fresh);
	auto write = MakeWriteBindData(table, verb, std::move(seam), std::move(fragment));
	if (!fill.remainder) {
		node = MakeFence(table_index, std::move(write));
		return;
	}
	node =
	    SeamInsert(context, table_index, std::move(write), returning, std::move(key_positions), std::move(fill), fresh);
}

void FillWrite(ClientContext &context, unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh,
               const CrossingIdentity &identity) {
	auto table = CrossingTableOfWrite(*node, identity);
	if (!table) {
		return;
	}
	auto verb = VerbOf(*node);
	RequireVerb(*table, verb);
	if (verb != CrossingVerb::INSERT) {
		RequireKey(*table, table->described, verb == CrossingVerb::UPDATE ? "an update" : "a delete");
	}
	if (node->children.size() != 1) {
		throw NotImplementedException("crossing: %s without a source of rows", CrossingVerbName(verb));
	}

	auto seam = SeamOf(*table, table->described, verb, SetColumnsOf(*node, *table));
	auto fragment = PlanWriteFragment(*table, verb, seam);
	auto keys = table->KeyColumnIndexes();
	auto returning = ReturnsRows(*node);
	MaterialiseConstantRows(context, node->children[0]);
	auto feed = FeedOf(*node, keys, std::move(node->children[0]), fresh);

	SeamObstacle stop;
	vector<idx_t> key_positions;
	if (verb != CrossingVerb::INSERT && !table->described.key_unique) {
		stop.node = feed.plan.get();
		stop.obstacle = "the source does not vouch for the key's uniqueness";
		for (idx_t k = 0; k < keys.size(); k++) {
			key_positions.push_back(returning ? keys[k] : k);
		}
	}
	if (returning) {
		stop.node = feed.image;
		stop.obstacle = "RETURNING needs the rows on the target";
	}

	FillFromFeed(context, node, *table, verb, std::move(seam), std::move(fragment), std::move(feed.plan), stop,
	             returning, std::move(key_positions), fresh);
}

optional_ptr<LogicalGet> OwnScan(LogicalOperator &op, CrossingTableCatalogEntry &table,
                                 const CrossingIdentity &identity) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	auto data = CrossingBindDataOf(get, identity);
	if (!data || &data->table != &table || !get.table_filters.filters.empty()) {
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
			auto &binding = expr.Cast<BoundColumnRefExpression>().binding;
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
		if (values[i]->return_type != types[i]) {
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

//! An update or delete that reads nothing of the rows it writes addresses every row of the table, so
//! it carries no key and its source runs it unnarrowed. An update's values are worked out once, here.
void FillBareWrite(ClientContext &context, unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh,
                   const CrossingIdentity &identity) {
	if (node->type != LogicalOperatorType::LOGICAL_UPDATE && node->type != LogicalOperatorType::LOGICAL_DELETE) {
		return;
	}
	auto table = CrossingTableOfWrite(*node, identity);
	if (!table || ReturnsRows(*node) || node->children.size() != 1) {
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
		// Nothing goes in: DuckDB has no scan of zero columns to stand for a seam, so the source is not
		// asked for a plan and the query carries a placeholder.
		auto fragment = make_shared_ptr<CrossingFragment>();
		fragment->plan = make_uniq<LogicalDummyScan>(fresh());
		fragment->ResolveTypesAndText();
		node = MakeFence(TableIndexOf(*node), MakeWriteBindData(*table, verb, std::move(seam), std::move(fragment)));
		return;
	}
	auto fragment = PlanWriteFragment(*table, verb, seam);
	auto feed = OneRowOf(context, std::move(values), seam.types, fresh);
	FillFromFeed(context, node, *table, verb, std::move(seam), std::move(fragment), std::move(feed), SeamObstacle(),
	             false, {}, fresh);
}

} // namespace

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

idx_t TableIndexOf(LogicalOperator &op) {
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
	if (op.type != LogicalOperatorType::LOGICAL_UPDATE) {
		return out;
	}
	for (auto &column_index : op.Cast<LogicalUpdate>().columns) {
		out.push_back(table.GetColumns().GetColumn(column_index).GetName());
	}
	return out;
}

} // namespace duckdb
