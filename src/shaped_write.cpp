#include "internal/shaped_write.hpp"

#include "internal/crossing_table_entry.hpp"
#include "internal/crossing_write.hpp"
#include "internal/fill.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

ShapedWrites &ShapedWrites::Of(ClientContext &context) {
	return *context.registered_state->GetOrCreate<ShapedWrites>(KEY);
}

shared_ptr<ShapedWrite> ShapedWrites::Take(const LogicalOperator &op) {
	auto found = writes.find(&op);
	if (found == writes.end()) {
		return nullptr;
	}
	auto shaped = found->second;
	writes.erase(found);
	return shaped;
}

namespace {

struct WriteScope {
	optional_ptr<LogicalGet> target;
	vector<ColumnBinding> source_bindings;
	CrossingSource *source = nullptr;
	bool reads_target = false;
	bool failed = false;
};

void RebindInto(unique_ptr<Expression> &expr, WriteScope &scope) {
	ExpressionIterator::EnumerateExpression(expr, [&](unique_ptr<Expression> &child) {
		if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return;
		}
		auto &ref = child->Cast<BoundColumnRefExpression>();
		auto &binding = ref.BindingMutable();
		if (binding.table_index == scope.target->table_index) {
			auto &id = scope.target->GetColumnIds()[binding.column_index.GetIndex()];
			if (id.IsVirtualColumn()) {
				scope.failed = true;
				return;
			}
			scope.reads_target = true;
			binding = ColumnBinding(TableIndex(SHAPED_TARGET_INDEX), ProjectionIndex(id.GetPrimaryIndex()));
			return;
		}
		for (idx_t p = 0; p < scope.source_bindings.size(); p++) {
			if (scope.source_bindings[p] == binding) {
				binding = ColumnBinding(TableIndex(SHAPED_SOURCE_INDEX), ProjectionIndex(p));
				return;
			}
		}
		scope.failed = true;
	});
}

unique_ptr<Expression> Rebound(const Expression &expr, WriteScope &scope) {
	auto copy = expr.Copy();
	RebindInto(copy, scope);
	if (scope.failed || !CanEvaluate(*copy, *scope.source)) {
		scope.failed = true;
	}
	return copy;
}

//! The projection's expression an operator reads through it, or the expression itself.
unique_ptr<Expression> ThroughProjection(const Expression &expr, optional_ptr<LogicalProjection> projection,
                                         WriteScope &scope) {
	auto copy = expr.Copy();
	if (projection) {
		ExpressionIterator::EnumerateExpression(copy, [&](unique_ptr<Expression> &child) {
			if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return;
			}
			auto &binding = child->Cast<BoundColumnRefExpression>().Binding();
			if (binding.table_index == projection->table_index) {
				child = projection->expressions[binding.column_index.GetIndex()]->Copy();
			}
		});
	}
	return Rebound(*copy, scope);
}

unique_ptr<Expression> AndAll(vector<unique_ptr<Expression>> parts) {
	unique_ptr<Expression> all;
	for (auto &part : parts) {
		all = all ? make_uniq_base<Expression, BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
		                                                                   std::move(all), std::move(part))
		          : std::move(part);
	}
	return all;
}

bool IsTargetScan(LogicalOperator &op, TableCatalogEntry &table) {
	return op.type == LogicalOperatorType::LOGICAL_GET && op.Cast<LogicalGet>().GetTable().get() == &table;
}

//! Moves the source below the write under an identity projection, so the source's columns have fixed positions.
void PlaceSource(LogicalOperator &write, unique_ptr<LogicalOperator> rest, ShapedWrite &shaped,
                 const FreshTableIndex &fresh) {
	write.children.clear();
	if (!rest) {
		return;
	}
	rest->ResolveOperatorTypes();
	shaped.has_source = true;
	shaped.source_types = rest->types;
	auto identity = LogicalProjection::CreateIdentity(fresh(), std::move(rest));
	write.children.push_back(std::move(identity));
}

//! `child` is the write's input below any projection: filters over the target, or over it crossed with a source.
bool SplitTargetAndSource(unique_ptr<LogicalOperator> &child, TableCatalogEntry &table, WriteScope &scope,
                          vector<reference<Expression>> &filters, unique_ptr<LogicalOperator> *&rest) {
	auto node = &child;
	while ((*node)->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = (*node)->Cast<LogicalFilter>();
		if (filter.HasProjectionMap()) {
			return false;
		}
		for (auto &expr : filter.expressions) {
			filters.push_back(*expr);
		}
		node = &filter.children[0];
	}
	if (IsTargetScan(**node, table)) {
		scope.target = (*node)->Cast<LogicalGet>();
		rest = nullptr;
		return true;
	}
	if ((*node)->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT || !IsTargetScan(*(*node)->children[0], table)) {
		return false;
	}
	scope.target = (*node)->children[0]->Cast<LogicalGet>();
	rest = &(*node)->children[1];
	(*rest)->ResolveOperatorTypes();
	scope.source_bindings = (*rest)->GetColumnBindings();
	return true;
}

bool ShapeUpdate(LogicalUpdate &update, CrossingTableCatalogEntry &table, ShapedWrite &shaped,
                 unique_ptr<LogicalOperator> &source, CrossingSource &crossing) {
	if (update.children.size() != 1 || update.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto &projection = update.children[0]->Cast<LogicalProjection>();
	WriteScope scope;
	scope.source = &crossing;
	vector<reference<Expression>> filters;
	unique_ptr<LogicalOperator> *rest = nullptr;
	if (!SplitTargetAndSource(projection.children[0], table, scope, filters, rest)) {
		return false;
	}
	vector<unique_ptr<Expression>> parts;
	for (auto &filter : filters) {
		parts.push_back(Rebound(filter.get(), scope));
	}
	for (idx_t i = 0; i < update.columns.size() && i < update.expressions.size(); i++) {
		auto &expr = *update.expressions[i];
		if (expr.GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			return false;
		}
		shaped.set_columns.push_back(table.GetColumns().GetColumn(update.columns[i]).GetName().GetIdentifierName());
		shaped.values.push_back(ThroughProjection(expr, &projection, scope));
	}
	if (scope.failed) {
		return false;
	}
	shaped.condition = AndAll(std::move(parts));
	shaped.reads_target = scope.reads_target;
	if (rest) {
		source = std::move(*rest);
	}
	return true;
}

bool ShapeDelete(LogicalDelete &del, CrossingTableCatalogEntry &table, ShapedWrite &shaped,
                 unique_ptr<LogicalOperator> &source, CrossingSource &crossing) {
	if (del.children.size() != 1) {
		return false;
	}
	WriteScope scope;
	scope.source = &crossing;
	vector<reference<Expression>> filters;
	unique_ptr<LogicalOperator> *rest = nullptr;
	if (!SplitTargetAndSource(del.children[0], table, scope, filters, rest)) {
		return false;
	}
	vector<unique_ptr<Expression>> parts;
	for (auto &filter : filters) {
		parts.push_back(Rebound(filter.get(), scope));
	}
	if (scope.failed) {
		return false;
	}
	shaped.condition = AndAll(std::move(parts));
	shaped.reads_target = scope.reads_target;
	if (rest) {
		source = std::move(*rest);
	}
	return true;
}

bool ShapeMerge(LogicalMergeInto &merge, CrossingTableCatalogEntry &table, ShapedWrite &shaped,
                unique_ptr<LogicalOperator> &source, CrossingSource &crossing) {
	if (merge.children.size() != 1 || merge.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION ||
	    merge.source_marker.IsValid()) {
		return false;
	}
	auto &projection = merge.children[0]->Cast<LogicalProjection>();
	auto &join = projection.children[0];
	if (join->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    join->type != LogicalOperatorType::LOGICAL_ANY_JOIN) {
		return false;
	}
	idx_t source_side;
	if (IsTargetScan(*join->children[1], table)) {
		source_side = 0;
	} else if (IsTargetScan(*join->children[0], table)) {
		source_side = 1;
	} else {
		return false;
	}
	WriteScope scope;
	scope.source = &crossing;
	scope.target = join->children[1 - source_side]->Cast<LogicalGet>();
	join->children[source_side]->ResolveOperatorTypes();
	scope.source_bindings = join->children[source_side]->GetColumnBindings();

	vector<unique_ptr<Expression>> parts;
	if (join->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		parts.push_back(Rebound(*join->Cast<LogicalAnyJoin>().condition, scope));
	} else {
		auto &comparison = join->Cast<LogicalComparisonJoin>();
		for (auto &condition : comparison.conditions) {
			if (condition.IsComparison()) {
				auto compare = BoundComparisonExpression::Create(condition.GetComparisonType(),
				                                                 condition.GetLHS().Copy(), condition.GetRHS().Copy());
				parts.push_back(Rebound(*compare, scope));
			} else {
				parts.push_back(Rebound(condition.GetJoinExpression(), scope));
			}
		}
	}
	auto names = TableColumnNames(table);
	for (auto &entry : merge.actions) {
		for (auto &action : entry.second) {
			ShapedAction shaped_action;
			shaped_action.when = entry.first;
			shaped_action.type = action->action_type;
			if (action->condition) {
				shaped_action.condition = ThroughProjection(*action->condition, &projection, scope);
			}
			if (action->action_type == MergeActionType::MERGE_UPDATE) {
				for (idx_t i = 0; i < action->columns.size() && i < action->expressions.size(); i++) {
					auto &expr = *action->expressions[i];
					if (expr.GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
						return false;
					}
					shaped_action.columns.push_back(
					    table.GetColumns().GetColumn(action->columns[i]).GetName().GetIdentifierName());
					shaped_action.values.push_back(ThroughProjection(expr, &projection, scope));
				}
			} else if (action->action_type == MergeActionType::MERGE_INSERT) {
				for (idx_t c = 0; c < action->expressions.size(); c++) {
					if (IsSourceDefault(*action->expressions[c])) {
						continue;
					}
					shaped_action.columns.push_back(names[c]);
					shaped_action.values.push_back(ThroughProjection(*action->expressions[c], &projection, scope));
				}
			} else if (action->action_type != MergeActionType::MERGE_DELETE &&
			           action->action_type != MergeActionType::MERGE_DO_NOTHING) {
				return false;
			}
			shaped.actions.push_back(std::move(shaped_action));
		}
	}
	if (scope.failed) {
		return false;
	}
	shaped.merge = true;
	shaped.condition = AndAll(std::move(parts));
	shaped.reads_target = true;
	source = std::move(join->children[source_side]);
	return true;
}

void ClearMergeExpressions(LogicalMergeInto &merge) {
	for (auto &entry : merge.actions) {
		for (auto &action : entry.second) {
			action->condition = nullptr;
			action->expressions.clear();
		}
	}
}

} // namespace

void ShapeWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                 const CrossingIdentity &identity) {
	for (auto &child : plan->children) {
		ShapeWrites(context, child, fresh, identity);
	}
	optional_ptr<TableCatalogEntry> written;
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UPDATE:
		written = plan->Cast<LogicalUpdate>().table;
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		written = plan->Cast<LogicalDelete>().table;
		break;
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		written = plan->Cast<LogicalMergeInto>().table;
		break;
	default:
		return;
	}
	auto table = CrossingTableOf(*written, identity);
	if (!table) {
		return;
	}
	auto shaped = make_shared_ptr<ShapedWrite>();
	unique_ptr<LogicalOperator> source;
	bool ok;
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UPDATE:
		ok = ShapeUpdate(plan->Cast<LogicalUpdate>(), *table, *shaped, source, table->source);
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		ok = ShapeDelete(plan->Cast<LogicalDelete>(), *table, *shaped, source, table->source);
		break;
	default:
		ok = ShapeMerge(plan->Cast<LogicalMergeInto>(), *table, *shaped, source, table->source);
		if (ok) {
			ClearMergeExpressions(plan->Cast<LogicalMergeInto>());
		}
		break;
	}
	if (!ok) {
		return;
	}
	PlaceSource(*plan, std::move(source), *shaped, fresh);
	ShapedWrites::Of(context).writes[plan.get()] = std::move(shaped);
}

} // namespace duckdb
