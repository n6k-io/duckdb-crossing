#include "internal/row_policy.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

namespace {

optional_ptr<LogicalGet> TargetScan(LogicalOperator &write, TableCatalogEntry &table) {
	reference<LogicalOperator> op = *write.children[0];
	while (true) {
		if (op.get().type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.get().Cast<LogicalGet>();
			if (get.GetTable().get() == &table) {
				return &get;
			}
		}
		if (op.get().children.empty()) {
			throw InternalException("crossing: the write on '%s' has no scan of its target", table.name);
		}
		op = *op.get().children[0];
	}
}

class RowPolicies {
public:
	RowPolicies(Binder &binder_p, const string &catalog_p, const CrossingFloorResolver &resolver_p, CrossingVerb verb_p,
	            bool reads_rows_p)
	    : binder(binder_p), catalog(catalog_p), resolver(resolver_p), verb(verb_p), reads_rows(reads_rows_p) {
	}

	void Visit(unique_ptr<LogicalOperator> &op) {
		if (op->type == LogicalOperatorType::LOGICAL_UPDATE) {
			target = TargetScan(*op, op->Cast<LogicalUpdate>().table);
		} else if (op->type == LogicalOperatorType::LOGICAL_DELETE) {
			target = TargetScan(*op, op->Cast<LogicalDelete>().table);
		}
		if (op->type == LogicalOperatorType::LOGICAL_GET) {
			Police(op);
			return;
		}
		for (auto &child : op->children) {
			Visit(child);
		}
	}

private:
	void Police(unique_ptr<LogicalOperator> &op) {
		auto &get = op->Cast<LogicalGet>();
		auto table = get.GetTable();
		if (!table) {
			return;
		}
		CrossingFloor floor;
		auto &table_catalog = table->ParentCatalog().GetName().GetIdentifierName();
		floor.catalog = table_catalog == catalog ? string() : table_catalog;
		floor.schema = table->ParentSchema().name.GetIdentifierName();
		floor.table = table->name.GetIdentifierName();
		if (target.get() == &get) {
			floor.verb = verb;
			floor.reads_rows = reads_rows;
		}
		auto policy = resolver(floor);
		if (!policy) {
			return;
		}
		if (!get.projection_ids.empty()) {
			throw InternalException("crossing: a scan of '%s' is projected before its row policy", floor.table);
		}

		auto select = make_uniq<SelectNode>();
		select->select_list.push_back(make_uniq<StarExpression>());
		select->from_table = std::move(policy);
		SelectStatement statement;
		statement.node = std::move(select);
		auto policy_binder = Binder::CreateBinder(binder.context, &binder);
		auto bound = policy_binder->Bind(static_cast<SQLStatement &>(statement));

		auto &columns = get.returned_types;
		bool has_row_id = bound.names.size() == columns.size() + 1 && bound.names.back().GetIdentifierName() == "rowid";
		vector<LogicalType> policy_columns(bound.types.begin(),
		                                   bound.types.begin() + MinValue(bound.types.size(), columns.size()));
		if ((bound.names.size() != columns.size() && !has_row_id) || policy_columns != columns) {
			throw CatalogException("crossing: the row policy on '%s' does not produce the table's columns, in order, "
			                       "then rowid",
			                       floor.table);
		}

		auto bindings = bound.plan->GetColumnBindings();
		auto column = [&](idx_t index) {
			return make_uniq<BoundColumnRefExpression>(bound.types[index], bindings[index]);
		};
		auto row_id = [&]() {
			if (!has_row_id) {
				throw BinderException("crossing: the row policy on '%s' hides rowid, so a write cannot reach its rows",
				                      floor.table);
			}
			return column(columns.size());
		};
		vector<unique_ptr<Expression>> expressions;
		if (get.GetColumnIds().empty()) {
			expressions.push_back(row_id());
		}
		for (auto &index : get.GetColumnIds()) {
			if (index.IsRowIdColumn()) {
				expressions.push_back(row_id());
			} else if (index.IsVirtualColumn() || index.HasChildren()) {
				throw NotImplementedException("crossing: the row policy on '%s' cannot give the scan's column %llu",
				                              floor.table, index.GetPrimaryIndex());
			} else {
				expressions.push_back(column(index.GetPrimaryIndex()));
			}
		}
		auto projection = make_uniq<LogicalProjection>(get.table_index, std::move(expressions));
		projection->children.push_back(std::move(bound.plan));
		auto view_name = floor.schema + "." + floor.table + " row policy";
		op = make_uniq<LogicalSecureView>(view_name, std::move(projection));
	}

	Binder &binder;
	const string &catalog;
	const CrossingFloorResolver &resolver;
	CrossingVerb verb;
	bool reads_rows;
	optional_ptr<LogicalGet> target;
};

} // namespace

void ApplyRowPolicies(Binder &binder, unique_ptr<LogicalOperator> &plan, const string &catalog,
                      const CrossingFloorResolver &resolver, CrossingVerb verb, bool reads_rows) {
	if (!resolver) {
		return;
	}
	RowPolicies(binder, catalog, resolver, verb, reads_rows).Visit(plan);
}

} // namespace duckdb
