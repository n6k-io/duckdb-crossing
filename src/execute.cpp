#include "internal/execute.hpp"

#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

namespace {

idx_t MaxTableIndex(const LogicalOperator &op) {
	idx_t result = 0;
	for (auto &child : op.children) {
		result = MaxValue(result, MaxTableIndex(*child));
	}
	for (auto index : op.GetTableIndex()) {
		result = MaxValue(result, index.index);
	}
	return result;
}

//! Copying a RelationStatement shares the relation, so the plan is never copied. Bound once: the
//! plan is handed over on the first Bind.
class PlanRelation : public Relation {
public:
	PlanRelation(const shared_ptr<ClientContext> &context, unique_ptr<LogicalOperator> plan_p)
	    : Relation(context, RelationType::QUERY_RELATION), plan(std::move(plan_p)) {
		plan->ResolveOperatorTypes();
		for (idx_t i = 0; i < plan->types.size(); i++) {
			columns.emplace_back(Identifier("col" + to_string(i)), plan->types[i]);
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
		while (binder.GenerateTableIndex().index < highest) {
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

} // namespace

unique_ptr<QueryResult> SubmitCrossingPlan(ClientContext &context, unique_ptr<LogicalOperator> plan) {
	auto relation = make_shared_ptr<PlanRelation>(context.shared_from_this(), std::move(plan));
	return context.Submit(relation, QueryParameters());
}

} // namespace duckdb
