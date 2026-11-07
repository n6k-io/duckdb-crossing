#pragma once

#include "internal/names.hpp"

#include "catch.hpp"
#include "internal/carrier.hpp"
#include "internal/function_tag.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <map>

namespace duckdb {

const TableIndex MEMORY_FLOOR_INDEX = TableIndex(0);
const TableIndex MEMORY_CROSSING_INDEX = TableIndex(1);
const TableIndex MEMORY_GET_INDEX = TableIndex(2);

inline const CrossingIdentity &MemoryIdentity() {
	static const CrossingIdentity identity;
	return identity;
}

inline const CrossingIdentity &OtherIdentity() {
	static const CrossingIdentity identity;
	return identity;
}

inline string FunctionNameOf(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return expr.Cast<BoundFunctionExpression>().Function().GetName().GetIdentifierName();
	case ExpressionClass::BOUND_AGGREGATE:
		return expr.Cast<BoundAggregateExpression>().Function().GetName().GetIdentifierName();
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		if (window.AggregateFunction()) {
			return window.AggregateFunction()->GetName().GetIdentifierName();
		}
		return StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType()));
	}
	default:
		return string();
	}
}

inline unique_ptr<Expression> BoundAmt() {
	return make_uniq<BoundColumnRefExpression>("amt", LogicalType::INTEGER,
	                                           ColumnBinding(MEMORY_GET_INDEX, ProjectionIndex(1)));
}

inline unique_ptr<Expression> Int(int32_t value) {
	return make_uniq<BoundConstantExpression>(Value::INTEGER(value));
}

inline unique_ptr<Expression> Call(const string &name, FunctionStability stability = FunctionStability::CONSISTENT) {
	vector<unique_ptr<Expression>> args;
	args.push_back(BoundAmt());
	ScalarFunction function(Identifier(name), vector<LogicalType> {LogicalType::INTEGER}, LogicalType::INTEGER,
	                        nullptr);
	function.SetStability(stability);
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(args), nullptr);
}

class MemorySource : public CrossingSource {
public:
	explicit MemorySource(case_insensitive_set_t known_p = case_insensitive_set_t(),
	                      const CrossingIdentity &identity_p = MemoryIdentity())
	    : known(std::move(known_p)), identity(identity_p) {
	}

	const CrossingIdentity &Identity() const override {
		return identity;
	}
	vector<string> Tables(const string &) override {
		throw InternalException("memory: the engine never lists tables");
	}
	CrossingTable Describe(const string &, const string &) override {
		throw InternalException("memory: the engine never describes tables");
	}
	unique_ptr<CrossingLink> Connect(ClientContext &) override {
		throw InternalException("memory: the engine never connects");
	}
	CrossingVerdict AcceptsCall(const Expression &expr) override {
		auto name = FunctionNameOf(expr);
		if (known.find(name) != known.end()) {
			return CrossingVerdict::Yes();
		}
		return CrossingVerdict::No("the memory source has no " + name);
	}
	CrossingVerdict AcceptsType(const LogicalType &) override {
		return CrossingVerdict::Yes();
	}

private:
	case_insensitive_set_t known;
	const CrossingIdentity &identity;
};

inline MemorySource &MemorySourceFor(const string &id, const case_insensitive_set_t &known,
                                     const CrossingIdentity &identity = MemoryIdentity()) {
	static std::map<string, unique_ptr<MemorySource>> sources;
	string key = id + "@" + std::to_string(reinterpret_cast<uintptr_t>(&identity));
	for (auto &name : known) {
		key += "|" + name;
	}
	auto &slot = sources[key];
	if (!slot) {
		slot = make_uniq<MemorySource>(known, identity);
	}
	return *slot;
}

struct MemorySourceBindData : public TableFunctionData, public CrossingCarrier {
	shared_ptr<CrossingFragment> fragment;
	optional_ptr<MemorySource> source;

	const CrossingIdentity &Identity() const override {
		return source->Identity();
	}
	CrossingVerb Verb() const override {
		return CrossingVerb::SELECT;
	}
	optional_ptr<CrossingFragment> Fragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return *source.get_mutable();
	}
};

inline shared_ptr<CrossingFragment> MemoryFragment() {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->column_names = {"id", "amt"};
	fragment->column_types = {LogicalType::INTEGER, LogicalType::INTEGER};
	fragment->floor_bindings = {ColumnBinding(MEMORY_FLOOR_INDEX, ProjectionIndex(0)),
	                            ColumnBinding(MEMORY_FLOOR_INDEX, ProjectionIndex(1))};
	fragment->table_index = MEMORY_CROSSING_INDEX;

	auto floor = make_uniq<LogicalDummyScan>(MEMORY_FLOOR_INDEX);
	fragment->SealFloor(*floor);
	fragment->floor = std::move(floor);
	return fragment;
}

inline unique_ptr<LogicalOperator> MemorySourceScan(case_insensitive_set_t known = case_insensitive_set_t(),
                                                    const string &id = "memory",
                                                    const CrossingIdentity &identity = MemoryIdentity()) {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({0, 1});

	auto bind_data = make_uniq<MemorySourceBindData>();
	bind_data->fragment = fragment;
	bind_data->source = &MemorySourceFor(id, known, identity);

	auto types = fragment->column_types;
	auto names = fragment->column_names;
	TableFunction function("memory_source_scan", {}, nullptr);
	TagCrossingFunction(function);
	auto get = make_uniq<LogicalGet>(MEMORY_GET_INDEX, BoundTableFunction(function), std::move(bind_data),
	                                 std::move(types), IdentifiersOf(names));
	get->SetColumnIds({ColumnIndex(0), ColumnIndex(1)});
	return std::move(get);
}

inline unique_ptr<LogicalOperator> OtherCrossingScan() {
	return MemorySourceScan({}, "other", OtherIdentity());
}

} // namespace duckdb
