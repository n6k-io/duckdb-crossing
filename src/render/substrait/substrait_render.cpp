#include "crossing.hpp"
#include "crossing_substrait.hpp"
#include "internal/seam.hpp"
#include "substrait_types.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/storage/arena_allocator.hpp"

#include <algorithm>
#include <cmath>
#include <unordered_map>

namespace duckdb {

using duckdb_yyjson::yyjson_mut_arr;
using duckdb_yyjson::yyjson_mut_arr_add_strcpy;
using duckdb_yyjson::yyjson_mut_arr_add_uint;
using duckdb_yyjson::yyjson_mut_arr_add_val;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_bool;
using duckdb_yyjson::yyjson_mut_obj_add_int;
using duckdb_yyjson::yyjson_mut_obj_add_real;
using duckdb_yyjson::yyjson_mut_obj_add_str;
using duckdb_yyjson::yyjson_mut_obj_add_strcpy;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_obj_add_uint;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_val;

namespace {

const char *ComparisonName(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "equal";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "not_equal";
	case ExpressionType::COMPARE_LESSTHAN:
		return "lt";
	case ExpressionType::COMPARE_GREATERTHAN:
		return "gt";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "lte";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return "gte";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "is_distinct_from";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "is_not_distinct_from";
	default:
		return nullptr;
	}
}

const char *JoinTypeName(JoinType type) {
	switch (type) {
	case JoinType::INNER:
		return "JOIN_TYPE_INNER";
	case JoinType::LEFT:
		return "JOIN_TYPE_LEFT";
	case JoinType::RIGHT:
		return "JOIN_TYPE_RIGHT";
	case JoinType::OUTER:
		return "JOIN_TYPE_OUTER";
	case JoinType::SEMI:
		return "JOIN_TYPE_LEFT_SEMI";
	case JoinType::ANTI:
		return "JOIN_TYPE_LEFT_ANTI";
	case JoinType::RIGHT_SEMI:
		return "JOIN_TYPE_RIGHT_SEMI";
	case JoinType::RIGHT_ANTI:
		return "JOIN_TYPE_RIGHT_ANTI";
	case JoinType::MARK:
		return "JOIN_TYPE_LEFT_MARK";
	case JoinType::SINGLE:
		return "JOIN_TYPE_LEFT_SINGLE";
	default:
		return nullptr;
	}
}

const char *WindowFunctionName(ExpressionType type) {
	switch (type) {
	case ExpressionType::WINDOW_ROW_NUMBER:
		return "row_number";
	case ExpressionType::WINDOW_RANK:
		return "rank";
	case ExpressionType::WINDOW_RANK_DENSE:
		return "dense_rank";
	case ExpressionType::WINDOW_PERCENT_RANK:
		return "percent_rank";
	case ExpressionType::WINDOW_CUME_DIST:
		return "cume_dist";
	case ExpressionType::WINDOW_NTILE:
		return "ntile";
	case ExpressionType::WINDOW_LEAD:
		return "lead";
	case ExpressionType::WINDOW_LAG:
		return "lag";
	case ExpressionType::WINDOW_FIRST_VALUE:
		return "first_value";
	case ExpressionType::WINDOW_LAST_VALUE:
		return "last_value";
	case ExpressionType::WINDOW_NTH_VALUE:
		return "nth_value";
	default:
		return nullptr;
	}
}

const char *SortDirection(OrderType type, OrderByNullType nulls) {
	bool asc = type != OrderType::DESCENDING;
	bool nulls_first = nulls == OrderByNullType::NULLS_FIRST;
	if (asc) {
		return nulls_first ? "SORT_DIRECTION_ASC_NULLS_FIRST" : "SORT_DIRECTION_ASC_NULLS_LAST";
	}
	return nulls_first ? "SORT_DIRECTION_DESC_NULLS_FIRST" : "SORT_DIRECTION_DESC_NULLS_LAST";
}

bool IsGroupsBoundary(WindowBoundary boundary) {
	switch (boundary) {
	case WindowBoundary::CURRENT_ROW_GROUPS:
	case WindowBoundary::EXPR_PRECEDING_GROUPS:
	case WindowBoundary::EXPR_FOLLOWING_GROUPS:
		return true;
	default:
		return false;
	}
}

bool IsRangeBoundary(WindowBoundary boundary) {
	switch (boundary) {
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return true;
	default:
		return false;
	}
}

bool IsBetween(const Expression &expr) {
	return expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN ||
	       expr.GetExpressionType() == ExpressionType::COMPARE_NOT_BETWEEN;
}

bool TryConstantValue(const Expression &expr, Value &out) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		out = expr.Cast<BoundConstantExpression>().GetValue();
		return true;
	}
	if (BoundCastExpression::IsCast(expr)) {
		Value inner;
		if (!TryConstantValue(BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>()), inner)) {
			return false;
		}
		out = inner.DefaultCastAs(expr.GetReturnType());
		return true;
	}
	return false;
}

bool IsConstantOrNull(const unique_ptr<Expression> &expr) {
	Value ignored;
	return !expr || TryConstantValue(*expr, ignored);
}

bool IsIntegralOrNull(const unique_ptr<Expression> &expr) {
	return !expr || expr->GetReturnType().IsIntegral();
}

string WindowObstacle(const BoundWindowExpression &window) {
	if (window.Filter()) {
		return "a window FILTER";
	}
	if (window.IgnoreNulls()) {
		return "IGNORE NULLS";
	}
	if (window.WindowExclude() != WindowExcludeMode::NO_OTHER) {
		return "a window EXCLUDE clause";
	}
	if (!window.ArgOrders().empty()) {
		return "a window argument ORDER BY";
	}
	if (IsGroupsBoundary(window.WindowStart()) || IsGroupsBoundary(window.WindowEnd())) {
		return "a GROUPS frame";
	}
	if (!IsConstantOrNull(window.StartExpr()) || !IsConstantOrNull(window.EndExpr())) {
		return "a non-constant frame bound";
	}
	if (!IsIntegralOrNull(window.StartExpr()) || !IsIntegralOrNull(window.EndExpr())) {
		return "a non-integer frame bound";
	}
	if (window.GetExpressionType() == ExpressionType::WINDOW_LEAD ||
	    window.GetExpressionType() == ExpressionType::WINDOW_LAG) {
		auto &args = window.GetChildren();
		for (idx_t i = 1; i < args.size(); i++) {
			if (!IsConstantOrNull(args[i])) {
				return "a non-constant lead/lag offset or default";
			}
		}
	}
	if (!window.AggregateFunction() && !WindowFunctionName(window.GetExpressionType())) {
		return string("window function ") + EnumUtil::ToChars(window.GetExpressionType());
	}
	return string();
}

bool IsRenderableExpressionClass(ExpressionClass cls) {
	switch (cls) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_FUNCTION:
	case ExpressionClass::BOUND_AGGREGATE:
	case ExpressionClass::BOUND_WINDOW:
	case ExpressionClass::BOUND_UNNEST:
		return true;
	default:
		return false;
	}
}

Value EmptyAggregateValue(BoundAggregateExpression &aggr) {
	auto &function = aggr.Function();
	AggregateStateInput state_input(function, aggr.BindInfo().get());
	auto state = make_unsafe_uniq_array<data_t>(function.GetStateSizeCallback()(state_input));
	data_ptr_t state_ptr = state.get();
	function.GetStateInitCallback()(state_input, &state_ptr, 1);
	Vector state_vector(Value::POINTER(CastPointerToValue(state.get())), count_t(1));
	Vector result_vector(aggr.GetReturnType());
	ArenaAllocator arena(Allocator::DefaultAllocator());
	AggregateFinalizeInputData input(aggr, arena, nullptr);
	function.GetStateFinalizeCallback()(state_vector, input, result_vector, 1, 0);
	return result_vector.GetValue(0);
}

class Renderer {
public:
	Renderer() : doc(yyjson_mut_doc_new(nullptr)) {
	}

	string Render(const LogicalOperator &plan) {
		vector<ColumnBinding> bindings;
		auto *rel = Rel(const_cast<LogicalOperator &>(plan), bindings); // NOLINT(cppcoreguidelines-pro-type-const-cast)
		auto *relations = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_val(relations, Root(rel, bindings.size()));
		return Plan(relations);
	}

	string Render(const vector<SubstraitSlice> &slices) {
		auto *relations = yyjson_mut_arr(doc);
		for (auto &slice : slices) {
			seam_slice = slice;
			vector<ColumnBinding> bindings;
			yyjson_mut_val *input = nullptr;
			if (slice.source) {
				input = Rel(const_cast<LogicalOperator &>(*slice.source), bindings); // NOLINT
			}
			if (slice.write) {
				input = Write(*slice.write, input, bindings);
				bindings.resize(slice.write->returning.empty() ? 1 : slice.write->returning.size());
			}
			auto *detail = Detail("Slice");
			yyjson_mut_obj_add_uint(doc, detail, "id", slice.id);
			yyjson_mut_obj_add_bool(doc, detail, "ordered", slice.ordered);
			yyjson_mut_arr_add_val(relations, Root(ExtensionSingle(input, detail), bindings.size()));
		}
		seam_slice = nullptr;
		return Plan(relations);
	}

private:
	yyjson_mut_val *Root(yyjson_mut_val *rel, idx_t width) {
		auto *root_obj = yyjson_mut_obj(doc);
		auto *root = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, root, "input", rel);
		auto *names = yyjson_mut_arr(doc);
		for (idx_t i = 0; i < width; i++) {
			yyjson_mut_arr_add_strcpy(doc, names, ("c" + std::to_string(i)).c_str());
		}
		yyjson_mut_obj_add_val(doc, root, "names", names);
		yyjson_mut_obj_add_val(doc, root_obj, "root", root);
		return root_obj;
	}

	yyjson_mut_val *Strings(const vector<string> &values) {
		auto *array = yyjson_mut_arr(doc);
		for (auto &value : values) {
			yyjson_mut_arr_add_strcpy(doc, array, value.c_str());
		}
		return array;
	}

	yyjson_mut_val *Exprs(const vector<reference<const Expression>> &values, const vector<ColumnBinding> &scope) {
		auto *array = yyjson_mut_arr(doc);
		for (auto &value : values) {
			yyjson_mut_arr_add_val(array, Expr(value.get(), scope));
		}
		return array;
	}

	//! `bindings` are the source's; an expression's fields count the target's columns first.
	yyjson_mut_val *Write(const SubstraitWrite &write, yyjson_mut_val *input, const vector<ColumnBinding> &bindings) {
		vector<ColumnBinding> scope;
		for (idx_t c = 0; c < write.target_columns.size(); c++) {
			scope.emplace_back(TableIndex(write.target_index), ProjectionIndex(c));
		}
		scope.insert(scope.end(), bindings.begin(), bindings.end());

		auto *detail = Detail("Write");
		yyjson_mut_obj_add_uint(doc, detail, "verb", static_cast<uint8_t>(write.verb));
		yyjson_mut_obj_add_bool(doc, detail, "merge", write.merge);
		yyjson_mut_obj_add_strcpy(doc, detail, "catalog", write.catalog.c_str());
		yyjson_mut_obj_add_strcpy(doc, detail, "schema", write.schema.c_str());
		yyjson_mut_obj_add_strcpy(doc, detail, "table", write.table.c_str());
		yyjson_mut_obj_add_val(doc, detail, "targetColumns", Strings(write.target_columns));
		yyjson_mut_obj_add_uint(doc, detail, "sourceWidth", bindings.size());
		if (write.condition) {
			yyjson_mut_obj_add_val(doc, detail, "condition", Expr(*write.condition, scope));
		}
		yyjson_mut_obj_add_val(doc, detail, "setColumns", Strings(write.set_columns));
		yyjson_mut_obj_add_val(doc, detail, "values", Exprs(write.values, scope));
		yyjson_mut_obj_add_val(doc, detail, "returning", Strings(write.returning));
		auto *actions = yyjson_mut_arr(doc);
		for (auto &action : write.actions) {
			auto *entry = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_strcpy(doc, entry, "when", EnumUtil::ToChars(action.when));
			yyjson_mut_obj_add_strcpy(doc, entry, "type", EnumUtil::ToChars(action.type));
			if (action.condition) {
				yyjson_mut_obj_add_val(doc, entry, "condition", Expr(*action.condition, scope));
			}
			yyjson_mut_obj_add_val(doc, entry, "columns", Strings(action.columns));
			yyjson_mut_obj_add_val(doc, entry, "values", Exprs(action.values, scope));
			yyjson_mut_arr_add_val(actions, entry);
		}
		yyjson_mut_obj_add_val(doc, detail, "actions", actions);
		if (input) {
			return ExtensionSingle(input, detail);
		}
		auto *rel = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, rel, "detail", detail);
		return Wrap("extensionLeaf", rel);
	}

	yyjson_mut_val *SeamRead(LogicalGet &get, vector<ColumnBinding> &out) {
		if (!seam_slice) {
			throw NotImplementedException("substrait: a seam outside a statement's slices");
		}
		auto *detail = Detail("Seam");
		yyjson_mut_obj_add_uint(doc, detail, "id", seam_slice->id);
		yyjson_mut_obj_add_bool(doc, detail, "ordered", seam_slice->seam_ordered);
		if (seam_slice->seam_estimate.IsValid()) {
			yyjson_mut_obj_add_uint(doc, detail, "estimate", seam_slice->seam_estimate.GetIndex());
		}
		auto *types = yyjson_mut_arr(doc);
		for (auto &type : get.returned_types) {
			yyjson_mut_arr_add_val(types, Type(type));
		}
		yyjson_mut_obj_add_val(doc, detail, "types", types);
		auto *rel = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, rel, "detail", detail);
		out = get.GetColumnBindings();
		return Wrap("extensionLeaf", rel);
	}

	string Plan(yyjson_mut_val *relations) {
		auto *plan_obj = yyjson_mut_obj(doc);
		auto *version = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_int(doc, version, "minorNumber", SUBSTRAIT_MINOR_VERSION);
		yyjson_mut_obj_add_str(doc, version, "producer", "crossing");
		yyjson_mut_obj_add_val(doc, plan_obj, "version", version);

		auto *uris = yyjson_mut_arr(doc);
		for (auto &uri : uri_list) {
			auto *entry = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_uint(doc, entry, "extensionUriAnchor", uri.second);
			yyjson_mut_obj_add_strcpy(doc, entry, "uri", uri.first.c_str());
			yyjson_mut_arr_add_val(uris, entry);
		}
		yyjson_mut_obj_add_val(doc, plan_obj, "extensionUris", uris);

		auto *extensions = yyjson_mut_arr(doc);
		for (auto &fn : function_list) {
			auto *entry = yyjson_mut_obj(doc);
			auto *ext = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_uint(doc, ext, "extensionUriReference", fn.uri_anchor);
			yyjson_mut_obj_add_uint(doc, ext, "functionAnchor", fn.anchor);
			yyjson_mut_obj_add_strcpy(doc, ext, "name", fn.name.c_str());
			yyjson_mut_obj_add_val(doc, entry, "extensionFunction", ext);
			yyjson_mut_arr_add_val(extensions, entry);
		}
		yyjson_mut_obj_add_val(doc, plan_obj, "extensions", extensions);
		yyjson_mut_obj_add_val(doc, plan_obj, "relations", relations);
		yyjson_mut_doc_set_root(doc, plan_obj);
		return SerializeAndFree(doc);
	}

private:
	struct FunctionEntry {
		string name;
		uint32_t anchor;
		uint32_t uri_anchor;
	};

	uint32_t UriAnchor(const string &uri) {
		for (auto &entry : uri_list) {
			if (entry.first == uri) {
				return entry.second;
			}
		}
		uri_list.emplace_back(uri, NumericCast<uint32_t>(uri_list.size() + 1));
		return uri_list.back().second;
	}

	uint32_t FunctionAnchor(const string &duckdb_name) {
		auto name = SubstraitFunctionName(duckdb_name);
		auto it = anchors.find(name);
		if (it != anchors.end()) {
			return it->second;
		}
		FunctionEntry entry;
		entry.name = name;
		entry.anchor = NumericCast<uint32_t>(function_list.size() + 1);
		entry.uri_anchor = UriAnchor(SubstraitFunctionUri(name));
		function_list.push_back(entry);
		anchors[name] = entry.anchor;
		return entry.anchor;
	}

	yyjson_mut_val *Wrap(const char *key, yyjson_mut_val *inner) {
		auto *outer = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, outer, key, inner);
		return outer;
	}

	yyjson_mut_val *Type(const LogicalType &type) {
		return SubstraitTypeToJson(doc, type);
	}

	yyjson_mut_val *Selection(idx_t field) {
		auto *sel = yyjson_mut_obj(doc);
		auto *direct = yyjson_mut_obj(doc);
		auto *struct_field = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_uint(doc, struct_field, "field", field);
		yyjson_mut_obj_add_val(doc, direct, "structField", struct_field);
		yyjson_mut_obj_add_val(doc, sel, "directReference", direct);
		yyjson_mut_obj_add_val(doc, sel, "rootReference", yyjson_mut_obj(doc));
		return Wrap("selection", sel);
	}

	yyjson_mut_val *TextCast(const Value &value) {
		auto text = value.ToString();
		auto *string_lit = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strncpy(doc, string_lit, "string", text.data(), text.size());
		auto *cast = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, cast, "type", Type(value.type()));
		yyjson_mut_obj_add_val(doc, cast, "input", Wrap("literal", string_lit));
		yyjson_mut_obj_add_str(doc, cast, "failureBehavior", "FAILURE_BEHAVIOR_THROW_EXCEPTION");
		return Wrap("cast", cast);
	}

	yyjson_mut_val *Literal(const Value &value) {
		auto *lit = yyjson_mut_obj(doc);
		auto &type = value.type();
		if (value.IsNull()) {
			yyjson_mut_obj_add_val(doc, lit, "null", Type(type));
			return Wrap("literal", lit);
		}
		switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			yyjson_mut_obj_add_bool(doc, lit, "boolean", value.GetValue<bool>());
			break;
		case LogicalTypeId::TINYINT:
			yyjson_mut_obj_add_int(doc, lit, "i8", value.GetValue<int8_t>());
			break;
		case LogicalTypeId::SMALLINT:
			yyjson_mut_obj_add_int(doc, lit, "i16", value.GetValue<int16_t>());
			break;
		case LogicalTypeId::INTEGER:
			yyjson_mut_obj_add_int(doc, lit, "i32", value.GetValue<int32_t>());
			break;
		case LogicalTypeId::BIGINT:
			yyjson_mut_obj_add_strcpy(doc, lit, "i64", std::to_string(value.GetValue<int64_t>()).c_str());
			break;
		case LogicalTypeId::FLOAT:
			if (!std::isfinite(value.GetValue<float>())) {
				return TextCast(value);
			}
			yyjson_mut_obj_add_real(doc, lit, "fp32", value.GetValue<float>());
			break;
		case LogicalTypeId::DOUBLE:
			if (!std::isfinite(value.GetValue<double>())) {
				return TextCast(value);
			}
			yyjson_mut_obj_add_real(doc, lit, "fp64", value.GetValue<double>());
			break;
		case LogicalTypeId::VARCHAR: {
			auto &text = StringValue::Get(value);
			yyjson_mut_obj_add_strncpy(doc, lit, "string", text.data(), text.size());
			break;
		}
		case LogicalTypeId::DATE:
			yyjson_mut_obj_add_int(doc, lit, "date", value.GetValue<date_t>().days);
			break;
		default:
			return TextCast(value);
		}
		return Wrap("literal", lit);
	}

	yyjson_mut_val *Arguments(const vector<yyjson_mut_val *> &args) {
		auto *arguments = yyjson_mut_arr(doc);
		for (auto *arg : args) {
			auto *holder = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_val(doc, holder, "value", arg);
			yyjson_mut_arr_add_val(arguments, holder);
		}
		return arguments;
	}

	yyjson_mut_val *Call(const string &duckdb_name, const LogicalType &return_type,
	                     const vector<yyjson_mut_val *> &args) {
		auto *fn = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_uint(doc, fn, "functionReference", FunctionAnchor(duckdb_name));
		yyjson_mut_obj_add_val(doc, fn, "outputType", Type(return_type));
		yyjson_mut_obj_add_val(doc, fn, "arguments", Arguments(args));
		return Wrap("scalarFunction", fn);
	}

	yyjson_mut_val *Comparison(ExpressionType type, const Expression &left, const Expression &right,
	                           const vector<ColumnBinding> &scope) {
		auto name = ComparisonName(type);
		if (!name) {
			throw NotImplementedException("substrait: comparison %s", EnumUtil::ToChars(type));
		}
		return Call(name, LogicalType::BOOLEAN, {Expr(left, scope), Expr(right, scope)});
	}

	yyjson_mut_val *Sorts(const vector<BoundOrderByNode> &orders, const vector<ColumnBinding> &scope) {
		auto *sorts = yyjson_mut_arr(doc);
		for (auto &order : orders) {
			auto *field = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_val(doc, field, "expr", Expr(*order.expression, scope));
			yyjson_mut_obj_add_str(doc, field, "direction", SortDirection(order.type, order.null_order));
			yyjson_mut_arr_add_val(sorts, field);
		}
		return sorts;
	}

	yyjson_mut_val *Bound(WindowBoundary boundary, const unique_ptr<Expression> &offset) {
		auto *bound = yyjson_mut_obj(doc);
		switch (boundary) {
		case WindowBoundary::UNBOUNDED_PRECEDING:
		case WindowBoundary::UNBOUNDED_FOLLOWING:
			yyjson_mut_obj_add_val(doc, bound, "unbounded", yyjson_mut_obj(doc));
			return bound;
		case WindowBoundary::CURRENT_ROW_RANGE:
		case WindowBoundary::CURRENT_ROW_ROWS:
			yyjson_mut_obj_add_val(doc, bound, "currentRow", yyjson_mut_obj(doc));
			return bound;
		case WindowBoundary::EXPR_PRECEDING_ROWS:
		case WindowBoundary::EXPR_PRECEDING_RANGE:
		case WindowBoundary::EXPR_FOLLOWING_ROWS:
		case WindowBoundary::EXPR_FOLLOWING_RANGE: {
			auto *side = yyjson_mut_obj(doc);
			Value constant;
			TryConstantValue(*offset, constant);
			yyjson_mut_obj_add_strcpy(doc, side, "offset", constant.ToString().c_str());
			bool preceding =
			    boundary == WindowBoundary::EXPR_PRECEDING_ROWS || boundary == WindowBoundary::EXPR_PRECEDING_RANGE;
			yyjson_mut_obj_add_val(doc, bound, preceding ? "preceding" : "following", side);
			return bound;
		}
		default:
			throw NotImplementedException("substrait: window boundary %s", EnumUtil::ToChars(boundary));
		}
	}

	yyjson_mut_val *Window(const BoundWindowExpression &window, const vector<ColumnBinding> &scope) {
		auto obstacle = WindowObstacle(window);
		if (!obstacle.empty()) {
			throw NotImplementedException("substrait: %s", obstacle);
		}
		string name = window.AggregateFunction() ? window.AggregateFunction()->GetName().GetIdentifierName()
		                                         : WindowFunctionName(window.GetExpressionType());
		auto *fn = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_uint(doc, fn, "functionReference", FunctionAnchor(name));
		vector<yyjson_mut_val *> args;
		for (auto &child : window.GetChildren()) {
			args.push_back(Expr(*child, scope));
		}
		yyjson_mut_obj_add_val(doc, fn, "arguments", Arguments(args));
		auto *partitions = yyjson_mut_arr(doc);
		for (auto &partition : window.Partitions()) {
			yyjson_mut_arr_add_val(partitions, Expr(*partition, scope));
		}
		yyjson_mut_obj_add_val(doc, fn, "partitions", partitions);
		yyjson_mut_obj_add_val(doc, fn, "sorts", Sorts(window.OrderBy(), scope));
		if (window.WindowStart() != WindowBoundary::INVALID) {
			bool range = IsRangeBoundary(window.WindowStart()) || IsRangeBoundary(window.WindowEnd());
			yyjson_mut_obj_add_str(doc, fn, "boundsType", range ? "BOUNDS_TYPE_RANGE" : "BOUNDS_TYPE_ROWS");
			yyjson_mut_obj_add_val(doc, fn, "lowerBound", Bound(window.WindowStart(), window.StartExpr()));
			yyjson_mut_obj_add_val(doc, fn, "upperBound", Bound(window.WindowEnd(), window.EndExpr()));
		}
		yyjson_mut_obj_add_val(doc, fn, "outputType", Type(window.GetReturnType()));
		yyjson_mut_obj_add_str(doc, fn, "invocation",
		                       window.Distinct() ? "AGGREGATION_INVOCATION_DISTINCT" : "AGGREGATION_INVOCATION_ALL");
		return Wrap("windowFunction", fn);
	}

	yyjson_mut_val *Expr(const Expression &expr, const vector<ColumnBinding> &scope) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_COLUMN_REF: {
			auto &binding = expr.Cast<BoundColumnRefExpression>().Binding();
			for (idx_t i = 0; i < scope.size(); i++) {
				if (scope[i] == binding) {
					return Selection(i);
				}
			}
			throw InternalException("substrait: column binding %llu.%llu is not in scope", binding.table_index.index,
			                        binding.column_index.GetIndex());
		}
		case ExpressionClass::BOUND_CONSTANT:
			return Literal(expr.Cast<BoundConstantExpression>().GetValue());
		case ExpressionClass::BOUND_CONJUNCTION: {
			auto &conj = expr.Cast<BoundConjunctionExpression>();
			vector<yyjson_mut_val *> args;
			for (auto &child : conj.GetChildren()) {
				args.push_back(Expr(*child, scope));
			}
			auto name = conj.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? "and" : "or";
			return Call(name, LogicalType::BOOLEAN, args);
		}
		case ExpressionClass::BOUND_OPERATOR: {
			auto &op = expr.Cast<BoundOperatorExpression>();
			vector<yyjson_mut_val *> args;
			for (auto &child : op.GetChildren()) {
				args.push_back(Expr(*child, scope));
			}
			switch (op.GetExpressionType()) {
			case ExpressionType::OPERATOR_NOT:
				return Call("not", LogicalType::BOOLEAN, args);
			case ExpressionType::OPERATOR_IS_NULL:
				return Call("is_null", LogicalType::BOOLEAN, args);
			case ExpressionType::OPERATOR_IS_NOT_NULL:
				return Call("is_not_null", LogicalType::BOOLEAN, args);
			case ExpressionType::OPERATOR_COALESCE:
				return Call("coalesce", op.GetReturnType(), args);
			case ExpressionType::COMPARE_IN:
			case ExpressionType::COMPARE_NOT_IN: {
				auto *in = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, in, "value", args[0]);
				auto *options = yyjson_mut_arr(doc);
				for (idx_t i = 1; i < args.size(); i++) {
					yyjson_mut_arr_add_val(options, args[i]);
				}
				yyjson_mut_obj_add_val(doc, in, "options", options);
				auto *wrapped = Wrap("singularOrList", in);
				if (op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
					return Call("not", LogicalType::BOOLEAN, {wrapped});
				}
				return wrapped;
			}
			default:
				throw NotImplementedException("substrait: operator %s", EnumUtil::ToChars(op.GetExpressionType()));
			}
		}
		case ExpressionClass::BOUND_CASE: {
			auto &case_expr = expr.Cast<BoundCaseExpression>();
			auto *if_then = yyjson_mut_obj(doc);
			auto *ifs = yyjson_mut_arr(doc);
			for (auto &check : case_expr.CaseChecks()) {
				auto *clause = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, clause, "if", Expr(*check.when_expr, scope));
				yyjson_mut_obj_add_val(doc, clause, "then", Expr(*check.then_expr, scope));
				yyjson_mut_arr_add_val(ifs, clause);
			}
			yyjson_mut_obj_add_val(doc, if_then, "ifs", ifs);
			yyjson_mut_obj_add_val(doc, if_then, "else", Expr(case_expr.Else(), scope));
			return Wrap("ifThen", if_then);
		}
		case ExpressionClass::BOUND_FUNCTION: {
			auto &fn = expr.Cast<BoundFunctionExpression>();
			if (BoundCastExpression::IsCast(fn)) {
				auto *cast = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, cast, "type", Type(BoundCastExpression::TargetType(fn)));
				yyjson_mut_obj_add_val(doc, cast, "input", Expr(BoundCastExpression::Child(fn), scope));
				yyjson_mut_obj_add_str(doc, cast, "failureBehavior",
				                       BoundCastExpression::IsTryCast(fn) ? "FAILURE_BEHAVIOR_RETURN_NULL"
				                                                          : "FAILURE_BEHAVIOR_THROW_EXCEPTION");
				return Wrap("cast", cast);
			}
			if (BoundComparisonExpression::IsComparison(fn)) {
				return Comparison(fn.GetExpressionType(), BoundComparisonExpression::Left(fn),
				                  BoundComparisonExpression::Right(fn), scope);
			}
			if (IsBetween(fn)) {
				auto &input = BoundBetweenExpression::Input(fn);
				auto *lower = Comparison(BoundBetweenExpression::LowerComparisonType(fn), input,
				                         BoundBetweenExpression::LowerBound(fn), scope);
				auto *upper = Comparison(BoundBetweenExpression::UpperComparisonType(fn), input,
				                         BoundBetweenExpression::UpperBound(fn), scope);
				auto *between = Call("and", LogicalType::BOOLEAN, {lower, upper});
				return fn.GetExpressionType() == ExpressionType::COMPARE_NOT_BETWEEN
				           ? Call("not", LogicalType::BOOLEAN, {between})
				           : between;
			}
			vector<yyjson_mut_val *> args;
			for (auto &child : fn.GetChildren()) {
				args.push_back(Expr(*child, scope));
			}
			return Call(fn.Function().GetName().GetIdentifierName(), fn.GetReturnType(), args);
		}
		case ExpressionClass::BOUND_WINDOW:
			return Window(expr.Cast<BoundWindowExpression>(), scope);
		case ExpressionClass::BOUND_UNNEST: {
			auto &unnest = expr.Cast<BoundUnnestExpression>();
			return Call("unnest", unnest.GetReturnType(), {Expr(*unnest.Child(), scope)});
		}
		default:
			throw NotImplementedException("substrait: expression %s", EnumUtil::ToChars(expr.GetExpressionClass()));
		}
	}

	yyjson_mut_val *Measure(const BoundAggregateExpression &agg, const vector<ColumnBinding> &scope) {
		if (agg.GetOrderBys() && !agg.GetOrderBys()->orders.empty()) {
			throw NotImplementedException("substrait: an aggregate ORDER BY");
		}
		auto *measure = yyjson_mut_obj(doc);
		auto *fn = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_uint(doc, fn, "functionReference",
		                        FunctionAnchor(agg.Function().GetName().GetIdentifierName()));
		yyjson_mut_obj_add_val(doc, fn, "outputType", Type(agg.GetReturnType()));
		yyjson_mut_obj_add_str(doc, fn, "phase", "AGGREGATION_PHASE_INITIAL_TO_RESULT");
		yyjson_mut_obj_add_str(doc, fn, "invocation",
		                       agg.IsDistinct() ? "AGGREGATION_INVOCATION_DISTINCT" : "AGGREGATION_INVOCATION_ALL");
		vector<yyjson_mut_val *> args;
		for (auto &child : agg.GetChildren()) {
			args.push_back(Expr(*child, scope));
		}
		yyjson_mut_obj_add_val(doc, fn, "arguments", Arguments(args));
		yyjson_mut_obj_add_val(doc, measure, "measure", fn);
		if (agg.GetFilter()) {
			yyjson_mut_obj_add_val(doc, measure, "filter", Expr(*agg.GetFilter(), scope));
		}
		return measure;
	}

	yyjson_mut_val *Read(LogicalGet &get, vector<ColumnBinding> &out) {
		auto *read = yyjson_mut_obj(doc);
		auto *base = yyjson_mut_obj(doc);
		auto *names = yyjson_mut_arr(doc);
		auto *types = yyjson_mut_arr(doc);
		auto *named = yyjson_mut_obj(doc);
		auto *table_names = yyjson_mut_arr(doc);

		if (auto floor = FloorOf(get)) {
			if (!floor->catalog.empty()) {
				yyjson_mut_arr_add_strcpy(doc, table_names, floor->catalog.c_str());
			}
			yyjson_mut_arr_add_strcpy(doc, table_names, floor->schema.c_str());
			yyjson_mut_arr_add_strcpy(doc, table_names, floor->table.c_str());
		} else if (auto table = get.GetTable()) {
			yyjson_mut_arr_add_strcpy(doc, table_names, table->schema.name.c_str());
			yyjson_mut_arr_add_strcpy(doc, table_names, table->name.c_str());
		} else {
			throw NotImplementedException("substrait: scan of '%s' is not a floor",
			                              get.function.GetName().GetIdentifierName());
		}
		for (idx_t i = 0; i < get.names.size(); i++) {
			yyjson_mut_arr_add_strcpy(doc, names, get.names[i].c_str());
			yyjson_mut_arr_add_val(types, Type(get.returned_types[i]));
		}
		auto *struct_type = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, struct_type, "types", types);
		yyjson_mut_obj_add_str(doc, struct_type, "nullability", "NULLABILITY_REQUIRED");
		yyjson_mut_obj_add_val(doc, base, "names", names);
		yyjson_mut_obj_add_val(doc, base, "struct", struct_type);
		yyjson_mut_obj_add_val(doc, read, "baseSchema", base);

		auto *projection = yyjson_mut_obj(doc);
		auto *select = yyjson_mut_obj(doc);
		auto *items = yyjson_mut_arr(doc);
		for (auto &column : get.GetColumnIds()) {
			auto *item = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_uint(doc, item, "field", column.IsVirtualColumn() ? 0 : column.GetPrimaryIndex());
			yyjson_mut_arr_add_val(items, item);
		}
		yyjson_mut_obj_add_val(doc, select, "structItems", items);
		yyjson_mut_obj_add_val(doc, projection, "select", select);
		yyjson_mut_obj_add_val(doc, read, "projection", projection);

		yyjson_mut_obj_add_val(doc, named, "names", table_names);
		yyjson_mut_obj_add_val(doc, read, "namedTable", named);
		out = get.GetColumnBindings();
		return Wrap("read", read);
	}

	yyjson_mut_val *BaseSchema(const vector<LogicalType> &column_types) {
		auto *base = yyjson_mut_obj(doc);
		auto *names = yyjson_mut_arr(doc);
		auto *types = yyjson_mut_arr(doc);
		for (idx_t i = 0; i < column_types.size(); i++) {
			yyjson_mut_arr_add_strcpy(doc, names, ("c" + std::to_string(i)).c_str());
			yyjson_mut_arr_add_val(types, Type(column_types[i]));
		}
		auto *struct_type = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, struct_type, "types", types);
		yyjson_mut_obj_add_str(doc, struct_type, "nullability", "NULLABILITY_REQUIRED");
		yyjson_mut_obj_add_val(doc, base, "names", names);
		yyjson_mut_obj_add_val(doc, base, "struct", struct_type);
		return base;
	}

	yyjson_mut_val *EmptyRead(LogicalEmptyResult &empty, vector<ColumnBinding> &out) {
		auto *read = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, read, "baseSchema", BaseSchema(empty.return_types));
		yyjson_mut_obj_add_val(doc, read, "virtualTable", yyjson_mut_obj(doc));
		out = empty.GetColumnBindings();
		return Wrap("read", read);
	}

	yyjson_mut_val *RowsRead(LogicalColumnDataGet &get, vector<ColumnBinding> &out) {
		auto *rows = yyjson_mut_arr(doc);
		for (auto &chunk : SeamRowsOf(get)->Chunks()) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto *fields = yyjson_mut_arr(doc);
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					yyjson_mut_arr_add_val(fields, Literal(chunk.GetValue(c, r)));
				}
				auto *row = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, row, "fields", fields);
				yyjson_mut_arr_add_val(rows, row);
			}
		}
		auto *table = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, table, "expressions", rows);
		auto *read = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, read, "baseSchema", BaseSchema(get.chunk_types));
		yyjson_mut_obj_add_val(doc, read, "virtualTable", table);
		out = get.GetColumnBindings();
		return Wrap("read", read);
	}

	yyjson_mut_val *Fetch(yyjson_mut_val *input, idx_t offset, idx_t count, bool has_count) {
		auto *fetch = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, fetch, "input", input);
		yyjson_mut_obj_add_strcpy(doc, fetch, "offset", std::to_string(offset).c_str());
		if (has_count) {
			yyjson_mut_obj_add_strcpy(doc, fetch, "count", std::to_string(count).c_str());
		}
		return Wrap("fetch", fetch);
	}

	yyjson_mut_val *Sort(yyjson_mut_val *input, const vector<BoundOrderByNode> &orders,
	                     const vector<ColumnBinding> &scope) {
		auto *sort = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, sort, "input", input);
		yyjson_mut_obj_add_val(doc, sort, "sorts", Sorts(orders, scope));
		return Wrap("sort", sort);
	}

	yyjson_mut_val *Detail(const char *kind) {
		auto *detail = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, detail, "@type", (string(SUBSTRAIT_TYPE_URL_PREFIX) + kind).c_str());
		return detail;
	}

	yyjson_mut_val *ExtensionSingle(yyjson_mut_val *input, yyjson_mut_val *detail) {
		auto *rel = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, rel, "input", input);
		yyjson_mut_obj_add_val(doc, rel, "detail", detail);
		return Wrap("extensionSingle", rel);
	}

	yyjson_mut_val *ExtensionMulti(yyjson_mut_val *left, yyjson_mut_val *right, yyjson_mut_val *detail) {
		auto *rel = yyjson_mut_obj(doc);
		auto *inputs = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_val(inputs, left);
		yyjson_mut_arr_add_val(inputs, right);
		yyjson_mut_obj_add_val(doc, rel, "inputs", inputs);
		yyjson_mut_obj_add_val(doc, rel, "detail", detail);
		return Wrap("extensionMulti", rel);
	}

	yyjson_mut_val *Join(LogicalOperator &op, vector<ColumnBinding> &out) {
		vector<ColumnBinding> left_scope;
		vector<ColumnBinding> right_scope;
		auto *left = Rel(*op.children[0], left_scope);
		auto *right = Rel(*op.children[1], right_scope);
		vector<ColumnBinding> scope = left_scope;
		scope.insert(scope.end(), right_scope.begin(), right_scope.end());

		if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			auto *cross = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_val(doc, cross, "left", left);
			yyjson_mut_obj_add_val(doc, cross, "right", right);
			out = std::move(scope);
			return Wrap("cross", cross);
		}
		if (op.type == LogicalOperatorType::LOGICAL_POSITIONAL_JOIN) {
			out = std::move(scope);
			return ExtensionMulti(left, right, Detail("PositionalJoin"));
		}
		auto &join = op.Cast<LogicalJoin>();
		auto type_name = JoinTypeName(join.join_type);
		if (!type_name) {
			throw NotImplementedException("substrait: join type %s", EnumUtil::ToChars(join.join_type));
		}
		vector<yyjson_mut_val *> parts;
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto &cmp = op.Cast<LogicalComparisonJoin>();
			if (!cmp.duplicate_eliminated_columns.empty()) {
				throw NotImplementedException("substrait: duplicate eliminated join");
			}
			for (auto &condition : cmp.conditions) {
				parts.push_back(
				    Comparison(condition.GetComparisonType(), condition.GetLHS(), condition.GetRHS(), scope));
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			parts.push_back(Expr(*op.Cast<LogicalAnyJoin>().condition, scope));
		} else {
			throw NotImplementedException("substrait: %s", EnumUtil::ToChars(op.type));
		}
		yyjson_mut_val *expression = nullptr;
		if (parts.size() == 1) {
			expression = parts[0];
		} else if (!parts.empty()) {
			expression = Call("and", LogicalType::BOOLEAN, parts);
		}
		out = op.GetColumnBindings();
		if (op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto *detail = Detail("AsOfJoin");
			yyjson_mut_obj_add_str(doc, detail, "type", type_name);
			if (expression) {
				yyjson_mut_obj_add_val(doc, detail, "expression", expression);
			}
			return ExtensionMulti(left, right, detail);
		}
		auto *rel = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, rel, "left", left);
		yyjson_mut_obj_add_val(doc, rel, "right", right);
		if (expression) {
			yyjson_mut_obj_add_val(doc, rel, "expression", expression);
		}
		yyjson_mut_obj_add_str(doc, rel, "type", type_name);
		auto *wrapped = Wrap("join", rel);
		if (join.left_projection_map.empty() && join.right_projection_map.empty()) {
			return wrapped;
		}
		vector<ColumnBinding> emitted = left_scope;
		if (join.join_type == JoinType::MARK) {
			emitted.emplace_back(join.mark_index, ProjectionIndex(0));
		} else if (join.join_type != JoinType::SEMI && join.join_type != JoinType::ANTI) {
			emitted.insert(emitted.end(), right_scope.begin(), right_scope.end());
		}
		auto *mapping = yyjson_mut_arr(doc);
		for (auto &binding : out) {
			auto at = std::find(emitted.begin(), emitted.end(), binding);
			if (at == emitted.end()) {
				throw InternalException("substrait: a join's projection map names a column it does not emit");
			}
			yyjson_mut_arr_add_uint(doc, mapping, NumericCast<uint64_t>(at - emitted.begin()));
		}
		auto *emit = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, emit, "outputMapping", mapping);
		auto *common = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, common, "emit", emit);
		auto *project = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, project, "common", common);
		yyjson_mut_obj_add_val(doc, project, "input", wrapped);
		return Wrap("project", project);
	}

	yyjson_mut_val *Aggregate(yyjson_mut_val *input, const vector<ColumnBinding> &scope,
	                          const vector<unique_ptr<Expression>> &groups,
	                          const vector<unique_ptr<Expression>> &measures) {
		auto *agg = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, agg, "input", input);
		auto *groupings = yyjson_mut_arr(doc);
		auto *grouping = yyjson_mut_obj(doc);
		auto *group_exprs = yyjson_mut_arr(doc);
		for (auto &group : groups) {
			yyjson_mut_arr_add_val(group_exprs, Expr(*group, scope));
		}
		yyjson_mut_obj_add_val(doc, grouping, "groupingExpressions", group_exprs);
		yyjson_mut_arr_add_val(groupings, grouping);
		yyjson_mut_obj_add_val(doc, agg, "groupings", groupings);
		auto *measure_list = yyjson_mut_arr(doc);
		for (auto &measure : measures) {
			if (measure->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				throw NotImplementedException("substrait: a non-aggregate measure");
			}
			yyjson_mut_arr_add_val(measure_list, Measure(measure->Cast<BoundAggregateExpression>(), scope));
		}
		yyjson_mut_obj_add_val(doc, agg, "measures", measure_list);
		return Wrap("aggregate", agg);
	}

	yyjson_mut_val *ProjectOver(LogicalOperator &op, const vector<unique_ptr<Expression>> &expressions,
	                            bool only_expressions, vector<ColumnBinding> &out) {
		vector<ColumnBinding> scope;
		auto *input = Rel(*op.children[0], scope);
		auto *rel = yyjson_mut_obj(doc);
		auto *exprs = yyjson_mut_arr(doc);
		for (auto &expr : expressions) {
			yyjson_mut_arr_add_val(exprs, Expr(*expr, scope));
		}
		if (only_expressions) {
			auto *common = yyjson_mut_obj(doc);
			auto *emit = yyjson_mut_obj(doc);
			auto *mapping = yyjson_mut_arr(doc);
			for (idx_t i = 0; i < expressions.size(); i++) {
				yyjson_mut_arr_add_uint(doc, mapping, scope.size() + i);
			}
			yyjson_mut_obj_add_val(doc, emit, "outputMapping", mapping);
			yyjson_mut_obj_add_val(doc, common, "emit", emit);
			yyjson_mut_obj_add_val(doc, rel, "common", common);
		}
		yyjson_mut_obj_add_val(doc, rel, "input", input);
		yyjson_mut_obj_add_val(doc, rel, "expressions", exprs);
		out = op.GetColumnBindings();
		return Wrap("project", rel);
	}

	yyjson_mut_val *Rel(LogicalOperator &op, vector<ColumnBinding> &out) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET:
			if (op.Cast<LogicalGet>().function.GetName() == CROSSING_SEAM_FUNCTION) {
				return SeamRead(op.Cast<LogicalGet>(), out);
			}
			return Read(op.Cast<LogicalGet>(), out);
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
			return RowsRead(op.Cast<LogicalColumnDataGet>(), out);
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
			return EmptyRead(op.Cast<LogicalEmptyResult>(), out);
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
			vector<ColumnBinding> definition_scope;
			auto *definition = Rel(*op.children[0], definition_scope);
			auto *body = Rel(*op.children[1], out);
			auto *detail = Detail("MaterializedCte");
			yyjson_mut_obj_add_uint(doc, detail, "cte", op.Cast<LogicalCTE>().table_index.index);
			return ExtensionMulti(definition, body, detail);
		}
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			auto *detail = Detail("CteRef");
			yyjson_mut_obj_add_uint(doc, detail, "cte", op.Cast<LogicalCTERef>().cte_index.index);
			auto *rel = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_val(doc, rel, "detail", detail);
			out = op.GetColumnBindings();
			return Wrap("extensionLeaf", rel);
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op.Cast<LogicalFilter>();
			if (filter.HasProjectionMap()) {
				throw NotImplementedException("substrait: a filter with a projection map");
			}
			vector<ColumnBinding> scope;
			auto *input = Rel(*op.children[0], scope);
			vector<yyjson_mut_val *> parts;
			for (auto &expr : filter.expressions) {
				parts.push_back(Expr(*expr, scope));
			}
			auto *rel = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_val(doc, rel, "input", input);
			yyjson_mut_obj_add_val(doc, rel, "condition",
			                       parts.size() == 1 ? parts[0] : Call("and", LogicalType::BOOLEAN, parts));
			out = std::move(scope);
			return Wrap("filter", rel);
		}
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return ProjectOver(op, op.expressions, true, out);
		case LogicalOperatorType::LOGICAL_WINDOW:
			return ProjectOver(op, op.expressions, false, out);
		case LogicalOperatorType::LOGICAL_UNNEST:
			return ProjectOver(op, op.expressions, false, out);
		case LogicalOperatorType::LOGICAL_LIMIT: {
			auto &limit = op.Cast<LogicalLimit>();
			auto *input = Rel(*op.children[0], out);
			idx_t offset = 0;
			idx_t count = 0;
			bool has_count = false;
			if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				offset = limit.offset_val.GetConstantValue();
			} else if (limit.offset_val.Type() != LimitNodeType::UNSET) {
				throw NotImplementedException("substrait: a non-constant offset");
			}
			if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				count = limit.limit_val.GetConstantValue();
				has_count = true;
			} else if (limit.limit_val.Type() != LimitNodeType::UNSET) {
				throw NotImplementedException("substrait: a non-constant limit");
			}
			return Fetch(input, offset, count, has_count);
		}
		case LogicalOperatorType::LOGICAL_ORDER_BY: {
			auto &order = op.Cast<LogicalOrder>();
			if (order.HasProjectionMap()) {
				throw NotImplementedException("substrait: an order with a projection map");
			}
			auto *input = Rel(*op.children[0], out);
			return Sort(input, order.orders, out);
		}
		case LogicalOperatorType::LOGICAL_TOP_N: {
			auto &top = op.Cast<LogicalTopN>();
			auto *input = Rel(*op.children[0], out);
			return Fetch(Sort(input, top.orders, out), top.offset, top.limit, true);
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &agg = op.Cast<LogicalAggregate>();
			if (agg.grouping_sets.size() > 1) {
				throw NotImplementedException("substrait: grouping sets");
			}
			vector<ColumnBinding> scope;
			auto *input = Rel(*op.children[0], scope);
			auto *rel = Aggregate(input, scope, agg.groups, agg.expressions);
			out = op.GetColumnBindings();
			return rel;
		}
		case LogicalOperatorType::LOGICAL_DISTINCT: {
			auto &distinct = op.Cast<LogicalDistinct>();
			vector<ColumnBinding> scope;
			auto *input = Rel(*op.children[0], scope);
			if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
				auto *detail = Detail("DistinctOn");
				auto *targets = yyjson_mut_arr(doc);
				for (auto &target : distinct.distinct_targets) {
					yyjson_mut_arr_add_val(targets, Expr(*target, scope));
				}
				yyjson_mut_obj_add_val(doc, detail, "targets", targets);
				if (distinct.order_by) {
					yyjson_mut_obj_add_val(doc, detail, "sorts", Sorts(distinct.order_by->orders, scope));
				}
				out = std::move(scope);
				return ExtensionSingle(input, detail);
			}
			vector<unique_ptr<Expression>> groups;
			auto &types = op.children[0]->types;
			for (idx_t i = 0; i < scope.size(); i++) {
				groups.push_back(make_uniq<BoundColumnRefExpression>(types[i], scope[i]));
			}
			auto *rel = Aggregate(input, scope, groups, {});
			out = std::move(scope);
			return rel;
		}
		case LogicalOperatorType::LOGICAL_SAMPLE: {
			auto &sample = op.Cast<LogicalSample>();
			auto *input = Rel(*op.children[0], out);
			auto &options = *sample.sample_options;
			auto *detail = Detail("Sample");
			yyjson_mut_obj_add_strcpy(doc, detail, "method", EnumUtil::ToChars(options.method));
			yyjson_mut_obj_add_strcpy(doc, detail, "size", options.sample_size.ToString().c_str());
			yyjson_mut_obj_add_bool(doc, detail, "percentage", options.is_percentage);
			if (options.seed.IsValid()) {
				yyjson_mut_obj_add_uint(doc, detail, "seed", options.seed.GetIndex());
			}
			return ExtensionSingle(input, detail);
		}
		case LogicalOperatorType::LOGICAL_PIVOT: {
			auto &pivot = op.Cast<LogicalPivot>();
			vector<ColumnBinding> scope;
			auto *input = Rel(*op.children[0], scope);
			auto &info = pivot.bound_pivot;
			auto *detail = Detail("Pivot");
			yyjson_mut_obj_add_uint(doc, detail, "groupCount", info.group_count);
			auto *values = yyjson_mut_arr(doc);
			for (auto &value : info.pivot_values) {
				yyjson_mut_arr_add_strcpy(doc, values, value.c_str());
			}
			yyjson_mut_obj_add_val(doc, detail, "pivotValues", values);
			auto *empty = yyjson_mut_arr(doc);
			for (auto &aggregate : info.aggregates) {
				yyjson_mut_arr_add_val(empty,
				                       Literal(EmptyAggregateValue(aggregate->Cast<BoundAggregateExpression>())));
			}
			yyjson_mut_obj_add_val(doc, detail, "empty", empty);
			auto *types = yyjson_mut_arr(doc);
			for (auto &type : info.types) {
				yyjson_mut_arr_add_val(types, Type(type));
			}
			yyjson_mut_obj_add_val(doc, detail, "types", types);
			out = op.GetColumnBindings();
			return ExtensionSingle(input, detail);
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
			return Join(op, out);
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT: {
			auto &setop = op.Cast<LogicalSetOperation>();
			auto *rel = yyjson_mut_obj(doc);
			auto *inputs = yyjson_mut_arr(doc);
			for (auto &child : op.children) {
				vector<ColumnBinding> ignored;
				yyjson_mut_arr_add_val(inputs, Rel(*child, ignored));
			}
			yyjson_mut_obj_add_val(doc, rel, "inputs", inputs);
			const char *name;
			if (op.type == LogicalOperatorType::LOGICAL_UNION) {
				name = setop.setop_all ? "SET_OP_UNION_ALL" : "SET_OP_UNION_DISTINCT";
			} else if (op.type == LogicalOperatorType::LOGICAL_EXCEPT) {
				name = setop.setop_all ? "SET_OP_MINUS_PRIMARY_ALL" : "SET_OP_MINUS_PRIMARY";
			} else {
				name = setop.setop_all ? "SET_OP_INTERSECTION_PRIMARY_ALL" : "SET_OP_INTERSECTION_PRIMARY";
			}
			yyjson_mut_obj_add_str(doc, rel, "op", name);
			out = op.GetColumnBindings();
			return Wrap("set", rel);
		}
		default:
			throw NotImplementedException("substrait: operator %s", EnumUtil::ToChars(op.type));
		}
	}

	yyjson_mut_doc *doc;
	optional_ptr<const SubstraitSlice> seam_slice;
	std::unordered_map<string, uint32_t> anchors;
	vector<FunctionEntry> function_list;
	vector<std::pair<string, uint32_t>> uri_list;
};

} // namespace

string RenderSubstraitJson(const LogicalOperator &plan) {
	Renderer renderer;
	return renderer.Render(plan);
}

string RenderSubstraitSlices(const vector<SubstraitSlice> &slices) {
	Renderer renderer;
	return renderer.Render(slices);
}

bool SubstraitCanRenderCall(const Expression &expr, string &reason) {
	auto cls = expr.GetExpressionClass();
	if (!IsRenderableExpressionClass(cls)) {
		reason = string("substrait cannot carry a ") + EnumUtil::ToChars(cls);
		return false;
	}
	if (cls == ExpressionClass::BOUND_OPERATOR) {
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
		case ExpressionType::OPERATOR_COALESCE:
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return true;
		default:
			reason = string("substrait cannot carry operator ") + EnumUtil::ToChars(expr.GetExpressionType());
			return false;
		}
	}
	if (cls == ExpressionClass::BOUND_FUNCTION &&
	    expr.Cast<BoundFunctionExpression>().Function().HasBindLambdaCallback()) {
		reason = "substrait cannot carry a lambda";
		return false;
	}
	if (cls == ExpressionClass::BOUND_AGGREGATE) {
		auto &order_bys = expr.Cast<BoundAggregateExpression>().GetOrderBys();
		if (order_bys && !order_bys->orders.empty()) {
			reason = "substrait cannot carry an aggregate ORDER BY";
			return false;
		}
	}
	if (BoundComparisonExpression::IsComparison(expr) && !ComparisonName(expr.GetExpressionType())) {
		reason = string("substrait cannot carry comparison ") + EnumUtil::ToChars(expr.GetExpressionType());
		return false;
	}
	if (cls == ExpressionClass::BOUND_WINDOW) {
		auto obstacle = WindowObstacle(expr.Cast<BoundWindowExpression>());
		if (!obstacle.empty()) {
			reason = "substrait cannot carry " + obstacle;
			return false;
		}
	}
	return true;
}

bool SubstraitCanRenderOperator(const LogicalOperator &op, string &reason) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	case LogicalOperatorType::LOGICAL_PIVOT:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return true;
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op.Cast<LogicalLimit>();
		for (auto *node : {&limit.limit_val, &limit.offset_val}) {
			if (node->Type() != LimitNodeType::UNSET && node->Type() != LimitNodeType::CONSTANT_VALUE) {
				reason = "substrait cannot carry a non-constant limit";
				return false;
			}
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		if (op.Cast<LogicalAggregate>().grouping_sets.size() > 1) {
			reason = "substrait cannot carry grouping sets";
			return false;
		}
		return true;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return true;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		if (!JoinTypeName(join.join_type)) {
			reason = string("substrait cannot carry a ") + EnumUtil::ToChars(join.join_type) + " join";
			return false;
		}
		if (op.type != LogicalOperatorType::LOGICAL_ANY_JOIN &&
		    !op.Cast<LogicalComparisonJoin>().duplicate_eliminated_columns.empty()) {
			reason = "substrait cannot carry a duplicate eliminated join";
			return false;
		}
		return true;
	}
	default:
		reason = string("substrait cannot carry ") + EnumUtil::ToChars(op.type);
		return false;
	}
}

} // namespace duckdb
