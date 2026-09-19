#include "crossing_substrait.hpp"
#include "substrait_types.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include <functional>
#include <unordered_map>

namespace duckdb {

using duckdb_yyjson::yyjson_arr_get;
using duckdb_yyjson::yyjson_arr_get_first;
using duckdb_yyjson::yyjson_arr_size;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_get_bool;
using duckdb_yyjson::yyjson_get_num;
using duckdb_yyjson::yyjson_get_sint;
using duckdb_yyjson::yyjson_get_str;
using duckdb_yyjson::yyjson_get_uint;
using duckdb_yyjson::yyjson_is_bool;
using duckdb_yyjson::yyjson_is_int;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_is_str;
using duckdb_yyjson::yyjson_obj_get;
using duckdb_yyjson::yyjson_obj_iter_get_val;
using duckdb_yyjson::yyjson_obj_iter_next;
using duckdb_yyjson::yyjson_obj_iter_with;
using duckdb_yyjson::yyjson_obj_size;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val;

namespace {

using Scope = std::function<unique_ptr<ParsedExpression>(idx_t field)>;

string ColumnName(idx_t i) {
	return "c" + std::to_string(i);
}

string StrOf(yyjson_val *obj, const char *key) {
	auto *val = obj ? yyjson_obj_get(obj, key) : nullptr;
	if (!val || !yyjson_is_str(val)) {
		return string();
	}
	return yyjson_get_str(val);
}

vector<string> StrArrayOf(yyjson_val *obj, const char *key) {
	vector<string> out;
	auto *arr = obj ? yyjson_obj_get(obj, key) : nullptr;
	if (!arr) {
		return out;
	}
	size_t idx, max;
	yyjson_val *item;
	yyjson_arr_foreach(arr, idx, max, item) {
		if (yyjson_is_str(item)) {
			out.emplace_back(yyjson_get_str(item));
		}
	}
	return out;
}

yyjson_val *Require(yyjson_val *obj, const char *key) {
	auto *val = obj ? yyjson_obj_get(obj, key) : nullptr;
	if (!val) {
		throw InvalidInputException("substrait: missing '%s'", key);
	}
	return val;
}

int64_t Int64Of(yyjson_val *val, int64_t fallback) {
	if (!val) {
		return fallback;
	}
	if (yyjson_is_int(val)) {
		return yyjson_get_sint(val);
	}
	if (yyjson_is_str(val)) {
		return std::stoll(yyjson_get_str(val));
	}
	throw InvalidInputException("substrait: expected an integer");
}

class Decoder {
public:
	Decoder(Connection &conn_p, const string &catalog_p, const string &seam_view_p)
	    : conn(conn_p), catalog(catalog_p), seam_view(seam_view_p) {
	}

	shared_ptr<Relation> Decode(yyjson_val *plan) {
		auto *extensions = yyjson_obj_get(plan, "extensions");
		if (extensions) {
			size_t idx, max;
			yyjson_val *entry;
			yyjson_arr_foreach(extensions, idx, max, entry) {
				auto *fn = yyjson_obj_get(entry, "extensionFunction");
				if (!fn) {
					continue;
				}
				auto anchor = Int64Of(yyjson_obj_get(fn, "functionAnchor"), -1);
				functions[NumericCast<uint64_t>(anchor)] = StrOf(fn, "name");
			}
		}
		auto *relations = Require(plan, "relations");
		auto *first = yyjson_arr_get_first(relations);
		auto *root = first ? yyjson_obj_get(first, "root") : nullptr;
		if (!root) {
			throw InvalidInputException("substrait: the plan has no root relation");
		}
		auto rel = Rel(Require(root, "input"));
		auto names = StrArrayOf(root, "names");
		if (names.empty()) {
			return rel;
		}
		vector<unique_ptr<ParsedExpression>> exprs;
		vector<string> aliases;
		for (idx_t i = 0; i < names.size(); i++) {
			exprs.push_back(make_uniq<ColumnRefExpression>(ColumnName(i)));
			aliases.push_back(names[i]);
		}
		return rel->Project(std::move(exprs), aliases);
	}

private:
	string FunctionName(yyjson_val *node) {
		auto ref = Int64Of(yyjson_obj_get(node, "functionReference"), -1);
		auto it = functions.find(NumericCast<uint64_t>(ref));
		if (it == functions.end()) {
			throw InvalidInputException("substrait: unknown function reference %lld", ref);
		}
		return it->second;
	}

	vector<unique_ptr<ParsedExpression>> Arguments(yyjson_val *node, const Scope &scope) {
		vector<unique_ptr<ParsedExpression>> out;
		auto *arguments = yyjson_obj_get(node, "arguments");
		if (!arguments) {
			return out;
		}
		size_t idx, max;
		yyjson_val *entry;
		yyjson_arr_foreach(arguments, idx, max, entry) {
			out.push_back(Expr(Require(entry, "value"), scope));
		}
		return out;
	}

	vector<OrderByNode> Orders(yyjson_val *sorts, const Scope &scope) {
		vector<OrderByNode> orders;
		if (!sorts) {
			return orders;
		}
		size_t idx, max;
		yyjson_val *field;
		yyjson_arr_foreach(sorts, idx, max, field) {
			auto direction = StrOf(field, "direction");
			auto type = direction.find("DESC") != string::npos ? OrderType::DESCENDING : OrderType::ASCENDING;
			auto nulls = direction.find("NULLS_FIRST") != string::npos ? OrderByNullType::NULLS_FIRST
			                                                           : OrderByNullType::NULLS_LAST;
			orders.emplace_back(type, nulls, Expr(Require(field, "expr"), scope));
		}
		return orders;
	}

	unique_ptr<ParsedExpression> Literal(yyjson_val *lit) {
		if (!yyjson_is_obj(lit) || yyjson_obj_size(lit) < 1) {
			throw InvalidInputException("substrait: a literal must be an object");
		}
		auto iter = yyjson_obj_iter_with(lit);
		auto *iter_key = yyjson_obj_iter_next(&iter);
		string key = yyjson_get_str(iter_key);
		auto *body = yyjson_obj_iter_get_val(iter_key);
		if (key == "null") {
			return make_uniq<ConstantExpression>(Value(SubstraitTypeFromJson(*conn.context, body)));
		}
		if (key == "boolean") {
			return make_uniq<ConstantExpression>(Value::BOOLEAN(yyjson_get_bool(body)));
		}
		if (key == "i8") {
			return make_uniq<ConstantExpression>(Value::TINYINT(NumericCast<int8_t>(Int64Of(body, 0))));
		}
		if (key == "i16") {
			return make_uniq<ConstantExpression>(Value::SMALLINT(NumericCast<int16_t>(Int64Of(body, 0))));
		}
		if (key == "i32") {
			return make_uniq<ConstantExpression>(Value::INTEGER(NumericCast<int32_t>(Int64Of(body, 0))));
		}
		if (key == "i64") {
			return make_uniq<ConstantExpression>(Value::BIGINT(Int64Of(body, 0)));
		}
		if (key == "fp32") {
			return make_uniq<ConstantExpression>(Value::FLOAT(static_cast<float>(yyjson_get_num(body))));
		}
		if (key == "fp64") {
			return make_uniq<ConstantExpression>(Value::DOUBLE(yyjson_get_num(body)));
		}
		if (key == "string") {
			return make_uniq<ConstantExpression>(Value(string(yyjson_get_str(body))));
		}
		if (key == "date") {
			return make_uniq<ConstantExpression>(Value::DATE(date_t(NumericCast<int32_t>(Int64Of(body, 0)))));
		}
		throw InvalidInputException("substrait: unsupported literal '%s'", key);
	}

	unique_ptr<ParsedExpression> ScalarFunction(yyjson_val *node, const Scope &scope) {
		auto name = FunctionName(node);
		auto args = Arguments(node, scope);
		auto binary = [&](ExpressionType type) -> unique_ptr<ParsedExpression> {
			if (args.size() != 2) {
				throw InvalidInputException("substrait: '%s' takes two arguments", name);
			}
			return make_uniq<ComparisonExpression>(type, std::move(args[0]), std::move(args[1]));
		};
		if (name == "equal") {
			return binary(ExpressionType::COMPARE_EQUAL);
		}
		if (name == "not_equal") {
			return binary(ExpressionType::COMPARE_NOTEQUAL);
		}
		if (name == "lt") {
			return binary(ExpressionType::COMPARE_LESSTHAN);
		}
		if (name == "lte") {
			return binary(ExpressionType::COMPARE_LESSTHANOREQUALTO);
		}
		if (name == "gt") {
			return binary(ExpressionType::COMPARE_GREATERTHAN);
		}
		if (name == "gte") {
			return binary(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		}
		if (name == "is_distinct_from") {
			return binary(ExpressionType::COMPARE_DISTINCT_FROM);
		}
		if (name == "is_not_distinct_from") {
			return binary(ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		}
		if (name == "and") {
			return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(args));
		}
		if (name == "or") {
			return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(args));
		}
		if (name == "not") {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(args));
		}
		if (name == "is_null") {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(args));
		}
		if (name == "is_not_null") {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(args));
		}
		if (name == "coalesce") {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(args));
		}
		return make_uniq<FunctionExpression>(DuckDBFunctionName(name), std::move(args));
	}

	static bool ApplyBound(yyjson_val *bound, bool lower, bool range, WindowBoundary &out,
	                       unique_ptr<ParsedExpression> &offset_out) {
		if (!bound) {
			return false;
		}
		if (yyjson_obj_get(bound, "unbounded")) {
			out = lower ? WindowBoundary::UNBOUNDED_PRECEDING : WindowBoundary::UNBOUNDED_FOLLOWING;
			return true;
		}
		if (yyjson_obj_get(bound, "currentRow")) {
			out = range ? WindowBoundary::CURRENT_ROW_RANGE : WindowBoundary::CURRENT_ROW_ROWS;
			return true;
		}
		auto *preceding = yyjson_obj_get(bound, "preceding");
		auto *following = yyjson_obj_get(bound, "following");
		auto *side = preceding ? preceding : following;
		if (!side) {
			return false;
		}
		auto offset = Int64Of(yyjson_obj_get(side, "offset"), 0);
		offset_out = make_uniq<ConstantExpression>(Value::BIGINT(offset));
		if (preceding) {
			out = range ? WindowBoundary::EXPR_PRECEDING_RANGE : WindowBoundary::EXPR_PRECEDING_ROWS;
		} else {
			out = range ? WindowBoundary::EXPR_FOLLOWING_RANGE : WindowBoundary::EXPR_FOLLOWING_ROWS;
		}
		return true;
	}

	unique_ptr<ParsedExpression> WindowFunction(yyjson_val *node, const Scope &scope) {
		auto name = DuckDBFunctionName(FunctionName(node));
		auto type = WindowExpression::WindowToExpressionType(name);
		auto window = make_uniq<WindowExpression>(type, "", "", name);
		auto args = Arguments(node, scope);
		if (type == ExpressionType::WINDOW_LEAD || type == ExpressionType::WINDOW_LAG) {
			if (args.size() > 2) {
				window->default_expr = std::move(args[2]);
			}
			if (args.size() > 1) {
				window->offset_expr = std::move(args[1]);
			}
			args.resize(MinValue<idx_t>(args.size(), 1));
		}
		window->children = std::move(args);
		if (auto *partitions = yyjson_obj_get(node, "partitions")) {
			size_t idx, max;
			yyjson_val *partition;
			yyjson_arr_foreach(partitions, idx, max, partition) {
				window->partitions.push_back(Expr(partition, scope));
			}
		}
		window->orders = Orders(yyjson_obj_get(node, "sorts"), scope);
		window->distinct = StrOf(node, "invocation") == "AGGREGATION_INVOCATION_DISTINCT";
		bool range = StrOf(node, "boundsType") == "BOUNDS_TYPE_RANGE";
		WindowBoundary start = WindowBoundary::INVALID;
		WindowBoundary end = WindowBoundary::INVALID;
		unique_ptr<ParsedExpression> start_expr;
		unique_ptr<ParsedExpression> end_expr;
		if (ApplyBound(yyjson_obj_get(node, "lowerBound"), true, range, start, start_expr) &&
		    ApplyBound(yyjson_obj_get(node, "upperBound"), false, range, end, end_expr)) {
			window->start = start;
			window->end = end;
			window->start_expr = std::move(start_expr);
			window->end_expr = std::move(end_expr);
		}
		return std::move(window);
	}

	unique_ptr<ParsedExpression> Expr(yyjson_val *node, const Scope &scope) {
		if (auto *selection = yyjson_obj_get(node, "selection")) {
			auto *direct = Require(selection, "directReference");
			auto *field = Require(direct, "structField");
			return scope(NumericCast<idx_t>(Int64Of(yyjson_obj_get(field, "field"), 0)));
		}
		if (auto *literal = yyjson_obj_get(node, "literal")) {
			return Literal(literal);
		}
		if (auto *fn = yyjson_obj_get(node, "scalarFunction")) {
			return ScalarFunction(fn, scope);
		}
		if (auto *window = yyjson_obj_get(node, "windowFunction")) {
			return WindowFunction(window, scope);
		}
		if (auto *cast = yyjson_obj_get(node, "cast")) {
			auto type = SubstraitTypeFromJson(*conn.context, Require(cast, "type"));
			auto behaviour = StrOf(cast, "failureBehavior");
			return make_uniq<CastExpression>(type, Expr(Require(cast, "input"), scope),
			                                 behaviour == "FAILURE_BEHAVIOR_RETURN_NULL");
		}
		if (auto *if_then = yyjson_obj_get(node, "ifThen")) {
			auto result = make_uniq<CaseExpression>();
			size_t idx, max;
			yyjson_val *clause;
			yyjson_arr_foreach(Require(if_then, "ifs"), idx, max, clause) {
				CaseCheck check;
				check.when_expr = Expr(Require(clause, "if"), scope);
				check.then_expr = Expr(Require(clause, "then"), scope);
				result->case_checks.push_back(std::move(check));
			}
			auto *else_node = yyjson_obj_get(if_then, "else");
			result->else_expr =
			    else_node ? Expr(else_node, scope) : make_uniq_base<ParsedExpression, ConstantExpression>(Value());
			return std::move(result);
		}
		if (auto *in = yyjson_obj_get(node, "singularOrList")) {
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(Expr(Require(in, "value"), scope));
			size_t idx, max;
			yyjson_val *option;
			yyjson_arr_foreach(Require(in, "options"), idx, max, option) {
				children.push_back(Expr(option, scope));
			}
			return make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(children));
		}
		throw InvalidInputException("substrait: unsupported expression");
	}

	static Scope PositionalScope() {
		return [](idx_t field) -> unique_ptr<ParsedExpression> {
			return make_uniq<ColumnRefExpression>(ColumnName(field));
		};
	}

	static shared_ptr<Relation> Renumber(const shared_ptr<Relation> &rel, vector<unique_ptr<ParsedExpression>> exprs) {
		vector<string> aliases;
		for (idx_t i = 0; i < exprs.size(); i++) {
			aliases.push_back(ColumnName(i));
		}
		return rel->Project(std::move(exprs), aliases);
	}

	shared_ptr<Relation> WrapSelect(const shared_ptr<Relation> &child, const std::function<void(SelectNode &)> &shape) {
		auto select = make_uniq<SelectNode>();
		select->select_list.push_back(make_uniq<StarExpression>());
		select->from_table = child->GetTableRef();
		shape(*select);
		auto statement = make_uniq<SelectStatement>();
		statement->node = std::move(select);
		return make_shared_ptr<QueryRelation>(conn.context, std::move(statement), "crossing_select");
	}

	shared_ptr<Relation> Read(yyjson_val *read) {
		auto *named = Require(read, "namedTable");
		auto names = StrArrayOf(named, "names");
		shared_ptr<Relation> rel;
		if (names.size() == 1 && names[0] == SUBSTRAIT_SEAM_TABLE) {
			if (seam_view.empty()) {
				throw InvalidInputException("substrait: the plan reads a seam but none was staged");
			}
			rel = conn.View(seam_view);
		} else if (names.size() == 2 || names.size() == 3) {
			auto ref = make_uniq<BaseTableRef>();
			ref->catalog_name = names.size() == 3 ? names[0] : catalog;
			ref->schema_name = names[names.size() - 2];
			ref->table_name = names.back();
			rel = make_shared_ptr<ViewRelation>(conn.context, std::move(ref), names.back());
		} else {
			throw InvalidInputException("substrait: a named table needs [schema, table]");
		}
		auto &columns = rel->Columns();
		vector<idx_t> fields;
		auto *projection = yyjson_obj_get(read, "projection");
		auto *select = projection ? yyjson_obj_get(projection, "select") : nullptr;
		auto *items = select ? yyjson_obj_get(select, "structItems") : nullptr;
		if (items) {
			size_t idx, max;
			yyjson_val *item;
			yyjson_arr_foreach(items, idx, max, item) {
				fields.push_back(NumericCast<idx_t>(Int64Of(yyjson_obj_get(item, "field"), 0)));
			}
		} else {
			for (idx_t i = 0; i < columns.size(); i++) {
				fields.push_back(i);
			}
		}
		vector<unique_ptr<ParsedExpression>> exprs;
		for (auto field : fields) {
			if (field >= columns.size()) {
				throw InvalidInputException("substrait: field %llu is past the end of the table", field);
			}
			exprs.push_back(make_uniq<ColumnRefExpression>(columns[field].Name()));
		}
		return Renumber(rel, std::move(exprs));
	}

	shared_ptr<Relation> JoinLike(yyjson_val *left_node, yyjson_val *right_node, const string &type_name,
	                              yyjson_val *expression, JoinRefType ref_type) {
		auto left = Rel(left_node);
		auto right = Rel(right_node);
		auto left_width = left->Columns().size();
		auto right_width = right->Columns().size();
		Scope scope = [left_width](idx_t field) -> unique_ptr<ParsedExpression> {
			if (field < left_width) {
				return make_uniq<ColumnRefExpression>(ColumnName(field), "l");
			}
			return make_uniq<ColumnRefExpression>(ColumnName(field - left_width), "r");
		};
		auto l = left->Alias("l");
		auto r = right->Alias("r");
		shared_ptr<Relation> joined;
		bool left_only = false;
		if (ref_type == JoinRefType::CROSS || ref_type == JoinRefType::POSITIONAL) {
			joined = l->CrossProduct(r, ref_type);
		} else {
			JoinType type;
			if (type_name == "JOIN_TYPE_INNER") {
				type = JoinType::INNER;
			} else if (type_name == "JOIN_TYPE_LEFT") {
				type = JoinType::LEFT;
			} else if (type_name == "JOIN_TYPE_RIGHT") {
				type = JoinType::RIGHT;
			} else if (type_name == "JOIN_TYPE_OUTER") {
				type = JoinType::OUTER;
			} else if (type_name == "JOIN_TYPE_LEFT_SEMI") {
				type = JoinType::SEMI;
				left_only = true;
			} else if (type_name == "JOIN_TYPE_LEFT_ANTI") {
				type = JoinType::ANTI;
				left_only = true;
			} else {
				throw InvalidInputException("substrait: unsupported join type '%s'", type_name);
			}
			vector<unique_ptr<ParsedExpression>> condition;
			if (expression) {
				condition.push_back(Expr(expression, scope));
			} else {
				condition.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(true)));
			}
			joined = l->Join(r, std::move(condition), type, ref_type);
		}
		vector<unique_ptr<ParsedExpression>> exprs;
		for (idx_t i = 0; i < left_width; i++) {
			exprs.push_back(make_uniq<ColumnRefExpression>(ColumnName(i), "l"));
		}
		if (!left_only) {
			for (idx_t i = 0; i < right_width; i++) {
				exprs.push_back(make_uniq<ColumnRefExpression>(ColumnName(i), "r"));
			}
		}
		return Renumber(joined, std::move(exprs));
	}

	shared_ptr<Relation> Aggregate(yyjson_val *node) {
		auto child = Rel(Require(node, "input"));
		auto scope = PositionalScope();
		vector<unique_ptr<ParsedExpression>> groups;
		auto *groupings = yyjson_obj_get(node, "groupings");
		if (groupings && yyjson_arr_size(groupings) > 1) {
			throw InvalidInputException("substrait: grouping sets are not supported");
		}
		auto *grouping = groupings ? yyjson_arr_get_first(groupings) : nullptr;
		auto *group_exprs = grouping ? yyjson_obj_get(grouping, "groupingExpressions") : nullptr;
		if (group_exprs) {
			size_t idx, max;
			yyjson_val *group;
			yyjson_arr_foreach(group_exprs, idx, max, group) {
				groups.push_back(Expr(group, scope));
			}
		}
		vector<unique_ptr<ParsedExpression>> select;
		for (auto &group : groups) {
			select.push_back(group->Copy());
		}
		auto *measures = yyjson_obj_get(node, "measures");
		if (measures) {
			size_t idx, max;
			yyjson_val *entry;
			yyjson_arr_foreach(measures, idx, max, entry) {
				auto *measure = Require(entry, "measure");
				auto name = DuckDBFunctionName(FunctionName(measure));
				auto args = Arguments(measure, scope);
				unique_ptr<ParsedExpression> filter;
				if (auto *filter_node = yyjson_obj_get(entry, "filter")) {
					filter = Expr(filter_node, scope);
				}
				bool distinct = StrOf(measure, "invocation") == "AGGREGATION_INVOCATION_DISTINCT";
				select.push_back(
				    make_uniq<FunctionExpression>(name, std::move(args), std::move(filter), nullptr, distinct));
			}
		}
		for (idx_t i = 0; i < select.size(); i++) {
			select[i]->SetAlias(ColumnName(i));
		}
		return make_shared_ptr<AggregateRelation>(std::move(child), std::move(select), std::move(groups));
	}

	shared_ptr<Relation> Project(yyjson_val *project) {
		auto child = Rel(Require(project, "input"));
		auto width = child->Columns().size();
		vector<unique_ptr<ParsedExpression>> all;
		for (idx_t i = 0; i < width; i++) {
			all.push_back(make_uniq<ColumnRefExpression>(ColumnName(i)));
		}
		if (auto *expressions = yyjson_obj_get(project, "expressions")) {
			size_t idx, max;
			yyjson_val *expression;
			yyjson_arr_foreach(expressions, idx, max, expression) {
				all.push_back(Expr(expression, PositionalScope()));
			}
		}
		auto *common = yyjson_obj_get(project, "common");
		auto *emit = common ? yyjson_obj_get(common, "emit") : nullptr;
		auto *mapping = emit ? yyjson_obj_get(emit, "outputMapping") : nullptr;
		vector<unique_ptr<ParsedExpression>> exprs;
		if (mapping) {
			size_t idx, max;
			yyjson_val *entry;
			yyjson_arr_foreach(mapping, idx, max, entry) {
				auto k = NumericCast<idx_t>(Int64Of(entry, 0));
				if (k >= all.size()) {
					throw InvalidInputException("substrait: emit index %llu out of range", k);
				}
				exprs.push_back(all[k]->Copy());
			}
		} else {
			exprs = std::move(all);
		}
		return Renumber(child, std::move(exprs));
	}

	shared_ptr<Relation> Extension(yyjson_val *node, bool multi) {
		auto *detail = Require(node, "detail");
		auto type_url = StrOf(detail, "@type");
		auto prefix = string(SUBSTRAIT_TYPE_URL_PREFIX);
		if (type_url.compare(0, prefix.size(), prefix) != 0) {
			throw InvalidInputException("substrait: unsupported extension relation '%s'", type_url);
		}
		auto kind = type_url.substr(prefix.size());
		if (multi) {
			auto *inputs = Require(node, "inputs");
			if (yyjson_arr_size(inputs) != 2) {
				throw InvalidInputException("substrait: %s needs two inputs", kind);
			}
			if (kind == "AsOfJoin") {
				return JoinLike(yyjson_arr_get(inputs, 0), yyjson_arr_get(inputs, 1), StrOf(detail, "type"),
				                yyjson_obj_get(detail, "expression"), JoinRefType::ASOF);
			}
			if (kind == "PositionalJoin") {
				return JoinLike(yyjson_arr_get(inputs, 0), yyjson_arr_get(inputs, 1), "", nullptr,
				                JoinRefType::POSITIONAL);
			}
			throw InvalidInputException("substrait: unsupported extension relation '%s'", kind);
		}
		auto child = Rel(Require(node, "input"));
		auto scope = PositionalScope();
		if (kind == "Sample") {
			auto method = EnumUtil::FromString<SampleMethod>(StrOf(detail, "method").c_str());
			auto size = StrOf(detail, "size");
			auto *percentage = yyjson_obj_get(detail, "percentage");
			bool is_percentage = percentage && yyjson_is_bool(percentage) && yyjson_get_bool(percentage);
			auto *seed = yyjson_obj_get(detail, "seed");
			return WrapSelect(child, [&](SelectNode &select) {
				auto options = make_uniq<SampleOptions>();
				options->method = method;
				options->is_percentage = is_percentage;
				options->sample_size = is_percentage ? Value::DOUBLE(std::stod(size)) : Value::BIGINT(std::stoll(size));
				if (seed && yyjson_is_int(seed)) {
					options->SetSeed(NumericCast<idx_t>(yyjson_get_uint(seed)));
				}
				select.sample = std::move(options);
			});
		}
		if (kind == "DistinctOn") {
			vector<unique_ptr<ParsedExpression>> targets;
			if (auto *target_nodes = yyjson_obj_get(detail, "targets")) {
				size_t idx, max;
				yyjson_val *target;
				yyjson_arr_foreach(target_nodes, idx, max, target) {
					targets.push_back(Expr(target, scope));
				}
			}
			auto orders = Orders(yyjson_obj_get(detail, "sorts"), scope);
			return WrapSelect(child, [&](SelectNode &select) {
				auto distinct = make_uniq<DistinctModifier>();
				distinct->distinct_on_targets = std::move(targets);
				select.modifiers.push_back(std::move(distinct));
				if (!orders.empty()) {
					auto order = make_uniq<OrderModifier>();
					order->orders = std::move(orders);
					select.modifiers.push_back(std::move(order));
				}
			});
		}
		if (kind == "Pivot") {
			auto group_count = NumericCast<idx_t>(Int64Of(yyjson_obj_get(detail, "groupCount"), 0));
			auto values = StrArrayOf(detail, "pivotValues");
			vector<unique_ptr<ParsedExpression>> empty;
			if (auto *empty_nodes = yyjson_obj_get(detail, "empty")) {
				size_t idx, max;
				yyjson_val *entry;
				yyjson_arr_foreach(empty_nodes, idx, max, entry) {
					empty.push_back(Expr(entry, scope));
				}
			}
			auto aggregate_count = empty.size();
			auto names_column = ColumnName(group_count + aggregate_count);
			vector<unique_ptr<ParsedExpression>> exprs;
			for (idx_t g = 0; g < group_count; g++) {
				exprs.push_back(make_uniq<ColumnRefExpression>(ColumnName(g)));
			}
			vector<string> seen;
			for (auto &value : values) {
				bool duplicate = false;
				for (auto &prior : seen) {
					duplicate = duplicate || prior == value;
				}
				if (duplicate) {
					continue;
				}
				seen.push_back(value);
				for (idx_t a = 0; a < aggregate_count; a++) {
					vector<unique_ptr<ParsedExpression>> position_args;
					position_args.push_back(make_uniq<ColumnRefExpression>(names_column));
					position_args.push_back(make_uniq<ConstantExpression>(Value(value)));
					vector<unique_ptr<ParsedExpression>> extract_args;
					extract_args.push_back(make_uniq<ColumnRefExpression>(ColumnName(group_count + a)));
					extract_args.push_back(make_uniq<FunctionExpression>("list_position", std::move(position_args)));
					vector<unique_ptr<ParsedExpression>> coalesce_args;
					coalesce_args.push_back(make_uniq<FunctionExpression>("list_extract", std::move(extract_args)));
					coalesce_args.push_back(empty[a]->Copy());
					exprs.push_back(
					    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(coalesce_args)));
				}
			}
			return Renumber(child, std::move(exprs));
		}
		throw InvalidInputException("substrait: unsupported extension relation '%s'", kind);
	}

	shared_ptr<Relation> Rel(yyjson_val *node) {
		if (auto *read = yyjson_obj_get(node, "read")) {
			return Read(read);
		}
		if (auto *filter = yyjson_obj_get(node, "filter")) {
			auto child = Rel(Require(filter, "input"));
			return child->Filter(Expr(Require(filter, "condition"), PositionalScope()));
		}
		if (auto *project = yyjson_obj_get(node, "project")) {
			return Project(project);
		}
		if (auto *fetch = yyjson_obj_get(node, "fetch")) {
			auto child = Rel(Require(fetch, "input"));
			auto offset = Int64Of(yyjson_obj_get(fetch, "offset"), 0);
			auto *count_node = yyjson_obj_get(fetch, "count");
			auto count = count_node ? Int64Of(count_node, 0) : NumericLimits<int64_t>::Maximum();
			return child->Limit(count, offset);
		}
		if (auto *sort = yyjson_obj_get(node, "sort")) {
			auto child = Rel(Require(sort, "input"));
			return child->Order(Orders(Require(sort, "sorts"), PositionalScope()));
		}
		if (auto *aggregate = yyjson_obj_get(node, "aggregate")) {
			return Aggregate(aggregate);
		}
		if (auto *join = yyjson_obj_get(node, "join")) {
			return JoinLike(Require(join, "left"), Require(join, "right"), StrOf(join, "type"),
			                yyjson_obj_get(join, "expression"), JoinRefType::REGULAR);
		}
		if (auto *cross = yyjson_obj_get(node, "cross")) {
			return JoinLike(Require(cross, "left"), Require(cross, "right"), "", nullptr, JoinRefType::CROSS);
		}
		if (auto *single = yyjson_obj_get(node, "extensionSingle")) {
			return Extension(single, false);
		}
		if (auto *multi = yyjson_obj_get(node, "extensionMulti")) {
			return Extension(multi, true);
		}
		if (auto *set = yyjson_obj_get(node, "set")) {
			auto *inputs = Require(set, "inputs");
			if (yyjson_arr_size(inputs) != 2) {
				throw InvalidInputException("substrait: a set operation needs two inputs");
			}
			auto left = Rel(yyjson_arr_get(inputs, 0));
			auto right = Rel(yyjson_arr_get(inputs, 1));
			auto op = StrOf(set, "op");
			SetOperationType type;
			if (op == "SET_OP_UNION_ALL" || op == "SET_OP_UNION_DISTINCT") {
				type = SetOperationType::UNION;
			} else if (op == "SET_OP_MINUS_PRIMARY" || op == "SET_OP_MINUS_PRIMARY_ALL") {
				type = SetOperationType::EXCEPT;
			} else if (op == "SET_OP_INTERSECTION_PRIMARY" || op == "SET_OP_INTERSECTION_PRIMARY_ALL") {
				type = SetOperationType::INTERSECT;
			} else {
				throw InvalidInputException("substrait: unsupported set op '%s'", op);
			}
			bool all = op.size() >= 4 && op.compare(op.size() - 4, 4, "_ALL") == 0;
			return make_shared_ptr<SetOpRelation>(std::move(left), std::move(right), type, all);
		}
		throw InvalidInputException("substrait: unsupported relation");
	}

	Connection &conn;
	const string &catalog;
	const string &seam_view;
	std::unordered_map<uint64_t, string> functions;
};

} // namespace

shared_ptr<Relation> DecodeSubstraitJson(Connection &conn, const string &catalog, const string &json,
                                         const string &seam_view) {
	auto *doc = yyjson_read(json.data(), json.size(), 0);
	if (!doc) {
		throw InvalidInputException("substrait: the plan is not valid JSON");
	}
	try {
		Decoder decoder(conn, catalog, seam_view);
		auto rel = decoder.Decode(yyjson_doc_get_root(doc));
		yyjson_doc_free(doc);
		return rel;
	} catch (...) {
		yyjson_doc_free(doc);
		throw;
	}
}

} // namespace duckdb
