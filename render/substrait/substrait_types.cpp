#include "substrait_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/main/client_context.hpp"

#include <cstdlib>
#include <unordered_map>

namespace duckdb {

using duckdb_yyjson::yyjson_arr_get_first;
using duckdb_yyjson::yyjson_get_int;
using duckdb_yyjson::yyjson_get_str;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_is_str;
using duckdb_yyjson::yyjson_mut_arr;
using duckdb_yyjson::yyjson_mut_arr_add_val;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_int;
using duckdb_yyjson::yyjson_mut_obj_add_str;
using duckdb_yyjson::yyjson_mut_obj_add_strcpy;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_mut_write;
using duckdb_yyjson::yyjson_obj_get;
using duckdb_yyjson::yyjson_obj_iter_get_val;
using duckdb_yyjson::yyjson_obj_iter_next;
using duckdb_yyjson::yyjson_obj_iter_with;
using duckdb_yyjson::yyjson_obj_size;
using duckdb_yyjson::yyjson_val;

namespace {

constexpr const char *NULLABLE = "NULLABILITY_NULLABLE";

const char *SimpleTypeKey(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::BOOLEAN:
		return "bool";
	case LogicalTypeId::TINYINT:
		return "i8";
	case LogicalTypeId::SMALLINT:
		return "i16";
	case LogicalTypeId::INTEGER:
		return "i32";
	case LogicalTypeId::BIGINT:
		return "i64";
	case LogicalTypeId::FLOAT:
		return "fp32";
	case LogicalTypeId::DOUBLE:
		return "fp64";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIMESTAMP:
		return "timestamp";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestampTz";
	case LogicalTypeId::UUID:
		return "uuid";
	default:
		return nullptr;
	}
}

LogicalTypeId SimpleTypeOf(const std::string &key) {
	static const std::unordered_map<std::string, LogicalTypeId> map = {
	    {"bool", LogicalTypeId::BOOLEAN},
	    {"i8", LogicalTypeId::TINYINT},
	    {"i16", LogicalTypeId::SMALLINT},
	    {"i32", LogicalTypeId::INTEGER},
	    {"i64", LogicalTypeId::BIGINT},
	    {"fp32", LogicalTypeId::FLOAT},
	    {"fp64", LogicalTypeId::DOUBLE},
	    {"string", LogicalTypeId::VARCHAR},
	    {"binary", LogicalTypeId::BLOB},
	    {"date", LogicalTypeId::DATE},
	    {"time", LogicalTypeId::TIME},
	    {"timestamp", LogicalTypeId::TIMESTAMP},
	    {"timestampTz", LogicalTypeId::TIMESTAMP_TZ},
	    {"uuid", LogicalTypeId::UUID},
	};
	auto it = map.find(key);
	return it == map.end() ? LogicalTypeId::INVALID : it->second;
}

const std::unordered_map<std::string, std::string> &ToSubstraitNames() {
	static const std::unordered_map<std::string, std::string> map = {
	    {"+", "add"},
	    {"-", "subtract"},
	    {"*", "multiply"},
	    {"/", "divide"},
	    {"//", "divide_int"},
	    {"%", "modulus"},
	    {"mod", "modulus"},
	    {"~~", "like"},
	    {"!~~", "not_like"},
	    {"~~*", "ilike"},
	    {"!~~*", "not_ilike"},
	    {"stddev", "std_dev"},
	    {"prefix", "starts_with"},
	    {"suffix", "ends_with"},
	    {"substr", "substring"},
	    {"length", "char_length"},
	    {"isnan", "is_nan"},
	    {"isfinite", "is_finite"},
	    {"isinf", "is_infinite"},
	    {"sum_no_overflow", "sum"},
	    {"count_star", "count"},
	    {"first", "any_value"},
	    {"&", "bitwise_and"},
	    {"|", "bitwise_or"},
	    {"xor", "bitwise_xor"},
	    {"strlen", "octet_length"},
	};
	return map;
}

const std::unordered_map<std::string, std::string> &FromSubstraitNames() {
	static const std::unordered_map<std::string, std::string> map = [] {
		std::unordered_map<std::string, std::string> out;
		for (auto &entry : ToSubstraitNames()) {
			out.emplace(entry.second, entry.first);
		}
		out["modulus"] = "%";
		out["sum"] = "sum";
		out["count"] = "count";
		out["any_value"] = "first";
		return out;
	}();
	return map;
}

const std::unordered_map<std::string, std::string> &StandardCategories() {
	static const std::unordered_map<std::string, std::string> map = {
	    {"add", "arithmetic"},
	    {"subtract", "arithmetic"},
	    {"multiply", "arithmetic"},
	    {"divide", "arithmetic"},
	    {"modulus", "arithmetic"},
	    {"negate", "arithmetic"},
	    {"abs", "arithmetic"},
	    {"sqrt", "arithmetic"},
	    {"power", "arithmetic"},
	    {"exp", "arithmetic"},
	    {"ln", "arithmetic"},
	    {"floor", "rounding"},
	    {"ceil", "rounding"},
	    {"round", "rounding"},
	    {"sum", "arithmetic"},
	    {"avg", "arithmetic"},
	    {"min", "arithmetic"},
	    {"max", "arithmetic"},
	    {"std_dev", "arithmetic"},
	    {"variance", "arithmetic"},
	    {"count", "aggregate_generic"},
	    {"any_value", "aggregate_generic"},
	    {"equal", "comparison"},
	    {"not_equal", "comparison"},
	    {"lt", "comparison"},
	    {"lte", "comparison"},
	    {"gt", "comparison"},
	    {"gte", "comparison"},
	    {"is_null", "comparison"},
	    {"is_not_null", "comparison"},
	    {"is_nan", "comparison"},
	    {"is_finite", "comparison"},
	    {"is_infinite", "comparison"},
	    {"coalesce", "comparison"},
	    {"is_distinct_from", "comparison"},
	    {"is_not_distinct_from", "comparison"},
	    {"and", "boolean"},
	    {"or", "boolean"},
	    {"not", "boolean"},
	    {"bitwise_and", "arithmetic"},
	    {"bitwise_or", "arithmetic"},
	    {"bitwise_xor", "arithmetic"},
	    {"like", "string"},
	    {"substring", "string"},
	    {"char_length", "string"},
	    {"octet_length", "string"},
	    {"starts_with", "string"},
	    {"ends_with", "string"},
	    {"concat", "string"},
	    {"lower", "string"},
	    {"upper", "string"},
	    {"trim", "string"},
	    {"ltrim", "string"},
	    {"rtrim", "string"},
	    {"replace", "string"},
	    {"contains", "string"},
	    {"extract", "datetime"},
	    {"row_number", "arithmetic"},
	    {"rank", "arithmetic"},
	    {"dense_rank", "arithmetic"},
	    {"percent_rank", "arithmetic"},
	    {"cume_dist", "arithmetic"},
	    {"ntile", "arithmetic"},
	    {"first_value", "arithmetic"},
	    {"last_value", "arithmetic"},
	    {"nth_value", "arithmetic"},
	    {"lead", "arithmetic"},
	    {"lag", "arithmetic"},
	};
	return map;
}

} // namespace

yyjson_mut_val *SubstraitTypeToJson(yyjson_mut_doc *doc, const LogicalType &type) {
	auto *outer = yyjson_mut_obj(doc);
	auto *inner = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_str(doc, inner, "nullability", NULLABLE);
	if (auto key = SimpleTypeKey(type.id())) {
		yyjson_mut_obj_add_val(doc, outer, key, inner);
		return outer;
	}
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		yyjson_mut_obj_add_int(doc, inner, "precision", DecimalType::GetWidth(type));
		yyjson_mut_obj_add_int(doc, inner, "scale", DecimalType::GetScale(type));
		yyjson_mut_obj_add_val(doc, outer, "decimal", inner);
		return outer;
	case LogicalTypeId::LIST:
		yyjson_mut_obj_add_val(doc, inner, "type", SubstraitTypeToJson(doc, ListType::GetChildType(type)));
		yyjson_mut_obj_add_val(doc, outer, "list", inner);
		return outer;
	default: {
		yyjson_mut_obj_add_int(doc, inner, "typeReference", 0);
		auto *params = yyjson_mut_arr(doc);
		auto *param = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, param, "string", type.ToString().c_str());
		yyjson_mut_arr_add_val(params, param);
		yyjson_mut_obj_add_val(doc, inner, "typeParameters", params);
		yyjson_mut_obj_add_val(doc, outer, "userDefined", inner);
		return outer;
	}
	}
}

LogicalType SubstraitTypeFromJson(ClientContext &context, yyjson_val *type) {
	if (!type || !yyjson_is_obj(type) || yyjson_obj_size(type) != 1) {
		throw InvalidInputException("substrait: a type must be an object with one key");
	}
	auto iter = yyjson_obj_iter_with(type);
	auto *iter_key = yyjson_obj_iter_next(&iter);
	std::string key = yyjson_get_str(iter_key);
	auto *body = yyjson_obj_iter_get_val(iter_key);
	auto simple = SimpleTypeOf(key);
	if (simple != LogicalTypeId::INVALID) {
		return LogicalType(simple);
	}
	if (key == "decimal") {
		auto precision = yyjson_get_int(yyjson_obj_get(body, "precision"));
		auto scale = yyjson_get_int(yyjson_obj_get(body, "scale"));
		return LogicalType::DECIMAL(NumericCast<uint8_t>(precision), NumericCast<uint8_t>(scale));
	}
	if (key == "list") {
		return LogicalType::LIST(SubstraitTypeFromJson(context, yyjson_obj_get(body, "type")));
	}
	if (key == "userDefined") {
		auto *params = yyjson_obj_get(body, "typeParameters");
		auto *first = params ? yyjson_arr_get_first(params) : nullptr;
		auto *name = first ? yyjson_obj_get(first, "string") : nullptr;
		if (!name || !yyjson_is_str(name)) {
			throw InvalidInputException("substrait: a userDefined type needs a string type parameter");
		}
		return context.ParseLogicalType(yyjson_get_str(name));
	}
	throw InvalidInputException("substrait: unsupported type '%s'", key);
}

std::string SubstraitFunctionName(const std::string &duckdb_name) {
	auto &map = ToSubstraitNames();
	auto it = map.find(duckdb_name);
	return it == map.end() ? duckdb_name : it->second;
}

std::string DuckDBFunctionName(const std::string &substrait_name) {
	auto &map = FromSubstraitNames();
	auto it = map.find(substrait_name);
	return it == map.end() ? substrait_name : it->second;
}

std::string SubstraitFunctionUri(const std::string &substrait_name) {
	auto &map = StandardCategories();
	auto it = map.find(substrait_name);
	if (it == map.end()) {
		return SUBSTRAIT_DUCKDB_URI;
	}
	return "https://github.com/substrait-io/substrait/blob/main/extensions/functions_" + it->second + ".yaml";
}

std::string SerializeAndFree(yyjson_mut_doc *doc) {
	size_t len = 0;
	auto *json = yyjson_mut_write(doc, 0, &len);
	std::string result(json, len);
	free(json);
	yyjson_mut_doc_free(doc);
	return result;
}

} // namespace duckdb
