#pragma once

#include "duckdb/common/types.hpp"
#include "yyjson.hpp"

#include <string>

namespace duckdb {
class ClientContext;

static constexpr const char *SUBSTRAIT_DUCKDB_URI = "https://duckdb.org/substrait/duckdb.yaml";
static constexpr const char *SUBSTRAIT_TYPE_URL_PREFIX = "type.googleapis.com/crossing.";
static constexpr int SUBSTRAIT_MINOR_VERSION = 98;

duckdb_yyjson::yyjson_mut_val *SubstraitTypeToJson(duckdb_yyjson::yyjson_mut_doc *doc, const LogicalType &type);
LogicalType SubstraitTypeFromJson(ClientContext &context, duckdb_yyjson::yyjson_val *type);

std::string SubstraitFunctionName(const std::string &duckdb_name);
std::string DuckDBFunctionName(const std::string &substrait_name);
std::string SubstraitFunctionUri(const std::string &substrait_name);

std::string SerializeAndFree(duckdb_yyjson::yyjson_mut_doc *doc);

} // namespace duckdb
