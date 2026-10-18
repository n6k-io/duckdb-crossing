#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

static constexpr const char *SUBSTRAIT_SEAM_TABLE = "seam";

string RenderSubstraitJson(const LogicalOperator &plan);

bool SubstraitCanRenderCall(const Expression &expr, string &reason);

bool SubstraitCanRenderOperator(const LogicalOperator &op, string &reason);

shared_ptr<Relation> DecodeSubstraitJson(Connection &conn, const string &catalog, const string &json,
                                         const string &seam_view);

//! A CREATE VIEW body (CrossingDdl::plan) as a query whose columns are `names`, in order. Tables are named
//! without `catalog`, so the view resolves them in the catalog it is created in.
unique_ptr<SelectStatement> DecodeSubstraitView(Connection &conn, const string &catalog, const string &json,
                                                const vector<string> &names);

} // namespace duckdb
