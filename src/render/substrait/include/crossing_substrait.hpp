#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

static constexpr const char *SUBSTRAIT_SEAM_TABLE = "seam";

string RenderSubstraitJson(const LogicalOperator &plan);

bool SubstraitCanRenderCall(const Expression &expr, string &reason);

bool SubstraitCanRenderOperator(const LogicalOperator &op, string &reason);

//! Called with each catalog a table is named in, before the table binds; throws to refuse the plan.
using SubstraitCatalogCheck = std::function<void(const string &catalog)>;

//! A table named [schema, table] is read from `catalog`; one named [catalog, schema, table] is checked
//! by `check` first.
shared_ptr<Relation> DecodeSubstraitJson(Connection &conn, const string &catalog, const string &json,
                                         const string &seam_view, const SubstraitCatalogCheck &check = nullptr);

//! A CREATE VIEW body (CrossingDdl::plan) as a query whose columns are `names`, in order. Tables in
//! `catalog` are named without it, so the view resolves them in the catalog it is created in.
unique_ptr<SelectStatement> DecodeSubstraitView(Connection &conn, const string &catalog, const string &json,
                                                const vector<string> &names,
                                                const SubstraitCatalogCheck &check = nullptr);

} // namespace duckdb
