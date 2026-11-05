#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>
#include <unordered_map>

namespace duckdb {

string RenderSubstraitJson(const LogicalOperator &plan);

//! Rows a plan carries, and the one-part table name its render reads them under.
struct SubstraitSeam {
	string table;
	const ColumnDataCollection &rows;
};

vector<SubstraitSeam> SubstraitSeamsOf(const LogicalOperator &plan);

//! A seam's table name to the view on the receiver holding its rows.
using SubstraitSeamViews = std::unordered_map<string, string>;

bool SubstraitCanRenderCall(const Expression &expr, string &reason);

bool SubstraitCanRenderOperator(const LogicalOperator &op, string &reason);

//! Called with each catalog a table is named in, before the table binds; throws to refuse the plan.
using SubstraitCatalogCheck = std::function<void(const string &catalog)>;

//! A table named [schema, table] is read from `catalog`; one named [catalog, schema, table] is checked
//! by `check` first.
shared_ptr<Relation> DecodeSubstraitJson(Connection &conn, const string &catalog, const string &json,
                                         const SubstraitSeamViews &seam_views,
                                         const SubstraitCatalogCheck &check = nullptr);

//! A CREATE VIEW body (CrossingDdl::plan) as a query whose columns are `names`, in order. Tables in
//! `catalog` are named without it, so the view resolves them in the catalog it is created in.
unique_ptr<SelectStatement> DecodeSubstraitView(Connection &conn, const string &catalog, const string &json,
                                                const vector<string> &names,
                                                const SubstraitCatalogCheck &check = nullptr);

} // namespace duckdb
