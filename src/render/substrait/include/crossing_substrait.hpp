#pragma once

#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

#include <functional>

namespace duckdb {

//! Complete: rows the plan holds are written into the JSON.
string RenderSubstraitJson(const LogicalOperator &plan);

bool SubstraitCanRenderCall(const Expression &expr, string &reason);

bool SubstraitCanRenderOperator(const LogicalOperator &op, string &reason);

//! Called with each catalog a table is named in, before the table binds; throws to refuse the plan.
using SubstraitCatalogCheck = std::function<void(const string &catalog)>;

//! A table named [schema, table] is read from `catalog`; one named [catalog, schema, table] is checked
//! by `check` first.
shared_ptr<Relation> DecodeSubstraitJson(Connection &conn, const string &catalog, const string &json,
                                         const SubstraitCatalogCheck &check = nullptr);

//! A CREATE VIEW body (CrossingDdl::plan) as a query whose columns are `names`, in order. Tables in
//! `catalog` are named without it, so the view resolves them in the catalog it is created in.
unique_ptr<SelectStatement> DecodeSubstraitView(Connection &conn, const string &catalog, const string &json,
                                                const vector<string> &names,
                                                const SubstraitCatalogCheck &check = nullptr);

struct SubstraitWriteAction {
	MergeActionCondition when;
	MergeActionType type;
	optional_ptr<const Expression> condition;
	vector<string> columns;
	vector<reference<const Expression>> values;
};

//! A write as its target and what it is asked to do. Its expressions see the target's columns, bound under
//! `target_index`, then the source's columns.
struct SubstraitWrite {
	CrossingVerb verb = CrossingVerb::INSERT;
	bool merge = false;
	string catalog;
	string schema;
	string table;
	vector<string> target_columns;
	idx_t target_index = 0;
	//! Valid: the expressions read the source's columns by position, under this table index.
	optional_idx source_index;
	optional_ptr<const Expression> condition;
	vector<string> set_columns;
	vector<reference<const Expression>> values;
	vector<SubstraitWriteAction> actions;
	vector<string> returning;
	vector<LogicalType> returning_types;
	//! A create: its OnCreateConflict.
	uint8_t on_conflict = 0;
};

//! One part of a statement's far plan. `source` holds at most one seam, whose rows come in on this slice's
//! streams. A write's source may be null.
struct SubstraitSlice {
	idx_t id = 0;
	bool ordered = false;
	bool seam_ordered = false;
	optional_idx seam_estimate;
	optional_ptr<const LogicalOperator> source;
	optional_ptr<const SubstraitWrite> write;
};

//! A statement's slices as one Substrait plan, one relation each.
string RenderSubstraitSlices(const vector<SubstraitSlice> &slices);

struct DecodedSeam {
	idx_t id = 0;
	vector<LogicalType> types;
	bool ordered = false;
	optional_idx estimate;
};

struct DecodedSlice {
	idx_t id = 0;
	bool ordered = false;
	unique_ptr<SQLStatement> statement;
	//! Set on a write: the catalog it changes.
	string write_catalog;
	//! What the write's RETURNING must produce, or empty.
	vector<LogicalType> returning_types;
};

struct DecodedStatement {
	vector<DecodedSlice> slices;
	vector<DecodedSeam> seams;
};

DecodedStatement DecodeSubstraitSlices(Connection &conn, const string &catalog, const string &json,
                                       const SubstraitCatalogCheck &check = nullptr,
                                       const CrossingFloorResolver &resolver = nullptr);

} // namespace duckdb
