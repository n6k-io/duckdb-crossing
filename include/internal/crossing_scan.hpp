#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "crossing.hpp"
#include "internal/source.hpp"

namespace duckdb {

class CrossingTableCatalogEntry;

static constexpr const char *CROSSING_SCAN_FUNCTION = "crossing_table_scan";

struct CrossingScanBindData : public TableFunctionData, public CrossingReadCarrier {
	optional_ptr<CrossingSource> source;
	string source_schema;
	string source_table;
	vector<string> column_names;
	vector<LogicalType> column_types;

	shared_ptr<CrossingFragment> fragment;

	optional_ptr<CrossingTableCatalogEntry> table;

	optional_ptr<CrossingFragment> GetReadFragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return *source.get_mutable();
	}

	//! The plan is bound against catalog entries this attach owns, and the source outlives neither.
	bool SupportStatementCache() const override {
		return false;
	}
};

TableFunction CrossingScanFunction();

//! The bind data for a scan of `entry`, with its fragment built and its floor sealed.
unique_ptr<FunctionData> MakeCrossingScanBindData(CrossingTableCatalogEntry &entry, CrossingSource &source);

} // namespace duckdb
