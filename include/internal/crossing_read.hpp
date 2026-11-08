#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "crossing.hpp"
#include "internal/carrier.hpp"
#include "internal/fragment.hpp"
#include "internal/floor.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/seam.hpp"

namespace duckdb {

static constexpr const char *CROSSING_READ_FUNCTION = "crossing_read";
static constexpr const char *CROSSING_WRITE_FUNCTION = "crossing_write";

struct ShapedWrite;

struct CrossingBindData : public TableFunctionData, public CrossingCarrier {
	CrossingBindData(CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
	                 shared_ptr<CrossingFragment> fragment);

	CrossingTableCatalogEntry &table;
	CrossingVerb verb;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;
	//! A write the feed of which stays near: why.
	string obstacle;
	//! Set when far's native statement says the write, rather than a match on its key.
	shared_ptr<ShapedWrite> shaped;

	const CrossingIdentity &Identity() const override;
	CrossingVerb Verb() const override {
		return verb;
	}
	optional_ptr<CrossingFragment> Fragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return table.source;
	}

	//! Always rebinds, so a prepared statement keeps no plan over entries of an attach that is gone.
	bool SupportStatementCache() const override {
		return false;
	}

	const string &SourceTable() const {
		return table.described.name;
	}
	string QualifiedTable() const;
	InsertionOrderPreservingMap<string> Params() const;
};

optional_ptr<CrossingBindData> CrossingBindDataOf(LogicalGet &get, const CrossingIdentity &identity);

TableFunction CrossingReadFunction();

unique_ptr<FunctionData> MakeReadBindData(CrossingTableCatalogEntry &entry);

shared_ptr<CrossingFragment> BuildScanFragment(unique_ptr<LogicalOperator> floor, const string &source_table,
                                               vector<string> column_names, vector<LogicalType> column_types);

class LogicalCrossingRead : public LogicalExtensionOperator {
public:
	LogicalCrossingRead(TableIndex table_index, vector<ColumnIndex> column_ids, vector<LogicalType> output_types,
	                    unique_ptr<CrossingBindData> bind_data);

	TableIndex table_index;
	vector<ColumnIndex> column_ids;
	vector<LogicalType> output_types;
	unique_ptr<CrossingBindData> bind_data;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	string GetExtensionName() const override;
	bool SupportSerialization() const override {
		return false;
	}
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
