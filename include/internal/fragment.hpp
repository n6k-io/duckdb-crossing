#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

struct CrossingFragment {
	CrossingFragment() = default;
	~CrossingFragment();

	unique_ptr<LogicalOperator> plan;
	string plan_text;

	vector<LogicalType> seam_types;

	vector<string> column_names;
	vector<LogicalType> column_types;

	unique_ptr<LogicalOperator> floor;
	vector<ColumnBinding> floor_bindings;
	optional_ptr<LogicalGet> floor_get;
	optional_idx row_id_column;

	optional_ptr<LogicalProjection> crossing_projection;
	vector<LogicalType> output_types;
	vector<column_t> projected_columns;
	TableIndex table_index = TableIndex(0);

	bool frozen = false;

	//! Sources other than the host's whose scans were folded in.
	vector<reference<CrossingSource>> folded_sources;

	//! The seam node (MakeSeamNode) still in the plan, or null once something has filled it.
	unique_ptr<LogicalOperator> *SeamSlot();
	bool HasSeam() {
		return SeamSlot() != nullptr;
	}

	//! The floor reads far's row id too, as a column past the table's. Its position.
	idx_t IncludeRowId();

	void SealFloor(const LogicalOperator &node);
	void RebuildPlanForColumns(const vector<column_t> &columns);
	void AdoptTableIndex(TableIndex index);
	void RemapTableIndices(const FreshTableIndex &fresh);

	void ResolveTypesAndText();
	void VerifyInvariants();

private:
	static bool Contains(const LogicalOperator &haystack, const LogicalOperator &needle);

	optional_ptr<const LogicalOperator> sealed_floor;
};

} // namespace duckdb
