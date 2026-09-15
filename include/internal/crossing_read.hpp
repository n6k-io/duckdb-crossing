#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "internal/crossing_scan.hpp"

namespace duckdb {

class LogicalCrossingRead : public LogicalExtensionOperator {
public:
	LogicalCrossingRead(idx_t table_index, vector<ColumnIndex> column_ids, vector<LogicalType> output_types,
	                    unique_ptr<CrossingScanBindData> bind_data);

	idx_t table_index;
	vector<ColumnIndex> column_ids;
	vector<LogicalType> output_types;
	unique_ptr<CrossingScanBindData> bind_data;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;
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

struct CrossingReadGlobalState : public GlobalSourceState {
	//! Borrows the plan from the fragment the bind data owns.
	unique_ptr<CrossingQuery> query;
	CrossingScan scan;
	vector<column_t> column_ids;
	//! Where each requested column sits in the chunk the fragment produces.
	vector<idx_t> source_position;
	idx_t source_column_count = 0;
	idx_t partitions = 1;
	atomic<idx_t> next_partition {0};
	atomic<idx_t> finished_partitions {0};
	atomic<idx_t> rows_emitted {0};

	idx_t MaxThreads() override {
		return partitions;
	}
};

struct CrossingReadLocalState : public LocalSourceState {
	DataChunk source_chunk;
	CrossingReader reader;
};

class CrossingRead : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingRead(PhysicalPlan &physical_plan, vector<LogicalType> types, unique_ptr<CrossingScanBindData> bind_data,
	             vector<column_t> column_ids, idx_t estimated_cardinality);

	unique_ptr<CrossingScanBindData> bind_data;
	vector<column_t> column_ids;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

InsertionOrderPreservingMap<string> CrossingScanParams(const CrossingScanBindData &bind_data);

} // namespace duckdb
