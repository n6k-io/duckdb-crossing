#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "crossing.hpp"
#include "internal/carrier.hpp"
#include "internal/crossing_table_entry.hpp"

namespace duckdb {

static constexpr const char *CROSSING_READ_FUNCTION = "crossing_read";
static constexpr const char *CROSSING_WRITE_FUNCTION = "crossing_write";

//! One shape for a read and a fence: what the table, the verb and the fragment say, nothing copied.
struct CrossingBindData : public TableFunctionData, public CrossingCarrier {
	CrossingBindData(CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
	                 shared_ptr<CrossingFragment> fragment);

	CrossingTableCatalogEntry &table;
	CrossingVerb verb;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;

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

	//! The plan is bound against catalog entries this attach owns, and the source outlives neither.
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

//! The plan the source returned, or the statement fails with why it declined.
unique_ptr<LogicalOperator> RequirePlan(CrossingPlan planned, CrossingVerb verb, const string &table);

//! The bind data for a scan of `entry`, with its fragment built and its floor sealed.
unique_ptr<FunctionData> MakeReadBindData(CrossingTableCatalogEntry &entry);

shared_ptr<CrossingFragment> BuildScanFragment(unique_ptr<LogicalOperator> floor, const string &source_table,
                                               vector<string> column_names, vector<LogicalType> column_types);

class LogicalCrossingRead : public LogicalExtensionOperator {
public:
	LogicalCrossingRead(idx_t table_index, vector<ColumnIndex> column_ids, vector<LogicalType> output_types,
	                    unique_ptr<CrossingBindData> bind_data);

	idx_t table_index;
	vector<ColumnIndex> column_ids;
	vector<LogicalType> output_types;
	unique_ptr<CrossingBindData> bind_data;

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
	atomic<idx_t> next_partition {0};
	atomic<idx_t> finished_partitions {0};
	atomic<idx_t> rows_emitted {0};

	idx_t MaxThreads() override {
		return scan.partitions;
	}
};

struct CrossingReadLocalState : public LocalSourceState {
	DataChunk source_chunk;
	CrossingReader reader;
};

class CrossingRead : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingRead(PhysicalPlan &physical_plan, vector<LogicalType> types, unique_ptr<CrossingBindData> bind_data,
	             vector<column_t> column_ids, idx_t estimated_cardinality);

	unique_ptr<CrossingBindData> bind_data;
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

struct CrossingParking;

//! After a WAIT. True with BLOCKED when parked, true with FINISHED when the operator can no longer
//! block; false when the waker already fired and the source is to be asked again.
bool ParkSource(CrossingParking &parking, GlobalSourceState &state, SourceResultType &result);

} // namespace duckdb
