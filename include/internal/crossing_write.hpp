#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "crossing.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/fragment.hpp"
#include "internal/source.hpp"

namespace duckdb {

class CrossingAttach;

//! Never registered with the database: it exists so served entries have a Catalog whose Plan*
//! reach crossing, and it answers to the attach's AttachedDatabase for transactions.
class CrossingWriteCatalog : public Catalog {
public:
	CrossingWriteCatalog(AttachedDatabase &db, CrossingAttach &attach);
	~CrossingWriteCatalog() override;

	CrossingAttach &attach;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
};

CrossingSeam SeamOf(CrossingTableCatalogEntry &table, CrossingVerb verb, vector<string> set_columns);

void RequireVerb(CrossingTableCatalogEntry &table, CrossingVerb verb);

void RequireKey(CrossingTableCatalogEntry &table, const char *what);

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam);

class CrossingSeamEntry : public TableCatalogEntry {
public:
	CrossingSeamEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                  CrossingTableCatalogEntry &target, CrossingVerb verb, CrossingSeam seam,
	                  shared_ptr<CrossingFragment> fragment, string obstacle);

	CrossingTableCatalogEntry &target;
	CrossingVerb verb;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;
	string obstacle;
	vector<idx_t> key_positions;

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

const string &CrossingSeamEntriesKey();

class CrossingSeamEntries : public ClientContextState {
public:
	void Hold(shared_ptr<CrossingSeamEntry> entry) {
		live.push_back(std::move(entry));
	}

	shared_ptr<CrossingSeamEntry> Share(CrossingSeamEntry &entry) {
		for (auto &held : live) {
			if (held.get() == &entry) {
				return held;
			}
		}
		throw InternalException("crossing: the seam of '%s' outlived its statement", entry.name);
	}

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		live.clear();
	}

	static shared_ptr<CrossingSeamEntries> Get(ClientContext &context) {
		return context.registered_state->GetOrCreate<CrossingSeamEntries>(CrossingSeamEntriesKey());
	}

private:
	vector<shared_ptr<CrossingSeamEntry>> live;
};

static constexpr const char *CROSSING_WRITE_FUNCTION = "crossing_table_write";

struct CrossingWriteBindData : public TableFunctionData, public CrossingWriteCarrier {
	optional_ptr<CrossingTableCatalogEntry> table;
	CrossingVerb verb = CrossingVerb::INSERT;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;

	optional_ptr<CrossingFragment> GetWriteFragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return table->Source();
	}
	bool SupportStatementCache() const override {
		return false;
	}
};

TableFunction CrossingWriteFunction();

class CrossingSeamFilledWrite {
public:
	CrossingSeamFilledWrite(CrossingTableCatalogEntry &table, CrossingVerb verb, const CrossingSeam &seam,
	                        shared_ptr<CrossingFragment> fragment, unique_ptr<ColumnDataCollection> rows);
	~CrossingSeamFilledWrite();

	CrossingWriteResult Ask(ClientContext &context, CrossingWaker waker);

private:
	CrossingTableCatalogEntry &table;
	shared_ptr<CrossingFragment> fragment;
	unique_ptr<LogicalOperator> *slot = nullptr;
	unique_ptr<LogicalOperator> seam_node;
	bool empty = false;
	unique_ptr<CrossingQuery> query;
	CrossingWriter writer;
};

class LogicalCrossingWholeWrite : public LogicalExtensionOperator {
public:
	LogicalCrossingWholeWrite(idx_t table_index, unique_ptr<CrossingWriteBindData> bind_data);

	idx_t table_index;
	unique_ptr<CrossingWriteBindData> bind_data;

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

class CrossingWholeWrite : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingWholeWrite(PhysicalPlan &physical_plan, unique_ptr<CrossingWriteBindData> bind_data,
	                   idx_t estimated_cardinality);

	unique_ptr<CrossingWriteBindData> bind_data;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

InsertionOrderPreservingMap<string> CrossingWriteParams(const CrossingWriteBindData &bind_data);

void WidenKeyedWritesForReturning(LogicalOperator &plan);

struct CrossingWriteState : public GlobalSinkState {
	unique_ptr<ColumnDataCollection> rows;
	unordered_set<hash_t> seen_keys;
	unique_ptr<ColumnDataCollection> returned;
	ColumnDataScanState returned_scan;
	bool returned_scanning = false;
	idx_t affected_rows = 0;
	unique_ptr<CrossingSeamFilledWrite> write;
	idx_t keys_sent = 0;

	void SeeKey(DataChunk &chunk, const vector<idx_t> &key_positions, idx_t index);
};

class CrossingWrite : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingWrite(PhysicalPlan &physical_plan, CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
	              shared_ptr<CrossingFragment> fragment, vector<LogicalType> types, idx_t estimated_cardinality);

	CrossingTableCatalogEntry &table;
	CrossingVerb verb;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;
	shared_ptr<CrossingSeamEntry> entry;
	bool return_chunk = false;
	string obstacle;

	vector<unique_ptr<Expression>> seam_row;
	vector<unique_ptr<Expression>> returned_row;
	vector<idx_t> key_positions;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
