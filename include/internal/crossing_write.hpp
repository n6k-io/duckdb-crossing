#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "crossing.hpp"
#include "crossing_attach.hpp"
#include "internal/crossing_read.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/fragment.hpp"

namespace duckdb {

//! Never registered with the database: it exists so served entries have a Catalog whose Plan*
//! reach crossing, and it answers to the attach's AttachedDatabase for transactions.
class CrossingWriteCatalog : public Catalog, public CrossingAttachOwner {
public:
	CrossingWriteCatalog(AttachedDatabase &db, CrossingAttach &attach);
	~CrossingWriteCatalog() override;

	CrossingAttach &Attach() override {
		return attach;
	}

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

	CrossingAttach &attach;
};

vector<LogicalType> ColumnTypesByName(TableCatalogEntry &table, const vector<string> &wanted);

vector<LogicalType> TableRowTypes(TableCatalogEntry &table);

vector<string> TableColumnNames(TableCatalogEntry &table);

vector<string> SetColumnNames(TableCatalogEntry &table, const vector<PhysicalIndex> &columns);

unique_ptr<Expression> RefAt(const LogicalType &type, idx_t position);

optional_ptr<LogicalGet> FindGetWithIndex(LogicalOperator &op, idx_t table_index);

vector<unique_ptr<Expression>> RowImageFrom(LogicalGet &get, TableCatalogEntry &table);

bool MergeDeletes(LogicalMergeInto &merge);

vector<unique_ptr<Expression>> ImageFromSetValues(TableCatalogEntry &table, const CrossingSeam &seam,
                                                  const vector<unique_ptr<Expression>> &set_values);

CrossingTableUse WrittenTable(const string &source_schema, const CrossingTable &described, const CrossingSeam &seam);

CrossingSeam SeamOf(TableCatalogEntry &table, const CrossingTable &described, CrossingVerb verb,
                    vector<string> set_columns);

void RequireVerb(CrossingTableCatalogEntry &table, CrossingVerb verb);

void RequireKey(TableCatalogEntry &table, const CrossingTable &described, const char *what);

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam);

unique_ptr<CrossingBindData> MakeWriteBindData(CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
                                               shared_ptr<CrossingFragment> fragment);

class CrossingSeamEntry : public TableCatalogEntry {
public:
	CrossingSeamEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                  unique_ptr<CrossingBindData> write, string obstacle);

	unique_ptr<CrossingBindData> write;
	string obstacle;
	vector<idx_t> key_positions;

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

const string &CrossingSeamEntriesKey();

class CrossingSeamEntries : public ClientContextState {
public:
	void Hold(shared_ptr<TableCatalogEntry> entry) {
		live.push_back(std::move(entry));
	}

	shared_ptr<TableCatalogEntry> Share(TableCatalogEntry &entry) {
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
	vector<shared_ptr<TableCatalogEntry>> live;
};

TableFunction CrossingWriteFunction();

class CrossingSeamFilledWrite {
public:
	CrossingSeamFilledWrite(const CrossingBindData &write, unique_ptr<ColumnDataCollection> rows);
	~CrossingSeamFilledWrite();

	CrossingWriteResult Pull(ClientContext &context, CrossingWaker waker);

private:
	CrossingTableCatalogEntry &table;
	shared_ptr<CrossingFragment> fragment;
	unique_ptr<LogicalOperator> *slot = nullptr;
	unique_ptr<LogicalOperator> seam_node;
	bool empty = false;
	unique_ptr<CrossingQuery> query;
	CrossingWriter writer;
};

class LogicalCrossingFence : public LogicalExtensionOperator {
public:
	LogicalCrossingFence(idx_t table_index, unique_ptr<CrossingBindData> bind_data);

	idx_t table_index;
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

class CrossingFence : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingFence(PhysicalPlan &physical_plan, unique_ptr<CrossingBindData> bind_data, idx_t estimated_cardinality);

	unique_ptr<CrossingBindData> bind_data;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

void AddRowImageForReturning(LogicalOperator &plan, const CrossingIdentity &identity);

struct CrossingWriteState : public GlobalSinkState {
	unique_ptr<ColumnDataCollection> rows;
	value_set_t seen_keys;
	unique_ptr<ColumnDataCollection> returned;
	ColumnDataScanState returned_scan;
	bool returned_scanning = false;
	idx_t affected_rows = 0;
	idx_t keys_sent = 0;
	unique_ptr<CrossingSeamFilledWrite> write;

	void SeeKey(DataChunk &chunk, const vector<idx_t> &key_positions, idx_t index);
};

class CrossingWrite : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingWrite(PhysicalPlan &physical_plan, unique_ptr<CrossingBindData> bind_data, vector<LogicalType> types,
	              idx_t estimated_cardinality);

	unique_ptr<CrossingBindData> bind_data;
	shared_ptr<TableCatalogEntry> entry;
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

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
