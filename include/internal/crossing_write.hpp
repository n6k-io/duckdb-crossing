#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
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
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

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

optional_ptr<LogicalGet> FindGetWithIndex(LogicalOperator &op, TableIndex table_index);

CrossingSeam SeamOf(TableCatalogEntry &table, const CrossingTable &described, CrossingVerb verb,
                    vector<string> set_columns);

void RequireVerb(CrossingTableCatalogEntry &table, CrossingVerb verb);

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam);

//! A write that takes nothing: its node has no child, so there is no seam to fill.
shared_ptr<CrossingFragment> PlanBareWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                                   const CrossingSeam &seam, TableIndex table_index);

unique_ptr<CrossingBindData> MakeWriteBindData(CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
                                               shared_ptr<CrossingFragment> fragment);

TableFunction CrossingWriteFunction();

//! Near's side of a write. Any child is the rows it sends; with none, the source runs all of it.
class LogicalCrossingNear : public LogicalExtensionOperator {
public:
	LogicalCrossingNear(TableIndex table_index, vector<ColumnBinding> bindings, vector<LogicalType> output_types,
	                    vector<optional_idx> emit, unique_ptr<CrossingBindData> bind_data);

	TableIndex table_index;
	vector<ColumnBinding> bindings;
	vector<LogicalType> output_types;
	//! For each output column, the returned column it is; invalid when nothing reads it.
	vector<optional_idx> emit;
	unique_ptr<CrossingBindData> bind_data;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}
	vector<TableIndex> GetTableIndex() const override {
		return {table_index};
	}
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	string GetExtensionName() const override;
	bool SupportSerialization() const override {
		return false;
	}
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override {
		types = output_types;
	}
};

} // namespace duckdb
