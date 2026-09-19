#include "own_catalog/own_catalog.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace own {

namespace {

BinderException NotHere(const char *what) {
	return BinderException("own: %s is not supported", what);
}

struct OwnStorageInfo : public StorageExtensionInfo {
	explicit OwnStorageInfo(shared_ptr<FarStore> store_p) : store(std::move(store_p)) {
	}
	shared_ptr<FarStore> store;
};

unique_ptr<Catalog> AttachOwn(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &, AttachedDatabase &db,
                              const string &, AttachInfo &info, AttachOptions &) {
	auto &own_info = static_cast<OwnStorageInfo &>(*storage_info);
	auto source = make_uniq<FarSource>(own_info.store, info.path);
	info.path = string();
	return make_uniq<OwnCatalog>(db, std::move(source));
}

unique_ptr<TransactionManager> CreateOwnTransactionManager(optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
                                                           Catalog &catalog) {
	return make_uniq<OwnTransactionManager>(db, catalog.Cast<OwnCatalog>().attach);
}

} // namespace

OwnCatalog::OwnCatalog(AttachedDatabase &db, unique_ptr<FarSource> source)
    : Catalog(db), attach(db, Crossing<FarSource>::Adapt(std::move(source))) {
}

OwnCatalog::~OwnCatalog() = default;

void OwnCatalog::Initialize(bool) {
	CreateSchemaInfo info;
	info.catalog = GetName();
	info.schema = "main";
	schema = make_uniq<OwnSchema>(*this, info);
}

string OwnCatalog::GetCatalogType() {
	return "owndb";
}

void OwnCatalog::OnDetach(ClientContext &context) {
	attach.Detach(context);
}

optional_ptr<CatalogEntry> OwnCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw NotHere("CREATE SCHEMA");
}

void OwnCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw NotHere("DROP SCHEMA");
}

optional_ptr<SchemaCatalogEntry> OwnCatalog::LookupSchema(CatalogTransaction, const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	if (StringUtil::CIEquals(schema_lookup.GetEntryName(), "main")) {
		return schema.get();
	}
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name %s does not exist!",
		                       schema_lookup.GetEntryName());
	}
	return nullptr;
}

void OwnCatalog::ScanSchemas(ClientContext &, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*schema);
}

PhysicalOperator &OwnCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                PhysicalOperator &) {
	throw NotHere("CREATE TABLE AS");
}

PhysicalOperator &OwnCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                         optional_ptr<PhysicalOperator>) {
	throw InternalException("own: a write reached the catalog instead of crossing");
}

PhysicalOperator &OwnCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                         PhysicalOperator &) {
	throw InternalException("own: a write reached the catalog instead of crossing");
}

PhysicalOperator &OwnCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                         PhysicalOperator &) {
	throw InternalException("own: a write reached the catalog instead of crossing");
}

DatabaseSize OwnCatalog::GetDatabaseSize(ClientContext &) {
	return DatabaseSize();
}

bool OwnCatalog::InMemory() {
	return true;
}

string OwnCatalog::GetDBPath() {
	return string();
}

void OwnCatalog::Register(ExtensionLoader &loader, shared_ptr<FarStore> store) {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = AttachOwn;
	ext->create_transaction_manager = CreateOwnTransactionManager;
	ext->storage_info = make_uniq<OwnStorageInfo>(std::move(store));
	auto &db = loader.GetDatabaseInstance();
	StorageExtension::Register(DBConfig::GetConfig(db), "owndb", std::move(ext));
	Crossing<FarSource>::RegisterPass(db);
}

OwnSchema::OwnSchema(OwnCatalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), attach(catalog.attach) {
}

void OwnSchema::Scan(ClientContext &, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback);
}

void OwnSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	case_insensitive_set_t seen;
	attach.ScanTables(name, *this, seen, callback);
}

optional_ptr<CatalogEntry> OwnSchema::LookupEntry(CatalogTransaction, const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	return attach.LookupTable(name, *this, lookup_info.GetEntryName());
}

void OwnSchema::DropEntry(ClientContext &, DropInfo &info) {
	attach.ThrowIfServed(name, info.name, "DROP");
	throw CatalogException::MissingEntry(info.type, info.name, string());
}

void OwnSchema::Alter(CatalogTransaction, AlterInfo &info) {
	attach.ThrowIfServed(name, info.name, "ALTER");
	throw CatalogException::MissingEntry(info.GetCatalogType(), info.name, string());
}

optional_ptr<CatalogEntry> OwnSchema::CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) {
	throw NotHere("CREATE INDEX");
}

optional_ptr<CatalogEntry> OwnSchema::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw NotHere("CREATE FUNCTION");
}

optional_ptr<CatalogEntry> OwnSchema::CreateTable(CatalogTransaction, BoundCreateTableInfo &) {
	throw NotHere("CREATE TABLE");
}

optional_ptr<CatalogEntry> OwnSchema::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw NotHere("CREATE VIEW");
}

optional_ptr<CatalogEntry> OwnSchema::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw NotHere("CREATE SEQUENCE");
}

optional_ptr<CatalogEntry> OwnSchema::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw NotHere("CREATE TABLE FUNCTION");
}

optional_ptr<CatalogEntry> OwnSchema::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw NotHere("CREATE COPY FUNCTION");
}

optional_ptr<CatalogEntry> OwnSchema::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw NotHere("CREATE PRAGMA FUNCTION");
}

optional_ptr<CatalogEntry> OwnSchema::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw NotHere("CREATE COLLATION");
}

optional_ptr<CatalogEntry> OwnSchema::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw NotHere("CREATE TYPE");
}

OwnTransactionManager::OwnTransactionManager(AttachedDatabase &db, CrossingAttach &attach_p)
    : TransactionManager(db), attach(attach_p) {
}

Transaction &OwnTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<Transaction>(*this, context);
	auto &raw = *transaction;
	lock_guard<mutex> guard(lock);
	live[&raw] = std::move(transaction);
	return raw;
}

ErrorData OwnTransactionManager::CommitTransaction(ClientContext &, Transaction &transaction) {
	auto error = CrossingAttach::Commit(attach.Release(transaction));
	lock_guard<mutex> guard(lock);
	live.erase(&transaction);
	return error;
}

void OwnTransactionManager::RollbackTransaction(Transaction &transaction) {
	CrossingAttach::Rollback(attach.Release(transaction));
	lock_guard<mutex> guard(lock);
	live.erase(&transaction);
}

void OwnTransactionManager::Checkpoint(ClientContext &, bool) {
}

} // namespace own
