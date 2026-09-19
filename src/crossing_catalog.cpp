#include "internal/crossing_catalog.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

namespace {

BinderException SchemasAreServed() {
	return BinderException("crossing: schemas are served by the source; CREATE SCHEMA and DROP SCHEMA are not "
	                       "supported");
}

InternalException DmlBypassedTheWriteCatalog() {
	return InternalException("crossing: a write reached the attach catalog instead of the write catalog");
}

BinderException CreateNotSupported(const string &schema) {
	return BinderException("crossing: schema '%s' is served by a source; CREATE is not supported", schema);
}

} // namespace

CrossingCatalog::CrossingCatalog(AttachedDatabase &db, string catalog_type_p, unique_ptr<CrossingSource> source)
    : Catalog(db), catalog_type(std::move(catalog_type_p)), attach(db, std::move(source)) {
}

CrossingCatalog::~CrossingCatalog() = default;

void CrossingCatalog::Initialize(bool) {
}

string CrossingCatalog::GetCatalogType() {
	return catalog_type;
}

void CrossingCatalog::OnDetach(ClientContext &context) {
	attach.Detach(context);
}

optional_ptr<CatalogEntry> CrossingCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw SchemasAreServed();
}

void CrossingCatalog::DropSchema(ClientContext &, DropInfo &info) {
	attach.ThrowIfSchemaServed(info.name);
	throw SchemasAreServed();
}

optional_ptr<SchemaCatalogEntry> CrossingCatalog::SchemaNamed(const string &name) {
	lock_guard<mutex> guard(schemas_lock);
	auto it = schemas.find(name);
	if (it != schemas.end()) {
		return it->second.get();
	}
	string spelling;
	for (auto &served : attach.Schemas()) {
		if (StringUtil::CIEquals(served, name)) {
			spelling = served;
			break;
		}
	}
	if (spelling.empty()) {
		return nullptr;
	}
	CreateSchemaInfo info;
	info.catalog = GetName();
	info.schema = spelling;
	auto entry = make_uniq<CrossingSchemaEntry>(*this, info);
	auto raw = entry.get();
	schemas[spelling] = std::move(entry);
	return raw;
}

optional_ptr<SchemaCatalogEntry> CrossingCatalog::LookupSchema(CatalogTransaction, const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto entry = SchemaNamed(schema_name);
	if (!entry && if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name %s does not exist!", schema_name);
	}
	return entry;
}

void CrossingCatalog::ScanSchemas(ClientContext &, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &name : attach.Schemas()) {
		callback(*SchemaNamed(name));
	}
}

PhysicalOperator &CrossingCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {
	return attach.PlanCreateTableAs(context, planner, op, plan);
}

PhysicalOperator &CrossingCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                              optional_ptr<PhysicalOperator>) {
	throw DmlBypassedTheWriteCatalog();
}

PhysicalOperator &CrossingCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                              PhysicalOperator &) {
	throw DmlBypassedTheWriteCatalog();
}

PhysicalOperator &CrossingCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                              PhysicalOperator &) {
	throw DmlBypassedTheWriteCatalog();
}

DatabaseSize CrossingCatalog::GetDatabaseSize(ClientContext &) {
	return DatabaseSize();
}

bool CrossingCatalog::InMemory() {
	return true;
}

string CrossingCatalog::GetDBPath() {
	return string();
}

CrossingSchemaEntry::CrossingSchemaEntry(CrossingCatalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), attach(catalog.Attach()) {
}

void CrossingSchemaEntry::Scan(ClientContext &context, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback, &Transaction::Get(context, catalog));
}

void CrossingSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback, nullptr);
}

void CrossingSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback,
                               optional_ptr<Transaction> transaction) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	case_insensitive_set_t seen;
	attach.ScanTables(name, *this, seen, callback, transaction);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                            const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	return attach.LookupTable(name, *this, lookup_info.GetEntryName(), transaction.transaction);
}

void CrossingSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	auto &transaction = Transaction::Get(context, catalog);
	if (info.type == CatalogType::TABLE_ENTRY && attach.ServesTable(name, info.name, &transaction)) {
		CrossingDdl ddl;
		ddl.verb = CrossingVerb::DROP;
		ddl.schema = name;
		ddl.table = info.name;
		ddl.drop = &info;
		attach.Ddl(context, transaction, *this, ddl);
		return;
	}
	attach.ThrowIfServed(name, info.name, "DROP");
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		return;
	}
	throw CatalogException::MissingEntry(info.type, info.name, string());
}

void CrossingSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.GetCatalogType() == CatalogType::TABLE_ENTRY &&
	    attach.ServesTable(name, info.name, transaction.transaction)) {
		CrossingDdl ddl;
		ddl.verb = CrossingVerb::ALTER;
		ddl.schema = name;
		ddl.table = info.name;
		ddl.alter = &info;
		attach.Ddl(transaction.GetContext(), *transaction.transaction, *this, ddl);
		return;
	}
	attach.ThrowIfServed(name, info.name, "ALTER");
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		return;
	}
	throw CatalogException::MissingEntry(info.GetCatalogType(), info.name, string());
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &,
                                                            TableCatalogEntry &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                            BoundCreateTableInfo &info) {
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::CREATE;
	ddl.schema = name;
	ddl.table = info.Base().table;
	ddl.create = info.base.get();
	attach.Ddl(transaction.GetContext(), *transaction.transaction, *this, ddl);
	return attach.LookupTable(name, *this, ddl.table, transaction.transaction);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw CreateNotSupported(name);
}

optional_ptr<CatalogEntry> CrossingSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw CreateNotSupported(name);
}

} // namespace duckdb
