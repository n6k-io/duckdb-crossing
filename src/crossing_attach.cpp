#include "crossing_attach.hpp"

#include "internal/crossing_table_entry.hpp"
#include "internal/crossing_write.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <algorithm>

namespace duckdb {

struct CrossingAttach::SchemaState {
	mutex lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> cache;
	//! Never freed while the attach lives: a binder can hold a raw CatalogEntry* for the rest of
	//! the statement.
	vector<unique_ptr<CatalogEntry>> retired;
};

CrossingAttach::CrossingAttach(AttachedDatabase &db_p, unique_ptr<CrossingSource> source_p)
    : db(db_p), source(std::move(source_p)), write_catalog(make_uniq<CrossingWriteCatalog>(db_p, *this)) {
	if (!source) {
		throw InternalException("crossing: an attach needs a source");
	}
}

CrossingAttach::~CrossingAttach() {
	for (auto &entry : begun) {
		Rollback(std::move(entry.second->session));
	}
}

CrossingSource &CrossingAttach::Source() {
	return *source;
}

const CrossingIdentity &CrossingAttach::Identity() const {
	return source->Identity();
}

AttachedDatabase &CrossingAttach::Database() {
	return db;
}

case_insensitive_map_t<CrossingAttach::Listing> &CrossingAttach::ListedSchemas() {
	if (!schemas_listed) {
		served.clear();
		for (auto &schema : source->Schemas()) {
			served[schema];
		}
		schemas_listed = true;
	}
	return served;
}

optional_ptr<const case_insensitive_set_t> CrossingAttach::TablesOf(const string &schema) {
	auto &listing = ListedSchemas();
	auto it = listing.find(schema);
	if (it == listing.end()) {
		return nullptr;
	}
	auto &entry = it->second;
	if (!entry.listed) {
		entry.tables.clear();
		for (auto &table : source->Tables(schema)) {
			entry.tables.insert(table);
		}
		entry.listed = true;
	}
	return &entry.tables;
}

vector<string> CrossingAttach::Schemas() {
	lock_guard<mutex> guard(schemas_lock);
	vector<string> out;
	for (auto &schema : ListedSchemas()) {
		out.push_back(schema.first);
	}
	std::sort(out.begin(), out.end());
	return out;
}

template <class F>
auto CrossingAttach::WithTables(const string &schema, optional_ptr<Transaction> transaction, F &&f) {
	auto overlay = OverlayOf(transaction, schema);
	if (overlay) {
		if (!overlay->listed) {
			for (auto &table : source->Tables(schema)) {
				overlay->tables.insert(table);
			}
			overlay->listed = true;
		}
		return f(optional_ptr<const case_insensitive_set_t>(&overlay->tables));
	}
	lock_guard<mutex> guard(schemas_lock);
	return f(TablesOf(schema));
}

vector<string> CrossingAttach::Tables(const string &schema, optional_ptr<Transaction> transaction) {
	return WithTables(schema, transaction, [](optional_ptr<const case_insensitive_set_t> tables) {
		vector<string> out;
		if (tables) {
			out.assign(tables->begin(), tables->end());
			std::sort(out.begin(), out.end());
		}
		return out;
	});
}

CrossingSchema CrossingAttach::DescribedSchema(const string &schema) {
	lock_guard<mutex> guard(schemas_lock);
	auto &listing = ListedSchemas();
	auto it = listing.find(schema);
	if (it == listing.end()) {
		CrossingSchema none;
		none.name = schema;
		return none;
	}
	auto &entry = it->second;
	if (!entry.described) {
		entry.schema = source->DescribeSchema(it->first);
		entry.described = true;
	}
	return entry.schema;
}

bool CrossingAttach::ServesSchema(const string &schema) {
	return WithTables(schema, nullptr, [](optional_ptr<const case_insensitive_set_t> tables) {
		return tables && !tables->empty();
	});
}

bool CrossingAttach::ServesTable(const string &schema, const string &table, optional_ptr<Transaction> transaction) {
	return WithTables(schema, transaction, [&](optional_ptr<const case_insensitive_set_t> tables) {
		return tables && tables->find(table) != tables->end();
	});
}

void CrossingAttach::RetireCache(SchemaState &state) {
	lock_guard<mutex> state_guard(state.lock);
	for (auto &cached : state.cache) {
		state.retired.push_back(std::move(cached.second));
	}
	state.cache.clear();
}

void CrossingAttach::Refresh() {
	lock_guard<mutex> guard(schemas_lock);
	schemas_listed = false;
	served.clear();
	for (auto &entry : schemas) {
		RetireCache(*entry.second);
	}
}

void CrossingAttach::Refresh(const string &schema) {
	lock_guard<mutex> guard(schemas_lock);
	auto it = served.find(schema);
	if (it != served.end()) {
		it->second = Listing();
	}
	auto state = schemas.find(schema);
	if (state != schemas.end()) {
		RetireCache(*state->second);
	}
}

CrossingAttach::SchemaState &CrossingAttach::StateOf(const string &schema) {
	lock_guard<mutex> guard(schemas_lock);
	auto &slot = schemas[schema];
	if (!slot) {
		slot = make_uniq<SchemaState>();
	}
	return *slot;
}

CrossingAttach::SchemaState &CrossingAttach::StateOf(const string &schema, optional_ptr<Transaction> transaction) {
	auto overlay = OverlayOf(transaction, schema);
	return overlay ? *overlay->state : StateOf(schema);
}

optional_ptr<CrossingAttach::Slot> CrossingAttach::SlotOf(optional_ptr<Transaction> transaction) {
	if (!transaction) {
		return nullptr;
	}
	lock_guard<mutex> guard(transactions_lock);
	auto it = begun.find(transaction.get());
	return it == begun.end() ? nullptr : it->second.get();
}

optional_ptr<CrossingAttach::Overlay> CrossingAttach::OverlayOf(optional_ptr<Transaction> transaction,
                                                                const string &schema) {
	auto slot = SlotOf(transaction);
	if (!slot) {
		return nullptr;
	}
	lock_guard<mutex> guard(slot->lock);
	auto it = slot->overlays.find(schema);
	return it == slot->overlays.end() ? nullptr : &it->second;
}

CatalogEntry &CrossingAttach::GetOrDescribe(SchemaState &state, const string &schema, SchemaCatalogEntry &owner,
                                            const string &name) {
	auto it = state.cache.find(name);
	if (it != state.cache.end()) {
		if (&it->second->Cast<CrossingTableCatalogEntry>().schema == &owner) {
			return *it->second;
		}
		state.retired.push_back(std::move(it->second));
		state.cache.erase(it);
	}

	auto described = source->Describe(schema, name);
	auto &column_names = described.column_names;
	auto &column_types = described.column_types;
	if (column_names.empty()) {
		throw InvalidInputException("crossing: the source described '%s' with no columns", name);
	}

	// `name`, not described.name: a source that renames a table under us would otherwise produce
	// an entry nothing can resolve.
	auto create_info = make_uniq<CreateTableInfo>(owner, name);
	for (idx_t c = 0; c < column_names.size(); c++) {
		create_info->columns.AddColumn(ColumnDefinition(column_names[c], column_types[c]));
	}
	for (auto &constraint : described.constraints) {
		create_info->constraints.push_back(constraint->Copy());
	}
	auto entry = make_uniq<CrossingTableCatalogEntry>(WriteCatalog(), owner, *create_info, *source, schema,
	                                                  std::move(described));
	auto &raw = *entry;
	state.cache[name] = std::move(entry);
	return raw;
}

optional_ptr<CatalogEntry> CrossingAttach::LookupTable(const string &schema, SchemaCatalogEntry &owner,
                                                       const string &name, optional_ptr<Transaction> transaction) {
	if (!ServesTable(schema, name, transaction)) {
		return nullptr;
	}
	auto &state = StateOf(schema, transaction);
	lock_guard<mutex> guard(state.lock);
	return &GetOrDescribe(state, schema, owner, name);
}

void CrossingAttach::ScanTables(const string &schema, SchemaCatalogEntry &owner, case_insensitive_set_t &seen,
                                const std::function<void(CatalogEntry &)> &callback,
                                optional_ptr<Transaction> transaction) {
	auto names = Tables(schema, transaction);
	if (names.empty()) {
		return;
	}
	auto &state = StateOf(schema, transaction);
	lock_guard<mutex> guard(state.lock);
	for (auto &name : names) {
		if (seen.count(name)) {
			continue;
		}
		callback(GetOrDescribe(state, schema, owner, name));
		seen.insert(name);
	}
}

optional_ptr<const CrossingTable> CrossingAttach::Described(const string &schema, SchemaCatalogEntry &owner,
                                                            const string &name,
                                                            optional_ptr<Transaction> transaction) {
	auto entry = LookupTable(schema, owner, name, transaction);
	if (!entry) {
		return nullptr;
	}
	return &entry->Cast<CrossingTableCatalogEntry>().described;
}

void CrossingAttach::ThrowIfServed(const string &schema, const string &table, const char *what) {
	if (ServesTable(schema, table, nullptr)) {
		throw BinderException("crossing: '%s' is served by a source; %s is not supported", table, what);
	}
}

void CrossingAttach::ThrowIfSchemaServed(const string &schema) {
	if (ServesSchema(schema)) {
		throw BinderException("crossing: schema '%s' is served by a source; DETACH the catalog "
		                      "instead of dropping it",
		                      schema);
	}
}

void CrossingAttach::Authorize(SchemaCatalogEntry &owner, const CrossingDdl &ddl,
                               optional_ptr<Transaction> transaction) {
	if (ddl.verb == CrossingVerb::CREATE) {
		if (!DescribedSchema(ddl.schema).Allows(ddl.verb)) {
			throw PermissionException("crossing: schema '%s' does not have '%s' permission", ddl.schema,
			                          CrossingVerbName(ddl.verb));
		}
		if (ddl.create && ddl.create->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			auto replaced = Described(ddl.schema, owner, ddl.table, transaction);
			if (replaced && !replaced->Allows(CrossingVerb::DROP)) {
				throw PermissionException("crossing: '%s' does not have '%s' permission", ddl.table,
				                          CrossingVerbName(CrossingVerb::DROP));
			}
		}
		return;
	}
	auto described = Described(ddl.schema, owner, ddl.table, transaction);
	if (!described) {
		throw CatalogException::MissingEntry(CatalogType::TABLE_ENTRY, ddl.table, string());
	}
	if (!described->Allows(ddl.verb)) {
		throw PermissionException("crossing: '%s' does not have '%s' permission", ddl.table,
		                          CrossingVerbName(ddl.verb));
	}
}

void CrossingAttach::Pin(const string &schema, SchemaCatalogEntry &owner, const string &table) {
	LookupTable(schema, owner, table, nullptr);
}

void CrossingAttach::Ddl(ClientContext &context, Transaction &transaction, SchemaCatalogEntry &owner,
                         const CrossingDdl &ddl) {
	Authorize(owner, ddl, &transaction);
	Pin(ddl.schema, owner, ddl.table);
	Session(context, transaction).Ddl(context, ddl);
	auto &slot = SlotOf(transaction);
	lock_guard<mutex> guard(slot.lock);
	auto &altered = slot.altered_schemas;
	if (std::find(altered.begin(), altered.end(), ddl.schema) == altered.end()) {
		altered.push_back(ddl.schema);
	}
	auto &overlay = slot.overlays[ddl.schema];
	overlay.listed = false;
	overlay.tables.clear();
	if (overlay.state) {
		RetireCache(*overlay.state);
	} else {
		overlay.state = make_uniq<SchemaState>();
	}
}

CrossingAttach::Slot &CrossingAttach::SlotOf(Transaction &transaction) {
	lock_guard<mutex> guard(transactions_lock);
	auto &slot = begun[&transaction];
	if (!slot) {
		slot = make_uniq<Slot>();
	}
	return *slot;
}

CrossingSession &CrossingAttach::Session(ClientContext &context, Transaction &transaction) {
	auto &slot = SlotOf(transaction);
	lock_guard<mutex> guard(slot.lock);
	if (!slot.session) {
		slot.session = source->Begin(context);
		if (!slot.session) {
			throw InternalException("crossing: the source began no session");
		}
	}
	return *slot.session;
}

unique_ptr<CrossingSession> CrossingAttach::Release(Transaction &transaction) {
	unique_ptr<Slot> slot;
	{
		lock_guard<mutex> guard(transactions_lock);
		auto it = begun.find(&transaction);
		if (it == begun.end()) {
			return nullptr;
		}
		slot = std::move(it->second);
		begun.erase(it);
	}
	lock_guard<mutex> guard(slot->lock);
	if (!slot->altered_schemas.empty()) {
		lock_guard<mutex> settling_guard(transactions_lock);
		settling[slot->session.get()] = std::move(slot->altered_schemas);
	}
	Retire(*slot);
	return std::move(slot->session);
}

void CrossingAttach::Retire(Slot &slot) {
	for (auto &overlay : slot.overlays) {
		auto &state = StateOf(overlay.first);
		auto &mine = *overlay.second.state;
		lock_guard<mutex> state_guard(state.lock);
		for (auto &cached : mine.cache) {
			state.retired.push_back(std::move(cached.second));
		}
		for (auto &retired : mine.retired) {
			state.retired.push_back(std::move(retired));
		}
	}
	slot.overlays.clear();
}

void CrossingAttach::Settled(CrossingSession &released) {
	vector<string> altered;
	{
		lock_guard<mutex> guard(transactions_lock);
		auto it = settling.find(&released);
		if (it == settling.end()) {
			return;
		}
		altered = std::move(it->second);
		settling.erase(it);
	}
	for (auto &schema : altered) {
		Refresh(schema);
	}
}

ErrorData CrossingAttach::Commit(unique_ptr<CrossingSession> released) {
	if (!released) {
		return ErrorData();
	}
	ErrorData error;
	try {
		released->Commit();
	} catch (std::exception &ex) {
		ErrorData failure(ex);
		error = ErrorData(failure.Type(), "crossing: commit on source failed: " + failure.RawMessage());
	}
	Settled(*released);
	return error;
}

void CrossingAttach::Rollback(unique_ptr<CrossingSession> released) {
	if (!released) {
		return;
	}
	try {
		released->Rollback();
	} catch (...) {
	}
	Settled(*released);
}

ErrorData CrossingAttach::Commit(Transaction &transaction) {
	return Commit(Release(transaction));
}

void CrossingAttach::Rollback(Transaction &transaction) {
	Rollback(Release(transaction));
}

void CrossingAttach::Detach(ClientContext &context) {
	source->Detach(context);
}

Catalog &CrossingAttach::WriteCatalog() {
	return *write_catalog;
}

CrossingAttach &CrossingAttach::Of(Catalog &catalog) {
	auto owner = dynamic_cast<CrossingAttachOwner *>(&catalog);
	if (!owner) {
		throw InternalException("crossing: '%s' is not a crossing catalog", catalog.GetName());
	}
	return owner->Attach();
}

} // namespace duckdb
