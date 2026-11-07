#include "crossing_attach.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/planner/subquery/flatten_dependent_join.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"

#include "internal/carrier.hpp"
#include "internal/control_frames.hpp"
#include "internal/crossing_remote.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/crossing_write.hpp"
#include "internal/fill.hpp"
#include "internal/pass.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <algorithm>

namespace duckdb {

CrossingOpenTransaction::CrossingOpenTransaction() = default;
CrossingOpenTransaction::~CrossingOpenTransaction() = default;

struct CrossingAttach::SchemaState {
	mutex lock;
	//! Set under schemas_lock, consumed under `lock`: ApplyRefreshes must not wait on `lock`, which a
	//! describe holds across a source call.
	atomic<bool> stale {false};
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
		Rollback(TakeOpen(*entry.second));
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

void CrossingAttach::ApplyRefreshes() {
	bool all;
	case_insensitive_set_t names;
	{
		lock_guard<mutex> guard(refresh_lock);
		all = refresh_all;
		refresh_all = false;
		names.swap(refresh_schemas);
	}
	if (all) {
		schemas_listed = false;
		served.clear();
		for (auto &entry : schemas) {
			entry.second->stale = true;
		}
		return;
	}
	for (auto &name : names) {
		auto it = served.find(name);
		if (it != served.end()) {
			it->second = Listing();
		}
		auto state = schemas.find(name);
		if (state != schemas.end()) {
			state->second->stale = true;
		}
	}
}

case_insensitive_map_t<CrossingAttach::Listing> &CrossingAttach::ListedSchemas() {
	ApplyRefreshes();
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
	auto slot = SlotOf(transaction);
	if (slot) {
		lock_guard<mutex> guard(slot->lock);
		auto it = slot->overlays.find(schema);
		if (it != slot->overlays.end()) {
			auto &overlay = it->second;
			if (!overlay.listed) {
				for (auto &table : slot->remote->Tables(*transaction->context.lock(), schema)) {
					overlay.tables.insert(table);
				}
				overlay.listed = true;
			}
			return f(optional_ptr<const case_insensitive_set_t>(&overlay.tables));
		}
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
	return WithTables(schema, nullptr,
	                  [](optional_ptr<const case_insensitive_set_t> tables) { return tables && !tables->empty(); });
}

bool CrossingAttach::ServesTable(const string &schema, const string &table, optional_ptr<Transaction> transaction) {
	return WithTables(schema, transaction, [&](optional_ptr<const case_insensitive_set_t> tables) {
		return tables && tables->find(table) != tables->end();
	});
}

void CrossingAttach::RetireCache(SchemaState &state) {
	lock_guard<mutex> state_guard(state.lock);
	RetireCacheLocked(state);
}

void CrossingAttach::RetireCacheLocked(SchemaState &state) {
	for (auto &cached : state.cache) {
		state.retired.push_back(std::move(cached.second));
	}
	state.cache.clear();
}

void CrossingAttach::RetireIfStale(SchemaState &state) {
	if (state.stale.exchange(false)) {
		RetireCacheLocked(state);
	}
}

void CrossingAttach::Refresh() {
	lock_guard<mutex> guard(refresh_lock);
	refresh_all = true;
	refresh_schemas.clear();
}

void CrossingAttach::Refresh(const string &schema) {
	lock_guard<mutex> guard(refresh_lock);
	if (!refresh_all) {
		refresh_schemas.insert(schema);
	}
}

CrossingAttach::SchemaState &CrossingAttach::StateOf(const string &schema) {
	lock_guard<mutex> guard(schemas_lock);
	ApplyRefreshes();
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

CrossingTable CrossingAttach::DescribeAs(const string &schema, const string &name,
                                         optional_ptr<Transaction> transaction) {
	auto slot = SlotOf(transaction);
	if (slot) {
		lock_guard<mutex> guard(slot->lock);
		if (slot->overlays.find(schema) != slot->overlays.end()) {
			auto table = slot->remote->Describe(*transaction->context.lock(), schema, name);
			source->DescribeChanged(schema, table);
			return table;
		}
	}
	return source->Describe(schema, name);
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
                                            const string &name, optional_ptr<Transaction> transaction) {
	auto it = state.cache.find(name);
	if (it != state.cache.end()) {
		if (&it->second->Cast<CrossingTableCatalogEntry>().schema == &owner) {
			return *it->second;
		}
		state.retired.push_back(std::move(it->second));
		state.cache.erase(it);
	}

	auto described = DescribeAs(schema, name, transaction);
	auto &column_names = described.column_names;
	auto &column_types = described.column_types;
	if (column_names.empty()) {
		throw InvalidInputException("crossing: the source described '%s' with no columns", name);
	}

	// `name`, not described.name: a source that renames a table under us would otherwise produce
	// an entry nothing can resolve.
	auto create_info = make_uniq<CreateTableInfo>(owner, Identifier(name));
	for (idx_t c = 0; c < column_names.size(); c++) {
		ColumnDefinition column(Identifier(column_names[c]), column_types[c]);
		column.SetDefaultValue(SourceDefaultExpression());
		create_info->columns.AddColumn(std::move(column));
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
	RetireIfStale(state);
	return &GetOrDescribe(state, schema, owner, name, transaction);
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
	RetireIfStale(state);
	for (auto &name : names) {
		if (seen.count(name)) {
			continue;
		}
		callback(GetOrDescribe(state, schema, owner, name, transaction));
		seen.insert(name);
	}
}

optional_ptr<const CrossingTable> CrossingAttach::Described(const string &schema, SchemaCatalogEntry &owner,
                                                            const string &name, optional_ptr<Transaction> transaction) {
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
		throw CatalogException::MissingEntry(CatalogType::TABLE_ENTRY, Identifier(ddl.table), string());
	}
	if (!described->Allows(ddl.verb)) {
		throw PermissionException("crossing: '%s' does not have '%s' permission", ddl.table,
		                          CrossingVerbName(ddl.verb));
	}
}

namespace {

string DdlFrame(const CrossingDdl &ddl) {
	if (ddl.plan) {
		string frame = string(CROSSING_CONTROL_VIEW) + ddl.schema + "\n" + ddl.table + "\n" +
		               to_string(static_cast<uint8_t>(ddl.create->on_conflict)) + "\n" +
		               StringUtil::Join(ddl.view_columns, ",") + "\n";
		return frame + RenderSubstraitJson(*ddl.plan);
	}
	switch (ddl.verb) {
	case CrossingVerb::CREATE:
		return ddl.create->ToString();
	case CrossingVerb::ALTER:
		return ddl.alter->ToString();
	default:
		return ddl.drop->ToString();
	}
}

} // namespace

void CrossingAttach::Pin(const string &schema, SchemaCatalogEntry &owner, const string &table) {
	LookupTable(schema, owner, table, nullptr);
}

void CrossingAttach::Ddl(ClientContext &context, Transaction &transaction, SchemaCatalogEntry &owner,
                         const CrossingDdl &ddl) {
	Authorize(owner, ddl, &transaction);
	Pin(ddl.schema, owner, ddl.table);
	RemoteOf(context, transaction).RunDdl(context, DdlFrame(ddl));
	NoteAltered(transaction, ddl.schema);
}

void CrossingAttach::NoteAltered(Transaction &transaction, const string &schema) {
	auto &slot = SlotOf(transaction);
	lock_guard<mutex> guard(slot.lock);
	auto &altered = slot.altered_schemas;
	if (std::find(altered.begin(), altered.end(), schema) == altered.end()) {
		altered.push_back(schema);
	}
	auto &overlay = slot.overlays[schema];
	overlay.listed = false;
	overlay.tables.clear();
	if (overlay.state) {
		RetireCache(*overlay.state);
	} else {
		overlay.state = make_uniq<SchemaState>();
	}
}

optional_ptr<CatalogEntry> CrossingAttach::CreateView(ClientContext &context, Transaction &transaction,
                                                      SchemaCatalogEntry &owner, CreateViewInfo &info) {
	auto &schema_name = owner.name.GetIdentifierName();
	auto &view_name = info.GetViewName().GetIdentifierName();
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT && ServesTable(schema_name, view_name, &transaction)) {
		return LookupTable(schema_name, owner, view_name, &transaction);
	}
	auto binder = Binder::CreateBinder(context);
	binder->SetSearchPath(owner.ParentCatalog(), owner.name);
	auto query = info.query->Copy();
	auto bound = binder->Bind(*query);
	// The Planner flattens dependent joins after binding, and this body never goes through one.
	RecursiveDependentJoinPlanner::Plan(*binder, bound.plan);
	bound.plan = FlattenDependentJoins::DecorrelateIndependent(*binder, std::move(bound.plan));
	auto &identity = Identity();
	FreshTableIndex fresh = [&binder]() {
		return binder->GenerateTableIndex();
	};
	// A view outlives this statement, so now() in its rows must not be frozen at creation; those rows do not
	// fold and the view is refused below.
	MaterialiseConstantRows(context, bound.plan, true);
	ResolveRowIds(*bound.plan, identity);
	FoldCrossableWorkIntoFragments(bound.plan, fresh, identity,
	                               Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(context), false);
	auto fragment = CrossingReadFragmentOf(*bound.plan, identity);
	if (!fragment || fragment->frozen || CrossingSourceOf(*bound.plan, identity)->Group() != source->Group()) {
		throw BinderException("crossing: the body of view '%s' does not run wholly on the source", view_name);
	}
	if (fragment->plan->types.size() != info.names.size()) {
		throw InternalException("crossing: view '%s' plans %llu columns but names %llu", view_name,
		                        fragment->plan->types.size(), info.names.size());
	}

	CrossingDdl ddl;
	ddl.verb = CrossingVerb::CREATE;
	ddl.schema = schema_name;
	ddl.table = view_name;
	ddl.create = &info;
	ddl.plan = fragment->plan.get();
	ddl.view_columns = CrossingViewColumnNames(info);
	Ddl(context, transaction, owner, ddl);
	return LookupTable(schema_name, owner, view_name, &transaction);
}

CrossingAttach::Slot &CrossingAttach::SlotOf(Transaction &transaction) {
	lock_guard<mutex> guard(transactions_lock);
	auto &slot = begun[&transaction];
	if (!slot) {
		slot = make_uniq<Slot>();
	}
	return *slot;
}

CrossingRemote &CrossingAttach::RemoteOf(ClientContext &context, Transaction &transaction) {
	auto &slot = SlotOf(transaction);
	lock_guard<mutex> guard(slot.lock);
	if (slot.remote && slot.remote->EndedByFar()) {
		auto finished = make_uniq<CrossingOpenTransaction>();
		finished->link = std::move(slot.link);
		finished->remote = std::move(slot.remote);
		slot.finished.push_back(std::move(finished));
		slot.statement = nullptr;
	}
	if (!slot.remote) {
		slot.link = source->Connect(context);
		if (!slot.link) {
			throw InternalException("crossing: the source connected no link");
		}
		slot.remote = make_uniq<CrossingRemote>(*slot.link);
	}
	return *slot.remote;
}

shared_ptr<CrossingRemoteStatement> CrossingAttach::RemoteStatement(ClientContext &context, Transaction &transaction) {
	auto &remote = RemoteOf(context, transaction);
	auto &slot = SlotOf(transaction);
	lock_guard<mutex> guard(slot.lock);
	auto query = context.transaction.GetActiveQuery();
	if (!slot.statement || slot.statement_query != query) {
		remote.DrainStatement(&context);
		slot.statement = make_shared_ptr<CrossingRemoteStatement>(remote);
		slot.statement->SetAutoCommit(context.transaction.IsAutoCommit());
		slot.statement_query = query;
	}
	return slot.statement;
}

unique_ptr<CrossingOpenTransaction> CrossingAttach::TakeOpen(Slot &slot) {
	if (!slot.remote) {
		return nullptr;
	}
	auto open = make_uniq<CrossingOpenTransaction>();
	open->link = std::move(slot.link);
	open->remote = std::move(slot.remote);
	open->finished = std::move(slot.finished);
	slot.statement = nullptr;
	return open;
}

unique_ptr<CrossingOpenTransaction> CrossingAttach::Release(Transaction &transaction) {
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
	auto open = TakeOpen(*slot);
	if (!slot->altered_schemas.empty() && open) {
		open->altered_schemas = std::move(slot->altered_schemas);
	}
	Retire(*slot);
	return open;
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

void CrossingAttach::Settled(CrossingOpenTransaction &released) {
	for (auto &schema : released.altered_schemas) {
		Refresh(schema);
	}
}

ErrorData CrossingAttach::Commit(unique_ptr<CrossingOpenTransaction> released) {
	if (!released) {
		return ErrorData();
	}
	ErrorData error;
	try {
		released->remote->Commit();
	} catch (std::exception &ex) {
		ErrorData failure(ex);
		error = ErrorData(failure.Type(), "crossing: commit on source failed: " + failure.RawMessage());
	}
	Settled(*released);
	return error;
}

void CrossingAttach::Rollback(unique_ptr<CrossingOpenTransaction> released) {
	if (!released) {
		return;
	}
	try {
		released->remote->Rollback();
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
	auto attach = TryOf(catalog);
	if (!attach) {
		throw InternalException("crossing: '%s' is not a crossing catalog", catalog.GetName());
	}
	return *attach;
}

optional_ptr<CrossingAttach> CrossingAttach::TryOf(Catalog &catalog) {
	auto owner = dynamic_cast<CrossingAttachOwner *>(&catalog);
	if (!owner) {
		return nullptr;
	}
	return owner->Attach();
}

} // namespace duckdb
