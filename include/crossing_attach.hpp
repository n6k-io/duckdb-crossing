#pragma once

// What a catalog needs from crossing to serve one attached source. Own one per ATTACH, from
// whatever Catalog base you choose, and forward table lookups and transaction ends to it. The
// catalog crossing ships (RegisterCrossing) is one such owner; see IMPLEMENTING.md.

#include "crossing.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class AttachedDatabase;
class CrossingRemote;
class CrossingRemoteStatement;
class CrossingLink;
struct CreateInfo;
struct CreateViewInfo;
struct AlterInfo;
struct DropInfo;
class Catalog;
class CatalogEntry;
class SchemaCatalogEntry;
class Transaction;
class CrossingWriteCatalog;
class CrossingAttach;
class PhysicalOperator;
class PhysicalPlanGenerator;
class LogicalCreateTable;

class CrossingAttachOwner {
public:
	virtual ~CrossingAttachOwner() = default;
	virtual CrossingAttach &Attach() = 0;
};

//! A DDL statement, as the attach authorizes it and far runs it.
struct CrossingDdl {
	CrossingVerb verb;
	string schema;
	string table;
	optional_ptr<const CreateInfo> create;
	optional_ptr<const AlterInfo> alter;
	optional_ptr<const DropInfo> drop;
	//! A CREATE VIEW's body, floors only, and the view's column names.
	optional_ptr<const LogicalOperator> plan;
	vector<string> view_columns;
};

//! A view's column names: its alias where given, else what its body names.
vector<string> CrossingViewColumnNames(const CreateViewInfo &view);

//! A transaction's far side, from Release until Commit or Rollback.
struct CrossingOpenTransaction {
	CrossingOpenTransaction();
	~CrossingOpenTransaction();

	unique_ptr<CrossingLink> link;
	unique_ptr<CrossingRemote> remote;
	vector<string> altered_schemas;
	vector<unique_ptr<CrossingOpenTransaction>> finished;
};

class CrossingAttach {
public:
	CrossingAttach(AttachedDatabase &db, unique_ptr<CrossingSource> source);
	~CrossingAttach();
	CrossingAttach(const CrossingAttach &) = delete;
	CrossingAttach &operator=(const CrossingAttach &) = delete;

	CrossingSource &Source();
	const CrossingIdentity &Identity() const;
	AttachedDatabase &Database();

	//! Sorted, in the source's spelling. The source is asked on first use.
	vector<string> Schemas();
	//! Sorted.
	vector<string> Tables(const string &schema, optional_ptr<Transaction> transaction);
	CrossingSchema DescribedSchema(const string &schema);
	bool ServesSchema(const string &schema);
	bool ServesTable(const string &schema, const string &table, optional_ptr<Transaction> transaction);
	//! Forgets what the source listed and described; the next lookup asks again. Entries already
	//! handed out stay valid (SchemaState::retired). Never blocks on a lookup in progress, so it is
	//! safe from the thread that delivers that lookup's reply; that lookup returns what it fetched.
	void Refresh();
	//! The same for one schema. The schema list itself is kept.
	void Refresh(const string &schema);

	//! The entry for a served table, described on first use and cached until Refresh. A transaction
	//! that ran Ddl on the schema keeps its own until it resolves.
	//! `owner` is the schema entry the table hangs off; an entry cached under a different owner is
	//! retired and rebuilt. Null when the source does not serve the name.
	optional_ptr<CatalogEntry> LookupTable(const string &schema, SchemaCatalogEntry &owner, const string &name,
	                                       optional_ptr<Transaction> transaction);
	void ScanTables(const string &schema, SchemaCatalogEntry &owner, case_insensitive_set_t &seen,
	                const std::function<void(CatalogEntry &)> &callback, optional_ptr<Transaction> transaction);
	optional_ptr<const CrossingTable> Described(const string &schema, SchemaCatalogEntry &owner, const string &name,
	                                            optional_ptr<Transaction> transaction);

	void ThrowIfServed(const string &schema, const string &table, const char *what);
	void ThrowIfSchemaServed(const string &schema);

	void Ddl(ClientContext &context, Transaction &transaction, SchemaCatalogEntry &owner, const CrossingDdl &ddl);
	//! Plans the view's body, which must run wholly on the source, and hands it to Ddl as a CREATE.
	optional_ptr<CatalogEntry> CreateView(ClientContext &context, Transaction &transaction, SchemaCatalogEntry &owner,
	                                      CreateViewInfo &info);
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan);

	//! The statement `context` is planning.
	shared_ptr<CrossingRemoteStatement> RemoteStatement(ClientContext &context, Transaction &transaction);
	//! Hands the transaction's far side back; null when it never touched far. Call this before a base transaction
	//! manager frees `transaction`, and hand the result to Commit or Rollback.
	unique_ptr<CrossingOpenTransaction> Release(Transaction &transaction);
	ErrorData Commit(unique_ptr<CrossingOpenTransaction> released);
	void Rollback(unique_ptr<CrossingOpenTransaction> released);
	ErrorData Commit(Transaction &transaction);
	void Rollback(Transaction &transaction);

	void Detach(ClientContext &context);

	//! The attach behind a CrossingAttachOwner catalog. Of throws for any other catalog; TryOf returns
	//! null.
	static CrossingAttach &Of(Catalog &catalog);
	static optional_ptr<CrossingAttach> TryOf(Catalog &catalog);

private:
	struct SchemaState;
	struct Listing {
		bool listed = false;
		case_insensitive_set_t tables;
		bool described = false;
		CrossingSchema schema;
	};
	struct Overlay {
		bool listed = false;
		case_insensitive_set_t tables;
		unique_ptr<SchemaState> state;
	};
	struct Slot {
		mutex lock;
		unique_ptr<CrossingLink> link;
		unique_ptr<CrossingRemote> remote;
		//! Links far ended with a read; statements may still point at them.
		vector<unique_ptr<CrossingOpenTransaction>> finished;
		shared_ptr<CrossingRemoteStatement> statement;
		idx_t statement_query = 0;
		vector<string> altered_schemas;
		case_insensitive_map_t<Overlay> overlays;
	};

	Catalog &WriteCatalog();
	Slot &SlotOf(Transaction &transaction);
	optional_ptr<Slot> SlotOf(optional_ptr<Transaction> transaction);
	optional_ptr<Overlay> OverlayOf(optional_ptr<Transaction> transaction, const string &schema);
	void Authorize(SchemaCatalogEntry &owner, const CrossingDdl &ddl, optional_ptr<Transaction> transaction);
	void Settled(CrossingOpenTransaction &released);
	void NoteAltered(Transaction &transaction, const string &schema);
	CrossingRemote &RemoteOf(ClientContext &context, Transaction &transaction);
	unique_ptr<CrossingOpenTransaction> TakeOpen(Slot &slot);

	//! Applies queued Refresh calls. Caller holds schemas_lock.
	void ApplyRefreshes();
	//! Caller holds schemas_lock.
	case_insensitive_map_t<Listing> &ListedSchemas();
	//! Caller holds schemas_lock.
	optional_ptr<const case_insensitive_set_t> TablesOf(const string &schema);
	template <class F>
	auto WithTables(const string &schema, optional_ptr<Transaction> transaction, F &&f);
	void Pin(const string &schema, SchemaCatalogEntry &owner, const string &table);
	void RetireCache(SchemaState &state);
	//! Caller holds state.lock.
	void RetireCacheLocked(SchemaState &state);
	//! Caller holds state.lock.
	void RetireIfStale(SchemaState &state);
	void Retire(Slot &slot);
	SchemaState &StateOf(const string &schema);
	SchemaState &StateOf(const string &schema, optional_ptr<Transaction> transaction);
	//! Caller holds state.lock.
	CatalogEntry &GetOrDescribe(SchemaState &state, const string &schema, SchemaCatalogEntry &owner, const string &name,
	                            optional_ptr<Transaction> transaction);
	CrossingTable DescribeAs(const string &schema, const string &name, optional_ptr<Transaction> transaction);

	AttachedDatabase &db;
	unique_ptr<CrossingSource> source;
	unique_ptr<CrossingWriteCatalog> write_catalog;
	mutex schemas_lock;
	bool schemas_listed = false;
	case_insensitive_map_t<Listing> served;
	case_insensitive_map_t<unique_ptr<SchemaState>> schemas;
	//! Never held across a source call.
	mutex refresh_lock;
	bool refresh_all = false;
	case_insensitive_set_t refresh_schemas;
	mutex transactions_lock;
	unordered_map<Transaction *, unique_ptr<Slot>> begun;
};

} // namespace duckdb
