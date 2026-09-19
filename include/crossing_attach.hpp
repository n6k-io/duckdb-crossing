#pragma once

// What a catalog needs from crossing to serve one attached source. Own one per ATTACH, from
// whatever Catalog base you choose, and forward table lookups and transaction ends to it. The
// catalog crossing ships (Crossing<S>::Register) is one such owner; see IMPLEMENTING.md.

#include "crossing.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class AttachedDatabase;
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

class CrossingAttach {
public:
	CrossingAttach(AttachedDatabase &db, unique_ptr<CrossingSource> source);
	~CrossingAttach();
	CrossingAttach(const CrossingAttach &) = delete;
	CrossingAttach &operator=(const CrossingAttach &) = delete;

	CrossingSource &Source();
	template <class S>
	S &Source() {
		return Crossing<S>::Native(Source());
	}
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
	//! handed out stay alive for the life of the attach.
	void Refresh();
	//! The same for one schema. The schema list itself is kept.
	void Refresh(const string &schema);

	//! The entry for a served table, described on first use and cached for the life of the attach.
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
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan);

	//! The source's side of `transaction`, begun on first use.
	CrossingSession &Session(ClientContext &context, Transaction &transaction);
	//! Hands the source's side back; null when the transaction never touched the source. Call this
	//! before a base transaction manager frees `transaction`, and hand the result to Commit or
	//! Rollback: a schema the transaction changed is forgotten once the source has resolved it.
	unique_ptr<CrossingSession> Release(Transaction &transaction);
	ErrorData Commit(unique_ptr<CrossingSession> released);
	void Rollback(unique_ptr<CrossingSession> released);
	ErrorData Commit(Transaction &transaction);
	void Rollback(Transaction &transaction);

	void Detach(ClientContext &context);

	//! The attach behind a catalog crossing built, or behind the catalog served entries name as
	//! their parent.
	static CrossingAttach &Of(Catalog &catalog);

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
		unique_ptr<CrossingSession> session;
		vector<string> altered_schemas;
		case_insensitive_map_t<Overlay> overlays;
	};

	Catalog &WriteCatalog();
	Slot &SlotOf(Transaction &transaction);
	optional_ptr<Slot> SlotOf(optional_ptr<Transaction> transaction);
	optional_ptr<Overlay> OverlayOf(optional_ptr<Transaction> transaction, const string &schema);
	void Authorize(SchemaCatalogEntry &owner, const CrossingDdl &ddl, optional_ptr<Transaction> transaction);
	void Settled(CrossingSession &released);

	case_insensitive_map_t<Listing> &ListedSchemas();
	optional_ptr<const case_insensitive_set_t> TablesOf(const string &schema);
	template <class F>
	auto WithTables(const string &schema, optional_ptr<Transaction> transaction, F &&f);
	void Pin(const string &schema, SchemaCatalogEntry &owner, const string &table);
	void RetireCache(SchemaState &state);
	void Retire(Slot &slot);
	SchemaState &StateOf(const string &schema);
	SchemaState &StateOf(const string &schema, optional_ptr<Transaction> transaction);
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
	mutex transactions_lock;
	unordered_map<Transaction *, unique_ptr<Slot>> begun;
	unordered_map<CrossingSession *, vector<string>> settling;
};

} // namespace duckdb
