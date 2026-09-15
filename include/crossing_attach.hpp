#pragma once

// What a catalog needs from crossing to serve one attached source. Own one per ATTACH, from
// whatever Catalog base you choose, and forward table lookups and transaction ends to it. The
// catalog crossing ships (CrossingSource::Register) is one such owner; see IMPLEMENTING.md.

#include "crossing.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class AttachedDatabase;
class Catalog;
class CatalogEntry;
class DatabaseInstance;
class SchemaCatalogEntry;
class Transaction;
class CrossingWriteCatalog;

class CrossingAttach {
public:
	CrossingAttach(AttachedDatabase &db, unique_ptr<CrossingSource> source);
	~CrossingAttach();
	CrossingAttach(const CrossingAttach &) = delete;
	CrossingAttach &operator=(const CrossingAttach &) = delete;

	CrossingSource &Source();
	AttachedDatabase &Database();

	//! Sorted, in the source's spelling. The source is asked on first use.
	vector<string> Schemas();
	//! Sorted.
	vector<string> Tables(const string &schema);
	bool ServesSchema(const string &schema);
	bool ServesTable(const string &schema, const string &table);
	//! Forgets what the source listed and described; the next lookup asks again. Entries already
	//! handed out stay alive for the life of the attach.
	void Refresh();
	//! The same for one schema. The schema list itself is kept.
	void Refresh(const string &schema);

	//! The entry for a served table, described on first use and cached for the life of the attach.
	//! `owner` is the schema entry the table hangs off; an entry cached under a different owner is
	//! retired and rebuilt. Null when the source does not serve the name.
	optional_ptr<CatalogEntry> LookupTable(const string &schema, SchemaCatalogEntry &owner, const string &name);
	void ScanTables(const string &schema, SchemaCatalogEntry &owner, case_insensitive_set_t &seen,
	                const std::function<void(CatalogEntry &)> &callback);
	optional_ptr<const CrossingTable> Described(const string &schema, SchemaCatalogEntry &owner, const string &name);

	void ThrowIfServed(const string &schema, const string &table, const char *what);
	void ThrowIfSchemaServed(const string &schema);

	//! The source's side of `transaction`, begun on first use.
	CrossingSession &Session(ClientContext &context, Transaction &transaction);
	//! Hands the source's side back; null when the transaction never touched the source. Call this
	//! before a base transaction manager frees `transaction`.
	unique_ptr<CrossingSession> Release(Transaction &transaction);
	static ErrorData Commit(unique_ptr<CrossingSession> released);
	static void Rollback(unique_ptr<CrossingSession> released);
	ErrorData Commit(Transaction &transaction);
	void Rollback(Transaction &transaction);

	void Detach(ClientContext &context);

	//! The catalog served entries name as their parent, so DuckDB's planner routes DML to crossing.
	Catalog &WriteCatalog();
	static CrossingAttach &Of(Catalog &write_catalog);

private:
	struct SchemaState;
	struct Listing {
		bool listed = false;
		case_insensitive_set_t tables;
	};

	case_insensitive_map_t<Listing> &ListedSchemas();
	optional_ptr<const case_insensitive_set_t> TablesOf(const string &schema);
	void RetireCache(SchemaState &state);
	SchemaState &StateOf(const string &schema);
	CatalogEntry &GetOrDescribe(SchemaState &state, const string &schema, SchemaCatalogEntry &owner,
	                            const string &name);

	AttachedDatabase &db;
	unique_ptr<CrossingSource> source;
	unique_ptr<CrossingWriteCatalog> phantom;
	mutex schemas_lock;
	bool schemas_listed = false;
	case_insensitive_map_t<Listing> served;
	case_insensitive_map_t<unique_ptr<SchemaState>> schemas;
	mutex transactions_lock;
	unordered_map<Transaction *, unique_ptr<CrossingSession>> begun;
};

//! Once per database instance. CrossingSource::Register calls it; a catalog registered another way
//! must.
void RegisterCrossingPass(DatabaseInstance &db);

} // namespace duckdb
