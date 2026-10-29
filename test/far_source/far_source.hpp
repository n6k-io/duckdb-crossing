#pragma once

#include "crossing.hpp"
#include "crossing_substrait.hpp"

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include <functional>
#include <thread>

namespace duckdb {

enum class Transport { NATIVE, SUBSTRAIT };

const char *TransportName(Transport transport);

struct FarCall {
	CrossingVerb kind;
	string plan_text;
	vector<CrossingTableUse> tables;
	CrossingTableUse written;
	vector<LogicalOperatorType> operators;
	vector<LogicalType> types;
	bool ordered = false;
	vector<string> key_columns;
	vector<string> set_columns;
	vector<vector<Value>> rows;
	string wire;
	string received_text;

	bool Has(LogicalOperatorType type) const;
	idx_t Count(LogicalOperatorType type) const;
	bool Mentions(const string &text) const;
};

struct FarDdl {
	CrossingVerb verb;
	string schema;
	string table;
};

struct FarStore {
	explicit FarStore(Transport transport);

	DuckDB db;
	Connection con;
	Transport transport;
	mutex lock;

	case_insensitive_set_t refused_functions;
	vector<LogicalTypeId> refused_types;
	vector<LogicalOperatorType> refused_operators;
	case_insensitive_map_t<vector<string>> keys;
	case_insensitive_map_t<bool> key_unique;
	case_insensitive_map_t<vector<CrossingVerb>> verbs;
	case_insensitive_map_t<vector<CrossingVerb>> schema_verbs;
	string declined;

	vector<FarCall> reads;
	vector<FarCall> writes;
	vector<FarDdl> ddls;
	//! Interned `GROUP` attach options; an element's address is the group of every attach naming it.
	set<string> groups;
	vector<string> attached_paths;
	vector<string> detached_paths;
	vector<string> transaction_ends;
	case_insensitive_map_t<idx_t> listings;
	idx_t sessions_begun = 0;

	idx_t read_partitions = 1;
	bool partition_ordered_reads = false;
	//! Non-zero: readers take blocks of this many rows from one counter per scan, block b with batch
	//! index b.
	idx_t batch_block_rows = 0;
	//! Non-zero: block b goes to partition b % partitions with batch index b, which breaks the
	//! contract once a thread drains a second partition.
	idx_t round_robin_batch_rows = 0;
	//! A batch-indexed scan whose pulls carry no index.
	bool drop_batch_index = false;
	//! An unindexed scan whose pulls carry an index.
	bool stray_batch_index = false;
	set<idx_t> partitions_read;
	bool wait_before_chunks = false;
	bool wake_before_returning = false;
	bool keep_wakers = false;
	set<idx_t> arrived;
	idx_t waits_served = 0;
	bool wait_before_write = false;
	bool write_arrived = false;
	idx_t write_waits_served = 0;
	vector<std::thread> arrivals;
	vector<CrossingWaker> kept_wakers;
	idx_t readers_open = 0;
	idx_t readers_closed = 0;
	//! Runs at the start of Schemas ("schemas") and Describe ("describe"), outside `lock`.
	std::function<void(const string &call)> on_source_call;
	//! Handed to the Substrait decode of every received plan.
	SubstraitCatalogCheck catalog_check;

	void JoinArrivals();
	const FarCall &LastRead();
	const FarCall &LastWrite();
};

class FarSession {
public:
	FarSession(shared_ptr<FarStore> store, string catalog);
	~FarSession();

	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	void Commit();
	void Rollback();

private:
	vector<vector<Value>> Evaluate(const LogicalOperator &plan, FarCall &call);
	vector<vector<Value>> EvaluateNative(const LogicalOperator &plan, FarCall &call);
	vector<vector<Value>> EvaluateSubstrait(const LogicalOperator &plan, FarCall &call);
	idx_t Apply(const CrossingQuery &query);
	string StageSeamRows(const LogicalOperator &plan);
	unique_ptr<CreateViewInfo> ReceiveView(const CreateViewInfo &view, const LogicalOperator &plan);
	FarCall Record(const CrossingQuery &query);

	shared_ptr<FarStore> store;
	string catalog;
	mutex far_lock;
	Connection far;
};

class FarSource {
public:
	using Session = FarSession;

	//! `catalog` is the store's catalog this attach serves; naming one puts it in every floor.
	FarSource(shared_ptr<FarStore> store, string path, const string &group = "", const string &catalog = "");

	vector<string> Schemas();
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	CrossingSchema DescribeSchema(const string &schema);
	static vector<string> TablesOn(Connection &con, const string &catalog, const string &schema);
	static bool IsView(Connection &con, const string &catalog, const string &schema, const string &name);
	static CrossingTable DescribeOn(FarStore &store, Connection &con, const string &catalog, const string &schema,
	                                const string &name);
	CrossingPlan Plan(const CrossingPlanRequest &request);
	CrossingVerdict AcceptsCall(const Expression &expr);
	CrossingVerdict AcceptsType(const LogicalType &type);
	CrossingVerdict AcceptsOperator(const LogicalOperator &op);
	unique_ptr<FarSession> Begin(ClientContext &context);
	void Detach(ClientContext &context);
	const void *Group();

	static void Register(ExtensionLoader &loader, shared_ptr<FarStore> store);

private:
	shared_ptr<FarStore> store;
	string path;
	const string *group = nullptr;
	string catalog;
	string floor_catalog;
};

} // namespace duckdb

namespace other {

class OtherSource : public duckdb::FarSource {
public:
	using FarSource::FarSource;

	static void Register(duckdb::ExtensionLoader &loader, duckdb::shared_ptr<duckdb::FarStore> store);
};

} // namespace other
