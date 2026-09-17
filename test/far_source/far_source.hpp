#pragma once

#include "crossing.hpp"

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/set.hpp"

#include <thread>

namespace duckdb {

enum class Transport { NATIVE, SUBSTRAIT };

const char *TransportName(Transport transport);

struct FarCall {
	CrossingVerb kind;
	string plan_text;
	vector<CrossingTableUse> tables;
	vector<LogicalOperatorType> operators;
	vector<LogicalType> types;
	vector<string> key_columns;
	vector<string> set_columns;
	vector<vector<Value>> rows;
	string wire;
	string received_text;

	bool Has(LogicalOperatorType type) const;
	idx_t Count(LogicalOperatorType type) const;
	bool Mentions(const string &text) const;
};

struct FarStore {
	explicit FarStore(Transport transport);

	DuckDB db;
	Connection con;
	Transport transport;
	mutex lock;

	case_insensitive_set_t refused_functions;
	vector<LogicalTypeId> refused_types;
	case_insensitive_map_t<vector<string>> keys;
	case_insensitive_map_t<bool> key_unique;
	case_insensitive_map_t<vector<CrossingVerb>> verbs;
	string declined;

	vector<FarCall> reads;
	vector<FarCall> writes;
	vector<string> attached_paths;
	vector<string> detached_paths;
	vector<string> transaction_ends;
	case_insensitive_map_t<idx_t> listings;
	idx_t sessions_begun = 0;

	idx_t read_partitions = 1;
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

	void JoinArrivals();
	const FarCall &LastRead();
	const FarCall &LastWrite();
};

class FarSource : public CrossingSource {
public:
	FarSource(shared_ptr<FarStore> store, string path);

	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingPlan Plan(const CrossingPlanRequest &request) override;
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;
	unique_ptr<CrossingSession> Begin(ClientContext &context) override;
	void Detach(ClientContext &context) override;

	static void Register(ExtensionLoader &loader, shared_ptr<FarStore> store);

private:
	shared_ptr<FarStore> store;
	string path;
};

class FarSession : public CrossingSession {
public:
	explicit FarSession(shared_ptr<FarStore> store);
	~FarSession() override;

	CrossingScan Read(ClientContext &context, const CrossingQuery &query) override;
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query) override;
	void Commit() override;
	void Rollback() override;

private:
	vector<vector<Value>> Evaluate(const LogicalOperator &plan, FarCall &call);
	vector<vector<Value>> EvaluateNative(const LogicalOperator &plan, FarCall &call);
	vector<vector<Value>> EvaluateSubstrait(const LogicalOperator &plan, FarCall &call);
	idx_t Apply(const CrossingQuery &query, const vector<vector<Value>> &rows);
	FarCall Record(const CrossingQuery &query);

	shared_ptr<FarStore> store;
	Connection far;
};

} // namespace duckdb
