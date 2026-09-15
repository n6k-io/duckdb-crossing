#pragma once

#include "crossing.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/set.hpp"

#include <thread>

namespace duckdb {

struct ToyTable {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> key;
	vector<vector<Value>> rows;
};

struct ToyStore {
	mutex lock;
	case_insensitive_map_t<ToyTable> tables;
	vector<string> attached_paths;
	vector<string> detached_paths;
	vector<string> transaction_ends;
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
	case_insensitive_map_t<idx_t> listings;
	bool ship_plans = false;
	idx_t plans_shipped = 0;

	void JoinArrivals();
};

class ToySource : public CrossingSource {
public:
	ToySource(shared_ptr<ToyStore> store, string path);

	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingPlan Plan(const CrossingPlanRequest &request) override;
	unique_ptr<CrossingSession> Begin(ClientContext &context) override;
	void Detach(ClientContext &context) override;

	static void Register(ExtensionLoader &loader, shared_ptr<ToyStore> store);

private:
	shared_ptr<ToyStore> store;
	string path;
};

class ToySession : public CrossingSession {
public:
	explicit ToySession(shared_ptr<ToyStore> store);

	CrossingScan Read(ClientContext &context, const CrossingQuery &query) override;
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query) override;
	void Commit() override;
	void Rollback() override;

private:
	vector<vector<Value>> Render(const LogicalOperator &op);
	vector<vector<Value>> RenderShipped(const CrossingQuery &query);
	idx_t Apply(const CrossingQuery &query);

	shared_ptr<ToyStore> store;
};

ToyTable &ToyTableNamed(ToyStore &store, const string &name);

} // namespace duckdb
