#pragma once

#include "crossing_far.hpp"
#include "framework/test_link.hpp"

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

enum class Transport { NATIVE, SUBSTRAIT, LINK };

const char *TransportName(Transport transport);

class TestResult {
public:
	explicit TestResult(unique_ptr<QueryResult> result_p) : result(std::move(result_p)) {
		if (!result->HasError()) {
			types = result->GetTypes();
			for (auto &name : result->GetNames()) {
				names.push_back(name.GetIdentifierName());
			}
		}
	}

	bool HasError() const {
		return result->HasError();
	}
	const string &GetError() const {
		return result->GetError();
	}
	void ThrowError() const {
		result->ThrowError();
	}
	idx_t ColumnCount() const {
		return types.size();
	}
	idx_t RowCount() {
		return result->RowCount();
	}
	Value GetValue(idx_t column, idx_t row) {
		if (!rows) {
			rows = make_uniq<ColumnDataRowCollection>(result->Collection());
		}
		return rows->GetValue(column, row);
	}

	vector<LogicalType> types;
	vector<string> names;

private:
	unique_ptr<QueryResult> result;
	//! ColumnDataCollection::GetValue rescans the whole collection on every call.
	unique_ptr<ColumnDataRowCollection> rows;
};

inline unique_ptr<TestResult> Collected(unique_ptr<QueryResult> result) {
	return make_uniq<TestResult>(std::move(result));
}

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
	vector<string> returning_columns;
	vector<vector<Value>> rows;
	bool rows_only = false;
	idx_t target_swap_bytes = 0;
	idx_t target_swap_bytes_after_return = 0;
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
	vector<string> calls_asked;
	vector<LogicalTypeId> refused_types;
	vector<LogicalOperatorType> refused_operators;
	case_insensitive_map_t<vector<string>> keys;
	case_insensitive_map_t<bool> key_unique;
	bool declare_key_constraints = false;
	bool returning = true;
	bool takes_seams = false;
	case_insensitive_map_t<vector<CrossingVerb>> verbs;
	case_insensitive_map_t<vector<CrossingVerb>> schema_verbs;
	string declined;

	vector<FarCall> reads;
	vector<FarCall> writes;
	idx_t link_seam_rows = 0;
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
	bool drop_batch_index = false;
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
	//! Runs at the start of the source calls that report themselves, with the call's name, outside `lock`.
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

	CrossingScan Run(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	void Commit();
	void Rollback();
	optional_ptr<CrossingLink> Link(ClientContext &context);
	void Planned(const CrossingQuery &query);

private:
	void EndServing();
	vector<vector<Value>> Evaluate(const LogicalOperator &plan, FarCall &call);
	vector<vector<Value>> EvaluateNative(const LogicalOperator &plan, FarCall &call);
	vector<vector<Value>> EvaluateSubstrait(const LogicalOperator &plan, FarCall &call);
	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingScan Write(const CrossingQuery &query);
	vector<vector<Value>> Apply(const CrossingQuery &query);
	unique_ptr<CreateViewInfo> ReceiveView(const CreateViewInfo &view, const LogicalOperator &plan);
	FarCall Record(const CrossingQuery &query);

	shared_ptr<FarStore> store;
	string catalog;
	mutex far_lock;
	Connection far;
	unique_ptr<TestLinkPair> link_pair;
	unique_ptr<CrossingFar> far_side;
	std::thread serving;
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
	CrossingVerdict AcceptsSeam(const vector<LogicalType> &types);
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
