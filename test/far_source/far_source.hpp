#pragma once

#include <atomic>

#include "crossing_far.hpp"
#include "framework/test_link.hpp"
#include "internal/floor.hpp"
#include "internal/seam.hpp"

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
	vector<string> set_columns;
	vector<string> returning_columns;
	//! A read's rows as far sent them; a write's as near sent them.
	vector<vector<Value>> rows;
	bool rows_only = false;
	string wire;

	bool Has(LogicalOperatorType type) const;
	idx_t Count(LogicalOperatorType type) const;
	bool Mentions(const string &text) const;
};

struct FarDdl {
	CrossingVerb verb;
	string table;
};

struct FarStore {
	FarStore();

	DuckDB db;
	Connection con;
	mutex lock;

	case_insensitive_set_t refused_functions;
	vector<string> calls_asked;
	vector<LogicalTypeId> refused_types;
	vector<LogicalOperatorType> refused_operators;
	case_insensitive_map_t<vector<string>> keys;
	bool declare_key_constraints = false;
	//! A transaction that changed a schema keeps every constraint far reports.
	bool keep_far_constraints = false;
	bool returning = true;
	bool takes_seams = false;
	case_insensitive_map_t<vector<CrossingVerb>> verbs;
	case_insensitive_map_t<vector<CrossingVerb>> schema_verbs;

	vector<FarCall> reads;
	vector<FarCall> writes;
	vector<FarDdl> ddls;
	//! Interned `GROUP` attach options; an element's address is the group of every attach naming it.
	set<string> groups;
	vector<string> attached_paths;
	vector<string> detached_paths;
	vector<string> transaction_ends;
	case_insensitive_map_t<idx_t> listings;
	idx_t links_connected = 0;

	idx_t streams = 1;
	idx_t capacity = 4;
	idx_t seam_rows = 0;
	idx_t waits = 0;
	idx_t near_control_replies = 0;
	idx_t most_held_for_near = 0;
	std::atomic<idx_t> far_rows_sent {0};
	idx_t near_recv_streams = 0;
	//! Near's Recv waits, as if far were slow: on control replies, and on a slice's rows.
	std::atomic<bool> stall_replies {false};
	std::atomic<bool> stall_results {false};
	//! Set while a link is open: ends every stall and lifts its limits.
	std::function<void()> release_link;
	void Release();
	//! Non-zero: what far's end of the link reports, which crossing must not ask.
	idx_t far_streams = 0;
	//! For each slice of the open transaction, whether it is a write and where its call is.
	vector<std::pair<bool, idx_t>> slices;
	//! Runs at the start of the source calls that report themselves, with the call's name, outside `lock`.
	std::function<void(const string &call)> on_source_call;
	//! Handed to the Substrait decode of every received plan.
	SubstraitCatalogCheck catalog_check;
	CrossingFloorResolver floors;
	std::function<void(SQLStatement &)> write_shaper;
	std::function<void(SQLStatement &)> ddl_shaper;

	const FarCall &LastRead();
	const FarCall &LastWrite();
};

class FarSource : public CrossingSource {
public:
	//! `catalog` is the store's catalog this attach serves; naming one puts it in every floor.
	FarSource(shared_ptr<FarStore> store, string path, const string &group = "", const string &catalog = "");

	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingSchema DescribeSchema(const string &schema) override;
	static vector<string> TablesOn(Connection &con, const string &catalog, const string &schema);
	static bool IsView(Connection &con, const string &catalog, const string &schema, const string &name);
	static CrossingTable DescribeOn(FarStore &store, Connection &con, const string &catalog, const string &schema,
	                                const string &name, const string &floor_catalog);
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;
	CrossingVerdict AcceptsOperator(const LogicalOperator &op) override;
	CrossingVerdict AcceptsSeam(const vector<LogicalType> &types) override;
	unique_ptr<CrossingLink> Connect(ClientContext &context) override;
	void DescribeChanged(const string &schema, CrossingTable &table) override;
	void Detach(ClientContext &context) override;
	const void *Group() const override;
	void Planned(const LogicalOperator &plan) override;

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

	const duckdb::CrossingIdentity &Identity() const override;

	static void Register(duckdb::ExtensionLoader &loader, duckdb::shared_ptr<duckdb::FarStore> store);
};

} // namespace other
