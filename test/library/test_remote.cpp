#include "catch.hpp"
#include "crossing.hpp"
#include "crossing_far.hpp"
#include "framework/test_link.hpp"
#include "internal/execute.hpp"
#include "internal/crossing_remote.hpp"
#include "internal/floor.hpp"
#include "internal/seam.hpp"

#include "duckdb.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/planner.hpp"

#include <algorithm>
#include <chrono>
#include <future>
#include <thread>

using namespace duckdb;

namespace {

constexpr idx_t REMOTE_TIMEOUT_SECONDS = 60;

const vector<LogicalType> TWO_BIGINTS = {LogicalType::BIGINT, LogicalType::BIGINT};

//! A near and a far database joined by a link, one far transaction open on `Serve`.
struct Pair {
	explicit Pair(idx_t streams = 1, idx_t capacity = 4, idx_t near_threads = 4, idx_t far_threads = 4)
	    : link(streams, capacity), near_db(nullptr), near_con(near_db), far_db(nullptr), far_con(far_db),
	      far_side(far_con, "memory") {
		Ok(near_con, "SET threads = " + to_string(near_threads));
		Ok(far_con, "SET threads = " + to_string(far_threads));
		Ok(far_con, "CREATE TABLE t AS SELECT i::BIGINT AS id, (i * 10)::BIGINT AS v FROM range(5000) r(i)");
		serving = std::async(std::launch::async, [this]() {
			try {
				far_side.Serve(link.Far());
				return ErrorData();
			} catch (std::exception &ex) {
				return ErrorData(ex);
			}
		});
		remote = make_uniq<CrossingRemote>(link.Near());
		statement = make_shared_ptr<CrossingRemoteStatement>(*remote);
	}

	static void Ok(Connection &con, const string &sql) {
		auto result = con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
	}

	unique_ptr<LogicalOperator> Floor(idx_t index) {
		return MakeFloorNode(index, "main", "t", {"id", "v"}, TWO_BIGINTS);
	}

	//! Plans `sql` on near, as a feed.
	unique_ptr<LogicalOperator> Feed(const string &sql) {
		near_con.BeginTransaction();
		Planner planner(*near_con.context);
		Parser parser(*near_con.context);
		parser.ParseQuery(sql);
		auto &select = parser.statements[0]->Cast<SelectStatement>();
		auto plan = planner.binder->Bind(*select.node).plan;
		planners.push_back(std::move(planner.binder));
		return plan;
	}

	vector<vector<Value>> Run(unique_ptr<LogicalOperator> plan) {
		if (!near_con.HasActiveTransaction()) {
			near_con.BeginTransaction();
		}
		auto result = SubmitCrossingPlan(*near_con.context, std::move(plan));
		result->ThrowIfError();
		result->Complete();
		result->ThrowIfError();
		vector<vector<Value>> rows;
		while (auto chunk = result->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				vector<Value> row;
				for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
					row.push_back(chunk->GetValue(c, r));
				}
				rows.push_back(std::move(row));
			}
		}
		result->ThrowIfError();
		result.reset();
		statement->Finish();
		near_con.Commit();
		statement = make_shared_ptr<CrossingRemoteStatement>(*remote);
		return rows;
	}

	idx_t Far(const string &sql) {
		auto result = far_side_con().Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		auto rows = result->Fetch();
		return NumericCast<idx_t>(rows->GetValue(0, 0).GetValue<int64_t>());
	}

	Connection &far_side_con() {
		return far_con;
	}

	void Commit() {
		remote->Commit();
		RequireServeEnds();
	}

	void Rollback() {
		remote->Rollback();
		RequireServeEnds();
	}

	void RequireServeEnds() {
		REQUIRE(serving.wait_for(std::chrono::seconds(REMOTE_TIMEOUT_SECONDS)) == std::future_status::ready);
		auto error = serving.get();
		if (error.HasError()) {
			FAIL(error.Message());
		}
	}

	~Pair() {
		remote.reset();
		if (serving.valid()) {
			serving.wait();
		}
	}

	CrossingRemoteSlice Slice(unique_ptr<LogicalOperator> source, bool ordered = false) {
		CrossingRemoteSlice slice;
		slice.id = remote->NextSliceId();
		slice.ordered = ordered;
		slice.source = std::move(source);
		return slice;
	}

	unique_ptr<LogicalOperator> Remote(idx_t table_index, vector<LogicalType> types, CrossingRemoteSlice slice,
	                                   unique_ptr<LogicalOperator> feed = nullptr) {
		return MakeCrossingRemote(TableIndex(table_index), std::move(types), statement, std::move(slice),
		                          std::move(feed));
	}

	TestLinkPair link;
	DuckDB near_db;
	Connection near_con;
	DuckDB far_db;
	Connection far_con;
	CrossingFar far_side;
	std::future<ErrorData> serving;
	unique_ptr<CrossingRemote> remote;
	shared_ptr<CrossingRemoteStatement> statement;
	vector<shared_ptr<Binder>> planners;
};

unique_ptr<Expression> Ref(idx_t table, idx_t column, const LogicalType &type = LogicalType::BIGINT) {
	return make_uniq<BoundColumnRefExpression>(type, ColumnBinding(TableIndex(table), ProjectionIndex(column)));
}

unique_ptr<Expression> Equal(unique_ptr<Expression> left, unique_ptr<Expression> right) {
	return BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(left), std::move(right));
}

//! far t, target index 100, source a seam at index 200 with (id, value).
SubstraitWrite TargetOfT(CrossingVerb verb) {
	SubstraitWrite write;
	write.verb = verb;
	write.schema = "main";
	write.table = "t";
	write.target_columns = {"id", "v"};
	write.target_index = 100;
	return write;
}

template <class FUNC>
ErrorData ErrorOf(FUNC &&run) {
	try {
		run();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	FAIL("expected an error");
	return ErrorData();
}

CrossingRemoteSlice SeamWrite(Pair &pair, unique_ptr<SubstraitWrite> write, vector<unique_ptr<Expression>> owned) {
	auto slice = pair.Slice(MakeSeamNode(200, TWO_BIGINTS));
	slice.write = std::move(write);
	slice.owned = std::move(owned);
	return slice;
}

} // namespace

TEST_CASE("a remote read brings far's rows to near", "[remote]") {
	auto [streams, near_threads, far_threads] =
	    GENERATE(std::make_tuple<idx_t, idx_t, idx_t>(1, 1, 1), std::make_tuple<idx_t, idx_t, idx_t>(4, 4, 4),
	             std::make_tuple<idx_t, idx_t, idx_t>(4, 1, 4), std::make_tuple<idx_t, idx_t, idx_t>(4, 4, 1));
	INFO(streams << " streams, " << near_threads << " near, " << far_threads << " far");
	Pair pair(streams, 2, near_threads, far_threads);

	auto rows = pair.Run(pair.Remote(1, TWO_BIGINTS, pair.Slice(pair.Floor(0))));

	REQUIRE(rows.size() == 5000);
	pair.Commit();
}

TEST_CASE("an ordered remote read keeps far's order across streams", "[remote]") {
	auto [streams, near_threads, far_threads] =
	    GENERATE(std::make_tuple<idx_t, idx_t, idx_t>(1, 1, 1), std::make_tuple<idx_t, idx_t, idx_t>(4, 4, 4),
	             std::make_tuple<idx_t, idx_t, idx_t>(4, 1, 4), std::make_tuple<idx_t, idx_t, idx_t>(2, 4, 1));
	INFO(streams << " streams, " << near_threads << " near, " << far_threads << " far");
	Pair pair(streams, 2, near_threads, far_threads);

	auto rows = pair.Run(pair.Remote(1, TWO_BIGINTS, pair.Slice(pair.Floor(0), true)));

	REQUIRE(rows.size() == 5000);
	for (idx_t r = 0; r < rows.size(); r++) {
		if (rows[r][0] != Value::BIGINT(NumericCast<int64_t>(r))) {
			FAIL("row " << r << " out of order: " << rows[r][0].ToString());
		}
	}
	pair.Commit();
}

TEST_CASE("a seam read joins near's rows to far's table on far", "[remote]") {
	auto [streams, preserve] = GENERATE(std::make_tuple<idx_t, bool>(1, true), std::make_tuple<idx_t, bool>(4, true),
	                                    std::make_tuple<idx_t, bool>(4, false));
	INFO(streams << " streams, preserve " << preserve);
	Pair pair(streams);
	Pair::Ok(pair.near_con, string("SET preserve_insertion_order = ") + (preserve ? "true" : "false"));

	auto source = LogicalCrossProduct::Create(pair.Floor(0), MakeSeamNode(1, TWO_BIGINTS));
	auto filter = make_uniq<LogicalFilter>(Equal(Ref(0, 0), Ref(1, 0)));
	filter->AddChild(std::move(source));
	auto rows = pair.Run(pair.Remote(
	    2, {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    pair.Slice(std::move(filter)), pair.Feed("SELECT i::BIGINT AS a, 1::BIGINT AS b FROM range(3000) r(i)")));

	REQUIRE(rows.size() == 3000);
	pair.Commit();
}

TEST_CASE("a seam keeps an ordered feed's order", "[remote]") {
	Pair pair(4);
	auto rows = pair.Run(
	    pair.Remote(1, TWO_BIGINTS, pair.Slice(MakeSeamNode(0, TWO_BIGINTS), true),
	                pair.Feed("SELECT i::BIGINT AS a, 1::BIGINT AS b FROM range(20000) r(i) ORDER BY i DESC")));

	REQUIRE(rows.size() == 20000);
	for (idx_t r = 0; r < rows.size(); r++) {
		if (rows[r][0] != Value::BIGINT(NumericCast<int64_t>(19999 - r))) {
			FAIL("row " << r << " out of order: " << rows[r][0].ToString());
		}
	}
	pair.Commit();
}

TEST_CASE("DuckDB schedules a seam's threads across the streams", "[remote]") {
	Pair pair(4, 1);
	Pair::Ok(pair.near_con, "SET preserve_insertion_order = false");
	auto rows = pair.Run(pair.Remote(1, TWO_BIGINTS, pair.Slice(MakeSeamNode(0, TWO_BIGINTS)),
	                                 pair.Feed("SELECT i::BIGINT AS a, 1::BIGINT AS b FROM range(2000000) r(i)")));

	REQUIRE(rows.size() == 2000000);
	REQUIRE(pair.link.UsedStreams().size() > 2);
	REQUIRE(pair.link.SendingThreads().size() > 2);
	REQUIRE(pair.link.Waits() > 0);
	pair.Commit();
}

TEST_CASE("two slices of one statement share one far transaction", "[remote]") {
	auto [near_threads, far_threads] = GENERATE(std::make_pair<idx_t, idx_t>(1, 1), std::make_pair<idx_t, idx_t>(4, 4),
	                                            std::make_pair<idx_t, idx_t>(1, 4));
	INFO(near_threads << " near, " << far_threads << " far");
	Pair pair(2, 2, near_threads, far_threads);

	auto left = pair.Remote(10, TWO_BIGINTS, pair.Slice(pair.Floor(0)));
	auto right = pair.Remote(11, TWO_BIGINTS, pair.Slice(pair.Floor(1)));
	auto filter = make_uniq<LogicalFilter>(Equal(Ref(10, 0), Ref(11, 0)));
	filter->AddChild(LogicalCrossProduct::Create(std::move(left), std::move(right)));

	auto rows = pair.Run(std::move(filter));

	REQUIRE(rows.size() == 5000);
	pair.Commit();
}

TEST_CASE("a seam's feed can read the same far transaction", "[remote]") {
	auto [near_threads, far_threads] = GENERATE(std::make_pair<idx_t, idx_t>(1, 1), std::make_pair<idx_t, idx_t>(4, 4),
	                                            std::make_pair<idx_t, idx_t>(4, 1));
	INFO(near_threads << " near, " << far_threads << " far");
	Pair pair(2, 1, near_threads, far_threads);

	auto feed = pair.Remote(10, TWO_BIGINTS, pair.Slice(pair.Floor(0)));
	auto rows = pair.Run(pair.Remote(20, TWO_BIGINTS, pair.Slice(MakeSeamNode(1, TWO_BIGINTS)), std::move(feed)));

	REQUIRE(rows.size() == 5000);
	pair.Commit();
}

TEST_CASE("an update runs on far as a native write", "[remote][write]") {
	Pair pair(2);
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::UPDATE));
	vector<unique_ptr<Expression>> owned;
	owned.push_back(Equal(Ref(100, 0), Ref(200, 0)));
	owned.push_back(Ref(200, 1));
	write->condition = owned[0].get();
	write->set_columns = {"v"};
	write->values.push_back(*owned[1]);

	auto rows = pair.Run(pair.Remote(1, {LogicalType::BIGINT}, SeamWrite(pair, std::move(write), std::move(owned)),
	                                 pair.Feed("SELECT i::BIGINT AS a, 7::BIGINT AS b FROM range(100) r(i)")));

	REQUIRE(rows.size() == 1);
	REQUIRE(rows[0][0] == Value::BIGINT(100));
	pair.Commit();
	REQUIRE(pair.Far("SELECT count(*) FROM t WHERE v = 7") == 100);
}

TEST_CASE("a delete with no key runs on far, reading only far", "[remote][write]") {
	Pair pair(2);
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::DELETE_));
	vector<unique_ptr<Expression>> owned;
	owned.push_back(Equal(Ref(100, 0), Ref(0, 0)));
	write->condition = owned[0].get();
	auto slice = pair.Slice(pair.Floor(0));
	slice.write = std::move(write);
	slice.owned = std::move(owned);

	auto rows = pair.Run(pair.Remote(1, {LogicalType::BIGINT}, std::move(slice)));

	REQUIRE(rows[0][0] == Value::BIGINT(5000));
	REQUIRE(pair.link.UsedStreams().size() <= 2);
	pair.Commit();
	REQUIRE(pair.Far("SELECT count(*) FROM t") == 0);
}

TEST_CASE("a merge runs on far as one operator, returning rows", "[remote][write]") {
	Pair pair(2);
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::INSERT));
	write->merge = true;
	write->returning = {"id", "v"};
	vector<unique_ptr<Expression>> owned;
	owned.push_back(Equal(Ref(100, 0), Ref(200, 0)));
	owned.push_back(Ref(200, 1));
	owned.push_back(Ref(200, 0));
	owned.push_back(Ref(200, 1));
	write->condition = owned[0].get();
	SubstraitWriteAction update;
	update.when = MergeActionCondition::WHEN_MATCHED;
	update.type = MergeActionType::MERGE_UPDATE;
	update.columns = {"v"};
	update.values.push_back(*owned[1]);
	write->actions.push_back(std::move(update));
	SubstraitWriteAction insert;
	insert.when = MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET;
	insert.type = MergeActionType::MERGE_INSERT;
	insert.columns = {"id", "v"};
	insert.values.push_back(*owned[2]);
	insert.values.push_back(*owned[3]);
	write->actions.push_back(std::move(insert));

	auto rows = pair.Run(pair.Remote(1, TWO_BIGINTS, SeamWrite(pair, std::move(write), std::move(owned)),
	                                 pair.Feed("SELECT (i * 5000)::BIGINT AS a, 9::BIGINT AS b FROM range(3) r(i)")));

	REQUIRE(rows.size() == 3);
	pair.Commit();
	REQUIRE(pair.Far("SELECT count(*) FROM t") == 5002);
	REQUIRE(pair.Far("SELECT count(*) FROM t WHERE v = 9") == 3);
}

TEST_CASE("an empty seam writes nothing and reports zero", "[remote][write]") {
	Pair pair;
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::INSERT));
	write->set_columns = {"id", "v"};

	auto rows = pair.Run(pair.Remote(1, {LogicalType::BIGINT}, SeamWrite(pair, std::move(write), {}),
	                                 pair.Feed("SELECT i::BIGINT AS a, 1::BIGINT AS b FROM range(0) r(i)")));

	REQUIRE(rows[0][0] == Value::BIGINT(0));
	pair.Commit();
	REQUIRE(pair.Far("SELECT count(*) FROM t") == 5000);
}

TEST_CASE("a rollback undoes every slice", "[remote][write]") {
	Pair pair;
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::INSERT));
	write->set_columns = {"id", "v"};
	pair.Run(pair.Remote(1, {LogicalType::BIGINT}, SeamWrite(pair, std::move(write), {}),
	                     pair.Feed("SELECT i::BIGINT AS a, 1::BIGINT AS b FROM range(10) r(i)")));

	pair.Rollback();
	REQUIRE(pair.Far("SELECT count(*) FROM t") == 5000);
}

TEST_CASE("a plan far cannot bind fails with far's error type before rows move", "[remote][failure]") {
	Pair pair;
	auto floor = MakeFloorNode(0, "main", "missing", {"id"}, {LogicalType::BIGINT});
	REQUIRE(ErrorOf([&]() { pair.Run(pair.Remote(1, {LogicalType::BIGINT}, pair.Slice(std::move(floor)))); }).Type() ==
	        ExceptionType::CATALOG);
	pair.near_con.Rollback();
	pair.Rollback();
}

TEST_CASE("a far statement that fails mid-run reaches near with its type", "[remote][failure]") {
	Pair pair(2);
	Pair::Ok(pair.far_con, "CREATE TABLE s(id BIGINT, CHECK (id < 100))");
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::INSERT));
	write->table = "s";
	write->target_columns = {"id"};
	write->set_columns = {"id"};
	auto slice = pair.Slice(MakeSeamNode(0, {LogicalType::BIGINT}));
	slice.write = std::move(write);

	auto feed = pair.Feed("SELECT i::BIGINT AS a FROM range(5000) r(i)");
	auto error = ErrorOf([&]() { pair.Run(pair.Remote(1, {LogicalType::BIGINT}, std::move(slice), std::move(feed))); });
	REQUIRE(error.Type() == ExceptionType::CONSTRAINT);
	pair.near_con.Rollback();
	pair.Rollback();
}

TEST_CASE("a feed that fails mid-send ends far's statement", "[remote][failure]") {
	Pair pair(2);
	auto rows_or_error = [&]() {
		return pair.Run(pair.Remote(1, TWO_BIGINTS, pair.Slice(MakeSeamNode(0, TWO_BIGINTS)),
		                            pair.Feed("SELECT i::BIGINT AS a, (CASE WHEN i = 4000 THEN error('boom') "
		                                      "ELSE 1 END)::BIGINT AS b FROM range(5000) r(i)")));
	};
	REQUIRE_THROWS(rows_or_error());
	pair.near_con.Rollback();
	pair.remote.reset();
	REQUIRE(pair.serving.wait_for(std::chrono::seconds(REMOTE_TIMEOUT_SECONDS)) == std::future_status::ready);
}

TEST_CASE("a create table as runs on far and reports its rows", "[remote][write]") {
	Pair pair(2);
	auto write = make_uniq<SubstraitWrite>();
	write->verb = CrossingVerb::CREATE;
	write->schema = "main";
	write->table = "made";
	write->target_columns = {"x", "y"};
	auto slice = pair.Slice(pair.Floor(0));
	slice.write = std::move(write);

	auto rows = pair.Run(pair.Remote(1, {LogicalType::BIGINT}, std::move(slice)));

	REQUIRE(rows[0][0] == Value::BIGINT(5000));
	pair.Commit();
	REQUIRE(pair.Far("SELECT sum(y) FROM made") == 124975000);
}

TEST_CASE("near stops reading and far stops sending", "[remote]") {
	Pair pair(2, 1);
	{
		pair.near_con.BeginTransaction();
		auto result =
		    SubmitCrossingPlan(*pair.near_con.context, pair.Remote(1, TWO_BIGINTS, pair.Slice(pair.Floor(0))));
		result->ThrowIfError();
		auto first = result->Fetch();
		REQUIRE(first);
		result.reset();
	}
	pair.statement->Finish();
	pair.near_con.Commit();
	pair.Commit();
}

TEST_CASE("a seam over near's memory limit stays off near's disk and lands whole on far", "[remote]") {
	Pair pair(2, 2);
	Pair::Ok(pair.near_con, "SET preserve_insertion_order = false");
	Pair::Ok(pair.near_con, "SET memory_limit = '40MB'");
	Pair::Ok(pair.far_con, "SET memory_limit = '40MB'");
	auto &near_buffers = BufferManager::GetBufferManager(*pair.near_con.context);
	auto write = make_uniq<SubstraitWrite>(TargetOfT(CrossingVerb::INSERT));
	write->set_columns = {"id", "v"};

	auto rows = pair.Run(pair.Remote(1, {LogicalType::BIGINT}, SeamWrite(pair, std::move(write), {}),
	                                 pair.Feed("SELECT i::BIGINT AS a, i::BIGINT AS b FROM range(6000000) r(i)")));

	REQUIRE(rows[0][0] == Value::BIGINT(6000000));
	REQUIRE(near_buffers.GetUsedSwap() == 0);
	pair.Commit();
	REQUIRE(pair.Far("SELECT count(*) FROM t") == 6005000);
}

TEST_CASE("far works on a seam's rows while they are still arriving", "[remote]") {
	Pair pair(2, 2);
	Pair::Ok(pair.near_con, "SET preserve_insertion_order = false");

	auto rows = pair.Run(pair.Remote(1, TWO_BIGINTS, pair.Slice(MakeSeamNode(0, TWO_BIGINTS)),
	                                 pair.Feed("SELECT i::BIGINT AS a, i::BIGINT AS b FROM range(2000000) r(i)")));

	REQUIRE(rows.size() == 2000000);
	REQUIRE(pair.link.FarSentBeforeSeamEnded());
	pair.Commit();
}
