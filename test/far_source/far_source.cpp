#include "far_source/far_source.hpp"
#include "crossing_substrait.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

namespace {

constexpr const char *FAR_CATALOG = "memory";

vector<vector<Value>> RowsOf(QueryResult &result) {
	if (result.HasError()) {
		result.ThrowError();
	}
	auto &materialized = result.Cast<MaterializedQueryResult>();
	vector<vector<Value>> out;
	for (idx_t r = 0; r < materialized.RowCount(); r++) {
		vector<Value> row;
		for (idx_t c = 0; c < materialized.ColumnCount(); c++) {
			row.push_back(materialized.GetValue(c, r));
		}
		out.push_back(std::move(row));
	}
	return out;
}

vector<vector<Value>> RowsOf(const ColumnDataCollection &collection) {
	vector<vector<Value>> out;
	for (auto &chunk : collection.Chunks()) {
		for (idx_t r = 0; r < chunk.size(); r++) {
			vector<Value> row;
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				row.push_back(chunk.GetValue(c, r));
			}
			out.push_back(std::move(row));
		}
	}
	return out;
}

optional_ptr<const ColumnDataCollection> FindSeamRows(const LogicalOperator &op) {
	if (auto rows = SeamRowsOf(op)) {
		return rows;
	}
	for (auto &child : op.children) {
		if (auto rows = FindSeamRows(*child)) {
			return rows;
		}
	}
	return nullptr;
}

void CollectOperators(const LogicalOperator &op, vector<LogicalOperatorType> &out) {
	out.push_back(op.type);
	for (auto &child : op.children) {
		CollectOperators(*child, out);
	}
}

unique_ptr<QueryResult> RunPlan(Connection &con, unique_ptr<LogicalOperator> plan) {
	auto pending = PendingCrossingPlan(*con.context, std::move(plan), false);
	if (pending->HasError()) {
		pending->ThrowError();
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->ThrowError();
	}
	return result;
}

void ArriveLater(FarStore &store, CrossingWaker waker, std::function<void()> arrive) {
	if (store.keep_wakers) {
		store.kept_wakers.push_back(waker);
	}
	if (store.wake_before_returning) {
		arrive();
		waker.Wake();
		return;
	}
	store.arrivals.emplace_back([&store, waker, arrive]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		{
			lock_guard<mutex> guard(store.lock);
			arrive();
		}
		waker.Wake();
	});
}

struct ReaderHandle {
	explicit ReaderHandle(shared_ptr<FarStore> store_p) : store(std::move(store_p)) {
		lock_guard<mutex> guard(store->lock);
		store->readers_open++;
	}
	~ReaderHandle() {
		lock_guard<mutex> guard(store->lock);
		store->readers_closed++;
	}
	shared_ptr<FarStore> store;
};

CrossingReader FarReader(shared_ptr<const vector<vector<Value>>> rows, idx_t partitions, idx_t partition,
                         shared_ptr<FarStore> store) {
	idx_t cursor = 0;
	auto handle = make_shared_ptr<ReaderHandle>(store);
	return [rows, partitions, partition, store, cursor, handle](ClientContext &, DataChunk &chunk,
	                                                            CrossingWaker waker) mutable {
		{
			lock_guard<mutex> guard(store->lock);
			if (store->wait_before_chunks && store->arrived.erase(partition) == 0) {
				auto raw = store.get();
				ArriveLater(*raw, std::move(waker), [raw, partition]() {
					raw->waits_served++;
					raw->arrived.insert(partition);
				});
				return CrossingReadResult::Wait();
			}
		}
		idx_t count = 0;
		while (cursor < rows->size() && count < STANDARD_VECTOR_SIZE) {
			if (cursor % partitions == partition) {
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					chunk.SetValue(c, count, (*rows)[cursor][c]);
				}
				count++;
			}
			cursor++;
		}
		chunk.SetCardinality(count);
		if (count == 0) {
			return CrossingReadResult::Done();
		}
		lock_guard<mutex> guard(store->lock);
		store->partitions_read.insert(partition);
		return CrossingReadResult::Rows();
	};
}

} // namespace

const char *TransportName(Transport transport) {
	return transport == Transport::NATIVE ? "native" : "substrait";
}

bool FarCall::Has(LogicalOperatorType type) const {
	return Count(type) > 0;
}

idx_t FarCall::Count(LogicalOperatorType type) const {
	return NumericCast<idx_t>(std::count(operators.begin(), operators.end(), type));
}

bool FarCall::Mentions(const string &text) const {
	return plan_text.find(text) != string::npos;
}

FarStore::FarStore(Transport transport_p) : db(nullptr), con(db), transport(transport_p) {
	RegisterCrossingPlanFunctions(*db.instance);
}

void FarStore::JoinArrivals() {
	vector<std::thread> pending;
	{
		lock_guard<mutex> guard(lock);
		pending.swap(arrivals);
	}
	for (auto &thread : pending) {
		thread.join();
	}
}

const FarCall &FarStore::LastRead() {
	if (reads.empty()) {
		throw InternalException("far: nothing was read");
	}
	return reads.back();
}

const FarCall &FarStore::LastWrite() {
	if (writes.empty()) {
		throw InternalException("far: nothing was written");
	}
	return writes.back();
}

FarSource::FarSource(shared_ptr<FarStore> store_p, string path_p) : store(std::move(store_p)), path(std::move(path_p)) {
	lock_guard<mutex> guard(store->lock);
	store->attached_paths.push_back(path);
}

void FarSource::Detach(ClientContext &) {
	lock_guard<mutex> guard(store->lock);
	store->detached_paths.push_back(path);
}

vector<string> FarSource::Schemas() {
	lock_guard<mutex> guard(store->lock);
	auto result =
	    store->con.Query("SELECT schema_name FROM duckdb_schemas() WHERE database_name = '" + string(FAR_CATALOG) +
	                     "' AND schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY 1");
	vector<string> names;
	for (auto &row : RowsOf(*result)) {
		names.push_back(row[0].GetValue<string>());
	}
	return names;
}

vector<string> FarSource::TablesOn(Connection &con, const string &schema) {
	auto prepared = con.Prepare("SELECT table_name FROM duckdb_tables() WHERE database_name = '" + string(FAR_CATALOG) +
	                            "' AND schema_name = ? ORDER BY 1");
	vector<Value> args {Value(schema)};
	auto result = prepared->Execute(args, false);
	vector<string> names;
	for (auto &row : RowsOf(*result)) {
		names.push_back(row[0].GetValue<string>());
	}
	return names;
}

vector<string> FarSource::Tables(const string &schema) {
	lock_guard<mutex> guard(store->lock);
	store->listings[schema]++;
	return TablesOn(store->con, schema);
}

CrossingTable FarSource::DescribeOn(FarStore &store, Connection &con, const string &schema, const string &name) {
	auto description = con.TableInfo(FAR_CATALOG, schema, name);
	if (!description) {
		throw CatalogException("far: no table '%s.%s'", schema, name);
	}
	CrossingTable table;
	table.name = name;
	for (auto &column : description->columns) {
		table.Column(column.Name(), column.Type());
	}
	auto key = store.keys.find(name);
	if (key != store.keys.end()) {
		table.key = key->second;
	} else {
		auto prepared = con.Prepare("SELECT constraint_column_names FROM duckdb_constraints() WHERE schema_name "
		                            "= ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'");
		vector<Value> args {Value(schema), Value(name)};
		auto rows = RowsOf(*prepared->Execute(args, false));
		if (!rows.empty()) {
			for (auto &column : ListValue::GetChildren(rows[0][0])) {
				table.key.push_back(column.GetValue<string>());
			}
		}
	}
	auto unique = store.key_unique.find(name);
	table.key_unique = unique != store.key_unique.end() ? unique->second : !table.key.empty();
	auto verbs = store.verbs.find(name);
	if (verbs != store.verbs.end()) {
		table.verbs = verbs->second;
	} else {
		table.verbs = {CrossingVerb::SELECT,  CrossingVerb::INSERT, CrossingVerb::UPDATE,
		               CrossingVerb::DELETE_, CrossingVerb::ALTER,  CrossingVerb::DROP};
	}
	return table;
}

CrossingTable FarSource::Describe(const string &schema, const string &name) {
	lock_guard<mutex> guard(store->lock);
	return DescribeOn(*store, store->con, schema, name);
}

CrossingSchema FarSource::DescribeSchema(const string &schema) {
	lock_guard<mutex> guard(store->lock);
	CrossingSchema described;
	described.name = schema;
	auto verbs = store->schema_verbs.find(schema);
	described.verbs = verbs != store->schema_verbs.end() ? verbs->second : vector<CrossingVerb> {CrossingVerb::CREATE};
	return described;
}

CrossingPlan FarSource::Plan(const CrossingPlanRequest &request) {
	{
		lock_guard<mutex> guard(store->lock);
		if (!store->declined.empty()) {
			return CrossingPlan::Declined(store->declined);
		}
	}
	if (request.verb == CrossingVerb::SELECT) {
		auto &described = *request.described;
		return CrossingPlan::Of(
		    MakeFloorNode(0, request.schema, request.table, described.column_names, described.column_types));
	}
	return CrossingPlan::Of(MakeSeamNode(0, request.seam.types));
}

CrossingVerdict FarSource::AcceptsCall(const Expression &expr) {
	string reason;
	if (!SubstraitCanRenderCall(expr, reason)) {
		return CrossingVerdict::No(reason);
	}
	string name;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		name = expr.Cast<BoundFunctionExpression>().function.name;
		break;
	case ExpressionClass::BOUND_AGGREGATE:
		name = expr.Cast<BoundAggregateExpression>().function.name;
		break;
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		name = window.aggregate ? window.aggregate->name
		                        : StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType()));
		break;
	}
	default:
		return CrossingVerdict::Yes();
	}
	lock_guard<mutex> guard(store->lock);
	if (store->refused_functions.find(name) != store->refused_functions.end()) {
		return CrossingVerdict::No("far refuses " + name);
	}
	return CrossingVerdict::Yes();
}

CrossingVerdict FarSource::AcceptsType(const LogicalType &type) {
	lock_guard<mutex> guard(store->lock);
	for (auto refused : store->refused_types) {
		if (type.id() == refused) {
			return CrossingVerdict::No("far refuses " + type.ToString());
		}
	}
	return CrossingVerdict::Yes();
}

CrossingVerdict FarSource::AcceptsOperator(const LogicalOperator &op) {
	string reason;
	if (!SubstraitCanRenderOperator(op, reason)) {
		return CrossingVerdict::No(reason);
	}
	lock_guard<mutex> guard(store->lock);
	for (auto refused : store->refused_operators) {
		if (op.type == refused) {
			return CrossingVerdict::No("far refuses " + string(LogicalOperatorToString(op.type)));
		}
	}
	return CrossingVerdict::Yes();
}

unique_ptr<FarSession> FarSource::Begin(ClientContext &) {
	lock_guard<mutex> guard(store->lock);
	store->sessions_begun++;
	return make_uniq<FarSession>(store);
}

void FarSource::Register(ExtensionLoader &loader, shared_ptr<FarStore> store) {
	Crossing<FarSource>::Register(
	    loader, "fardb", [store](ClientContext &, AttachInfo &info) { return make_uniq<FarSource>(store, info.path); });
}

FarSession::FarSession(shared_ptr<FarStore> store_p) : store(std::move(store_p)), far(store->db) {
	far.BeginTransaction();
}

FarSession::~FarSession() {
	if (far.context->transaction.HasActiveTransaction()) {
		far.Rollback();
	}
}

vector<string> FarSession::Tables(const string &schema) {
	lock_guard<mutex> guard(far_lock);
	return FarSource::TablesOn(far, schema);
}

CrossingTable FarSession::Describe(const string &schema, const string &name) {
	lock_guard<mutex> guard(far_lock);
	lock_guard<mutex> store_guard(store->lock);
	return FarSource::DescribeOn(*store, far, schema, name);
}

void FarSession::Commit() {
	far.Commit();
	lock_guard<mutex> guard(store->lock);
	store->transaction_ends.push_back("commit");
}

void FarSession::Rollback() {
	far.Rollback();
	lock_guard<mutex> guard(store->lock);
	store->transaction_ends.push_back("rollback");
}

void FarSession::Ddl(ClientContext &, const CrossingDdl &ddl) {
	unique_ptr<SQLStatement> statement;
	switch (ddl.verb) {
	case CrossingVerb::CREATE: {
		auto create = make_uniq<CreateStatement>();
		create->info = ddl.create->Copy();
		create->info->catalog = FAR_CATALOG;
		statement = std::move(create);
		break;
	}
	case CrossingVerb::ALTER: {
		auto alter = make_uniq<AlterStatement>();
		alter->info = ddl.alter->Copy();
		alter->info->catalog = FAR_CATALOG;
		statement = std::move(alter);
		break;
	}
	case CrossingVerb::DROP: {
		auto drop = make_uniq<DropStatement>();
		drop->info = ddl.drop->Copy();
		drop->info->catalog = FAR_CATALOG;
		statement = std::move(drop);
		break;
	}
	default:
		throw InternalException("far: '%s' is not a DDL verb", CrossingVerbName(ddl.verb));
	}
	lock_guard<mutex> guard(far_lock);
	auto pending = far.context->PendingQuery(std::move(statement), QueryParameters(false));
	if (pending->HasError()) {
		pending->ThrowError();
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->ThrowError();
	}
	lock_guard<mutex> store_guard(store->lock);
	store->ddls.push_back(FarDdl {ddl.verb, ddl.schema, ddl.table});
}

FarCall FarSession::Record(const CrossingQuery &query) {
	FarCall call;
	call.kind = query.kind;
	call.plan_text = query.plan.ToString();
	call.tables = query.tables;
	call.written = query.written;
	CollectOperators(query.plan, call.operators);
	call.types = query.types;
	call.ordered = query.ordered;
	call.key_columns = query.key_columns;
	call.set_columns = query.set_columns;
	return call;
}

vector<vector<Value>> FarSession::EvaluateNative(const LogicalOperator &plan, FarCall &call) {
	call.wire = SerializeCrossingPlan(plan);
	auto received = DeserializeCrossingPlan(*far.context, call.wire);
	call.received_text = received->ToString();
	BindFloors(*far.context, received, FAR_CATALOG);
	return RowsOf(*RunPlan(far, std::move(received)));
}

string FarSession::StageSeamRows(const LogicalOperator &plan) {
	auto rows = FindSeamRows(plan);
	if (!rows) {
		return "";
	}
	string seam_view = "crossing_seam_rows";
	string columns;
	for (idx_t c = 0; c < rows->Types().size(); c++) {
		columns += (c ? ", c" : "c") + to_string(c) + " " + rows->Types()[c].ToString();
	}
	auto created = far.Query("CREATE OR REPLACE TEMP TABLE " + seam_view + "(" + columns + ")");
	if (created->HasError()) {
		created->ThrowError();
	}
	Appender appender(far, "temp", "main", seam_view);
	for (auto &chunk : rows->Chunks()) {
		appender.AppendDataChunk(chunk);
	}
	appender.Close();
	return seam_view;
}

vector<vector<Value>> FarSession::EvaluateSubstrait(const LogicalOperator &plan, FarCall &call) {
	call.wire = RenderSubstraitJson(plan);
	auto rel = DecodeSubstraitJson(far, FAR_CATALOG, call.wire, StageSeamRows(plan));
	call.received_text = rel->ToString();
	return RowsOf(*rel->Execute());
}

vector<vector<Value>> FarSession::Evaluate(const LogicalOperator &plan, FarCall &call) {
	if (auto rows = SeamRowsOf(plan)) {
		return RowsOf(*rows);
	}
	lock_guard<mutex> guard(far_lock);
	if (store->transport == Transport::NATIVE) {
		return EvaluateNative(plan, call);
	}
	return EvaluateSubstrait(plan, call);
}

CrossingScan FarSession::Read(ClientContext &, const CrossingQuery &query) {
	auto call = Record(query);
	auto rows = make_shared_ptr<const vector<vector<Value>>>(Evaluate(query.plan, call));
	call.rows = *rows;
	lock_guard<mutex> guard(store->lock);
	store->reads.push_back(std::move(call));
	auto partitions = query.ordered && !store->partition_ordered_reads ? 1 : store->read_partitions;
	auto store_ref = store;
	CrossingScan scan;
	scan.partitions = partitions;
	scan.open = [=](ClientContext &, idx_t partition) {
		return FarReader(rows, partitions, partition, store_ref);
	};
	return scan;
}

CrossingWriter FarSession::Write(ClientContext &, const CrossingQuery &query) {
	return [this, &query](ClientContext &, CrossingWaker waker) {
		{
			lock_guard<mutex> guard(store->lock);
			if (store->wait_before_write && !store->write_arrived) {
				auto raw = store.get();
				ArriveLater(*raw, std::move(waker), [raw]() {
					raw->write_waits_served++;
					raw->write_arrived = true;
				});
				return CrossingWriteResult::Wait();
			}
			store->write_arrived = false;
		}
		auto call = Record(query);
		auto rows = Evaluate(query.plan, call);
		call.rows = rows;
		{
			lock_guard<mutex> guard(store->lock);
			store->writes.push_back(std::move(call));
		}
		lock_guard<mutex> guard(far_lock);
		return CrossingWriteResult::Done(Apply(query));
	};
}

idx_t FarSession::Apply(const CrossingQuery &query) {
	if (query.written.table.empty()) {
		throw InternalException("far: a write names no table");
	}
	CrossingWriteTarget target;
	target.catalog = FAR_CATALOG;
	target.schema = query.written.schema;
	target.table = query.written.table;
	target.verb = query.kind;
	target.key_columns = query.key_columns;
	target.set_columns = query.set_columns;
	if (auto rows = SeamRowsOf(query.plan)) {
		return ExecuteWrite(*far.context, target, SeamRefOfRows(*rows));
	}
	if (store->transport == Transport::NATIVE) {
		auto received = DeserializeCrossingPlan(*far.context, SerializeCrossingPlan(query.plan));
		BindFloors(*far.context, received, FAR_CATALOG);
		return ExecuteWrite(*far.context, target, std::move(received));
	}
	auto rel = DecodeSubstraitJson(far, FAR_CATALOG, RenderSubstraitJson(query.plan), StageSeamRows(query.plan));
	return ExecuteWrite(*far.context, target, rel->GetTableRef());
}

} // namespace duckdb

namespace other {

void OtherSource::Register(duckdb::ExtensionLoader &loader, duckdb::shared_ptr<duckdb::FarStore> store) {
	duckdb::Crossing<OtherSource>::Register(loader, "otherdb",
	                                        [store](duckdb::ClientContext &, duckdb::AttachInfo &info) {
		                                        return duckdb::make_uniq<OtherSource>(store, info.path);
	                                        });
}

} // namespace other
