#include "far_source/far_source.hpp"
#include "crossing_substrait.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

namespace {

constexpr const char *FAR_CATALOG = "memory";

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

vector<vector<Value>> RowsOf(QueryResult &result) {
	if (result.HasError()) {
		result.ThrowError();
	}
	return RowsOf(result.Collection());
}

void CollectOperators(const LogicalOperator &op, vector<LogicalOperatorType> &out) {
	out.push_back(op.type);
	for (auto &child : op.children) {
		CollectOperators(*child, out);
	}
}

unique_ptr<QueryResult> RunPlan(Connection &con, unique_ptr<LogicalOperator> plan) {
	auto result = SubmitCrossingPlan(*con.context, std::move(plan));
	result->ThrowIfError();
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

void CopyBlock(const vector<vector<Value>> &rows, idx_t first, idx_t count, DataChunk &chunk) {
	for (idx_t r = 0; r < count; r++) {
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			chunk.data[c].SetValue(r, rows[first + r][c]);
		}
	}
	chunk.SetChildCardinality(count);
}

CrossingReadResult IndexedRows(const FarStore &store, idx_t batch_index) {
	return store.drop_batch_index ? CrossingReadResult::Rows() : CrossingReadResult::Rows(batch_index);
}

CrossingReader FarReader(shared_ptr<const vector<vector<Value>>> rows, idx_t partitions, idx_t partition,
                         shared_ptr<FarStore> store, shared_ptr<idx_t> next_block) {
	idx_t cursor = 0;
	auto handle = make_shared_ptr<ReaderHandle>(store);
	return [rows, partitions, partition, store, next_block, cursor, handle](ClientContext &, DataChunk &chunk,
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
		if (store->batch_block_rows > 0) {
			auto block_rows = MinValue<idx_t>(store->batch_block_rows, STANDARD_VECTOR_SIZE);
			lock_guard<mutex> guard(store->lock);
			idx_t block = *next_block;
			idx_t first = block * block_rows;
			if (first >= rows->size()) {
				return CrossingReadResult::Done();
			}
			(*next_block)++;
			CopyBlock(*rows, first, MinValue<idx_t>(block_rows, rows->size() - first), chunk);
			store->partitions_read.insert(partition);
			return IndexedRows(*store, block);
		}
		if (store->round_robin_batch_rows > 0) {
			auto block_rows = MinValue<idx_t>(store->round_robin_batch_rows, STANDARD_VECTOR_SIZE);
			idx_t block = cursor;
			while (block % partitions != partition) {
				block++;
			}
			idx_t first = block * block_rows;
			if (first >= rows->size()) {
				return CrossingReadResult::Done();
			}
			CopyBlock(*rows, first, MinValue<idx_t>(block_rows, rows->size() - first), chunk);
			cursor = block + 1;
			lock_guard<mutex> guard(store->lock);
			store->partitions_read.insert(partition);
			return IndexedRows(*store, block);
		}
		idx_t count = 0;
		while (cursor < rows->size() && count < STANDARD_VECTOR_SIZE) {
			if (cursor % partitions == partition) {
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					chunk.data[c].SetValue(count, (*rows)[cursor][c]);
				}
				count++;
			}
			cursor++;
		}
		chunk.SetChildCardinality(count);
		if (count == 0) {
			return CrossingReadResult::Done();
		}
		lock_guard<mutex> guard(store->lock);
		store->partitions_read.insert(partition);
		return store->stray_batch_index ? CrossingReadResult::Rows(0) : CrossingReadResult::Rows();
	};
}

} // namespace

const char *TransportName(Transport transport) {
	switch (transport) {
	case Transport::NATIVE:
		return "native";
	case Transport::SUBSTRAIT:
		return "substrait";
	default:
		return "link";
	}
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

FarSource::FarSource(shared_ptr<FarStore> store_p, string path_p, const string &group_p, const string &catalog_p)
    : store(std::move(store_p)), path(std::move(path_p)), catalog(catalog_p.empty() ? FAR_CATALOG : catalog_p),
      floor_catalog(catalog_p) {
	lock_guard<mutex> guard(store->lock);
	store->attached_paths.push_back(path);
	if (!group_p.empty()) {
		group = &*store->groups.insert(group_p).first;
	}
}

const void *FarSource::Group() {
	return group;
}

void FarSource::Detach(ClientContext &) {
	lock_guard<mutex> guard(store->lock);
	store->detached_paths.push_back(path);
}

vector<string> FarSource::Schemas() {
	if (store->on_source_call) {
		store->on_source_call("schemas");
	}
	lock_guard<mutex> guard(store->lock);
	auto result = store->con.Query("SELECT schema_name FROM duckdb_schemas() WHERE database_name = '" + catalog +
	                               "' AND schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY 1");
	vector<string> names;
	for (auto &row : RowsOf(*result)) {
		names.push_back(row[0].GetValue<string>());
	}
	return names;
}

vector<string> FarSource::TablesOn(Connection &con, const string &catalog, const string &schema) {
	auto prepared = con.Prepare("SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? "
	                            "UNION ALL SELECT view_name FROM duckdb_views() WHERE database_name = ? AND "
	                            "schema_name = ? AND NOT internal ORDER BY 1");
	vector<Value> args {Value(catalog), Value(schema), Value(catalog), Value(schema)};
	auto result = prepared->Execute(args);
	vector<string> names;
	for (auto &row : RowsOf(*result)) {
		names.push_back(row[0].GetValue<string>());
	}
	return names;
}

vector<string> FarSource::Tables(const string &schema) {
	lock_guard<mutex> guard(store->lock);
	store->listings[schema]++;
	return TablesOn(store->con, catalog, schema);
}

bool FarSource::IsView(Connection &con, const string &catalog, const string &schema, const string &name) {
	auto prepared = con.Prepare(
	    "SELECT count(*) FROM duckdb_views() WHERE database_name = ? AND schema_name = ? AND view_name = ?");
	vector<Value> args {Value(catalog), Value(schema), Value(name)};
	return RowsOf(*prepared->Execute(args))[0][0].GetValue<int64_t>() > 0;
}

CrossingTable FarSource::DescribeOn(FarStore &store, Connection &con, const string &catalog, const string &schema,
                                    const string &name) {
	CrossingTable table;
	table.name = name;
	if (IsView(con, catalog, schema, name)) {
		auto result = con.Query("SELECT * FROM " + SQLIdentifier::ToString(catalog) + "." +
		                        SQLIdentifier::ToString(schema) + "." + SQLIdentifier::ToString(name) + " LIMIT 0");
		if (result->HasError()) {
			result->ThrowError();
		}
		for (idx_t c = 0; c < result->ColumnCount(); c++) {
			table.Column(result->GetNames()[c].GetIdentifierName(), result->GetTypes()[c]);
		}
		auto verbs = store.verbs.find(name);
		table.verbs = verbs != store.verbs.end() ? verbs->second
		                                         : vector<CrossingVerb> {CrossingVerb::SELECT, CrossingVerb::DROP};
		return table;
	}
	auto description = con.TableInfo(Identifier(catalog), Identifier(schema), Identifier(name));
	if (!description) {
		throw CatalogException("far: no table '%s.%s'", schema, name);
	}
	for (auto &column : description->columns) {
		table.Column(column.Name().GetIdentifierName(), column.Type());
	}
	auto key = store.keys.find(name);
	if (key != store.keys.end()) {
		table.key = key->second;
	} else {
		auto prepared = con.Prepare("SELECT constraint_column_names FROM duckdb_constraints() WHERE database_name "
		                            "= ? AND schema_name = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'");
		vector<Value> args {Value(catalog), Value(schema), Value(name)};
		auto rows = RowsOf(*prepared->Execute(args));
		if (!rows.empty()) {
			for (auto &column : ListValue::GetChildren(rows[0][0])) {
				table.key.push_back(column.GetValue<string>());
			}
		}
	}
	auto unique = store.key_unique.find(name);
	table.key_unique = unique != store.key_unique.end() ? unique->second : !table.key.empty();
	table.returning = store.returning;
	if (store.declare_key_constraints && !table.key.empty()) {
		vector<Identifier> key_columns;
		for (auto &column : table.key) {
			key_columns.emplace_back(column);
		}
		table.constraints.push_back(make_uniq<UniqueConstraint>(std::move(key_columns), true));
	}
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
	if (store->on_source_call) {
		store->on_source_call("describe");
	}
	lock_guard<mutex> guard(store->lock);
	return DescribeOn(*store, store->con, catalog, schema, name);
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
	auto &described = *request.described;
	return CrossingPlan::Of(MakeFloorNode(0, request.schema, request.table, described.column_names,
	                                      described.column_types, floor_catalog));
}

CrossingVerdict FarSource::AcceptsCall(const Expression &expr) {
	string reason;
	if (!SubstraitCanRenderCall(expr, reason)) {
		return CrossingVerdict::No(reason);
	}
	string name;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		name = expr.Cast<BoundFunctionExpression>().Function().GetName().GetIdentifierName();
		break;
	case ExpressionClass::BOUND_AGGREGATE:
		name = expr.Cast<BoundAggregateExpression>().Function().GetName().GetIdentifierName();
		break;
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		name = window.AggregateFunction() ? window.AggregateFunction()->GetName().GetIdentifierName()
		                                  : StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType()));
		break;
	}
	default:
		return CrossingVerdict::Yes();
	}
	lock_guard<mutex> guard(store->lock);
	store->calls_asked.push_back(name);
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

CrossingVerdict FarSource::AcceptsSeam(const vector<LogicalType> &types) {
	lock_guard<mutex> guard(store->lock);
	if (!store->takes_seams) {
		return CrossingVerdict::No("far takes no rows into a read");
	}
	for (auto &type : types) {
		for (auto refused : store->refused_types) {
			if (type.id() == refused) {
				return CrossingVerdict::No("far refuses " + type.ToString());
			}
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
	return make_uniq<FarSession>(store, catalog);
}

void FarSource::Register(ExtensionLoader &loader, shared_ptr<FarStore> store) {
	Crossing<FarSource>::Register(loader, "fardb", [store](ClientContext &, AttachInfo &info) {
		auto option = [&info](const string &name) {
			auto found = info.options.find(name);
			return found == info.options.end() ? string() : found->second.ToString();
		};
		return make_uniq<FarSource>(store, info.path, option("group"), option("catalog"));
	});
}

FarSession::FarSession(shared_ptr<FarStore> store_p, string catalog_p)
    : store(std::move(store_p)), catalog(std::move(catalog_p)), far(store->db) {
	far.BeginTransaction();
}

FarSession::~FarSession() {
	EndServing();
	if (far.context->transaction.HasActiveTransaction()) {
		far.Rollback();
	}
}

vector<string> FarSession::Tables(const string &schema) {
	lock_guard<mutex> guard(far_lock);
	return FarSource::TablesOn(far, catalog, schema);
}

CrossingTable FarSession::Describe(const string &schema, const string &name) {
	lock_guard<mutex> guard(far_lock);
	lock_guard<mutex> store_guard(store->lock);
	return FarSource::DescribeOn(*store, far, catalog, schema, name);
}

void FarSession::EndServing() {
	if (serving.joinable()) {
		serving.join();
		lock_guard<mutex> guard(store->lock);
		store->link_seam_rows += link_pair->SeamRows();
	}
}

optional_ptr<CrossingLink> FarSession::Link(ClientContext &) {
	if (store->transport != Transport::LINK) {
		return nullptr;
	}
	lock_guard<mutex> guard(far_lock);
	if (!link_pair) {
		link_pair = make_uniq<TestLinkPair>(store->read_partitions);
		CrossingFarOptions options;
		options.catalogs = store->catalog_check;
		options.transaction_open = true;
		far_side = make_uniq<CrossingFar>(far, catalog, options);
		serving = std::thread([this]() {
			try {
				far_side->Serve(link_pair->Far());
			} catch (...) {
			}
		});
	}
	return link_pair->Near();
}

void FarSession::Planned(const CrossingQuery &query) {
	auto call = Record(query);
	lock_guard<mutex> guard(store->lock);
	if (WriteOf(query.plan)) {
		store->writes.push_back(std::move(call));
	} else {
		store->reads.push_back(std::move(call));
	}
}

void FarSession::Commit() {
	EndServing();
	if (far.context->transaction.HasActiveTransaction()) {
		far.Commit();
	}
	lock_guard<mutex> guard(store->lock);
	store->transaction_ends.push_back("commit");
}

void FarSession::Rollback() {
	EndServing();
	if (far.context->transaction.HasActiveTransaction()) {
		far.Rollback();
	}
	lock_guard<mutex> guard(store->lock);
	store->transaction_ends.push_back("rollback");
}

void FarSession::Ddl(ClientContext &, const CrossingDdl &ddl) {
	unique_ptr<SQLStatement> statement;
	switch (ddl.verb) {
	case CrossingVerb::CREATE: {
		auto create = make_uniq<CreateStatement>();
		if (ddl.plan) {
			create->info = ReceiveView(ddl.create->Cast<CreateViewInfo>(), *ddl.plan);
		} else {
			create->info = ddl.create->Copy();
			create->info->SetCatalog(Identifier(catalog));
		}
		statement = std::move(create);
		break;
	}
	case CrossingVerb::ALTER: {
		auto alter = make_uniq<AlterStatement>();
		alter->info = ddl.alter->Copy();
		auto &altered = alter->info->GetQualifiedName();
		alter->info->SetQualifiedName(Identifier(catalog), altered.Schema(), altered.Name());
		statement = std::move(alter);
		break;
	}
	case CrossingVerb::DROP: {
		auto drop = make_uniq<DropStatement>();
		drop->info = ddl.drop->Copy();
		drop->info->SetCatalog(Identifier(catalog));
		statement = std::move(drop);
		break;
	}
	default:
		throw InternalException("far: '%s' is not a DDL verb", CrossingVerbName(ddl.verb));
	}
	lock_guard<mutex> guard(far_lock);
	far.context->Query(std::move(statement), QueryParameters())->ThrowIfError();
	lock_guard<mutex> store_guard(store->lock);
	store->ddls.push_back(FarDdl {ddl.verb, ddl.schema, ddl.table});
}

// Over Substrait whatever the transport: a native plan cannot be turned back into a SelectStatement.
unique_ptr<CreateViewInfo> FarSession::ReceiveView(const CreateViewInfo &view, const LogicalOperator &plan) {
	lock_guard<mutex> guard(far_lock);
	auto received = make_uniq<CreateViewInfo>(
	    QualifiedName(Identifier(catalog), view.GetQualifiedName().Schema(), view.GetViewName()));
	received->query = DecodeSubstraitView(far, catalog, RenderSubstraitJson(plan), CrossingViewColumnNames(view),
	                                      store->catalog_check);
	received->on_conflict = view.on_conflict;
	return received;
}

FarCall FarSession::Record(const CrossingQuery &query) {
	FarCall call;
	call.kind = CrossingVerb::SELECT;
	call.tables = query.tables;
	call.ordered = query.ordered;
	auto node = WriteOf(query.plan);
	if (!node) {
		call.plan_text = query.plan.ToString();
		CollectOperators(query.plan, call.operators);
		call.types = query.types;
		return call;
	}
	call.kind = node->verb;
	call.written.schema = node->schema;
	call.written.table = node->table;
	call.key_columns = node->key_columns;
	call.set_columns = node->set_columns;
	call.returning_columns = node->returning_columns;
	if (!query.plan.children.empty()) {
		auto &feed = *query.plan.children[0];
		call.plan_text = feed.ToString();
		CollectOperators(feed, call.operators);
		call.types = feed.types;
	}
	return call;
}

vector<vector<Value>> FarSession::EvaluateNative(const LogicalOperator &plan, FarCall &call) {
	call.wire = SerializeCrossingPlan(plan);
	auto received = DeserializeCrossingPlan(*far.context, call.wire);
	call.received_text = received->ToString();
	BindFloors(*far.context, received, catalog);
	return RowsOf(*RunPlan(far, std::move(received)));
}

vector<vector<Value>> FarSession::EvaluateSubstrait(const LogicalOperator &plan, FarCall &call) {
	call.wire = RenderSubstraitJson(plan);
	auto rel = DecodeSubstraitJson(far, catalog, call.wire, store->catalog_check);
	call.received_text = rel->ToString();
	return RowsOf(*rel->Execute());
}

vector<vector<Value>> FarSession::Evaluate(const LogicalOperator &plan, FarCall &call) {
	if (auto rows = SeamRowsOf(plan)) {
		call.rows_only = true;
		return RowsOf(*rows);
	}
	lock_guard<mutex> guard(far_lock);
	if (store->transport == Transport::NATIVE) {
		return EvaluateNative(plan, call);
	}
	return EvaluateSubstrait(plan, call);
}

CrossingScan FarSession::Run(ClientContext &context, const CrossingQuery &query) {
	return WriteOf(query.plan) ? Write(query) : Read(context, query);
}

CrossingScan FarSession::Read(ClientContext &context, const CrossingQuery &query) {
	auto call = Record(query);
	call.target_swap_bytes = BufferManager::GetBufferManager(context).GetUsedSwap();
	auto rows = make_shared_ptr<const vector<vector<Value>>>(Evaluate(query.plan, call));
	call.rows = *rows;
	lock_guard<mutex> guard(store->lock);
	store->reads.push_back(std::move(call));
	auto batch_indexed = store->batch_block_rows > 0 || store->round_robin_batch_rows > 0;
	auto partitions = query.ordered && !store->partition_ordered_reads && !batch_indexed ? 1 : store->read_partitions;
	auto store_ref = store;
	auto next_block = make_shared_ptr<idx_t>(0);
	CrossingScan scan;
	scan.partitions = partitions;
	scan.batch_indexed = batch_indexed;
	scan.open = [=](ClientContext &, idx_t partition) {
		return FarReader(rows, partitions, partition, store_ref, next_block);
	};
	return scan;
}

CrossingScan FarSession::Write(const CrossingQuery &query) {
	CrossingScan scan;
	scan.open = [this, &query](ClientContext &, idx_t) -> CrossingReader {
		auto started = make_shared_ptr<bool>(false);
		auto rows = make_shared_ptr<vector<vector<Value>>>();
		auto cursor = make_shared_ptr<idx_t>(0);
		return [this, &query, started, rows, cursor](ClientContext &context, DataChunk &chunk, CrossingWaker waker) {
			if (!*started) {
				{
					lock_guard<mutex> guard(store->lock);
					if (store->wait_before_write && !store->write_arrived) {
						auto raw = store.get();
						ArriveLater(*raw, std::move(waker), [raw]() {
							raw->write_waits_served++;
							raw->write_arrived = true;
						});
						return CrossingReadResult::Wait();
					}
					store->write_arrived = false;
				}
				auto call = Record(query);
				call.target_swap_bytes = BufferManager::GetBufferManager(context).GetUsedSwap();
				call.rows =
				    query.plan.children.empty() ? vector<vector<Value>>() : Evaluate(*query.plan.children[0], call);
				{
					lock_guard<mutex> guard(store->lock);
					store->writes.push_back(std::move(call));
				}
				lock_guard<mutex> guard(far_lock);
				*rows = Apply(query);
				*started = true;
			}
			if (*cursor >= rows->size()) {
				lock_guard<mutex> guard(store->lock);
				store->writes.back().target_swap_bytes_after_return =
				    BufferManager::GetBufferManager(context).GetUsedSwap();
				return CrossingReadResult::Done();
			}
			auto count = MinValue<idx_t>(rows->size() - *cursor, STANDARD_VECTOR_SIZE);
			CopyBlock(*rows, *cursor, count, chunk);
			*cursor += count;
			return CrossingReadResult::Rows();
		};
	};
	return scan;
}

vector<vector<Value>> FarSession::Apply(const CrossingQuery &query) {
	auto target = CrossingWriteTargetOf(catalog, *WriteOf(query.plan));
	auto submit = [&]() -> unique_ptr<QueryResult> {
		if (query.plan.children.empty()) {
			return SubmitWrite(*far.context, target, unique_ptr<TableRef>());
		}
		auto &feed = *query.plan.children[0];
		if (auto rows = SeamRowsOf(feed)) {
			return SubmitWrite(*far.context, target, SeamRefOfRows(*rows));
		}
		if (store->transport == Transport::NATIVE) {
			auto received = DeserializeCrossingPlan(*far.context, SerializeCrossingPlan(query.plan));
			auto plan = std::move(received->children[0]);
			BindFloors(*far.context, plan, catalog);
			return SubmitWrite(*far.context, target, std::move(plan));
		}
		auto rel = DecodeSubstraitJson(far, catalog, RenderSubstraitJson(feed), store->catalog_check);
		return SubmitWrite(*far.context, target, rel->GetTableRef());
	};
	auto result = submit();
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
	return rows;
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
