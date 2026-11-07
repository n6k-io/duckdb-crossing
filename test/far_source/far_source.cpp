#include "far_source/far_source.hpp"
#include "crossing_substrait.hpp"

#include "internal/control_frames.hpp"
#include "internal/far_plan.hpp"
#include "internal/stream_sink.hpp"

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
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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
	if (op.type == LogicalOperatorType::LOGICAL_GET &&
	    op.Cast<LogicalGet>().function.GetName() == CROSSING_SEAM_FUNCTION) {
		out.push_back(LogicalOperatorType::LOGICAL_CHUNK_GET);
		return;
	}
	out.push_back(op.type);
	for (auto &child : op.children) {
		CollectOperators(*child, out);
	}
}

vector<vector<Value>> RowsOfChunk(DataChunk &chunk) {
	vector<vector<Value>> out;
	for (idx_t r = 0; r < chunk.size(); r++) {
		vector<Value> row;
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			row.push_back(chunk.GetValue(c, r));
		}
		out.push_back(std::move(row));
	}
	return out;
}

FarDdl DdlOfFrame(ClientContext &context, const string &frame) {
	const string view(CROSSING_CONTROL_VIEW);
	if (frame.compare(0, view.size(), view) == 0) {
		return FarDdl {CrossingVerb::CREATE, StringUtil::Split(frame.substr(view.size()), '\n')[1]};
	}
	Parser parser(context);
	parser.ParseQuery(frame);
	auto &statement = *parser.statements[0];
	switch (statement.type) {
	case StatementType::CREATE_STATEMENT:
		return FarDdl {CrossingVerb::CREATE,
		               statement.Cast<CreateStatement>().info->GetQualifiedName().Name().GetIdentifierName()};
	case StatementType::ALTER_STATEMENT:
		return FarDdl {CrossingVerb::ALTER,
		               statement.Cast<AlterStatement>().info->GetQualifiedName().Name().GetIdentifierName()};
	default:
		return FarDdl {CrossingVerb::DROP,
		               statement.Cast<DropStatement>().info->GetQualifiedName().Name().GetIdentifierName()};
	}
}

bool HoldsSeam(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET &&
	    op.Cast<LogicalGet>().function.GetName() == CROSSING_SEAM_FUNCTION) {
		return true;
	}
	for (auto &child : op.children) {
		if (HoldsSeam(*child)) {
			return true;
		}
	}
	return false;
}

bool PlanHoldsOrder(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY || op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		return true;
	}
	for (auto &child : op.children) {
		if (PlanHoldsOrder(*child)) {
			return true;
		}
	}
	return false;
}

//! Near's end of a link to a far transaction this process serves on its own connection.
class FarLink : public CrossingLink {
public:
	FarLink(shared_ptr<FarStore> store_p, string catalog_p, string floor_catalog_p)
	    : store(std::move(store_p)), catalog(std::move(catalog_p)), floor_catalog(std::move(floor_catalog_p)),
	      pair(store->streams, store->capacity, store->far_streams), far(store->db) {
		pair.on_rows = [this](idx_t id, int side, DataChunk &rows) {
			Observe(id, side, rows);
		};
		pair.on_control = [this](const string &header) {
			end_word = header == CROSSING_CONTROL_COMMIT ? "commit" : end_word;
			if (header.compare(0, 5, CROSSING_CONTROL_PLAN) == 0 && header[5] == '1') {
				end_word = "commit";
			}
			if (header.compare(0, 4, CROSSING_CONTROL_DDL) == 0) {
				auto ddl = DdlOfFrame(*far.context, header.substr(4));
				lock_guard<mutex> guard(store->lock);
				store->ddls.push_back(std::move(ddl));
			}
		};
		CrossingFarOptions options;
		options.catalogs = store->catalog_check;
		options.floors = store->floors;
		far_side = make_uniq<CrossingFar>(far, catalog, options);
		serving = std::thread([this]() {
			try {
				far_side->Serve(pair.Far());
			} catch (...) {
			}
		});
	}

	~FarLink() override {
		serving.join();
		lock_guard<mutex> guard(store->lock);
		store->transaction_ends.push_back(end_word);
		store->seam_rows += pair.SeamRows();
		store->waits += pair.Waits();
		store->near_control_replies += pair.NearControlReplies();
	}

	idx_t Streams() override {
		return pair.Near().Streams();
	}
	unique_ptr<CrossingStream> Open(idx_t id) override {
		return pair.Near().Open(id);
	}

private:
	void Observe(idx_t id, int side, DataChunk &rows) {
		if (id == 0) {
			return;
		}
		auto slice = ((id - 1) >> 17);
		bool into_far = ((id - 1) >> 16) & 1;
		lock_guard<mutex> guard(store->lock);
		if (slice >= store->slices.size()) {
			return;
		}
		auto &entry = store->slices[slice];
		if (entry.first != into_far) {
			return;
		}
		auto &call = entry.first ? store->writes[entry.second] : store->reads[entry.second];
		for (auto &row : RowsOfChunk(rows)) {
			call.rows.push_back(std::move(row));
		}
	}

	shared_ptr<FarStore> store;
	string catalog;
	string floor_catalog;
	TestLinkPair pair;
	Connection far;
	unique_ptr<CrossingFar> far_side;
	std::thread serving;
	string end_word = "rollback";
};

} // namespace

bool FarCall::Has(LogicalOperatorType type) const {
	return Count(type) > 0;
}

idx_t FarCall::Count(LogicalOperatorType type) const {
	return NumericCast<idx_t>(std::count(operators.begin(), operators.end(), type));
}

bool FarCall::Mentions(const string &text) const {
	return plan_text.find(text) != string::npos;
}

FarStore::FarStore() : db(nullptr), con(db) {
	RegisterCrossingFarFunctions(*db.instance);
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

const void *FarSource::Group() const {
	return group ? static_cast<const void *>(group) : this;
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
                                    const string &name, const string &floor_catalog) {
	CrossingTable table;
	table.name = name;
	table.catalog = floor_catalog;
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
		table.row_ids = false;
		return table;
	}
	auto description = con.TableInfo(Identifier(catalog), Identifier(schema), Identifier(name));
	if (!description) {
		throw CatalogException("far: no table '%s.%s'", schema, name);
	}
	for (auto &column : description->columns) {
		table.Column(column.Name().GetIdentifierName(), column.Type());
	}
	vector<string> declared;
	auto key = store.keys.find(name);
	if (key != store.keys.end()) {
		declared = key->second;
	} else {
		auto prepared = con.Prepare("SELECT constraint_column_names FROM duckdb_constraints() WHERE database_name "
		                            "= ? AND schema_name = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'");
		vector<Value> args {Value(catalog), Value(schema), Value(name)};
		auto rows = RowsOf(*prepared->Execute(args));
		if (!rows.empty()) {
			for (auto &column : ListValue::GetChildren(rows[0][0])) {
				declared.push_back(column.GetValue<string>());
			}
		}
	}
	table.returning = store.returning;
	if (store.declare_key_constraints && !declared.empty()) {
		vector<Identifier> key_columns;
		for (auto &column : declared) {
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
	return DescribeOn(*store, store->con, catalog, schema, name, floor_catalog);
}

CrossingSchema FarSource::DescribeSchema(const string &schema) {
	lock_guard<mutex> guard(store->lock);
	CrossingSchema described;
	described.name = schema;
	auto verbs = store->schema_verbs.find(schema);
	described.verbs = verbs != store->schema_verbs.end() ? verbs->second : vector<CrossingVerb> {CrossingVerb::CREATE};
	return described;
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

unique_ptr<CrossingLink> FarSource::Connect(ClientContext &) {
	{
		lock_guard<mutex> guard(store->lock);
		store->links_connected++;
		store->slices.clear();
	}
	return make_uniq<FarLink>(store, catalog, floor_catalog);
}

void FarSource::DescribeChanged(const string &schema, CrossingTable &table) {
	CrossingSource::DescribeChanged(schema, table);
	table.catalog = floor_catalog;
	lock_guard<mutex> guard(store->lock);
	table.returning = store->returning;
	auto verbs = store->verbs.find(table.name);
	if (verbs != store->verbs.end()) {
		table.verbs = verbs->second;
	}
	auto key = store->keys.find(table.name);
	if (store->declare_key_constraints && key != store->keys.end()) {
		vector<Identifier> key_columns;
		for (auto &column : key->second) {
			key_columns.emplace_back(column);
		}
		table.constraints.push_back(make_uniq<UniqueConstraint>(std::move(key_columns), true));
	}
}

void FarSource::Planned(const LogicalOperator &plan) {
	FarCall call;
	call.kind = CrossingVerb::SELECT;
	call.tables = CrossingTablesOf(plan);
	call.ordered = PlanHoldsOrder(plan);
	auto node = WriteOf(plan);
	optional_ptr<const LogicalOperator> feed = &plan;
	if (node) {
		call.kind = node->verb;
		call.written.schema = node->schema;
		call.written.table = node->table;
		call.set_columns = node->set_columns;
		call.returning_columns = node->returning_columns;
		feed = plan.children.empty() ? nullptr : plan.children[0].get();
	}
	if (feed) {
		call.plan_text = feed->ToString();
		CollectOperators(*feed, call.operators);
		call.types = feed->types;
		call.rows_only = node && feed->type == LogicalOperatorType::LOGICAL_GET &&
		                 feed->Cast<LogicalGet>().function.GetName() == CROSSING_SEAM_FUNCTION;
		if (!HoldsSeam(*feed)) {
			call.wire = RenderSubstraitJson(*feed);
		} else {
			SubstraitSlice slice;
			slice.source = feed;
			call.wire = RenderSubstraitSlices({slice});
		}
	}
	lock_guard<mutex> guard(store->lock);
	auto &calls = node ? store->writes : store->reads;
	store->slices.emplace_back(node != nullptr, calls.size());
	calls.push_back(std::move(call));
}

void FarSource::Register(ExtensionLoader &loader, shared_ptr<FarStore> store) {
	RegisterCrossing(loader, "fardb", [store](ClientContext &, AttachInfo &info) -> unique_ptr<CrossingSource> {
		auto option = [&info](const string &name) {
			auto found = info.options.find(name);
			return found == info.options.end() ? string() : found->second.ToString();
		};
		return make_uniq<FarSource>(store, info.path, option("group"), option("catalog"));
	});
}

} // namespace duckdb

namespace other {

static const duckdb::CrossingIdentity &OtherIdentityOf() {
	static const duckdb::CrossingIdentity identity;
	return identity;
}

const duckdb::CrossingIdentity &OtherSource::Identity() const {
	return OtherIdentityOf();
}

void OtherSource::Register(duckdb::ExtensionLoader &loader, duckdb::shared_ptr<duckdb::FarStore> store) {
	duckdb::RegisterCrossing(
	    loader, "otherdb",
	    [store](duckdb::ClientContext &, duckdb::AttachInfo &info) -> duckdb::unique_ptr<duckdb::CrossingSource> {
		    return duckdb::make_uniq<OtherSource>(store, info.path);
	    },
	    OtherIdentityOf());
}

} // namespace other
