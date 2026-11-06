#include "framework/twin.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

namespace {

constexpr const char *NEAR_NODE = "near_node";
constexpr const char *NEAR_NODE_BARE = "near_node_bare";

struct NearNodeRecord : public OptimizerExtensionInfo {
	idx_t nodes = 0;
	idx_t calls = 0;
	idx_t seam_rows = 0;
	idx_t seam_width = 0;
	bool filter_above = false;
	string cross_left;
};

class LogicalPassThrough : public LogicalExtensionOperator {
public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}
	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &) override {
		throw InternalException("a pass-through outlived the optimizers");
	}
	string GetName() const override {
		return "PASS_THROUGH";
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

struct NearNodeSinkState : public GlobalSinkState {
	unique_ptr<ColumnDataCollection> rows;
};

struct NearNodeSourceState : public GlobalSourceState {
	ColumnDataScanState scan;
	bool scanning = false;
};

class PhysicalNearNode : public PhysicalOperator {
public:
	PhysicalNearNode(PhysicalPlan &plan, vector<LogicalType> types, vector<optional_idx> emit_p,
	                 NearNodeRecord &record_p, idx_t cardinality)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), cardinality),
	      emit(std::move(emit_p)), record(record_p) {
	}

	vector<optional_idx> emit;
	NearNodeRecord &record;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		auto state = make_uniq<NearNodeSinkState>();
		state->rows = make_uniq<ColumnDataCollection>(context, children[0].get().GetTypes());
		return std::move(state);
	}
	SinkResultType Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const override {
		input.global_state.Cast<NearNodeSinkState>().rows->Append(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &, OperatorSinkFinalizeInput &input) const override {
		auto &rows = *input.global_state.Cast<NearNodeSinkState>().rows;
		record.calls++;
		record.seam_rows = rows.Count();
		record.seam_width = rows.ColumnCount();
		return SinkFinalizeType::READY;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &) const override {
		return make_uniq<NearNodeSourceState>();
	}
	SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk, OperatorSourceInput &input) const override {
		auto &rows = *sink_state->Cast<NearNodeSinkState>().rows;
		auto &state = input.global_state.Cast<NearNodeSourceState>();
		if (!state.scanning) {
			rows.InitializeScan(state.scan);
			state.scanning = true;
		}
		DataChunk scanned;
		rows.InitializeScanChunk(scanned);
		rows.Scan(state.scan, scanned);
		if (scanned.size() == 0) {
			return SourceResultType::FINISHED;
		}
		chunk.SetChildCardinality(scanned.size());
		for (idx_t c = 0; c < emit.size(); c++) {
			if (emit[c].IsValid()) {
				VectorOperations::Copy(scanned.data[emit[c].GetIndex()], chunk.data[c], scanned.size(), 0, 0);
				continue;
			}
			for (idx_t r = 0; r < scanned.size(); r++) {
				chunk.data[c].SetValue(r, Value(types[c]));
			}
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	bool IsSource() const override {
		return true;
	}
	string GetName() const override {
		return "NEAR_NODE";
	}
};

class LogicalNearNode : public LogicalExtensionOperator {
public:
	LogicalNearNode(vector<ColumnBinding> bindings_p, vector<LogicalType> output_types, vector<optional_idx> emit_p,
	                NearNodeRecord &record_p)
	    : bindings(std::move(bindings_p)), output_types(std::move(output_types)), emit(std::move(emit_p)),
	      record(record_p) {
	}

	vector<ColumnBinding> bindings;
	vector<LogicalType> output_types;
	vector<optional_idx> emit;
	NearNodeRecord &record;

	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}
	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		auto &node = planner.Make<PhysicalNearNode>(output_types, emit, record, estimated_cardinality);
		node.children.push_back(child);
		return node;
	}
	string GetName() const override {
		return "NEAR_NODE";
	}

protected:
	void ResolveTypes() override {
		types = output_types;
	}
};

NearNodeRecord &RecordOf(OptimizerExtensionInput &input) {
	return static_cast<NearNodeRecord &>(*input.info);
}

bool IsNearNodeGet(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &name = op.Cast<LogicalGet>().function.GetName();
	return name == NEAR_NODE || name == NEAR_NODE_BARE;
}

unique_ptr<LogicalOperator> InSeamOrder(unique_ptr<LogicalOperator> rows, Binder &binder) {
	rows->ResolveOperatorTypes();
	auto bindings = rows->GetColumnBindings();
	vector<unique_ptr<Expression>> columns;
	for (idx_t i = 0; i < bindings.size(); i++) {
		columns.push_back(make_uniq<BoundColumnRefExpression>(rows->types[i], bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(columns));
	projection->children.push_back(std::move(rows));
	return std::move(projection);
}

void WrapFeeds(LogicalOperator &op, Binder &binder) {
	for (auto &child : op.children) {
		WrapFeeds(*child, binder);
	}
	if (!IsNearNodeGet(op)) {
		return;
	}
	auto ordered = InSeamOrder(std::move(op.children[0]), binder);
	if (op.Cast<LogicalGet>().function.GetName() == NEAR_NODE_BARE) {
		op.children[0] = std::move(ordered);
		return;
	}
	auto pass_through = make_uniq<LogicalPassThrough>();
	pass_through->children.push_back(std::move(ordered));
	op.children[0] = std::move(pass_through);
}

void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	WrapFeeds(*plan, input.optimizer.binder);
}

string FirstTableUnder(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto table = op.Cast<LogicalGet>().GetTable();
		if (table) {
			return table->name.GetIdentifierName();
		}
	}
	for (auto &child : op.children) {
		auto found = FirstTableUnder(*child);
		if (!found.empty()) {
			return found;
		}
	}
	return string();
}

void SwapNearNodes(unique_ptr<LogicalOperator> &op, NearNodeRecord &record) {
	for (auto &child : op->children) {
		SwapNearNodes(child, record);
	}
	if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		record.cross_left = FirstTableUnder(*op->children[0]);
	}
	if (op->type == LogicalOperatorType::LOGICAL_FILTER && op->children[0]->GetName() == "NEAR_NODE") {
		record.filter_above = true;
	}
	if (!IsNearNodeGet(*op)) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	get.ResolveOperatorTypes();
	auto feed = std::move(get.children[0]);
	if (feed->GetName() == "PASS_THROUGH") {
		feed = std::move(feed->children[0]);
	}

	vector<optional_idx> emit;
	auto &ids = get.GetColumnIds();
	auto add = [&](const ColumnIndex &id) {
		emit.push_back(id.IsVirtualColumn() ? optional_idx() : optional_idx(id.GetPrimaryIndex()));
	};
	if (get.projection_ids.empty()) {
		for (auto &id : ids) {
			add(id);
		}
	} else {
		for (auto position : get.projection_ids) {
			add(ids[position.GetIndex()]);
		}
	}
	auto node = make_uniq<LogicalNearNode>(get.GetColumnBindings(), get.types, std::move(emit), record);
	node->children.push_back(std::move(feed));
	record.nodes++;
	op = std::move(node);
}

void PostOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	SwapNearNodes(plan, RecordOf(input));
}

unique_ptr<FunctionData> NearNodeBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                      vector<Identifier> &names) {
	return_types = input.input_table_types;
	for (auto &name : input.input_table_names) {
		names.push_back(name);
	}
	return make_uniq<TableFunctionData>();
}

OperatorResultType NearNodeInOut(ExecutionContext &, TableFunctionInput &, DataChunk &, DataChunk &) {
	throw InternalException("a near node outlived the optimizers as a table function");
}

struct NearDb {
	DuckDB db;
	Connection con;
	shared_ptr<NearNodeRecord> record;

	NearDb() : db(nullptr), con(db), record(make_shared_ptr<NearNodeRecord>()) {
		auto &config = DBConfig::GetConfig(*db.instance);
		OptimizerExtension pass;
		pass.pre_optimize_function = PreOptimize;
		pass.optimize_function = PostOptimize;
		pass.optimizer_info = record;
		OptimizerExtension::Register(config, pass);

		auto &system = Catalog::GetSystemCatalog(*db.instance);
		auto transaction = CatalogTransaction::GetSystemTransaction(*db.instance);
		for (auto name : {NEAR_NODE, NEAR_NODE_BARE}) {
			TableFunction function(name, FunctionSignature().AddPositionalOnly("input", LogicalType::TABLE), nullptr,
			                       NearNodeBind);
			function.in_out_function = NearNodeInOut;
			CreateTableFunctionInfo info(function);
			system.CreateFunction(transaction, info);
		}
		Run("CREATE TABLE small AS SELECT i::INTEGER AS x FROM range(1, 3) r(i)");
		Run("CREATE TABLE big AS SELECT i::INTEGER AS y FROM range(1000, 2000) r(i)");
		Run("CREATE TABLE t AS SELECT i::INTEGER AS a, (i * 10)::INTEGER AS b FROM range(1, 6) r(i)");
	}

	unique_ptr<TestResult> Run(const string &sql) {
		*record = NearNodeRecord();
		auto result = Collected(con.Query(sql));
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		return result;
	}
};

void RequireSmallThenBig(TestResult &result) {
	REQUIRE(result.RowCount() == 2000);
	for (idx_t r = 0; r < result.RowCount(); r++) {
		auto x = result.GetValue(0, r).GetValue<int32_t>();
		auto y = result.GetValue(1, r).GetValue<int32_t>();
		if (x < 1 || x > 2 || y < 1000 || y >= 2000) {
			FAIL("row " << r << " holds x=" << x << " y=" << y);
		}
	}
}

} // namespace

TEST_CASE("a near node keeps its seam's column order when a cross product flips", "[near_node]") {
	NearDb near;

	auto small_first = near.Run("SELECT * FROM near_node((SELECT a.x, b.y FROM small a, big b))");
	auto left_when_small_first = near.record->cross_left;
	RequireSmallThenBig(*small_first);

	auto big_first = near.Run("SELECT * FROM near_node((SELECT a.x, b.y FROM big b, small a))");
	auto left_when_big_first = near.record->cross_left;
	RequireSmallThenBig(*big_first);

	INFO("left when small first: " << left_when_small_first << ", when big first: " << left_when_big_first);
	REQUIRE((left_when_small_first != "small" || left_when_big_first != "big"));
}

TEST_CASE("a near node gets every seam column when what is above it reads fewer", "[near_node]") {
	NearDb near;

	auto result = near.Run("SELECT b FROM near_node((SELECT a, b FROM t)) ORDER BY b");

	REQUIRE(result->RowCount() == 5);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(10));
	REQUIRE(result->GetValue(0, 4) == Value::INTEGER(50));
	REQUIRE(near.record->seam_width == 2);
}

TEST_CASE("a near node gets every seam column when nothing above it reads one", "[near_node]") {
	NearDb near;

	auto result = near.Run("SELECT count(*) FROM near_node((SELECT a, b FROM t))");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(5));
	REQUIRE(near.record->seam_width == 2);
	REQUIRE(near.record->seam_rows == 5);
}

TEST_CASE("a near node over a feed that optimizes to empty still runs", "[near_node]") {
	auto sql = GENERATE(as<string> {}, "SELECT count(*) FROM near_node((SELECT t.a FROM t, (SELECT 1 WHERE false) e))",
	                    "SELECT * FROM near_node((SELECT a FROM t WHERE false))");
	INFO(sql);
	NearDb near;

	near.Run(sql);

	REQUIRE(near.record->nodes == 1);
	REQUIRE(near.record->calls == 1);
	REQUIRE(near.record->seam_rows == 0);
}

TEST_CASE("a near node over an empty feed answers as the query would", "[near_node]") {
	NearDb near;

	auto counted = near.Run("SELECT count(*) FROM near_node((SELECT t.a FROM t, (SELECT 1 WHERE false) e))");
	REQUIRE(counted->GetValue(0, 0) == Value::BIGINT(0));

	auto rows = near.Run("SELECT * FROM near_node((SELECT a FROM t WHERE false))");
	REQUIRE(rows->RowCount() == 0);

	auto one_row = near.Run("SELECT * FROM near_node((SELECT count(*) AS c FROM t, (SELECT 1 WHERE false) e))");
	REQUIRE(one_row->RowCount() == 1);
	REQUIRE(one_row->GetValue(0, 0) == Value::BIGINT(0));
}

TEST_CASE("an insert over a near node with an empty feed still runs it", "[near_node]") {
	NearDb near;
	near.Run("CREATE TABLE out_t(a INTEGER)");

	auto result = near.Run("INSERT INTO out_t SELECT * FROM near_node((SELECT a FROM t WHERE false))");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE(near.record->nodes == 1);
	REQUIRE(near.record->calls == 1);
}

TEST_CASE("without the pass-through an empty feed takes the whole near node with it", "[near_node]") {
	NearDb near;

	near.Run("SELECT * FROM near_node_bare((SELECT a FROM t WHERE false))");

	REQUIRE(near.record->nodes == 0);
	REQUIRE(near.record->calls == 0);
}

TEST_CASE("a filter above a near node stays above it", "[near_node]") {
	NearDb near;

	auto result = near.Run("SELECT a FROM near_node((SELECT a, b FROM t)) WHERE b > 30 ORDER BY a");

	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(4));
	REQUIRE(near.record->filter_above);
	REQUIRE(near.record->seam_rows == 5);
}

TEST_CASE("a near node keeps the order of an ordered feed", "[near_node]") {
	NearDb near;
	near.Run("PRAGMA threads=4");

	auto result = near.Run("SELECT * FROM near_node((SELECT y FROM big ORDER BY y DESC))");

	REQUIRE(result->RowCount() == 1000);
	for (idx_t r = 0; r < result->RowCount(); r++) {
		if (result->GetValue(0, r) != Value::INTEGER(NumericCast<int32_t>(1999 - r))) {
			FAIL("row " << r << " out of order");
		}
	}
}

TEST_CASE("each merge action runs through a near node and returns what the source stored", "[near_node]") {
	Twin twin;
	twin.store->keys["m"] = {"id"};
	twin.Far("CREATE TABLE m(id INTEGER, v INTEGER)");
	twin.Far("INSERT INTO m VALUES (1, 10), (2, 20)");
	twin.Attach();

	auto result = twin.Query("MERGE INTO far.m t USING (VALUES (1, 11), (2, 0), (3, 33)) s(id, v) ON t.id = s.id "
	                         "WHEN MATCHED AND s.v = 0 THEN DELETE "
	                         "WHEN MATCHED THEN UPDATE SET v = s.v "
	                         "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v) "
	                         "RETURNING id, v, merge_action");

	REQUIRE(result->RowCount() == 3);
	map<int32_t, string> actions;
	for (idx_t r = 0; r < result->RowCount(); r++) {
		actions[result->GetValue(0, r).GetValue<int32_t>()] = result->GetValue(2, r).ToString();
	}
	REQUIRE(actions[1] == "UPDATE");
	REQUIRE(actions[2] == "DELETE");
	REQUIRE(actions[3] == "INSERT");
}
