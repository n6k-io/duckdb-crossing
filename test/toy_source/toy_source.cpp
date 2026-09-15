#include "toy_source/toy_source.hpp"
#include "internal/plan_wire.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

namespace {

void ArriveLater(ToyStore &store, CrossingWaker waker, std::function<void()> arrive) {
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

CrossingReader ToyReader(shared_ptr<const vector<vector<Value>>> rows, idx_t partitions, idx_t partition,
                         shared_ptr<ToyStore> store) {
	idx_t cursor = 0;
	return [=](ClientContext &, DataChunk &chunk, CrossingWaker waker) mutable {
		{
			lock_guard<mutex> guard(store->lock);
			if (store->wait_before_chunks && store->arrived.erase(partition) == 0) {
				auto raw = store.get();
				ArriveLater(*raw, std::move(waker), [raw, partition]() {
					raw->waits_served++;
					raw->arrived.insert(partition);
				});
				return CrossingPull::Wait();
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
			return CrossingPull::Done();
		}
		lock_guard<mutex> guard(store->lock);
		store->partitions_read.insert(partition);
		return CrossingPull::Rows();
	};
}

CrossingScan ToyScan(vector<vector<Value>> rows_p, idx_t partitions, shared_ptr<ToyStore> store) {
	auto rows = make_shared_ptr<const vector<vector<Value>>>(std::move(rows_p));
	CrossingScan scan;
	scan.partitions = partitions;
	scan.open = [=](ClientContext &, idx_t partition) {
		return ToyReader(rows, partitions, partition, store);
	};
	return scan;
}

vector<idx_t> PositionsOf(const ToyTable &table, const vector<string> &columns) {
	vector<idx_t> positions;
	for (auto &column : columns) {
		auto it = std::find(table.column_names.begin(), table.column_names.end(), column);
		if (it == table.column_names.end()) {
			throw InternalException("toydb: no column '%s'", column);
		}
		positions.push_back(NumericCast<idx_t>(it - table.column_names.begin()));
	}
	return positions;
}

bool KeyMatches(const vector<Value> &row, const vector<idx_t> &key_positions, const vector<Value> &seam_row) {
	for (idx_t k = 0; k < key_positions.size(); k++) {
		if (row[key_positions[k]] != seam_row[k]) {
			return false;
		}
	}
	return true;
}

} // namespace

ToySource::ToySource(shared_ptr<ToyStore> store_p, string path_p) : store(std::move(store_p)), path(std::move(path_p)) {
	lock_guard<mutex> guard(store->lock);
	store->attached_paths.push_back(path);
}

void ToySource::Detach(ClientContext &) {
	lock_guard<mutex> guard(store->lock);
	store->detached_paths.push_back(path);
}

ToyTable &ToyTableNamed(ToyStore &store, const string &name) {
	auto it = store.tables.find(name);
	if (it == store.tables.end()) {
		throw CatalogException("toydb: no table '%s'", name);
	}
	return it->second;
}

vector<string> ToySource::Schemas() {
	return {"main", "aux"};
}

vector<string> ToySource::Tables(const string &schema) {
	vector<string> names;
	lock_guard<mutex> guard(store->lock);
	store->listings[schema]++;
	if (schema != "main") {
		return names;
	}
	for (auto &entry : store->tables) {
		names.push_back(entry.first);
	}
	std::sort(names.begin(), names.end());
	return names;
}

CrossingTable ToySource::Describe(const string &schema, const string &name) {
	lock_guard<mutex> guard(store->lock);
	auto &toy = ToyTableNamed(*store, name);
	CrossingTable table;
	table.name = name;
	table.column_names = toy.column_names;
	table.column_types = toy.column_types;
	table.key = toy.key;
	table.key_unique = !toy.key.empty();
	table.verbs = {CrossingVerb::SELECT, CrossingVerb::INSERT, CrossingVerb::UPDATE, CrossingVerb::DELETE_};
	return table;
}

CrossingPlan ToySource::Plan(const CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		lock_guard<mutex> guard(store->lock);
		auto &toy = ToyTableNamed(*store, request.table);
		return CrossingPlan::Of(MakeFloorNode(0, request.schema, request.table, toy.column_names, toy.column_types));
	}
	return CrossingPlan::Of(MakeSeamNode(0, request.seam.types));
}

unique_ptr<CrossingSession> ToySource::Begin(ClientContext &) {
	return make_uniq<ToySession>(store);
}

ToySession::ToySession(shared_ptr<ToyStore> store_p) : store(std::move(store_p)) {
}

void ToySession::Commit() {
	lock_guard<mutex> guard(store->lock);
	store->transaction_ends.push_back("commit");
}

void ToySession::Rollback() {
	lock_guard<mutex> guard(store->lock);
	store->transaction_ends.push_back("rollback");
}

vector<vector<Value>> ToySession::Render(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto floor = FloorOf(op);
		if (!floor) {
			break;
		}
		auto &get = op.Cast<LogicalGet>();
		auto &toy = ToyTableNamed(*store, floor->table);
		vector<vector<Value>> out;
		for (auto &row : toy.rows) {
			vector<Value> projected;
			for (auto &column : get.GetColumnIds()) {
				projected.push_back(row[column.GetPrimaryIndex()]);
			}
			out.push_back(std::move(projected));
		}
		return out;
	}
	case LogicalOperatorType::LOGICAL_CHUNK_GET: {
		auto rows = SeamRowsOf(op);
		if (!rows) {
			break;
		}
		vector<vector<Value>> out;
		for (auto &chunk : rows->Chunks()) {
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
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto rows = Render(*op.children[0]);
		auto bindings = op.children[0]->GetColumnBindings();
		vector<idx_t> positions;
		for (auto &expr : op.expressions) {
			if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				throw NotImplementedException("toydb: cannot render %s", expr->ToString());
			}
			auto &binding = expr->Cast<BoundColumnRefExpression>().binding;
			auto it = std::find(bindings.begin(), bindings.end(), binding);
			if (it == bindings.end()) {
				throw InternalException("toydb: %s names nothing below it", expr->ToString());
			}
			positions.push_back(NumericCast<idx_t>(it - bindings.begin()));
		}
		vector<vector<Value>> out;
		for (auto &row : rows) {
			vector<Value> projected;
			for (auto position : positions) {
				projected.push_back(row[position]);
			}
			out.push_back(std::move(projected));
		}
		return out;
	}
	default:
		break;
	}
	throw NotImplementedException("toydb: cannot render a %s", LogicalOperatorToString(op.type));
}

vector<vector<Value>> ToySession::RenderShipped(const CrossingQuery &query) {
	if (!store->ship_plans) {
		return Render(query.plan);
	}
	DuckDB far(nullptr);
	RegisterCrossingPlanFunctions(*far.instance);
	Connection con(far);
	con.BeginTransaction();
	auto received = DeserializeCrossingPlan(*con.context, SerializeCrossingPlan(query.plan));
	con.Rollback();
	store->plans_shipped++;
	return Render(*received);
}

CrossingScan ToySession::Read(ClientContext &, const CrossingQuery &query) {
	lock_guard<mutex> guard(store->lock);
	return ToyScan(RenderShipped(query), store->read_partitions, store);
}

CrossingWriter ToySession::Write(ClientContext &, const CrossingQuery &query) {
	return [this, &query](ClientContext &, CrossingWaker waker) {
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
		return CrossingWriteResult::Done(Apply(query));
	};
}

idx_t ToySession::Apply(const CrossingQuery &query) {
	auto rows = RenderShipped(query);
	auto &toy = ToyTableNamed(*store, query.tables[0].table);
	auto key_positions = PositionsOf(toy, query.key_columns);
	auto set_positions = PositionsOf(toy, query.set_columns);
	idx_t affected = 0;
	switch (query.kind) {
	case CrossingVerb::INSERT:
		for (auto &row : rows) {
			toy.rows.push_back(row);
			affected++;
		}
		break;
	case CrossingVerb::UPDATE:
		for (auto &seam_row : rows) {
			for (auto &row : toy.rows) {
				if (!KeyMatches(row, key_positions, seam_row)) {
					continue;
				}
				for (idx_t s = 0; s < set_positions.size(); s++) {
					row[set_positions[s]] = seam_row[key_positions.size() + s];
				}
				affected++;
			}
		}
		break;
	case CrossingVerb::DELETE_:
		for (auto &seam_row : rows) {
			auto before = toy.rows.size();
			toy.rows.erase(
			    std::remove_if(toy.rows.begin(), toy.rows.end(),
			                   [&](const vector<Value> &row) { return KeyMatches(row, key_positions, seam_row); }),
			    toy.rows.end());
			affected += before - toy.rows.size();
		}
		break;
	default:
		throw InternalException("toydb: cannot write a %s", CrossingVerbName(query.kind));
	}
	return affected;
}

void ToyStore::JoinArrivals() {
	vector<std::thread> pending;
	{
		lock_guard<mutex> guard(lock);
		pending.swap(arrivals);
	}
	for (auto &thread : pending) {
		thread.join();
	}
}

void ToySource::Register(ExtensionLoader &loader, shared_ptr<ToyStore> store) {
	CrossingSource::Register(
	    loader, "toydb", [store](ClientContext &, AttachInfo &info) { return make_uniq<ToySource>(store, info.path); });
}

} // namespace duckdb
