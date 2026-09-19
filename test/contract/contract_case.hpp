#pragma once

#include "crossing.hpp"

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct GoodSession {
	CrossingScan Read(ClientContext &, const CrossingQuery &) {
		return {};
	}
};

struct GoodSource {
	using Session = GoodSession;
	vector<string> Tables(const string &) {
		return {};
	}
	CrossingTable Describe(const string &, const string &) {
		return {};
	}
	CrossingPlan Plan(const CrossingPlanRequest &) {
		return {};
	}
	unique_ptr<Session> Begin(ClientContext &) {
		return nullptr;
	}
};

} // namespace duckdb

#define CROSSING_CONTRACT_SOURCE(S)  static_assert(sizeof(duckdb::CrossingSourceAdapter<S>) > 0, "")
#define CROSSING_CONTRACT_SESSION(S) static_assert(sizeof(duckdb::CrossingSessionAdapter<S>) > 0, "")
