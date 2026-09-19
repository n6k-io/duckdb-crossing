#pragma once

#include "crossing.hpp"

#include "duckdb.hpp"

namespace readonly {

using namespace duckdb;

struct ReadOnlySource {
	struct Session {
		CrossingScan Read(ClientContext &, const CrossingQuery &);
	};

	vector<string> Tables(const string &);
	CrossingTable Describe(const string &, const string &name);
	CrossingPlan Plan(const CrossingPlanRequest &request);
	unique_ptr<Session> Begin(ClientContext &);

	static void Register(ExtensionLoader &loader);
};

} // namespace readonly
