#include "contract_case.hpp"

namespace duckdb {

struct Bad {
	using Session = GoodSession;
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

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
