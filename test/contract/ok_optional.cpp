#include "contract_case.hpp"

namespace duckdb {

struct FullSession {
	CrossingScan Read(ClientContext &, const CrossingQuery &) {
		return {};
	}
	CrossingWriter Write(ClientContext &, const CrossingQuery &) {
		return {};
	}
	void Ddl(ClientContext &, const CrossingDdl &) {
	}
	void Commit() {
	}
	void Rollback() {
	}
};

struct FullSource {
	using Session = FullSession;
	vector<string> Schemas() {
		return {};
	}
	const vector<string> &Tables(const string &) {
		static vector<string> none;
		return none;
	}
	CrossingTable Describe(const string &, const string &) {
		return {};
	}
	CrossingSchema DescribeSchema(const string &) {
		return {};
	}
	CrossingPlan Plan(const CrossingPlanRequest &) {
		return {};
	}
	CrossingVerdict AcceptsCall(const Expression &) {
		return {};
	}
	CrossingVerdict AcceptsType(const LogicalType &) {
		return {};
	}
	CrossingVerdict AcceptsOperator(const LogicalOperator &) {
		return {};
	}
	unique_ptr<Session> Begin(ClientContext &) {
		return nullptr;
	}
	void Detach(ClientContext &) {
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::FullSource);
CROSSING_CONTRACT_SESSION(duckdb::FullSession);
