#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/sql_statement.hpp"

#include "crossing_link.hpp"
#include "crossing_substrait.hpp"

namespace duckdb {

class CrossingFailure;
class CrossingSeamInbox;

struct CrossingFarOptions {
	//! What stands for a table a plan reads, such as a subquery with a row filter.
	CrossingFloorResolver floors;
	//! Called with each catalog a plan names; throws to refuse the plan.
	SubstraitCatalogCheck catalogs;
	//! Called with each INSERT, UPDATE, DELETE or MERGE before it binds. It may change the statement, such as to
	//! check the values written, or throw to refuse it. The target is aliased `crossing_target`, except on an INSERT,
	//! and the rows written from are `crossing_source`, with columns c0, c1, ...
	std::function<void(SQLStatement &)> writes;
	//! Called with each CREATE, ALTER or DROP before it runs, CREATE TABLE AS included. It may change the statement
	//! or throw to refuse it.
	std::function<void(SQLStatement &)> ddl;
};

//! The far side of a link: everything but moving its frames. Serve holds one transaction on `connection`
//! until the near side ends it or closes the link, and returns then.
class CrossingFar {
public:
	CrossingFar(Connection &connection, string catalog, CrossingFarOptions options = CrossingFarOptions());

	void Serve(CrossingLink &link);
	//! Ends what Serve is doing: its statement fails with `reason`, the transaction rolls back and Serve returns. From
	//! any thread, at any time.
	void Cancel(const string &reason = "crossing: far was cancelled");

private:
	//! True when the statement ended the transaction.
	bool RunStatement(CrossingLink &link, CrossingStream &control, const string &body);
	void RunDdl(CrossingStream &control, const string &frame);
	void AnswerTables(CrossingStream &control, const string &schema);
	void AnswerDescribe(CrossingStream &control, const string &question);

	Connection &connection;
	string catalog;
	CrossingFarOptions options;
	mutex lock;
	ErrorData cancelled;
	shared_ptr<CrossingFailure> running;
	vector<shared_ptr<CrossingSeamInbox>> inboxes;
};

} // namespace duckdb
