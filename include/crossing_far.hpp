#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"

#include "crossing_link.hpp"
#include "crossing_substrait.hpp"

namespace duckdb {

struct CrossingParking;
class CrossingFailure;
class CrossingSeamInbox;

struct CrossingFarOptions {
	//! What stands for a table a plan reads, such as a subquery with a row filter.
	CrossingFloorResolver floors;
	//! Called with each catalog a plan names; throws to refuse the plan.
	SubstraitCatalogCheck catalogs;
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
	bool NextControl(CrossingStream &control, string &header);

	Connection &connection;
	string catalog;
	CrossingFarOptions options;
	mutex lock;
	ErrorData cancelled;
	shared_ptr<CrossingParking> control_wait;
	shared_ptr<CrossingFailure> running;
	vector<shared_ptr<CrossingSeamInbox>> inboxes;
};

} // namespace duckdb
