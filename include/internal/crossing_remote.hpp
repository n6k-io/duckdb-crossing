#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing_link.hpp"
#include "crossing_substrait.hpp"
#include "internal/control_frames.hpp"

namespace duckdb {

//! Near's side of a link: one far transaction, whose statements go one at a time.
class CrossingRemote {
public:
	explicit CrossingRemote(CrossingLink &link);
	~CrossingRemote();

	CrossingLink &Link() {
		return link;
	}
	CrossingStream &Control() {
		return *control;
	}
	//! One DDL statement on far, in this transaction. Throws what far reported. An interrupt ends the wait.
	void RunDdl(ClientContext &context, const string &frame);
	//! Unique within the transaction, as a slice's streams are.
	idx_t NextSliceId() {
		return next_slice++;
	}
	//! Whichever statement far is running ends first: a failure in it fails Commit, which then rolls back. When far
	//! already ended the transaction, only closes the link.
	void Commit();
	void Rollback();
	void StatementSent(bool ends_transaction) {
		used = true;
		ended_by_far = ends_transaction;
		statement_running = !ends_transaction;
	}
	bool Used() const {
		return used;
	}
	//! What this transaction sees of a schema it changed.
	vector<string> Tables(ClientContext &context, const string &schema);
	//! Far's view of the table in this transaction: columns, row ids and constraints.
	CrossingTable Describe(ClientContext &context, const string &schema, const string &name);
	//! Far ended this transaction with its statement; another statement needs another link.
	bool EndedByFar() const {
		return ended_by_far;
	}
	//! Waits out a statement near has finished with. What far reports of it is returned, not thrown. With `context`,
	//! an interrupt ends the wait by throwing.
	ErrorData DrainStatement(optional_ptr<ClientContext> context = nullptr);

private:
	void End(const char *word);
	string Ask(ClientContext &context, const string &question);
	ErrorData DrainReply(optional_ptr<ClientContext> context);

	CrossingLink &link;
	CrossingControlQueue replies;
	unique_ptr<CrossingStream> control;
	idx_t unread_replies = 0;
	idx_t next_slice = 0;
	bool ended = false;
	bool statement_running = false;
	bool used = false;
	bool ended_by_far = false;
};

//! A slice as near plans it. `owned` keeps alive the expressions `write` refers to; `keep` what `borrowed`
//! points into.
struct CrossingRemoteSlice {
	idx_t id = 0;
	bool ordered = false;
	unique_ptr<LogicalOperator> source;
	optional_ptr<const LogicalOperator> borrowed;
	shared_ptr<void> keep;
	unique_ptr<SubstraitWrite> write;
	vector<unique_ptr<Expression>> owned;

	optional_ptr<const LogicalOperator> Source() const {
		return source ? optional_ptr<const LogicalOperator>(source.get()) : borrowed;
	}
};

//! One statement's slices. Every slice is added while planning; the first operator to start sends them.
class CrossingRemoteStatement {
public:
	explicit CrossingRemoteStatement(CrossingRemote &remote);

	CrossingRemote &Remote() {
		return remote;
	}
	//! The statement is its whole transaction.
	void SetAutoCommit(bool auto_commit_p) {
		auto_commit = auto_commit_p;
	}
	void Add(CrossingRemoteSlice slice, bool seam_ordered, optional_idx seam_estimate);
	void Planned(PhysicalOperator &op);
	bool WasPlanned(const PhysicalOperator &op);
	void PutWrite(idx_t slice, unique_ptr<SubstraitWrite> write, vector<unique_ptr<Expression>> owned);
	//! What EXPLAIN shows of a slice: what the source runs and returns; with `seam`, the rows sent to it too.
	InsertionOrderPreservingMap<string> Explain(idx_t slice, optional_ptr<const vector<LogicalType>> seam);
	//! Sends the statement's slices on the first call; every call rethrows that send's failure. Far's errors come back
	//! through Finish.
	void Start();
	//! Blocks until far has ended the statement. Throws what far reported.
	void Finish();

private:
	struct Added {
		CrossingRemoteSlice slice;
		bool seam_ordered = false;
		optional_idx seam_estimate;
	};

	CrossingRemote &remote;
	mutex lock;
	vector<Added> added;
	vector<const PhysicalOperator *> planned;
	bool auto_commit = false;
	bool started = false;
	bool finished = false;
	ErrorData start_error;
};

struct CrossingRemoteColumns {
	static constexpr idx_t NULL_COLUMN = DConstants::INVALID_INDEX;

	vector<LogicalType> received;
	//! For each output column, its received column, or NULL.
	vector<idx_t> positions;

	static CrossingRemoteColumns Identity(const vector<LogicalType> &types);
};

struct CrossingRemoteSeamSink {
	vector<LogicalType> types;
	bool ordered = false;
	bool parallel = false;
	//! Nothing reads this operator as a source, as with MERGE's actions: the sink waits out far's answer.
	bool awaits_result = false;
};

PhysicalOperator &PlanCrossingRemote(ClientContext &context, PhysicalPlanGenerator &planner, vector<LogicalType> types,
                                     shared_ptr<CrossingRemoteStatement> statement, CrossingRemoteSlice slice,
                                     CrossingRemoteColumns columns, unique_ptr<CrossingRemoteSeamSink> seam,
                                     optional_ptr<PhysicalOperator> feed, optional_idx seam_estimate,
                                     idx_t estimated_cardinality);

//! When `plan` is a read of this statement, makes it the source of `write`: true, with `write` and `owned` taken.
bool TryPutWriteOnRemote(PhysicalOperator &plan, CrossingRemoteStatement &statement, unique_ptr<SubstraitWrite> &write,
                         vector<unique_ptr<Expression>> &owned, const CrossingRemoteColumns &columns,
                         const vector<LogicalType> &types);

//! How a feed's rows must travel, as a result collector chooses.
unique_ptr<CrossingRemoteSeamSink> CrossingSeamSinkOf(ClientContext &context, PhysicalOperator &feed);

//! `crossing[...]` on near: a source of the slice's results, and with `feed`, a sink of its seam's rows.
unique_ptr<LogicalOperator> MakeCrossingRemote(TableIndex table_index, vector<LogicalType> types,
                                               shared_ptr<CrossingRemoteStatement> statement, CrossingRemoteSlice slice,
                                               unique_ptr<LogicalOperator> feed);

} // namespace duckdb
