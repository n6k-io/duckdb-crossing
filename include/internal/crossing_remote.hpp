#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing_link.hpp"
#include "crossing_substrait.hpp"

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
	//! One DDL statement on far, in this transaction. Throws what far reported.
	void RunDdl(const string &frame);
	//! Unique within the transaction, as a slice's streams are.
	idx_t NextSliceId() {
		return next_slice++;
	}
	//! Whichever statement far is running ends first: a failure in it fails Commit, which then rolls back.
	void Commit();
	void Rollback();
	void StatementRunning(bool running) {
		statement_running = running;
	}
	//! Waits out a statement near has finished with. What far reports of it is returned, not thrown.
	ErrorData DrainStatement();

private:
	void End(const char *word);

	CrossingLink &link;
	unique_ptr<CrossingStream> control;
	idx_t next_slice = 0;
	bool ended = false;
	bool statement_running = false;
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
	void Add(CrossingRemoteSlice slice, bool seam_ordered, optional_idx seam_estimate);
	void PutWrite(idx_t slice, unique_ptr<SubstraitWrite> write, vector<unique_ptr<Expression>> owned);
	//! Blocks until far has bound the plan. Throws what far reported, every time it is called.
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
	bool started = false;
	bool finished = false;
	ErrorData start_error;
};

//! How near's operator turns far's rows into its own.
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

//! Adds the slice to the statement and plans its operator; a seam's feed becomes its child when given.
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
