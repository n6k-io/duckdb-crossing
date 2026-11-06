#pragma once

#include "duckdb/common/mutex.hpp"
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

private:
	void End(const char *word);
	ErrorData DrainStatement();

	CrossingLink &link;
	unique_ptr<CrossingStream> control;
	idx_t next_slice = 0;
	bool ended = false;
	bool statement_running = false;
};

//! A slice as near plans it. `owned` keeps alive the expressions `write` refers to.
struct CrossingRemoteSlice {
	idx_t id = 0;
	bool ordered = false;
	unique_ptr<LogicalOperator> source;
	unique_ptr<SubstraitWrite> write;
	vector<unique_ptr<Expression>> owned;
};

//! One statement's slices. Every slice is added while planning; the first operator to start sends them.
class CrossingRemoteStatement {
public:
	explicit CrossingRemoteStatement(CrossingRemote &remote);

	CrossingRemote &Remote() {
		return remote;
	}
	void Add(CrossingRemoteSlice slice, bool seam_ordered, optional_idx seam_estimate);
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

//! `crossing[...]` on near: a source of the slice's results, and with `feed`, a sink of its seam's rows.
unique_ptr<LogicalOperator> MakeCrossingRemote(TableIndex table_index, vector<LogicalType> types,
                                               shared_ptr<CrossingRemoteStatement> statement, CrossingRemoteSlice slice,
                                               unique_ptr<LogicalOperator> feed);

} // namespace duckdb
