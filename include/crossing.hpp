#pragma once

// The API a source implements. See IMPLEMENTING.md.
//
// Vocabulary. One word per concept, everywhere:
//   crossing   the library, and a scan or write that runs on a source
//   source     the database a CrossingSource fronts; the target is DuckDB
//   attach     one ATTACH of a source, and the object that serves it to a catalog
//   session    one target transaction's side of the source; every read and write in it goes through it
//   scan       one Read, which opens a reader per partition
//   fragment   the plan a crossing carries to the source
//   floor      the source's own scan at the bottom of a read fragment; sealed once built
//   seam       the hole in a write fragment where the rows go
//   fence      the logical operator standing in for a write the source runs alone
//   feed       what a fence takes rows from
//   fold       moving a read subtree into its fragment
//   fill       moving a feed into a seam
//   frozen     a fragment the pass must leave as bound
//   verdict    a yes, or a no with a reason
//   obstacle   why a write cannot run wholly on its source
//   declined   why a source returned no plan

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/parser/constraint.hpp"

#include <functional>

namespace duckdb {

class ExtensionLoader;
class Expression;
class LogicalOperator;
class ClientContext;
class ColumnDataCollection;
struct AttachInfo;

//! DELETE_ trails an underscore: DELETE is a macro in some Windows SDK headers.
enum class CrossingVerb : uint8_t { SELECT = 0, INSERT = 1, UPDATE = 2, DELETE_ = 3 };

static constexpr idx_t CROSSING_VERB_COUNT = 4;

//! Lowercase wire spelling, as reported by the permissions function.
const char *CrossingVerbName(CrossingVerb verb);

struct CrossingTable {
	string name;
	//! Source order. That order is what every column position in a query means.
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! A verb absent here is refused at bind time; the source is never asked about it.
	vector<CrossingVerb> verbs;
	vector<string> key;
	//! The source vouches `key` is unique. Only then may a keyed write run wholly on the source,
	//! since nothing counts what the target sent.
	bool key_unique = false;
	//! Constraints the source enforces. Declaring one your source does not enforce claims a
	//! guarantee nothing keeps, so only pass what is real.
	vector<unique_ptr<Constraint>> constraints;

	void Column(string column_name, LogicalType type) {
		column_names.push_back(std::move(column_name));
		column_types.push_back(std::move(type));
	}
	bool Allows(CrossingVerb verb) const {
		for (auto allowed : verbs) {
			if (allowed == verb) {
				return true;
			}
		}
		return false;
	}
};

struct CrossingTableUse {
	string schema;
	string table;
	//! Indexes into the table's columns, in the order Describe declared them.
	vector<idx_t> columns;
};

struct CrossingFloor {
	string schema;
	string table;
	vector<string> column_names;
};

unique_ptr<LogicalOperator> MakeFloorNode(idx_t table_index, string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types);

optional_ptr<const CrossingFloor> FloorOf(const LogicalOperator &op);

//! Every floor in `plan`, with the columns each is read for.
vector<CrossingTableUse> CrossingTablesOf(const LogicalOperator &plan);

//! Bytes a plan can travel as. The receiving side needs RegisterCrossingPass on its own instance.
string SerializeCrossingPlan(const LogicalOperator &plan);
unique_ptr<LogicalOperator> DeserializeCrossingPlan(ClientContext &context, const string &bytes);

//! What a read and a write both carry. Borrowed for the call.
struct CrossingQuery {
	CrossingQuery(CrossingVerb kind_p, const LogicalOperator &plan_p) : kind(kind_p), plan(plan_p) {
	}

	//! SELECT on a read, the write verb on a write.
	CrossingVerb kind;
	//! Unoptimized. A read is the floor with the crossed work on it. A write is the statement with
	//! its seam already filled: by a plan of yours when the rows come from this source, otherwise by
	//! the rows the target gathered. Run it as it is.
	const LogicalOperator &plan;
	//! The row types: what a read produces, what a write is handed.
	vector<LogicalType> types;
	bool ordered = false;
	vector<CrossingTableUse> tables;
	//! What an update or a delete addresses rows by. An insert names none.
	vector<string> key_columns;
	//! The columns a write sets. An insert names every column of the table, in the order Describe
	//! declared them. An update names the columns it sets. A delete names none.
	vector<string> set_columns;
};

struct CrossingVerdict {
	bool ok = true;
	string reason;

	static CrossingVerdict Yes() {
		return CrossingVerdict();
	}
	static CrossingVerdict No(string reason) {
		CrossingVerdict verdict;
		verdict.ok = false;
		verdict.reason = std::move(reason);
		return verdict;
	}
};

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types);

//! The rows the target gathered for a filled seam, when `op` is that seam.
optional_ptr<const ColumnDataCollection> SeamRowsOf(const LogicalOperator &op);

struct CrossingParking;

class CrossingWaker {
public:
	CrossingWaker() = default;

	void Wake() const;

private:
	friend struct CrossingParking;
	explicit CrossingWaker(shared_ptr<CrossingParking> parking_p) : parking(std::move(parking_p)) {
	}

	shared_ptr<CrossingParking> parking;
};

//! What one pull produced. ROWS: `chunk` is filled. DONE: the partition is exhausted. WAIT: nothing
//! yet; Wake() the waker and pull again.
struct CrossingPull {
	enum class Outcome : uint8_t { ROWS, DONE, WAIT };

	Outcome outcome = Outcome::DONE;

	static CrossingPull Rows() {
		CrossingPull pull;
		pull.outcome = Outcome::ROWS;
		return pull;
	}
	static CrossingPull Done() {
		return CrossingPull();
	}
	static CrossingPull Wait() {
		CrossingPull pull;
		pull.outcome = Outcome::WAIT;
		return pull;
	}
};

//! One partition's rows, pulled by one thread. Fills the chunk with the next rows.
using CrossingReader = std::function<CrossingPull(ClientContext &context, DataChunk &chunk, CrossingWaker waker)>;

//! One Read. Each partition is opened once, by whichever thread takes it.
struct CrossingScan {
	idx_t partitions = 1;
	std::function<CrossingReader(ClientContext &context, idx_t partition)> open;
};

struct CrossingWriteResult {
	enum class Outcome : uint8_t { DONE, WAIT };

	Outcome outcome = Outcome::DONE;
	idx_t affected_rows = 0;

	static CrossingWriteResult Done(idx_t affected_rows) {
		CrossingWriteResult result;
		result.affected_rows = affected_rows;
		return result;
	}
	static CrossingWriteResult Wait() {
		CrossingWriteResult result;
		result.outcome = Outcome::WAIT;
		return result;
	}
};

using CrossingWriter = std::function<CrossingWriteResult(ClientContext &context, CrossingWaker waker)>;

//! One connection's side of the source: begun the first time a target transaction touches the
//! source, resolved when that transaction does.
class CrossingSession {
public:
	virtual ~CrossingSession() {
	}

	virtual CrossingScan Read(ClientContext &context, const CrossingQuery &query) = 0;

	virtual CrossingWriter Write(ClientContext &context, const CrossingQuery &query);

	virtual void Commit();
	virtual void Rollback();
};

struct CrossingSeam {
	vector<string> key_columns;
	vector<string> set_columns;
	vector<LogicalType> types;
};

//! What the source is asked to plan. SELECT wants a scan of the table emitting one column per
//! column Describe declared, in that order. A write wants the statement with a seam node
//! (MakeSeamNode) where the rows go.
struct CrossingPlanRequest {
	CrossingVerb verb;
	string schema;
	string table;
	CrossingSeam seam;
};

struct CrossingPlan {
	unique_ptr<LogicalOperator> plan;
	string declined;

	static CrossingPlan Of(unique_ptr<LogicalOperator> plan) {
		CrossingPlan result;
		result.plan = std::move(plan);
		return result;
	}
	static CrossingPlan Declined(string reason) {
		CrossingPlan result;
		result.declined = std::move(reason);
		return result;
	}
};

class CrossingSource {
public:
	virtual ~CrossingSource() {
	}

	virtual vector<string> Schemas();

	virtual vector<string> Tables(const string &schema) = 0;

	virtual CrossingTable Describe(const string &schema, const string &name) = 0;

	virtual CrossingPlan Plan(const CrossingPlanRequest &request) = 0;

	virtual CrossingVerdict AcceptsCall(const Expression &expr);

	virtual CrossingVerdict AcceptsType(const LogicalType &type);

	virtual CrossingVerdict AcceptsOperator(const LogicalOperator &op);

	virtual unique_ptr<CrossingSession> Begin(ClientContext &context) = 0;

	virtual void Detach(ClientContext &context);

	using Factory = std::function<unique_ptr<CrossingSource>(ClientContext &context, AttachInfo &info)>;

	static void Register(ExtensionLoader &loader, const string &type, Factory factory);
};

} // namespace duckdb
