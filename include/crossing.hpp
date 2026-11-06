#pragma once

// The API a source implements. See IMPLEMENTING.md.
//
// Vocabulary:
//   crossing   the library, and a scan or write that runs on a source
//   source     the database the implementor's type S fronts; the target is DuckDB
//   identity   what tells one copy of crossing's nodes from another copy's
//   attach     one ATTACH of a source, and the object that serves it to a catalog
//   session    one target transaction's side of the source; every read and write in it goes through it
//   scan       one Run, which opens a reader per partition
//   waker      what a reader that has nothing yet calls when it does
//   fragment   the plan a crossing carries to the source
//   carrier    the bind data a fragment rides in, hung off a LogicalGet
//   floor      the source's own scan at the bottom of a read fragment; sealed once built
//   seam       the hole in a write fragment where the rows go
//   write node the top of a write fragment, naming the write; what it writes is its child
//   near node  near's side of a write: gathers any rows, makes the one call, holds what comes back
//   feed       what a write takes rows from
//   fold       moving a read subtree into its fragment
//   fill       moving a feed into a seam
//   remainder  what a fill leaves on the target
//   label      which source a subtree could run on
//   rule       one operator type's answer to whether it crosses at all
//   key alias  the virtual column a key column stands behind so the binder addresses rows by it
//   frozen     a fragment the pass must leave as bound
//   verdict    a yes, or a no with a reason
//   obstacle   why a write cannot run wholly on its source

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/constraint.hpp"

#include <functional>
#include <type_traits>
#include <utility>

namespace duckdb {

class ExtensionLoader;
class Expression;
class LogicalOperator;
class ClientContext;
class ColumnDataCollection;
class DatabaseInstance;
class TableRef;
class SQLStatement;
class QueryResult;
struct AttachInfo;
struct CreateInfo;
struct CreateViewInfo;
struct AlterInfo;
struct DropInfo;

//! DELETE_ trails an underscore: DELETE is a macro in some Windows SDK headers.
enum class CrossingVerb : uint8_t { SELECT = 0, INSERT = 1, UPDATE = 2, DELETE_ = 3, CREATE = 4, ALTER = 5, DROP = 6 };

const char *CrossingVerbName(CrossingVerb verb);

const vector<CrossingVerb> &CrossingVerbs();

bool IsDdlVerb(CrossingVerb verb);

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

struct CrossingTable {
	string name;
	//! Source order. That order is what every column position in a query means.
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! A verb absent here is refused before the statement runs. An update or delete that reads rows
	//! (CrossingWriteTarget::reads_rows) also needs SELECT. With UPDATE or DELETE but no SELECT, Plan(SELECT)
	//! is still asked at bind, since every update and delete binds a scan.
	vector<CrossingVerb> verbs;
	vector<string> key;
	//! The source vouches `key` is unique. Only then may a keyed write run wholly on the source,
	//! since nothing counts what the target sent.
	bool key_unique = false;
	//! The source can return the rows a write leaves. Without it RETURNING is refused at bind.
	bool returning = false;
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

struct CrossingSchema {
	string name;
	vector<CrossingVerb> verbs;

	bool Allows(CrossingVerb verb) const {
		for (auto allowed : verbs) {
			if (allowed == verb) {
				return true;
			}
		}
		return false;
	}
};

struct CrossingDdl {
	CrossingVerb verb;
	string schema;
	string table;
	optional_ptr<const CreateInfo> create;
	optional_ptr<const AlterInfo> alter;
	optional_ptr<const DropInfo> drop;
	//! A CREATE VIEW's body, floors only: `create` is then a CreateViewInfo whose `query` is the
	//! target's spelling. Its columns are CrossingViewColumnNames(view), in order.
	optional_ptr<const LogicalOperator> plan;
};

//! A view's column names: its alias where given, else what its body names.
vector<string> CrossingViewColumnNames(const CreateViewInfo &view);

struct CrossingTableUse {
	//! Empty unless the floor names one.
	string catalog;
	string schema;
	string table;
	//! Indexes into the table's columns, in the order Describe declared them.
	vector<idx_t> columns;
};

struct CrossingFloor {
	//! Empty: the catalog the receiver binds in. Set by a source whose group spans catalogs.
	string catalog;
	string schema;
	string table;
	vector<string> column_names;
};

unique_ptr<LogicalOperator> MakeFloorNode(idx_t table_index, string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types, string catalog = string());
unique_ptr<LogicalOperator> MakeFloorNode(string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types, string catalog = string());

optional_ptr<const CrossingFloor> FloorOf(const LogicalOperator &op);

//! Every floor in `plan`, and every scan of a catalog table, with the columns each is read for.
vector<CrossingTableUse> CrossingTablesOf(const LogicalOperator &plan);

//! Bytes a plan can travel as. The receiving side needs RegisterCrossingPlanFunctions on its own
//! instance.
string SerializeCrossingPlan(const LogicalOperator &plan);
unique_ptr<LogicalOperator> DeserializeCrossingPlan(ClientContext &context, const string &bytes);

//! Makes the floor and seam functions resolvable by name, which DeserializeCrossingPlan needs.
void RegisterCrossingPlanFunctions(DatabaseInstance &db);

//! What a read and a write both carry. Lives as long as the scan it was handed to.
struct CrossingQuery {
	explicit CrossingQuery(const LogicalOperator &plan_p) : plan(plan_p) {
	}

	//! Not yet optimized by DuckDB. A read is the crossed work on a projection crossing adds, on the
	//! floor. A write has a write node on top (WriteOf); see CrossingWriteNode. Run it as it is.
	const LogicalOperator &plan;
	//! What the readers produce: a read's rows; a write's one BIGINT, the rows it changed; or, with
	//! `returning_columns`, the rows it left.
	vector<LogicalType> types;
	bool ordered = false;
	//! Every table the plan reads. On a write whose seam the target filled, none.
	vector<CrossingTableUse> tables;
};

//! A write the source is asked to run. Its child in the plan is what it writes: work reading this source
//! when every part of the feed can run on it, otherwise the rows the target gathered, all the feed's work
//! done (see SeamRowsOf). No child means it takes nothing, as a bare delete and `DEFAULT VALUES` do.
struct CrossingWriteNode {
	CrossingVerb verb = CrossingVerb::INSERT;
	string catalog;
	string schema;
	string table;
	//! What an update or a delete addresses rows by. An insert names none. An update or delete naming
	//! none reads nothing of the rows it writes and addresses every row of the table.
	vector<string> key_columns;
	//! The columns a write sets. An insert names the columns the statement gave a value, in the order
	//! Describe declared them; the source applies its own defaults to the rest. An update names the
	//! columns it sets. A delete names none.
	vector<string> set_columns;
	//! Non-empty: the write returns the rows it leaves, these columns of them. The types are what the
	//! target expects them to be.
	vector<string> returning_columns;
	vector<LogicalType> returning_types;
};

//! The write `op` is the node of. Null for any other operator.
optional_ptr<const CrossingWriteNode> WriteOf(const LogicalOperator &op);

//! The rows a CHUNK_GET holds: the seam the target filled, or a VALUES list. Null for any other operator.
optional_ptr<const ColumnDataCollection> SeamRowsOf(const LogicalOperator &op);

//! What stands for a floor when it is bound: a TableRef producing the floor's column names and types,
//! in order, or BindFloors throws CatalogException. It must bind to a scan or a projection, or
//! BindFloors throws InternalException. Called twice for one floor when the first ref's names differ.
//! Null means the default, `catalog.schema.table`.
using CrossingFloorResolver = std::function<unique_ptr<TableRef>(const CrossingFloor &)>;

//! Replaces every floor in `plan` with the table it names, bound on `context`, which must have a
//! transaction open. A floor naming no catalog is bound in `catalog`. Each floor keeps its table index and column
//! bindings. A table whose columns no longer match the floor is a CatalogException.
void BindFloors(ClientContext &context, unique_ptr<LogicalOperator> &plan, const string &catalog,
                const CrossingFloorResolver &resolver = nullptr);

//! Runs a bound plan on `context` as a query. Use this rather than LogicalPlanStatement: DuckDB
//! copies a statement before planning it when a ClientContextState can request a rebind, and that
//! statement cannot be copied.
unique_ptr<QueryResult> SubmitCrossingPlan(ClientContext &context, unique_ptr<LogicalOperator> plan);

struct CrossingWriteTarget {
	string catalog;
	string schema;
	string table;
	CrossingVerb verb;
	vector<string> key_columns;
	vector<string> set_columns;
	//! False on an update or delete that reads nothing of the rows it writes: no WHERE, no FROM or USING,
	//! no RETURNING, and every SET value foldable. It names no key columns and addresses every row of the
	//! table. A source with row policies can use this to tell such a write from one that reads.
	bool reads_rows = true;
	vector<string> returning_columns;
	vector<LogicalType> returning_types;
};

//! The target running `node` needs, on `catalog` when the node names none.
CrossingWriteTarget CrossingWriteTargetOf(const string &catalog, const CrossingWriteNode &node);

//! An INSERT, UPDATE or DELETE of the target reading its rows from the seam, as parser nodes.
//! `seam_alias` names the seam and `seam_columns` its columns, in BuildWriteStatement's order.
struct CrossingWriteStatement {
	unique_ptr<SQLStatement> statement;
	string seam_alias;
	vector<string> seam_columns;
};

//! A chance to change the statement before it binds, such as adding a row policy.
using CrossingWriteShaper = std::function<void(CrossingWriteStatement &)>;

//! A seam over rows already in memory. The rows must outlive the statement.
unique_ptr<TableRef> SeamRefOfRows(const ColumnDataCollection &rows);

//! The statement `SubmitWrite` binds. `seam` is any TableRef whose columns are, by position, the
//! key columns then the set columns; null when there are none.
CrossingWriteStatement BuildWriteStatement(const CrossingWriteTarget &target, unique_ptr<TableRef> seam);

//! Builds, binds and runs the write on `context`, which must have a transaction open. The result is one
//! row of one BIGINT, the rows the target changed, or with `returning_columns` the rows it left. A table
//! that no longer has those columns as `returning_types` says is a CatalogException before anything is
//! written. The plan form takes a bound plan already on `context`,
//! such as what is under a received write node after BindFloors.
unique_ptr<QueryResult> SubmitWrite(ClientContext &context, const CrossingWriteTarget &target,
                                    unique_ptr<TableRef> seam, const CrossingWriteShaper &shape = nullptr);
unique_ptr<QueryResult> SubmitWrite(ClientContext &context, const CrossingWriteTarget &target,
                                    unique_ptr<LogicalOperator> seam, const CrossingWriteShaper &shape = nullptr);

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

//! What one pull produced. ROWS: `chunk` is filled; an empty chunk ends the partition like DONE.
//! DONE: the partition is exhausted. WAIT: nothing yet; call the waker's Wake() when there is and
//! DuckDB pulls again. A reader that never wakes stalls the scan.
//! `batch_index` places the rows in the scan's order, and is given exactly when the scan is
//! `batch_indexed`; otherwise the pull throws InvalidInputException.
struct CrossingReadResult {
	enum class Outcome : uint8_t { ROWS, DONE, WAIT };

	Outcome outcome = Outcome::DONE;
	optional_idx batch_index;

	static CrossingReadResult Rows() {
		CrossingReadResult result;
		result.outcome = Outcome::ROWS;
		return result;
	}
	static CrossingReadResult Rows(idx_t batch_index) {
		auto result = Rows();
		result.batch_index = batch_index;
		return result;
	}
	static CrossingReadResult Done() {
		return CrossingReadResult();
	}
	static CrossingReadResult Wait() {
		CrossingReadResult result;
		result.outcome = Outcome::WAIT;
		return result;
	}
};

//! One partition's rows, pulled by one thread at a time; the thread may change after a WAIT.
using CrossingReader = std::function<CrossingReadResult(ClientContext &context, DataChunk &chunk, CrossingWaker waker)>;

//! One Run. Each partition is opened once, by whichever thread claims it; partitions are claimed
//! in increasing order and a thread drains several in turn. An ordered scan must be one partition
//! unless `batch_indexed`, or it throws InvalidInputException. Unindexed, a partition's rows are
//! ordered by its number.
//! `batch_indexed`: every ROWS carries a batch index, and DuckDB keeps rows in batch-index order
//! wherever order matters. Across the scan no index is handed out after a larger one, as from one
//! counter taken in stream order. An index going down on one thread throws InvalidInputException,
//! as indexes interleaved across partitions do once a thread drains a second partition; crossing
//! checks nothing across threads. One batch may span several pulls of the same reader; no two
//! readers share one.
struct CrossingScan {
	idx_t partitions = 1;
	bool batch_indexed = false;
	std::function<CrossingReader(ClientContext &context, idx_t partition)> open;
};

//! What the source is asked to plan: a scan of the table emitting one column per column of `described`,
//! in that order.
struct CrossingPlanRequest {
	string schema;
	string table;
	optional_ptr<const CrossingTable> described;
};

struct CrossingPlan {
	unique_ptr<LogicalOperator> plan;
	CrossingVerdict verdict;

	static CrossingPlan Of(unique_ptr<LogicalOperator> plan) {
		CrossingPlan result;
		result.plan = std::move(plan);
		return result;
	}
	static CrossingPlan Declined(string reason) {
		CrossingPlan result;
		result.verdict = CrossingVerdict::No(std::move(reason));
		return result;
	}
};

struct CrossingIdentity {
	CrossingIdentity() = default;
	CrossingIdentity(const CrossingIdentity &) = delete;
	CrossingIdentity &operator=(const CrossingIdentity &) = delete;
};

class CrossingSession {
public:
	virtual ~CrossingSession() = default;

	virtual CrossingScan Run(ClientContext &context, const CrossingQuery &query) = 0;
	virtual void Ddl(ClientContext &context, const CrossingDdl &ddl);
	virtual vector<string> Tables(const string &schema) = 0;
	virtual CrossingTable Describe(const string &schema, const string &name) = 0;
	virtual void Commit() {
	}
	virtual void Rollback() {
	}
};

class CrossingSource {
public:
	virtual ~CrossingSource() = default;

	virtual const CrossingIdentity &Identity() const = 0;

	virtual vector<string> Schemas() {
		return {"main"};
	}
	virtual vector<string> Tables(const string &schema) = 0;
	virtual CrossingTable Describe(const string &schema, const string &name) = 0;
	virtual CrossingSchema DescribeSchema(const string &schema) {
		CrossingSchema result;
		result.name = schema;
		return result;
	}
	virtual CrossingPlan Plan(const CrossingPlanRequest &request) = 0;

	virtual CrossingVerdict AcceptsCall(const Expression &expr) {
		return CrossingVerdict::No("this source computes nothing");
	}
	virtual CrossingVerdict AcceptsType(const LogicalType &type) {
		return CrossingVerdict::No("this source holds only what it was described with");
	}
	virtual CrossingVerdict AcceptsOperator(const LogicalOperator &op) {
		return CrossingVerdict::Yes();
	}
	//! Whether a join may run here on rows the target sends, with these column types. Off by default: a
	//! source that says yes gets those rows in the plan, as any size of CHUNK_GET.
	virtual CrossingVerdict AcceptsSeam(const vector<LogicalType> &types) {
		return CrossingVerdict::No("this source takes no rows into a read");
	}

	//! Sources with one group serve one store through one transport, so a plan may join them. They
	//! must answer the Accepts* calls alike.
	virtual const void *Group() const {
		return this;
	}

	virtual unique_ptr<CrossingSession> Begin(ClientContext &context) = 0;
	virtual void Detach(ClientContext &context) {
	}
};

using CrossingSourceFactory = std::function<unique_ptr<CrossingSource>(ClientContext &context, AttachInfo &info)>;

void RegisterCrossing(ExtensionLoader &loader, const string &type, const CrossingIdentity &identity,
                      CrossingSourceFactory factory);
void RegisterCrossingPass(DatabaseInstance &db, const CrossingIdentity &identity);

namespace crossing_contract {

template <class...>
struct Void {
	using type = void;
};
template <class... Ts>
using void_t = typename Void<Ts...>::type;

template <class...>
struct Args {};

struct NotCallable {};
struct Nothing {};

template <class S, class = void>
struct SessionType {
	using type = void;
};
template <class S>
struct SessionType<S, void_t<typename S::Session>> {
	using type = typename S::Session;
};
template <class S>
using SessionOf = typename SessionType<S>::type;

template <bool Ok, class Expected, class Found>
struct ReturnCheck {
	static_assert(Ok, "crossing: this member returns Found; it must return something convertible to Expected");
};

#define CROSSING_MEMBER(NAME, SIGNATURE, EXPECTED, ...)                                                                \
	template <class T>                                                                                                 \
	class Names##NAME {                                                                                                \
		struct Fallback {                                                                                              \
			int NAME;                                                                                                  \
		};                                                                                                             \
		struct Derived : std::conditional<std::is_final<T>::value, Nothing, T>::type, Fallback {};                     \
		template <class U, U>                                                                                          \
		struct Check;                                                                                                  \
		template <class U>                                                                                             \
		static std::false_type Test(Check<int Fallback::*, &U::NAME> *);                                               \
		template <class U>                                                                                             \
		static std::true_type Test(...);                                                                               \
                                                                                                                       \
	public:                                                                                                            \
		static constexpr bool value = decltype(Test<Derived>(nullptr))::value;                                         \
	};                                                                                                                 \
	template <class T, class A, class = void>                                                                          \
	struct Returns##NAME {                                                                                             \
		using type = NotCallable;                                                                                      \
	};                                                                                                                 \
	template <class T, class... A>                                                                                     \
	struct Returns##NAME<T, Args<A...>, void_t<decltype(std::declval<T &>().NAME(std::declval<A>()...))>> {            \
		using type = decltype(std::declval<T &>().NAME(std::declval<A>()...));                                         \
	};                                                                                                                 \
	template <class T>                                                                                                 \
	struct Expected##NAME {                                                                                            \
		using type = EXPECTED;                                                                                         \
	};                                                                                                                 \
	template <class T>                                                                                                 \
	struct Calls##NAME                                                                                                 \
	    : std::integral_constant<                                                                                      \
	          bool, !std::is_same<typename Returns##NAME<T, Args<__VA_ARGS__>>::type, NotCallable>::value> {};         \
	template <class T>                                                                                                 \
	struct Has##NAME                                                                                                   \
	    : std::integral_constant<bool, Calls##NAME<T>::value &&                                                        \
	                                       std::is_convertible<typename Returns##NAME<T, Args<__VA_ARGS__>>::type,     \
	                                                           typename Expected##NAME<T>::type>::value> {};           \
	template <class T, bool Required>                                                                                  \
	struct Check##NAME : ReturnCheck<!Calls##NAME<T>::value || Has##NAME<T>::value, typename Expected##NAME<T>::type,  \
	                                 typename Returns##NAME<T, Args<__VA_ARGS__>>::type> {                             \
		static_assert(!Required || Names##NAME<T>::value || Calls##NAME<T>::value,                                     \
		              "crossing: missing required member `" SIGNATURE "`");                                            \
		static_assert(!Names##NAME<T>::value || Calls##NAME<T>::value,                                                 \
		              "crossing: `" #NAME "` exists but is not callable as `" SIGNATURE                                \
		              "`; check its parameters and that it is public");                                                \
	}

CROSSING_MEMBER(Schemas, "vector<string> Schemas()", vector<string>);
CROSSING_MEMBER(Tables, "vector<string> Tables(const string &schema)", vector<string>, const string &);
CROSSING_MEMBER(Describe, "CrossingTable Describe(const string &schema, const string &name)", CrossingTable,
                const string &, const string &);
CROSSING_MEMBER(DescribeSchema, "CrossingSchema DescribeSchema(const string &schema)", CrossingSchema, const string &);
CROSSING_MEMBER(Plan, "CrossingPlan Plan(const CrossingPlanRequest &request)", CrossingPlan,
                const CrossingPlanRequest &);
CROSSING_MEMBER(AcceptsCall, "CrossingVerdict AcceptsCall(const Expression &expr)", CrossingVerdict,
                const Expression &);
CROSSING_MEMBER(AcceptsType, "CrossingVerdict AcceptsType(const LogicalType &type)", CrossingVerdict,
                const LogicalType &);
CROSSING_MEMBER(AcceptsOperator, "CrossingVerdict AcceptsOperator(const LogicalOperator &op)", CrossingVerdict,
                const LogicalOperator &);
CROSSING_MEMBER(AcceptsSeam, "CrossingVerdict AcceptsSeam(const vector<LogicalType> &types)", CrossingVerdict,
                const vector<LogicalType> &);
CROSSING_MEMBER(Begin, "unique_ptr<Session> Begin(ClientContext &context)", unique_ptr<SessionOf<T>>, ClientContext &);
CROSSING_MEMBER(Detach, "void Detach(ClientContext &context)", void, ClientContext &);
CROSSING_MEMBER(Group, "const void *Group()", const void *);
CROSSING_MEMBER(Run, "CrossingScan Run(ClientContext &context, const CrossingQuery &query)", CrossingScan,
                ClientContext &, const CrossingQuery &);
CROSSING_MEMBER(Ddl, "void Ddl(ClientContext &context, const CrossingDdl &ddl)", void, ClientContext &,
                const CrossingDdl &);
CROSSING_MEMBER(Commit, "void Commit()", void);
CROSSING_MEMBER(Rollback, "void Rollback()", void);

#undef CROSSING_MEMBER

template <class S>
struct SourceContract : CheckTables<S, true>,
                        CheckDescribe<S, true>,
                        CheckPlan<S, true>,
                        CheckBegin<S, true>,
                        CheckSchemas<S, false>,
                        CheckDescribeSchema<S, false>,
                        CheckAcceptsCall<S, false>,
                        CheckAcceptsType<S, false>,
                        CheckAcceptsOperator<S, false>,
                        CheckAcceptsSeam<S, false>,
                        CheckDetach<S, false>,
                        CheckGroup<S, false> {
	static_assert(!std::is_final<S>::value, "crossing: a source type must not be final");
	static_assert(!std::is_void<SessionOf<S>>::value, "crossing: S needs `using Session = <your session type>;`");
};

template <class Sess>
struct SessionContract : CheckRun<Sess, true>,
                         CheckDdl<Sess, false>,
                         CheckTables<Sess, false>,
                         CheckDescribe<Sess, false>,
                         CheckCommit<Sess, false>,
                         CheckRollback<Sess, false> {
	static_assert(!std::is_final<Sess>::value, "crossing: a session type must not be final");
};

template <class Sess>
void DdlOn(Sess &session, ClientContext &context, const CrossingDdl &ddl, std::true_type) {
	session.Ddl(context, ddl);
}
template <class Sess>
void DdlOn(Sess &, ClientContext &, const CrossingDdl &, std::false_type) {
	throw NotImplementedException("crossing: this source's session has no Ddl");
}
template <class Sess>
vector<string> TablesOn(Sess &session, CrossingSource &, const string &schema, std::true_type) {
	return session.Tables(schema);
}
template <class Sess>
vector<string> TablesOn(Sess &, CrossingSource &source, const string &schema, std::false_type) {
	return source.Tables(schema);
}
template <class Sess>
CrossingTable DescribeOn(Sess &session, CrossingSource &, const string &schema, const string &name, std::true_type) {
	return session.Describe(schema, name);
}
template <class Sess>
CrossingTable DescribeOn(Sess &, CrossingSource &source, const string &schema, const string &name, std::false_type) {
	return source.Describe(schema, name);
}
template <class Sess>
void CommitOn(Sess &session, std::true_type) {
	session.Commit();
}
template <class Sess>
void CommitOn(Sess &, std::false_type) {
}
template <class Sess>
void RollbackOn(Sess &session, std::true_type) {
	session.Rollback();
}
template <class Sess>
void RollbackOn(Sess &, std::false_type) {
}
template <class S>
vector<string> SchemasOn(S &source, std::true_type) {
	return source.Schemas();
}
template <class S>
vector<string> SchemasOn(S &, std::false_type) {
	return {"main"};
}
template <class S>
CrossingSchema DescribeSchemaOn(S &source, const string &schema, std::true_type) {
	return source.DescribeSchema(schema);
}
template <class S>
CrossingSchema DescribeSchemaOn(S &, const string &schema, std::false_type) {
	CrossingSchema result;
	result.name = schema;
	return result;
}
template <class S>
CrossingVerdict AcceptsCallOn(S &source, const Expression &expr, std::true_type) {
	return source.AcceptsCall(expr);
}
template <class S>
CrossingVerdict AcceptsCallOn(S &, const Expression &, std::false_type) {
	return CrossingVerdict::No("this source computes nothing");
}
template <class S>
CrossingVerdict AcceptsTypeOn(S &source, const LogicalType &type, std::true_type) {
	return source.AcceptsType(type);
}
template <class S>
CrossingVerdict AcceptsTypeOn(S &, const LogicalType &, std::false_type) {
	return CrossingVerdict::No("this source holds only what it was described with");
}
template <class S>
CrossingVerdict AcceptsOperatorOn(S &source, const LogicalOperator &op, std::true_type) {
	return source.AcceptsOperator(op);
}
template <class S>
CrossingVerdict AcceptsOperatorOn(S &, const LogicalOperator &, std::false_type) {
	return CrossingVerdict::Yes();
}
template <class S>
CrossingVerdict AcceptsSeamOn(S &source, const vector<LogicalType> &types, std::true_type) {
	return source.AcceptsSeam(types);
}
template <class S>
CrossingVerdict AcceptsSeamOn(S &, const vector<LogicalType> &, std::false_type) {
	return CrossingVerdict::No("this source takes no rows into a read");
}
template <class S>
void DetachOn(S &source, ClientContext &context, std::true_type) {
	source.Detach(context);
}
template <class S>
void DetachOn(S &, ClientContext &, std::false_type) {
}
template <class S>
const void *GroupOn(S &source, std::true_type) {
	return source.Group();
}
template <class S>
const void *GroupOn(S &, std::false_type) {
	return nullptr;
}

} // namespace crossing_contract

template <class S>
struct Crossing;

template <class Sess>
void CheckDescribedVerbs(const string &name, const CrossingTable &table) {
	for (auto verb : table.verbs) {
		if (IsDdlVerb(verb) && !crossing_contract::HasDdl<Sess>::value) {
			throw NotImplementedException("crossing: '%s' declares '%s' but the session has no Ddl", name,
			                              CrossingVerbName(verb));
		}
	}
}

template <class Sess>
class CrossingSessionAdapter final : public CrossingSession, crossing_contract::SessionContract<Sess> {
public:
	CrossingSessionAdapter(CrossingSource &source_p, unique_ptr<Sess> session_p)
	    : source(source_p), session(std::move(session_p)) {
	}

	CrossingScan Run(ClientContext &context, const CrossingQuery &query) override {
		return session->Run(context, query);
	}
	void Ddl(ClientContext &context, const CrossingDdl &ddl) override {
		crossing_contract::DdlOn(*session, context, ddl, crossing_contract::HasDdl<Sess> {});
	}
	vector<string> Tables(const string &schema) override {
		return crossing_contract::TablesOn(*session, source, schema, crossing_contract::HasTables<Sess> {});
	}
	CrossingTable Describe(const string &schema, const string &name) override {
		auto table =
		    crossing_contract::DescribeOn(*session, source, schema, name, crossing_contract::HasDescribe<Sess> {});
		CheckDescribedVerbs<Sess>(name, table);
		return table;
	}
	void Commit() override {
		crossing_contract::CommitOn(*session, crossing_contract::HasCommit<Sess> {});
	}
	void Rollback() override {
		crossing_contract::RollbackOn(*session, crossing_contract::HasRollback<Sess> {});
	}

private:
	CrossingSource &source;
	unique_ptr<Sess> session;
};

template <class S>
class CrossingSourceAdapter final : public CrossingSource, crossing_contract::SourceContract<S> {
public:
	using Session = crossing_contract::SessionOf<S>;

	CrossingSourceAdapter(const CrossingIdentity &identity_p, unique_ptr<S> source_p)
	    : identity(identity_p), source(std::move(source_p)) {
	}

	const CrossingIdentity &Identity() const override {
		return identity;
	}
	vector<string> Schemas() override {
		return crossing_contract::SchemasOn(*source, crossing_contract::HasSchemas<S> {});
	}
	vector<string> Tables(const string &schema) override {
		return source->Tables(schema);
	}
	CrossingTable Describe(const string &schema, const string &name) override {
		auto table = source->Describe(schema, name);
		CheckDescribedVerbs<Session>(name, table);
		return table;
	}
	CrossingSchema DescribeSchema(const string &schema) override {
		auto described =
		    crossing_contract::DescribeSchemaOn(*source, schema, crossing_contract::HasDescribeSchema<S> {});
		if (!crossing_contract::HasDdl<Session>::value) {
			for (auto verb : described.verbs) {
				throw NotImplementedException("crossing: schema '%s' declares '%s' but the session has no Ddl", schema,
				                              CrossingVerbName(verb));
			}
		}
		return described;
	}
	CrossingPlan Plan(const CrossingPlanRequest &request) override {
		return source->Plan(request);
	}
	CrossingVerdict AcceptsCall(const Expression &expr) override {
		return crossing_contract::AcceptsCallOn(*source, expr, crossing_contract::HasAcceptsCall<S> {});
	}
	CrossingVerdict AcceptsType(const LogicalType &type) override {
		return crossing_contract::AcceptsTypeOn(*source, type, crossing_contract::HasAcceptsType<S> {});
	}
	CrossingVerdict AcceptsOperator(const LogicalOperator &op) override {
		return crossing_contract::AcceptsOperatorOn(*source, op, crossing_contract::HasAcceptsOperator<S> {});
	}
	CrossingVerdict AcceptsSeam(const vector<LogicalType> &types) override {
		return crossing_contract::AcceptsSeamOn(*source, types, crossing_contract::HasAcceptsSeam<S> {});
	}
	unique_ptr<CrossingSession> Begin(ClientContext &context) override {
		auto session = source->Begin(context);
		if (!session) {
			return nullptr;
		}
		return make_uniq<CrossingSessionAdapter<Session>>(*this, std::move(session));
	}
	void Detach(ClientContext &context) override {
		crossing_contract::DetachOn(*source, context, crossing_contract::HasDetach<S> {});
	}
	const void *Group() const override {
		auto group = crossing_contract::GroupOn(*source, crossing_contract::HasGroup<S> {});
		return group ? group : this;
	}

private:
	friend struct Crossing<S>;

	const CrossingIdentity &identity;
	unique_ptr<S> source;
};

template <class S>
struct Crossing {
	using Factory = std::function<unique_ptr<S>(ClientContext &context, AttachInfo &info)>;

	static void Register(ExtensionLoader &loader, const string &type, Factory factory) {
		RegisterCrossing(loader, type, Identity(),
		                 [factory](ClientContext &context, AttachInfo &info) { return Adapt(factory(context, info)); });
	}

	static void RegisterPass(DatabaseInstance &db) {
		RegisterCrossingPass(db, Identity());
	}

	static unique_ptr<CrossingSource> Adapt(unique_ptr<S> source) {
		if (!source) {
			return nullptr;
		}
		return make_uniq<CrossingSourceAdapter<S>>(Identity(), std::move(source));
	}

	static S &Native(CrossingSource &source) {
		if (&source.Identity() != &Identity()) {
			throw InternalException("crossing: the source is not one this Crossing<S> adapted");
		}
		return *static_cast<CrossingSourceAdapter<S> &>(source).source;
	}

private:
	static const CrossingIdentity &Identity() {
		static const CrossingIdentity identity;
		return identity;
	}
};

} // namespace duckdb
