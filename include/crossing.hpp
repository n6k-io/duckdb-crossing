#pragma once

// The API a source implements. See IMPLEMENTING.md.
//
// Vocabulary. One word per concept, everywhere:
//   crossing   the library, and a scan or write that runs on a source
//   source     the database the implementor's type S fronts; the target is DuckDB
//   identity   what tells one copy of crossing's nodes from another copy's
//   attach     one ATTACH of a source, and the object that serves it to a catalog
//   session    one target transaction's side of the source; every read and write in it goes through it
//   scan       one Read, which opens a reader per partition
//   waker      what a reader or writer that has nothing yet calls when it does
//   fragment   the plan a crossing carries to the source
//   carrier    the bind data a fragment rides in, hung off a LogicalGet
//   floor      the source's own scan at the bottom of a read fragment; sealed once built
//   seam       the hole in a write fragment where the rows go
//   fence      the logical operator standing in for a write the source runs alone
//   feed       what a fence takes rows from
//   fold       moving a read subtree into its fragment
//   fill       moving a feed into a seam
//   remainder  what a fill leaves on the target
//   label      which source a subtree could run on
//   rule       one operator type's answer to whether it crosses at all
//   key alias  the virtual column a key column stands behind so the binder addresses rows by it
//   frozen     a fragment the pass must leave as bound
//   verdict    a yes, or a no with a reason
//   obstacle   why a write cannot run wholly on its source

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
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
struct AttachInfo;
struct CreateInfo;
struct AlterInfo;
struct DropInfo;

//! DELETE_ trails an underscore: DELETE is a macro in some Windows SDK headers.
enum class CrossingVerb : uint8_t { SELECT = 0, INSERT = 1, UPDATE = 2, DELETE_ = 3, CREATE = 4, ALTER = 5, DROP = 6 };

//! Lowercase wire spelling, as reported by the permissions function.
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
	//! A verb absent here is refused before the statement runs; the source is never asked about it.
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
unique_ptr<LogicalOperator> MakeFloorNode(string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types);

optional_ptr<const CrossingFloor> FloorOf(const LogicalOperator &op);

//! Every floor in `plan`, and every scan of a catalog table, with the columns each is read for.
vector<CrossingTableUse> CrossingTablesOf(const LogicalOperator &plan);

//! Bytes a plan can travel as. The receiving side needs RegisterCrossingPlanFunctions on its own
//! instance.
string SerializeCrossingPlan(const LogicalOperator &plan);
unique_ptr<LogicalOperator> DeserializeCrossingPlan(ClientContext &context, const string &bytes);

//! Makes crossing_floor and crossing_seam resolvable by name, which DeserializeCrossingPlan needs.
void RegisterCrossingPlanFunctions(DatabaseInstance &db);

//! What a read and a write both carry. Lives as long as the scan or writer it was handed to.
struct CrossingQuery {
	CrossingQuery(CrossingVerb kind_p, const LogicalOperator &plan_p) : kind(kind_p), plan(plan_p) {
	}

	//! SELECT on a read, the write verb on a write.
	CrossingVerb kind;
	//! Not yet optimized by DuckDB. A read is the floor with the crossed work on it, under a
	//! projection crossing adds. A write is the statement with its seam already filled: by a plan
	//! of yours when the rows come from this source, otherwise by the rows the target gathered. Run
	//! it as it is.
	const LogicalOperator &plan;
	//! The row types: what a read produces, what a write is handed.
	vector<LogicalType> types;
	bool ordered = false;
	//! Every table the plan reads. On a write whose seam the target filled, none.
	vector<CrossingTableUse> tables;
	//! The table a write writes, with the columns the seam carries. Empty on a read.
	CrossingTableUse written;
	//! What an update or a delete addresses rows by. An insert names none.
	vector<string> key_columns;
	//! The columns a write sets. An insert names every column of the table, in the order Describe
	//! declared them. An update names the columns it sets. A delete names none.
	vector<string> set_columns;
};

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types);
unique_ptr<LogicalOperator> MakeSeamNode(vector<LogicalType> types);

//! The rows a materialised-rows node holds: the seam the target filled, or a VALUES list.
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

//! What one pull produced. ROWS: `chunk` is filled; an empty chunk ends the partition like DONE.
//! DONE: the partition is exhausted. WAIT: nothing yet; Wake() the waker and pull again.
struct CrossingReadResult {
	enum class Outcome : uint8_t { ROWS, DONE, WAIT };

	Outcome outcome = Outcome::DONE;

	static CrossingReadResult Rows() {
		CrossingReadResult result;
		result.outcome = Outcome::ROWS;
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

//! One partition's rows, pulled by one thread. Fills the chunk with the next rows.
using CrossingReader = std::function<CrossingReadResult(ClientContext &context, DataChunk &chunk, CrossingWaker waker)>;

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

struct CrossingSeam {
	vector<string> key_columns;
	vector<string> set_columns;
	vector<LogicalType> types;
};

//! What the source is asked to plan. SELECT wants a scan of the table emitting one column per
//! column of `described`, in that order. A write wants the statement with a seam node
//! (MakeSeamNode) where the rows go.
struct CrossingPlanRequest {
	CrossingVerb verb;
	string schema;
	string table;
	optional_ptr<const CrossingTable> described;
	CrossingSeam seam;
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

	virtual CrossingScan Read(ClientContext &context, const CrossingQuery &query) = 0;
	virtual CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	virtual void Ddl(ClientContext &context, const CrossingDdl &ddl);
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
CROSSING_MEMBER(Begin, "unique_ptr<Session> Begin(ClientContext &context)", unique_ptr<SessionOf<T>>, ClientContext &);
CROSSING_MEMBER(Detach, "void Detach(ClientContext &context)", void, ClientContext &);
CROSSING_MEMBER(Read, "CrossingScan Read(ClientContext &context, const CrossingQuery &query)", CrossingScan,
                ClientContext &, const CrossingQuery &);
CROSSING_MEMBER(Write, "CrossingWriter Write(ClientContext &context, const CrossingQuery &query)", CrossingWriter,
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
                        CheckDetach<S, false> {
	static_assert(!std::is_final<S>::value, "crossing: a source type must not be final");
	static_assert(!std::is_void<SessionOf<S>>::value, "crossing: S needs `using Session = <your session type>;`");
};

template <class Sess>
struct SessionContract : CheckRead<Sess, true>,
                         CheckWrite<Sess, false>,
                         CheckDdl<Sess, false>,
                         CheckCommit<Sess, false>,
                         CheckRollback<Sess, false> {
	static_assert(!std::is_final<Sess>::value, "crossing: a session type must not be final");
};

template <class Sess>
CrossingWriter WriteOn(Sess &session, ClientContext &context, const CrossingQuery &query, std::true_type) {
	return session.Write(context, query);
}
template <class Sess>
CrossingWriter WriteOn(Sess &, ClientContext &, const CrossingQuery &, std::false_type) {
	throw NotImplementedException("crossing: this source's session has no Write");
}
template <class Sess>
void DdlOn(Sess &session, ClientContext &context, const CrossingDdl &ddl, std::true_type) {
	session.Ddl(context, ddl);
}
template <class Sess>
void DdlOn(Sess &, ClientContext &, const CrossingDdl &, std::false_type) {
	throw NotImplementedException("crossing: this source's session has no Ddl");
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
void DetachOn(S &source, ClientContext &context, std::true_type) {
	source.Detach(context);
}
template <class S>
void DetachOn(S &, ClientContext &, std::false_type) {
}

} // namespace crossing_contract

template <class S>
struct Crossing;

template <class Sess>
class CrossingSessionAdapter final : public CrossingSession, crossing_contract::SessionContract<Sess> {
public:
	explicit CrossingSessionAdapter(unique_ptr<Sess> session_p) : session(std::move(session_p)) {
	}

	CrossingScan Read(ClientContext &context, const CrossingQuery &query) override {
		return session->Read(context, query);
	}
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query) override {
		return crossing_contract::WriteOn(*session, context, query, crossing_contract::HasWrite<Sess> {});
	}
	void Ddl(ClientContext &context, const CrossingDdl &ddl) override {
		crossing_contract::DdlOn(*session, context, ddl, crossing_contract::HasDdl<Sess> {});
	}
	void Commit() override {
		crossing_contract::CommitOn(*session, crossing_contract::HasCommit<Sess> {});
	}
	void Rollback() override {
		crossing_contract::RollbackOn(*session, crossing_contract::HasRollback<Sess> {});
	}

private:
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
		for (auto verb : table.verbs) {
			if (IsDdlVerb(verb)) {
				if (!crossing_contract::HasDdl<Session>::value) {
					throw NotImplementedException("crossing: '%s' declares '%s' but the session has no Ddl", name,
					                              CrossingVerbName(verb));
				}
			} else if (verb != CrossingVerb::SELECT && !crossing_contract::HasWrite<Session>::value) {
				throw NotImplementedException("crossing: '%s' declares '%s' but the session has no Write", name,
				                              CrossingVerbName(verb));
			}
		}
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
	unique_ptr<CrossingSession> Begin(ClientContext &context) override {
		auto session = source->Begin(context);
		if (!session) {
			return nullptr;
		}
		return make_uniq<CrossingSessionAdapter<Session>>(std::move(session));
	}
	void Detach(ClientContext &context) override {
		crossing_contract::DetachOn(*source, context, crossing_contract::HasDetach<S> {});
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
