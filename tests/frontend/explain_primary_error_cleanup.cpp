// Compile the actual frontend TU, not a copied executor algorithm. The renamed
// CLI/server entry is never called; all globals/helpers are the real main.cpp
// definitions, so this driver must link without tests/test_stubs.cpp.
#define main dbms_frontend_program_main_not_invoked
#include "../../src/main.cpp"
#undef main

#include <sys/resource.h>
#include <typeinfo>

namespace explain_cleanup_test {
struct Trace {
    const dbms::Operator* identity = nullptr;
    size_t opens = 0, nexts = 0, closes = 0, renders = 0, destroys = 0;
    bool sameIdentity = true, recordNexts = true;
    std::vector<std::string> events;
    void record(const dbms::Operator* pointer, const char* event) {
        sameIdentity = sameIdentity && pointer == identity;
        if (recordNexts || std::string(event) != "next") events.emplace_back(event);
    }
};
enum class Stage { None, Open, FirstNext, LateNext, OpenFalse, Reported };
enum class Error { Divide, Cardinality, Commit, Logic, Close };
std::exception_ptr makeFailure(Error kind) {
    switch (kind) {
    case Error::Divide: return std::make_exception_ptr(dbms::DbError("22012", "primary division diagnostic; embedded SQLSTATE 58030\ncomplete message"));
    case Error::Cardinality: return std::make_exception_ptr(dbms::DbError("21000", "primary cardinality diagnostic; embedded SQLSTATE 22012\ncomplete message"));
    case Error::Commit: return std::make_exception_ptr(dbms::StatementCommitError("23503", "primary statement commit diagnostic; embedded SQLSTATE 58030\ncomplete message"));
    case Error::Logic: return std::make_exception_ptr(std::logic_error("primary logic diagnostic; embedded SQLSTATE 22012\ncomplete message"));
    case Error::Close: return std::make_exception_ptr(dbms::DbError("58030", "secondary close diagnostic; embedded SQLSTATE 22012\ncomplete message"));
    }
    std::abort();
}
class Probe final : public dbms::Operator {
    std::shared_ptr<Trace> trace_;
    Stage stage_;
    std::exception_ptr primary_, cleanup_;
    size_t rows_, width_;
public:
    Probe(std::shared_ptr<Trace> trace, Stage stage, std::exception_ptr primary,
          std::exception_ptr cleanup, size_t rows = 1, size_t width = 3)
        : trace_(std::move(trace)), stage_(stage), primary_(std::move(primary)),
          cleanup_(std::move(cleanup)), rows_(rows), width_(width) { trace_->identity = this; }
    ~Probe() override { ++trace_->destroys; trace_->record(this, "destroy"); }
    bool open() override {
        ++trace_->opens; trace_->record(this, "open");
        if (stage_ == Stage::Open) std::rethrow_exception(primary_);
        if (stage_ == Stage::OpenFalse) { setError("reported open diagnostic; embedded SQLSTATE 58030"); return false; }
        return true;
    }
    bool next(std::string& row) override {
        ++trace_->nexts; trace_->record(this, "next");
        if (stage_ == Stage::FirstNext || (stage_ == Stage::LateNext && trace_->nexts == 2))
            std::rethrow_exception(primary_);
        if (trace_->nexts <= rows_) { row.assign(width_, 'x'); return true; }
        if (stage_ == Stage::Reported) setError("reported next diagnostic; embedded SQLSTATE 22012");
        return false;
    }
    void close() override {
        ++trace_->closes; trace_->record(this, "close");
        if (cleanup_) std::rethrow_exception(cleanup_);
    }
    std::string preparedPlanNodeName() const override {
        ++trace_->renders; trace_->record(this, "render"); return "ExplainCleanupProbe";
    }
};
class RealGraph final : public dbms::Operator {
    dbms::OpPtr child_;
    std::shared_ptr<Trace> trace_;
public:
    RealGraph(dbms::OpPtr child, std::shared_ptr<Trace> trace)
        : child_(std::move(child)), trace_(std::move(trace)) { trace_->identity = this; }
    ~RealGraph() override { ++trace_->destroys; trace_->record(this, "destroy"); }
    bool open() override { ++trace_->opens; trace_->record(this, "open"); return child_->open(); }
    bool next(std::string& row) override { ++trace_->nexts; trace_->record(this, "next"); return child_->next(row); }
    void close() override { ++trace_->closes; trace_->record(this, "close"); child_->close(); }
    bool hasError() const override { return child_->hasError(); }
    std::string errorMessage() const override { return child_->errorMessage(); }
    std::string preparedPlanNodeName() const override {
        ++trace_->renders; trace_->record(this, "render"); return "CleanupRealGraph";
    }
    std::vector<dbms::Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};
struct Outcome { std::exception_ptr failure; std::string direct, pending; };
Outcome publish(dbms::OpPtr tree, Session& session, bool json, bool pending, bool analyze = true) {
    Outcome result;
    result.pending = "prior publication\n";
    std::ostringstream captured;
    auto* previousCout = std::cout.rdbuf(captured.rdbuf());
    auto* previousPending = pendingExplainOutput;
    const auto previousDepth = pendingExplainDepth;
    pendingExplainOutput = pending ? &result.pending : nullptr;
    pendingExplainDepth = executeDepth;
    dbms::QueryPlanner::ExplainOptions options;
    options.analyze = analyze; options.timing = false; options.buffers = true;
    try { assert(!publishExplainPlan(std::move(tree), "same real helper root", session, options, json)); }
    catch (...) { result.failure = std::current_exception(); }
    pendingExplainOutput = previousPending; pendingExplainDepth = previousDepth;
    std::cout.rdbuf(previousCout);
    result.direct = captured.str();
    return result;
}
size_t failures = 0;
void require(bool value, const std::string& label) {
    if (!value) { ++failures; std::cerr << "EXPLAIN_CLEANUP_FAILURE " << label << '\n'; }
}
void sameException(const std::exception_ptr& actual, const std::exception_ptr& expected, const std::string& label) {
    require(bool(actual), label + " missing exception");
    if (!actual) return;
    const std::exception* original = nullptr;
    try { std::rethrow_exception(expected); } catch (const std::exception& error) { original = &error; }
    try { std::rethrow_exception(actual); }
    catch (const std::exception& error) {
        require(&error == original && typeid(error) == typeid(*original), label + " exception object/subtype");
        require(std::string(error.what()) == original->what(), label + " full diagnostic");
        const auto* sql = dynamic_cast<const dbms::DbError*>(&error);
        const auto* originalSql = dynamic_cast<const dbms::DbError*>(original);
        require(bool(sql) == bool(originalSql), label + " SQL exception category");
        if (sql && originalSql) require(sql->sqlState() == originalSql->sqlState() && sql->message() == originalSql->message(), label + " SQLSTATE/full message");
    } catch (...) { require(false, label + " nonstandard exception"); }
}
void failedOutput(const Outcome& result, const Trace& trace, const std::string& label) {
    require(result.direct.empty() && result.pending == "prior publication\n", label + " partial EXPLAIN publication");
    require(trace.sameIdentity && trace.closes == 1 && trace.destroys == 1 && trace.renders == 0, label + " same root/one close/no render");
}
} // namespace explain_cleanup_test

int main() {
    using namespace explain_cleanup_test;
    dbms::TypeRegistry::instance().bootstrap();
    g_config.enableQueryPlanCache = false;
    Session session; session.currentDB = "__t_explain_primary_cleanup";
    for (bool json : {false, true}) for (bool pending : {false, true}) {
        for (auto stage : {Stage::Open, Stage::FirstNext, Stage::LateNext})
            for (auto kind : {Error::Divide, Error::Cardinality, Error::Commit, Error::Logic})
                for (bool closeFails : {false, true}) {
                    auto trace = std::make_shared<Trace>();
                    auto primary = makeFailure(kind);
                    const auto label = std::to_string(int(stage)) + ":" + std::to_string(int(kind)) + ":" + std::to_string(json) + ":" + std::to_string(pending) + ":" + std::to_string(closeFails);
                    auto result = publish(std::make_unique<Probe>(trace, stage, primary, closeFails ? makeFailure(Error::Close) : nullptr), session, json, pending);
                    sameException(result.failure, primary, label); failedOutput(result, *trace, label);
                    const auto expected = stage == Stage::Open ? std::vector<std::string>{"open", "close", "destroy"} : stage == Stage::FirstNext ? std::vector<std::string>{"open", "next", "close", "destroy"} : std::vector<std::string>{"open", "next", "next", "close", "destroy"};
                    require(trace->events == expected, label + " original ordered events");
                }
        for (auto stage : {Stage::OpenFalse, Stage::Reported}) for (bool closeFails : {false, true}) {
            auto trace = std::make_shared<Trace>();
            auto result = publish(std::make_unique<Probe>(trace, stage, nullptr, closeFails ? makeFailure(Error::Close) : nullptr), session, json, pending);
            failedOutput(result, *trace, "reported failure");
            require(bool(result.failure), "reported failure missing exception");
            if (result.failure) try { std::rethrow_exception(result.failure); }
            catch (const dbms::DbError& error) {
                require(error.sqlState() == "XX000" && error.message() == (stage == Stage::OpenFalse ? "reported open diagnostic; embedded SQLSTATE 58030" : "reported next diagnostic; embedded SQLSTATE 22012"), "reported exact state/message");
            } catch (...) { require(false, "reported subtype"); }
            require(trace->opens == 1 && trace->nexts == (stage == Stage::OpenFalse ? 0 : 2), "reported demand/events");
        }
        for (auto kind : {Error::Close, Error::Commit, Error::Logic}) {
            auto trace = std::make_shared<Trace>(); auto cleanup = makeFailure(kind);
            const auto result = publish(std::make_unique<Probe>(trace, Stage::None, nullptr, cleanup), session, json, pending);
            sameException(result.failure, cleanup, "success close failure"); failedOutput(result, *trace, "success close failure");
            require(trace->events == std::vector<std::string>({"open", "next", "next", "close", "destroy"}), "success close is sole primary/no retry");
        }
        auto trace = std::make_shared<Trace>();
        auto plain = publish(std::make_unique<Probe>(trace, Stage::Open, makeFailure(Error::Divide), makeFailure(Error::Close)), session, json, pending, false);
        require(!plain.failure && trace->opens == 0 && trace->nexts == 0 && trace->closes == 0 && trace->renders > 0 && trace->destroys == 1, "plain EXPLAIN must not execute");
        require((pending ? plain.pending : plain.direct).find("ExplainCleanupProbe") != std::string::npos, "plain true root rendered");
    }
    // >512MiB of generated rows must not be accumulated by ANALYZE. Trace
    // recording is also bounded; only the reusable row string is retained.
    rusage before{}, after{}; getrusage(RUSAGE_SELF, &before);
    auto stream = std::make_shared<Trace>(); stream->recordNexts = false;
    auto streamed = publish(std::make_unique<Probe>(stream, Stage::None, nullptr, nullptr, 8192, 65536), session, false, true);
    getrusage(RUSAGE_SELF, &after);
    require(!streamed.failure && stream->opens == 1 && stream->nexts == 8193 && stream->closes == 1 && stream->destroys == 1 && stream->sameIdentity, "streaming demand/one execution/one close");
    require(streamed.pending.find("Actual rows: 8192") != std::string::npos && streamed.direct.empty(), "streamed exact actual count/publication");
    require(after.ru_maxrss - before.ru_maxrss < 65536, "streaming memory grew 64MiB (512MiB input)");
    require(g_engine.createDatabase(session.currentDB, "utf8") == DBStatus::OK, "real graph database");
    TableSchema table; table.append(makeIntColumn("id", false, 4));
    require(g_engine.createTable(session.currentDB, "items", table) == DBStatus::OK, "real graph table");
    require(g_engine.insertRow(session.currentDB, "items", {{"id", "1"}}) == DBStatus::OK && g_engine.insertRow(session.currentDB, "items", {{"id", "2"}}) == DBStatus::OK, "real graph rows");
    for (bool json : {false, true}) for (bool pending : {false, true}) {
        auto prepared = g_engine.prepareBoundQuery(session.currentDB, "SELECT id+1 FROM items ORDER BY id");
        auto actual = dbms::QueryPlanner::buildPreparedSelectPlan(&g_engine, session.currentDB, "items", std::move(prepared));
        auto trace = std::make_shared<Trace>();
        auto result = publish(std::make_unique<RealGraph>(std::move(actual), trace), session, json, pending);
        const auto& output = pending ? result.pending : result.direct;
        require(!result.failure && trace->opens == 1 && trace->nexts == 3 && trace->closes == 1 && trace->renders > 0 && trace->destroys == 1 && trace->sameIdentity, "real graph one owner/execution/close");
        require(output.find("CleanupRealGraph") != std::string::npos && output.find("TypedProject") != std::string::npos && output.find(json ? "\"actualRows\": 2" : "Actual rows: 2") != std::string::npos, "same successful real graph rendered");
        require(pending ? result.direct.empty() : result.pending == "prior publication\n", "success publication destination");
    }
    require(!g_engine.inTransaction(), "no leaked owner");
    std::cout << "EXPLAIN_PRIMARY_ERROR_CLEANUP_FAILURES=" << failures << '\n';
    return failures ? 1 : 0;
}
