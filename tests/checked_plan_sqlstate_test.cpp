#include "Session.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"

#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace {
struct Calls { unsigned opens = 0, nexts = 0, closes = 0, destroys = 0; };
class Probe final : public dbms::Operator {
public:
    enum Failure { None, Metadata, Open, OpenFalse, Next, Cells, Reported, Unknown, CommitPhase };
    Probe(std::shared_ptr<Calls> calls, Failure failure, bool closeFails = false)
        : calls_(std::move(calls)), failure_(failure), closeFails_(closeFails) {}
    ~Probe() override { ++calls_->destroys; }
    bool supportsStructuredRows() const override {
        if (failure_ == Metadata) fail();
        return true;
    }
    bool open() override {
        ++calls_->opens;
        if (failure_ == Open) fail();
        if (failure_ == OpenFalse) { setError("operator open failure"); return false; }
        if (failure_ == CommitPhase)
            throw dbms::StatementCommitError("23503", "deferred constraint failure");
        return true;
    }
    bool next(std::string& row) override {
        ++calls_->nexts;
        if (calls_->nexts == 1) { row = "one"; return true; }
        if (failure_ == Next) fail();
        if (failure_ == Unknown) throw std::logic_error("primary non-SQL failure");
        if (failure_ == Reported) setError("operator reported failure");
        return false;
    }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override {
        if (failure_ == Cells) fail();
        cells = {"one"}; nulls = {false}; return true;
    }
    void close() override {
        ++calls_->closes;
        if (closeFails_) throw dbms::DbError("58030", "secondary close failure");
    }
private:
    [[noreturn]] static void fail() {
        throw dbms::DbError("P1234", "primary diagnostic contains SQLSTATE 22P02");
    }
    std::shared_ptr<Calls> calls_;
    Failure failure_;
    bool closeFails_;
};
void checkFailure(const dbms::PlanExecutionResult& result, const std::string& state) {
    assert(!result.ok && result.errorSqlState == state);
    assert(result.rows.empty() && result.structuredRows.empty() &&
           result.structuredNulls.empty() && !result.structuredRowsAvailable);
    bool thrown = false;
    try { result.throwIfFailed(); }
    catch (const dbms::DbError& error) {
        assert(error.sqlState() == state && error.message() == result.errorMessage);
        thrown = true;
    }
    assert(thrown);
}
} // namespace

int main() {
    for (const auto failure : {Probe::Metadata, Probe::Open, Probe::Next, Probe::Cells}) {
        for (const bool closeFails : {false, true}) {
            auto calls = std::make_shared<Calls>();
            const auto result = dbms::QueryPlanner::executePlanChecked(
                std::make_unique<Probe>(calls, failure, closeFails));
            checkFailure(result, "P1234");
            assert(result.errorMessage == "primary diagnostic contains SQLSTATE 22P02");
            assert(calls->closes == 1 && calls->destroys == 1);
        }
    }
    for (const bool closeFails : {false, true}) {
        auto openCalls = std::make_shared<Calls>();
        const auto openFailure = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<Probe>(openCalls, Probe::OpenFalse, closeFails));
        checkFailure(openFailure, "XX000");
        assert(openFailure.errorMessage == "operator open failure");
        assert(openCalls->closes == 1 && openCalls->destroys == 1);
        auto calls = std::make_shared<Calls>();
        const auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<Probe>(calls, Probe::Reported, closeFails));
        checkFailure(result, "XX000");
        assert(result.errorMessage == "operator reported failure");
        assert(calls->closes == 1 && calls->destroys == 1);
    }
    auto calls = std::make_shared<Calls>();
    const auto closeFailure = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<Probe>(calls, Probe::None, true));
    checkFailure(closeFailure, "58030");
    assert(calls->closes == 1 && calls->destroys == 1);
    calls = std::make_shared<Calls>();
    bool unknown = false;
    try {
        (void)dbms::QueryPlanner::executePlanChecked(
            std::make_unique<Probe>(calls, Probe::Unknown, true));
    } catch (const std::logic_error& error) {
        assert(std::string(error.what()) == "primary non-SQL failure");
        unknown = true;
    }
    assert(unknown && calls->closes == 1 && calls->destroys == 1);
    checkFailure(dbms::QueryPlanner::executePlanChecked({}), "XX000");
    for (const bool terminate : {false, true}) {
        auto interrupt = std::make_shared<SessionInterruptState>();
        interrupt->cancelRequested = !terminate;
        interrupt->terminateRequested = terminate;
        dbms::setCurrentQueryInterruptState(interrupt);
        calls = std::make_shared<Calls>();
        const auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<Probe>(calls, Probe::None));
        dbms::setCurrentQueryInterruptState({});
        checkFailure(result, terminate ? "57P01" : "57014");
        assert(calls->opens == 0 && calls->closes == 1 && calls->destroys == 1);
    }
    calls = std::make_shared<Calls>();
    const auto limited = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<Probe>(calls, Probe::Next), 1);
    assert(limited.ok && limited.rows == std::vector<std::string>{"one"});
    assert(limited.structuredRows == std::vector<std::vector<std::string>>{{"one"}});
    assert(limited.structuredNulls == std::vector<std::vector<bool>>{{false}});
    assert(limited.errorSqlState.empty() && calls->nexts == 1 && calls->closes == 1);
    limited.throwIfFailed();
    calls = std::make_shared<Calls>();
    const auto commitFailure = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<Probe>(calls, Probe::CommitPhase, true));
    checkFailure(commitFailure, "23503");
    bool phasePreserved = false;
    try { commitFailure.throwIfFailed(); }
    catch (const dbms::StatementCommitError& error) {
        assert(error.sqlState() == "23503" && error.message() == "deferred constraint failure");
        phasePreserved = true;
    }
    assert(phasePreserved && calls->closes == 1 && calls->destroys == 1);
    std::cout << "[CHECKED PLAN SQLSTATE] typed metadata/partial rows/cleanup/interrupt/demand passed\n";
}
