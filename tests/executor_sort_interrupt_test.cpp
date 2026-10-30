#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "parser/query_binding.h"
#include "Session.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <new>

namespace {
thread_local SessionInterruptState* requestedState = nullptr;
thread_local bool allocationArmed = false;
thread_local size_t allocationSignals = 0;
thread_local int requestedMode = 0;
void requestInterrupt() noexcept {
    if (!requestedState || requestedMode < 2) return;
    requestedState->timeoutRequested.store(requestedMode == 3, std::memory_order_release);
    requestedState->cancelRequested.store(true, std::memory_order_release);
}
}

// Observe the real libstdc++ stable_sort scratch allocation. The allocation
// and all comparisons remain real; only a deterministic statement flag is
// published between draining the input and the first comparison.
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    void* result = nullptr;
    try { result = ::operator new(size); } catch (...) { return nullptr; }
    if (allocationArmed) {
        allocationArmed = false;
        ++allocationSignals;
        requestInterrupt();
    }
    return result;
}

class IntegerSource final : public dbms::Operator {
public:
    IntegerSource(size_t count, size_t& eof) : count_(count), eof_(eof) {}
    bool open() override { position_ = 0; return true; }
    bool next(std::string& row) override {
        if (position_ == count_) { ++eof_; requestInterrupt(); return false; }
        const int32_t value = static_cast<int32_t>(count_ - position_++);
        row.resize(sizeof(value));
        std::memcpy(row.data(), &value, sizeof(value));
        return true;
    }
    void close() override {}
private:
    size_t count_, position_ = 0;
    size_t& eof_;
};

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& name) {
        return QueryRelationMetadata{"public", name, {{"n", "integer"}}, {}};
    };
    TableSchema schema;
    schema.tablename = "sort_input";
    schema.append(makeIntColumn("n", false, 2));
    assert(schema.cols[0].dataType == "int" && schema.cols[0].dsize == sizeof(int32_t));
    size_t checked = 0, failures = 0;
    const auto run = [&](bool prepared, bool atAllocation, size_t count, bool ascending, int mode) {
        const auto previous = currentQueryInterruptState();
        auto state = mode ? std::make_shared<SessionInterruptState>() : nullptr;
        setCurrentQueryInterruptState(state);
        requestedState = state.get(); requestedMode = mode;
        allocationArmed = false; allocationSignals = 0;
        size_t eof = 0;
        OpPtr plan;
        if (prepared) {
            auto query = std::make_shared<PreparedQuery>(prepareQuery(
                std::string("SELECT n FROM sort_input ORDER BY n ") + (ascending ? "ASC" : "DESC"), {}, metadata));
            auto* select = dynamic_cast<SelectStmt*>(query->ast.get());
            assert(select && query->sourceRanges.size() == 1);
            auto binding = std::make_shared<PreparedQueryExecution>(query, &engine, "");
            const auto ordinal = query->sourceRanges.front().ordinal;
            auto source = std::make_unique<PreparedSourceContextsOp>(
                [&, binding, ordinal](size_t position, RowContext& row) {
                    if (position == count) {
                        ++eof;
                        if (atAllocation) allocationArmed = true;
                        else requestInterrupt();
                        return false;
                    }
                    row = RowContext{};
                    binding->setSourceRow(row, ordinal,
                        {ExprValue("integer", std::to_string(count - position))});
                    return true;
                });
            plan = QueryPlanner::buildPreparedSelectPlan(&engine, "", query, select, TableSchema{}, std::move(source));
        } else {
            plan = std::make_unique<SortOp>(std::make_unique<IntegerSource>(count, eof), schema, "n", ascending);
        }
        std::string actualState, message;
        bool opened = false, ordered = true;
        std::vector<std::string> values;
        try {
            opened = plan->open();
            // Observe the open boundary itself: an outer checked cursor can
            // only notice cancellation AFTER an uncooperative sort finishes.
            if (mode < 2 && opened) {
                std::string row;
                while (plan->next(row)) {
                    if (prepared) {
                        std::vector<ExprValue> cells;
                        if (!plan->lastStructuredValues(cells) || cells.size() != 1 || cells[0].isNull) { ordered = false; break; }
                        values.push_back(cells[0].value);
                    } else values.push_back(StorageEngine::extractColumnValueStatic(row, schema, 0));
                }
                ordered = ordered && values.size() == count;
                for (size_t i = 0; ordered && i < values.size(); ++i)
                    ordered = values[i] == std::to_string(ascending ? i + 1 : count - i);
            }
        } catch (const DbError& error) { actualState = error.sqlState(); message = error.message(); }
          catch (const std::exception& error) { actualState = "UNEXPECTED"; message = error.what(); }
        const bool prerequisite = eof == 1 && (!atAllocation || allocationSignals == 1);
        allocationArmed = false; requestedState = nullptr;
        setCurrentQueryInterruptState(previous);
        bool closed = true;
        try { plan->close(); } catch (...) { closed = false; }
        const bool valid = prerequisite && closed && (mode < 2 ? opened && actualState.empty() && ordered
            : actualState == "57014" && message == (mode == 3
                ? "canceling statement due to statement timeout" : "canceling statement due to user request"));
        ++checked;
        if (!valid) {
            ++failures;
            std::cout << "SORT_INTERRUPT_FAILURE prepared=" << prepared << " allocation=" << atAllocation
                      << " rows=" << count << " asc=" << ascending << " mode=" << mode
                      << " eof=" << eof << " allocations=" << allocationSignals
                      << " opened=" << opened << " ordered=" << ordered << " state=" << actualState
                      << " message=" << message << '\n';
        }
    };
    for (bool prepared : {false, true})
        for (size_t count : {size_t(0), size_t(1), size_t(17), size_t(257)})
            for (bool ascending : {true, false})
                for (int mode : {0, 1, 2, 3}) run(prepared, false, count, ascending, mode);
    for (size_t count : {size_t(17), size_t(257)})
        for (bool ascending : {true, false})
            for (int mode : {0, 1, 2, 3}) run(true, true, count, ascending, mode);
    assert(checked == 80);
    std::cout << "SORT_INTERRUPT_NATIVE_COMPLETE " << checked << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
