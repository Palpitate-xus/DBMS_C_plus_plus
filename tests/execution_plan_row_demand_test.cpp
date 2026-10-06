#include "executor/ExecutionPlan.h"

#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace {
struct Trace { size_t next = 0; size_t closed = 0; };
class CountedRows final : public dbms::Operator {
public:
    CountedRows(Trace& trace, bool thirdThrows) : trace_(trace), thirdThrows_(thirdThrows) {}
    bool open() override { return true; }
    bool next(std::string& row) override {
        ++trace_.next;
        if (trace_.next == 3 && thirdThrows_) throw std::runtime_error("third row");
        if (trace_.next > 3) return false;
        row = std::to_string(trace_.next);
        return true;
    }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells,
                           std::vector<bool>& nulls) const override {
        cells = {std::to_string(trace_.next), ""};
        nulls = {false, true};
        return true;
    }
    void close() override { ++trace_.closed; }
private:
    Trace& trace_;
    bool thirdThrows_;
};
}

int main() {
    for (size_t demand : {size_t{1}, size_t{2}}) {
        Trace trace;
        const auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<CountedRows>(trace, true), demand);
        assert(result.ok && result.rows.size() == demand);
        assert(trace.next == demand && trace.closed == 1);
        assert(result.structuredRowsAvailable && result.structuredRows.size() == demand);
        assert(result.structuredNulls.back() == std::vector<bool>({false, true}));
    }
    Trace all;
    const auto result = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<CountedRows>(all, false));
    assert(result.ok && result.rows.size() == 3 && all.next == 4 && all.closed == 1);
    Trace failed;
    bool threw = false;
    try {
        dbms::QueryPlanner::executePlanChecked(std::make_unique<CountedRows>(failed, true));
    } catch (const std::runtime_error&) { threw = true; }
    assert(threw && failed.next == 3 && failed.closed == 1);
    std::cout << "[EXECUTION PLAN ROW DEMAND] passed\n";
}
