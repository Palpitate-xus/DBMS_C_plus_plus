#include "executor/ExecutionPlan.h"

#include <cassert>
#include <iostream>
#include <stdexcept>

class PrefixSource final : public dbms::Operator {
    int mode_;
    size_t position_ = 0;
    size_t& closed_;
    std::exception_ptr external_;
public:
    PrefixSource(int mode, size_t& closed, std::exception_ptr external)
        : mode_(mode), closed_(closed), external_(std::move(external)) {}
    bool open() override {
        position_ = 0; clearError();
        if (mode_ == 3) throw dbms::DbError("22003", "open failure");
        return true;
    }
    bool next(std::string& row) override {
        if (position_ == 3) {
            if (mode_ == 0 || mode_ == 5) throw dbms::DbError("22012", "row failure");
            if (mode_ == 1) setError("reported row failure");
            if (mode_ == 4) std::rethrow_exception(external_);
            return false;
        }
        row = "row" + std::to_string(position_++);
        return true;
    }
    void close() override {
        ++closed_;
        if (mode_ == 2 || mode_ == 5) throw dbms::DbError("22023", "close failure");
    }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        assert(position_ > 0 && position_ <= 3);
        static const std::vector<std::vector<std::string>> values = {
            {"", ""}, {"NULL", "β space"}, {"7", ""}};
        cells = values[position_ - 1]; nulls = {position_ == 1, false}; return true;
    }
};

int main() {
    using namespace dbms;
    const auto external = std::make_exception_ptr(std::logic_error("external failure"));
    const std::vector<std::vector<std::string>> values = {{"", ""}, {"NULL", "β space"}, {"7", ""}};
    const std::vector<std::vector<bool>> nulls = {{true, false}, {false, false}, {false, false}};
    size_t controls = 0, failures = 0;
    for (bool prefix : {false, true}) for (int mode = 0; mode < 8; ++mode) {
        size_t closed = 0;
        const size_t maximum = mode == 6 ? 1 : 0;
        bool valid = false;
        try {
            auto source = std::make_unique<PrefixSource>(mode, closed, external);
            const auto result = prefix ? QueryPlanner::executePlanWithPrefix(std::move(source), maximum)
                                       : QueryPlanner::executePlanChecked(std::move(source), maximum);
            const bool success = mode >= 6;
            const size_t count = success ? (maximum ? maximum : 3) : prefix && mode != 3 ? 3 : 0;
            valid = result.ok == success && result.rows.size() == count &&
                result.structuredRows == std::vector<std::vector<std::string>>(values.begin(), values.begin() + count) &&
                result.structuredNulls == std::vector<std::vector<bool>>(nulls.begin(), nulls.begin() + count);
            if (!success) {
                const std::string expected = mode == 1 ? "XX000" : mode == 2 ? "22023" : mode == 3 ? "22003" : "22012";
                valid = valid && result.errorSqlState == expected;
                bool threw = false;
                try { result.throwIfFailed(); }
                catch (const DbError& error) { threw = error.sqlState() == expected; }
                valid = valid && threw;
            }
        } catch (const std::logic_error&) {
            valid = mode == 4 && std::current_exception() == external;
        }
        valid = valid && closed == 1;
        ++controls; failures += !valid;
        std::cout << "RESULT_PREFIX mode=" << mode << " prefix=" << prefix << " passed=" << valid << '\n';
    }
    std::cout << "RESULT_PREFIX_CHECKED=" << controls << " FAILURES=" << failures << '\n';
    return failures ? 1 : 0;
}
