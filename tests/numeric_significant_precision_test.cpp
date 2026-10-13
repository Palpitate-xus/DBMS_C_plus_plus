// test_sources: src/types/numeric.cpp
#include "types/numeric.h"

#include <cassert>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

using dbms::Numeric;

int main() {
    struct Case { const char* input; int precision; const char* expected; };
    const Case cases[] = {
        {"12345", 3, "12300"}, {"999", 2, "1000"},
        {"-12345", 3, "-12300"}, {"-999", 2, "-1000"},
        {"149", 1, "100"}, {"150", 1, "200"},
        {"-150", 1, "-200"}, {"1000", 1, "1000"},
        {"999.99", 3, "1000"}, {"1.2345", 3, "1.23"},
        {"0.00123", 2, "0.0012"}, {"0.001250", 2, "0.0013"},
        {"0.00009999", 2, "0.000100"},
        {"0", 1, "0"}, {"0.000", 1, "0.000"},
        {"123.456", 4, "123.5"}, {"1.2345", 1000, "1.2345"},
        {"NaN", 1, "NaN"}, {"Infinity", 1, "Infinity"},
        {"-Infinity", 1, "-Infinity"}
    };
    for (const auto& c : cases) {
        const Numeric original(c.input);
        const auto result = original.withPrecision(c.precision).toString();
        if (result != c.expected)
            std::cerr << c.input << " precision " << c.precision
                      << " expected " << c.expected << " got " << result << '\n';
        assert(result == c.expected);
        assert(original.toString() == Numeric(c.input).toString());
    }
    for (int precision : {std::numeric_limits<int>::min(), -1, 0,
                          Numeric::kMaxPrecision + 1,
                          std::numeric_limits<int>::max()})
        for (const char* input : {"1", "0", "NaN", "Infinity"}) {
            bool rejected = false;
            try { (void)Numeric(input).withPrecision(precision); }
            catch (const std::invalid_argument&) { rejected = true; }
            assert(rejected);
        }
    // Rounding may carry into a new integer digit, but must not change the
    // value's power of ten, including at the implementation precision limit.
    const Numeric maximum(std::string(Numeric::kMaxPrecision, '9'));
    bool tooWide = false;
    try { (void)maximum.withPrecision(1); }
    catch (const std::invalid_argument&) { tooWide = true; }
    assert(tooWide);
    std::cout << "[NUMERIC PRECISION] 20 rounding, 20 invalid and maximum carry controls passed\n";
}
