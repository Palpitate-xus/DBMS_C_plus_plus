// test_sources: src/types/numeric.cpp
#include "types/numeric.h"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

using dbms::Numeric;

int main() {
    const std::pair<const char*, const char*> cases[] = {
        {"0", "0.000000000000000"}, {"0.000", "0.000000000000000"},
        {"1", "1.000000000000000"}, {"2", "1.414213562373095"},
        {"4", "2.000000000000000"}, {"9", "3.000000000000000"},
        {"16", "4.000000000000000"}, {"0.1", "0.31622776601683793"},
        {"0.01", "0.10000000000000000"}, {"0.0001", "0.01000000000000000"},
        {"1e40", "100000000000000000000"},
        {"10000000000000000000200000000000000000000", "100000000000000000001"},
        {"9999999999999999999999999999999999999999", "100000000000000000000"},
        {"9.0000000000000000000000000000", "3.0000000000000000000000000000"},
        {"2.0000000000000000000000000000", "1.4142135623730950488016887242"},
        {"1e-30", "0.0000000000000010000000000000000"},
        {"NaN", "NaN"}, {"Infinity", "Infinity"}
    };
    for (const auto& c : cases) {
        const auto got = Numeric(c.first).squareRoot().toString();
        if (got != c.second) std::cerr << c.first << " got " << got << '\n';
        assert(got == c.second);
    }
    assert(Numeric("1e998").squareRoot().toString() == "1" + std::string(499, '0'));
    assert(Numeric("1e-1000").squareRoot().toString() ==
        "0." + std::string(499, '0') + "1" + std::string(500, '0'));
    for (const char* value : {"-1", "-0.01", "-Infinity"}) {
        bool rejected = false;
        try { (void)Numeric(value).squareRoot(); }
        catch (const std::domain_error&) { rejected = true; }
        assert(rejected);
    }
    // Independent exact squares, not a binary floating-point oracle.
    for (int64_t i = 1; i <= 1000; ++i)
        assert(Numeric(i * i).squareRoot() == Numeric(i));
    std::cout << "[NUMERIC SQRT] 20 display, 3 domain, 1000 exact-square controls passed\n";
}
