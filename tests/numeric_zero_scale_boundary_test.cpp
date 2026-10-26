// test_sources: src/types/numeric.cpp
#include "types/numeric.h"
#include <array>
#include <climits>
#include <iostream>
#include <stdexcept>
#include <string>

using dbms::Numeric;

static Numeric source(unsigned kind) {
    if (kind == 0) return Numeric{};
    if (kind == 1) return Numeric(int64_t{0});
    if (kind == 2) return Numeric(1) - Numeric(1);
    if (kind == 3) return Numeric("0e3");
    const std::array<int, 7> scales{0, 1, 2, 17, 500, 999, 1000};
    const int scale = scales[(kind - 4) / 2];
    return Numeric(std::string((kind - 4) % 2 ? "-0" : "0") +
        (scale ? "." + std::string(scale, '0') : ""));
}

static std::string zeroText(int scale) {
    return scale > 0 ? "0." + std::string(scale, '0') : "0";
}

static bool zero(unsigned kind, int scale, bool legacyRange = false) {
    const Numeric before = source(kind);
    const std::string original = before.toString();
    const bool allowed = !legacyRange || static_cast<int64_t>(scale) - before.scale() <= 999;
    try {
        const Numeric result = before.withScale(scale);
        return allowed && result.toString() == zeroText(scale) && result.sign() == 0 &&
            result.precision() == 1 && result.scale() == (scale > 0 ? scale : 0) &&
            result.isFinite() && !result.isNaN() && !result.isInfinite() && before.toString() == original;
    } catch (const std::invalid_argument&) {
        return !allowed && before.toString() == original;
    }
}

static bool nonzero(const std::string& input, int scale, const std::string& expected, bool rejected = false) {
    try {
        const Numeric result = Numeric(input).withScale(scale);
        return !rejected && result.toString() == expected;
    }
    catch (const std::invalid_argument&) { return rejected; }
}

int main() {
    int controls = 0, failures = 0;
    auto check = [&](bool ok, const std::string& label) {
        ++controls;
        if (!ok) { ++failures; std::cout << "ZERO_SCALE_BOUNDARY_FAILED " << label << '\n'; }
    };
    try {
        for (unsigned kind = 0; kind < 18; ++kind) {
            for (int scale = 0; scale <= 1000; ++scale)
                check(zero(kind, scale), "kind=" + std::to_string(kind) + " scale=" + std::to_string(scale));
            for (int scale : {INT_MIN, -1001, -1000, -1})
                check(zero(kind, scale), "negative kind=" + std::to_string(kind) + " scale=" + std::to_string(scale));
            // Above the fromString scale domain, retain the old admission
            // policy rather than accidentally permitting INT_MAX allocation.
            for (int scale : {1001, 1500, 1999, 2000, INT_MAX})
                check(zero(kind, scale, true), "legacy kind=" + std::to_string(kind) + " scale=" + std::to_string(scale));
        }
        check(nonzero("1", 0, "1"), "one");
        check(nonzero("1", 999, "1." + std::string(999, '0')), "one999");
        // Existing nonzero precision limit: this is not a PG ROUND oracle.
        check(nonzero("1", 1000, "", true), "nonzero existing limit still open");
        check(nonzero("0.1", 1000, "0.1" + std::string(999, '0')), "fraction1000");
        check(nonzero("-0.1", 1000, "-0.1" + std::string(999, '0')), "negative1000");
        check(nonzero("1e-1000", 1000, "0." + std::string(999, '0') + "1"), "tiny1000");
        check(nonzero("1e-1000", 999, zeroText(999)), "tiny rounds zero");
        check(nonzero("123.45", -1, "120"), "integer rounding");
        check(nonzero("-150", -2, "-200"), "negative rounding");
        check(nonzero("0.0005", 3, "0.001"), "carry rounding");
        for (const std::string input : {"NaN", "Infinity", "-Infinity"})
            for (int scale : {INT_MIN, -1, 0, 1, 999, 1000, 1001, 1999, INT_MAX})
                check(Numeric(input).withScale(scale).toString() == input, "special " + input);
    } catch (const std::exception& error) {
        std::cerr << "CONTROL_ERROR " << error.what() << '\n'; return 2;
    }
    if (controls != 18217) { std::cerr << "CONTROL_ERROR matrix size\n"; return 2; }
    std::cout << "NUMERIC_ZERO_SCALE_BOUNDARY_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
