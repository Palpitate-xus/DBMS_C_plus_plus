#include "common/IntegerTextCodec.h"

#include <cassert>
#include <iostream>
#include <limits>

int main() {
    using dbms::IntegerTextError;
    const auto value = [](const char* input, unsigned bits, int64_t expected) {
        const auto parsed = dbms::parsePostgresIntegerText(input, bits);
        assert(parsed.error == IntegerTextError::None);
        assert(parsed.value == expected);
    };
    const auto error = [](const char* input, unsigned bits, IntegerTextError expected) {
        assert(dbms::parsePostgresIntegerText(input, bits).error == expected);
    };
    value("-32768", 16, -32768);
    value("32767", 16, 32767);
    value("-2147483648", 32, -2147483648LL);
    value("2147483647", 32, 2147483647LL);
    value("-9223372036854775808", 64, std::numeric_limits<int64_t>::min());
    value("9223372036854775807", 64, std::numeric_limits<int64_t>::max());
    value(" +0x_Ff \t", 16, 255);
    value("-0b_1000_0000", 16, -128);
    value("0O_10", 32, 8);
    value("1_000", 32, 1000);
    value("010", 32, 10);
    value("-0x8000_0000_0000_0000", 64, std::numeric_limits<int64_t>::min());
    for (const char* invalid : {"", " ", "bad", "1.2", "+ 1", "--1",
                               "_1", "1_", "1__2", "0x", "0x__f",
                               "0b2", "0o8", "1 2", "0xF_"}) {
        error(invalid, 32, IntegerTextError::InvalidSyntax);
    }
    error("32768", 16, IntegerTextError::OutOfRange);
    error("-32769", 16, IntegerTextError::OutOfRange);
    error("2147483648", 32, IntegerTextError::OutOfRange);
    error("-2147483649", 32, IntegerTextError::OutOfRange);
    error("9223372036854775808", 64, IntegerTextError::OutOfRange);
    error("-9223372036854775809", 64, IntegerTextError::OutOfRange);
    error("2147483648bad", 32, IntegerTextError::InvalidSyntax);
    error("999999999999999999999bad", 32, IntegerTextError::OutOfRange);
    error("0x8000_0000_0000_0000", 64, IntegerTextError::OutOfRange);
    std::cout << "[INTEGER TEXT CODEC] passed\n";
}
