#include "network/PostgresNumeric.h"
#include "types/numeric.h"

#include <cstdint>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
std::vector<uint8_t> wire(int scale, int weight = 0, bool negative = false) {
    std::vector<uint8_t> raw;
    for (uint16_t value : {uint16_t(0), static_cast<uint16_t>(weight),
                           uint16_t(negative ? 0x4000 : 0), static_cast<uint16_t>(scale)}) {
        raw.push_back(static_cast<uint8_t>(value >> 8));
        raw.push_back(static_cast<uint8_t>(value));
    }
    return raw;
}
void require(bool condition) {
    if (!condition) throw std::runtime_error("value mismatch");
}
}

int main() {
    size_t controls = 0, failures = 0;
    const auto check = [&](const std::string& label, const std::function<void()>& action) {
        ++controls;
        try { action(); }
        catch (const std::exception& error) {
            ++failures;
            std::cerr << "NUMERIC_BINARY_ZERO_FAIL " << label << ": " << error.what() << '\n';
        }
    };
    for (int scale : {0, 1, 2, 6, 32, 999, 1000}) {
        const std::string expected = "0" + (scale ? "." + std::string(scale, '0') : "");
        for (int weight : {0, -1, 1, -32768, 32767}) {
            for (bool negative : {false, true}) {
                const auto raw = wire(scale, weight, negative);
                const auto label = std::to_string(scale) + "/" + std::to_string(weight) + "/" + std::to_string(negative);
                check("decode " + label, [&] {
                    std::string text;
                    require(dbms::decodePostgresNumeric(raw, text) && text == expected);
                });
                check("roundtrip " + label, [&] {
                    std::string text;
                    std::vector<uint8_t> canonical;
                    require(dbms::decodePostgresNumeric(raw, text));
                    require(dbms::encodePostgresNumeric(text, canonical) && canonical == wire(scale));
                });
                check("value " + label, [&] {
                    std::string text;
                    require(dbms::decodePostgresNumeric(raw, text));
                    const dbms::Numeric value(text);
                    require(value.scale() == scale && value.toString() == expected);
                });
            }
        }
    }
    for (size_t size = 0; size < 8; ++size) {
        check("truncated " + std::to_string(size), [&] {
            std::string text;
            require(!dbms::decodePostgresNumeric(std::vector<uint8_t>(size), text));
        });
    }
    // These retain the project's current input domain, not PG's wider domain.
    for (int scale : {-1, 1001, 32767}) {
        check("existing scale guard " + std::to_string(scale), [&] {
            std::string text;
            require(!dbms::decodePostgresNumeric(wire(scale), text));
        });
    }
    check("invalid sign", [&] {
        auto raw = wire(2); raw[4] = 0x12; raw[5] = 0x34;
        std::string text;
        require(!dbms::decodePostgresNumeric(raw, text));
    });
    check("invalid digit", [&] {
        auto raw = wire(2); raw[1] = 1; raw.push_back(0x27); raw.push_back(0x10);
        std::string text;
        require(!dbms::decodePostgresNumeric(raw, text));
    });
    for (const auto& value : {std::string("NaN"), std::string("Infinity"), std::string("-Infinity"),
                              std::string("1.2300"), std::string("-42.00"), std::string("0.001"), std::string("1234")}) {
        check("unchanged " + value, [&] {
            std::vector<uint8_t> raw;
            std::string text;
            require(dbms::encodePostgresNumeric(value, raw));
            require(dbms::decodePostgresNumeric(raw, text) && text == value);
        });
    }
    if (controls != 230) {
        ++failures;
        std::cerr << "NUMERIC_BINARY_ZERO_INVENTORY " << controls << '\n';
    }
    std::cout << "NUMERIC_BINARY_ZERO_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
