#include "network/PostgresNumeric.h"

#include <cassert>
#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <vector>

using dbms::decodePostgresNumeric;
using dbms::encodePostgresNumeric;

static std::vector<uint8_t> numericWire(uint16_t ndigits, int16_t weight,
                                        uint16_t sign, int16_t dscale,
                                        std::initializer_list<uint16_t> groups) {
    std::vector<uint8_t> raw;
    auto append = [&raw](uint16_t value) {
        raw.push_back(static_cast<uint8_t>(value >> 8));
        raw.push_back(static_cast<uint8_t>(value & 0xff));
    };
    append(ndigits);
    append(static_cast<uint16_t>(weight));
    append(sign);
    append(static_cast<uint16_t>(dscale));
    for (uint16_t group : groups) append(group);
    return raw;
}

static void test_encode_vectors() {
    std::vector<uint8_t> raw;
    assert(encodePostgresNumeric("12345.67", raw));
    assert(raw == numericWire(3, 1, 0x0000, 2, {1, 2345, 6700}));

    assert(encodePostgresNumeric("0.00123", raw));
    assert(raw == numericWire(2, -1, 0x0000, 5, {12, 3000}));

    assert(encodePostgresNumeric("-42.00", raw));
    assert(raw == numericWire(1, 0, 0x4000, 2, {42}));
    std::cout << "[PG NUMERIC] encode vectors OK" << std::endl;
}

static void test_decode_vectors() {
    std::string text;
    assert(decodePostgresNumeric(numericWire(3, 1, 0x0000, 2, {1, 2345, 6700}), text));
    assert(text == "12345.67");
    assert(decodePostgresNumeric(numericWire(2, -1, 0x4000, 5, {12, 3000}), text));
    assert(text == "-0.00123");
    assert(decodePostgresNumeric(numericWire(0, 0, 0xC000, 0, {}), text));
    assert(text == "NaN");
    assert(decodePostgresNumeric(numericWire(0, 0, 0xF000, 0, {}), text));
    assert(text == "-Infinity");
    std::cout << "[PG NUMERIC] decode vectors OK" << std::endl;
}

static void test_reject_malformed() {
    std::string text;
    assert(!decodePostgresNumeric(numericWire(1, 0, 0x1234, 0, {1}), text));
    assert(!decodePostgresNumeric(numericWire(1, 0, 0x0000, 0, {10000}), text));
    assert(!decodePostgresNumeric({0, 1, 0, 0, 0, 0, 0, 0}, text));

    std::vector<uint8_t> raw;
    assert(!encodePostgresNumeric("1e3", raw));
    std::cout << "[PG NUMERIC] malformed input rejection OK" << std::endl;
}

static void test_round_trip() {
    for (const std::string value : {"0", "1.2300", "-999999.0001", "0.00000001"}) {
        std::vector<uint8_t> raw;
        std::string decoded;
        assert(encodePostgresNumeric(value, raw));
        assert(decodePostgresNumeric(raw, decoded));
        assert(decoded == value);
    }
    std::cout << "[PG NUMERIC] round trip OK" << std::endl;
}

int main() {
    test_encode_vectors();
    test_decode_vectors();
    test_reject_malformed();
    test_round_trip();
    std::cout << "[PG NUMERIC] all passed" << std::endl;
    return 0;
}
