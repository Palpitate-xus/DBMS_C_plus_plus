#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

// PostgreSQL's numeric binary representation is a signed base-10000 value:
// ndigits, weight, sign, display scale, followed by base-10000 digits.
// These helpers translate that wire representation to the engine's textual
// Numeric input/output boundary without leaking protocol details elsewhere.
bool decodePostgresNumeric(const std::vector<uint8_t>& raw, std::string& text);
bool encodePostgresNumeric(const std::string& text, std::vector<uint8_t>& raw);

} // namespace dbms
