#pragma once

#include <cstdint>

namespace dbms::bloom_index_format {

constexpr uint32_t kLegacyMagic = 0x314D4C42u;       // BLM1
constexpr uint32_t kChecksummedMagic = 0x324D4C42u;  // BLM2
constexpr uint32_t kChecksumBytes = sizeof(uint32_t);
constexpr uint32_t kHeaderBytes = sizeof(uint32_t) * 4;

}  // namespace dbms::bloom_index_format
