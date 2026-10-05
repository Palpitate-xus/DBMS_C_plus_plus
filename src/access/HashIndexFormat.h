#pragma once

#include <cstdint>

namespace dbms::hash_index_format {

constexpr uint32_t kMagic = 0x48494458;  // HIDX
constexpr uint32_t kLegacyVersion = 1;
constexpr uint32_t kChecksumVersion = 2;
constexpr uint32_t kChecksumBytes = sizeof(uint32_t);
constexpr uint64_t kMaxEntries = 1'000'000;
constexpr uint64_t kMaxKeyLength = 10'000;
constexpr uint64_t kMaxValuesPerKey = 1'000'000;

}  // namespace dbms::hash_index_format
