#pragma once

#include <cstddef>
#include <cstdint>

namespace dbms::index_checksum {

inline uint32_t crc32cUpdate(uint32_t state, const char* bytes, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        state ^= static_cast<uint8_t>(bytes[i]);
        for (unsigned bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                ((state & 1u) ? 0x82F63B78u : 0u);
        }
    }
    return state;
}

inline uint32_t crc32cFinish(uint32_t state) {
    return state ^ 0xFFFFFFFFu;
}

inline uint32_t crc32c(const char* bytes, size_t size) {
    return crc32cFinish(crc32cUpdate(0xFFFFFFFFu, bytes, size));
}

}  // namespace dbms::index_checksum
