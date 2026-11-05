#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if (defined(__GNUC__) || defined(__clang__)) && \
    (defined(__x86_64__) || defined(__i386__))
#include <nmmintrin.h>
#endif

namespace dbms::index_checksum {

namespace detail {

constexpr std::array<uint32_t, 256> makeCrc32cTable() {
    std::array<uint32_t, 256> table{};
    for (size_t i = 0; i < table.size(); ++i) {
        uint32_t state = static_cast<uint32_t>(i);
        for (unsigned bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                ((state & 1u) ? 0x82F63B78u : 0u);
        }
        table[i] = state;
    }
    return table;
}

inline constexpr auto kCrc32cTable = makeCrc32cTable();

inline uint32_t crc32cPortableUpdate(uint32_t state, const char* bytes,
                                    size_t size) {
    for (size_t i = 0; i < size; ++i)
        state = kCrc32cTable[(state ^ static_cast<uint8_t>(bytes[i])) & 0xFFu] ^
                (state >> 8);
    return state;
}

inline bool hardwareCrc32cAvailable() {
#if (defined(__GNUC__) || defined(__clang__)) && \
    (defined(__x86_64__) || defined(__i386__))
    return __builtin_cpu_supports("sse4.2");
#else
    return false;
#endif
}

#if (defined(__GNUC__) || defined(__clang__)) && \
    (defined(__x86_64__) || defined(__i386__))
// Only this function permits CRC instructions. Neither the build nor its
// callers require SSE4.2. memcpy permits unaligned, alias-safe reads without
// reading past the supplied bytes, including the final short chunk.
__attribute__((target("sse4.2")))
inline uint32_t crc32cSse42Update(uint32_t state, const char* bytes,
                                size_t size) {
#if defined(__x86_64__)
    while (size >= sizeof(uint64_t)) {
        uint64_t word;
        std::memcpy(&word, bytes, sizeof(word));
        state = static_cast<uint32_t>(_mm_crc32_u64(state, word));
        bytes += sizeof(word);
        size -= sizeof(word);
    }
#endif
    while (size >= sizeof(uint32_t)) {
        uint32_t word;
        std::memcpy(&word, bytes, sizeof(word));
        state = _mm_crc32_u32(state, word);
        bytes += sizeof(word);
        size -= sizeof(word);
    }
    while (size > 0) {
        state = _mm_crc32_u8(state, static_cast<uint8_t>(*bytes++));
        --size;
    }
    return state;
}
#endif

}  // namespace detail

inline uint32_t crc32cUpdate(uint32_t state, const char* bytes, size_t size) {
    // The builtin reads an already initialized CPU feature bit. This adds
    // no function-local static guard, allocation or synchronization to a
    // checksum operation, including concurrent first use and forked readers.
#if (defined(__GNUC__) || defined(__clang__)) && \
    (defined(__x86_64__) || defined(__i386__))
    if (detail::hardwareCrc32cAvailable())
        return detail::crc32cSse42Update(state, bytes, size);
#endif
    return detail::crc32cPortableUpdate(state, bytes, size);
}

inline uint32_t crc32cFinish(uint32_t state) {
    return state ^ 0xFFFFFFFFu;
}

inline uint32_t crc32c(const char* bytes, size_t size) {
    return crc32cFinish(crc32cUpdate(0xFFFFFFFFu, bytes, size));
}

}  // namespace dbms::index_checksum
