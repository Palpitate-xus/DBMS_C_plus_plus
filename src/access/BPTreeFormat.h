#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace dbms::bptree_format {

constexpr size_t kPageSize = 4096;
constexpr size_t kKeySize = 20;
constexpr size_t kChecksumOffset = kPageSize - sizeof(uint32_t);
constexpr uint16_t kContentChecksumFormat = 0xC551;
constexpr uint16_t kPageBoundChecksumFormat = 0xC552;

struct FileHeader {
    uint32_t rootPage = 0;
    uint32_t nextFreePage = 1;
    uint16_t order = 100;
    uint16_t reserved = 0;
};
static_assert(sizeof(FileHeader) == 12, "unexpected B+ tree file header size");

constexpr size_t kMaxLeafKeys =
    (kPageSize - 3 - 2 * sizeof(uint32_t)) /
    (kKeySize + sizeof(int64_t));
constexpr size_t kMaxInternalKeys =
    (kPageSize - 3 - 2 * sizeof(uint32_t)) /
    (kKeySize + sizeof(uint32_t));
constexpr size_t kMaxNodeOrder =
    kMaxLeafKeys < kMaxInternalKeys ? kMaxLeafKeys : kMaxInternalKeys;

inline uint32_t pageChecksum(const char* page, bool bindPageId,
                             uint32_t pageId) {
    uint32_t crc = 0xFFFFFFFFu;
    const auto update = [&crc](uint8_t byte) {
        crc ^= byte;
        for (unsigned bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1u) ? 0x82F63B78u : 0u);
        }
    };
    for (size_t i = 0; i < kPageSize; ++i) {
        update(i >= kChecksumOffset ? 0 : static_cast<uint8_t>(page[i]));
    }
    if (bindPageId) {
        // Use a fixed byte order for the block identity, independently of the
        // native-endian fields used by this database's on-disk B+ tree format.
        for (unsigned shift = 0; shift < 32; shift += 8) {
            update(static_cast<uint8_t>((pageId >> shift) & 0xFFu));
        }
    }
    crc ^= 0xFFFFFFFFu;
    return crc == 0 ? 0xFFFFFFFFu : crc;
}

inline bool verifyPageChecksum(const char* page, uint32_t pageId,
                               bool bindPageId) {
    uint32_t stored = 0;
    std::memcpy(&stored, page + kChecksumOffset, sizeof(stored));
    return stored != 0 &&
           stored == pageChecksum(page, bindPageId, pageId);
}

inline void writePageChecksum(char* page, uint32_t pageId) {
    const uint32_t zero = 0;
    std::memcpy(page + kChecksumOffset, &zero, sizeof(zero));
    const uint32_t checksum = pageChecksum(page, true, pageId);
    std::memcpy(page + kChecksumOffset, &checksum, sizeof(checksum));
}

}  // namespace dbms::bptree_format
