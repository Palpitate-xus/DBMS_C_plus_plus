#pragma once

#include <cstddef>
#include <cstdint>

namespace dbms {

// Header stored in page zero of every heap data file.  The data format is
// intentionally single-versioned: a new database must use the PostgreSQL-
// style 8 KiB heap page layout and format version 2.
static constexpr uint32_t DATA_FILE_MAGIC = 0x44415441; // "DATA"
static constexpr uint32_t DATA_FILE_FORMAT_VERSION = 2;

#pragma pack(push, 1)
struct DataFileHeader {
    uint32_t magic;
    uint32_t numPages;
    uint32_t freeListHead;
    uint32_t rowSize; // logical row width; large values may be externalized by TOAST
    uint32_t formatVersion;
    uint32_t headerChecksum;
};
#pragma pack(pop)

// The file header is not a PgPage, so it has its own checksum. Treating the
// checksum field as outside the hashed prefix makes validation deterministic.
inline uint32_t computeDataFileHeaderChecksum(const DataFileHeader& header) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(&header);
    constexpr size_t checksumOffset = offsetof(DataFileHeader, headerChecksum);
    uint32_t hash = 2166136261u; // FNV-1a
    for (size_t i = 0; i < checksumOffset; ++i) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

} // namespace dbms
