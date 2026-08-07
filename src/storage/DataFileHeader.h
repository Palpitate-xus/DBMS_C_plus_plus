#pragma once

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
    uint32_t rowSize;
    uint32_t formatVersion;
};
#pragma pack(pop)

} // namespace dbms
