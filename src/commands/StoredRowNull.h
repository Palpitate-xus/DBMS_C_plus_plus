#pragma once

#include "commands/TableManage.h"
#include "storage/HeapTupleHeader.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"

namespace dbms::detail {

// Capture one tuple's bitmap under the existing scan/schema owner. This does
// not cache across rows or skip the allocator's current-file validation.
inline std::optional<std::vector<bool>> storedRowNullBitmap(
    const StorageEngine& engine, const std::string& database,
    const std::string& table, const TableSchema& schema, int64_t rid) {
    if (schema.formatVersion != DATA_FILE_FORMAT_VERSION || rid <= 0)
        return std::nullopt;
    auto* allocator = engine.getPageAllocator(database, table);
    if (!allocator) return std::nullopt;
    uint32_t pageId = 0;
    uint16_t slotId = 0;
    StorageEngine::decodeRid(rid, pageId, slotId);
    char* buffer = allocator->fetchPage(pageId);
    if (!buffer) return std::nullopt;
    struct Pin {
        PageAllocator* allocator;
        uint32_t page;
        ~Pin() { allocator->unpinPage(page); }
    } pin{allocator, pageId};
    PageWrapper page(buffer, allocator->pageSize(), schema.formatVersion);
    const char* tuple = nullptr;
    size_t length = 0;
    if (!page.read(slotId, tuple, length) || length < sizeof(HeapTupleHeaderData))
        return std::nullopt;
    std::vector<bool> nulls(schema.len, false);
    for (size_t i = 0; i < schema.len; ++i)
        if (schema.cols[i].isNull)
            nulls[i] = dbms::isNull(castHeapHeader(tuple), static_cast<int>(i));
    return nulls;
}

// Decode with a captured physical bitmap while preserving virtual-column and
// TOAST handling. The implementation restores nested bindings on all exits.
void decodeStoredRowValues(StorageEngine& engine, const std::string& database,
                          const std::string& raw, const TableSchema& schema,
                          const std::vector<bool>& storedNulls,
                          std::vector<std::string>& values,
                          std::vector<bool>& nulls);

// A scan/decoder already owns this physical schema under its relation lock.
// Reading the tuple bitmap must not re-resolve domains or reload the catalog
// for every cell. Unbound native callers still resolve their schema first.
inline bool storedRowColumnIsNull(const StorageEngine& engine,
                                 const std::string& database,
                                 const std::string& table,
                                 const TableSchema& schema,
                                 int64_t rid, size_t column) {
    if (column >= schema.len || !schema.cols[column].isNull) return false;
    auto* allocator = engine.getPageAllocator(database, table);
    if (!allocator) return false;
    uint32_t pageId = 0;
    uint16_t slotId = 0;
    StorageEngine::decodeRid(rid, pageId, slotId);
    char* buffer = allocator->fetchPage(pageId);
    if (!buffer) return false;
    struct Pin {
        PageAllocator* allocator;
        uint32_t page;
        ~Pin() { allocator->unpinPage(page); }
    } pin{allocator, pageId};
    PageWrapper page(buffer, allocator->pageSize(), schema.formatVersion);
    const char* tuple = nullptr;
    size_t length = 0;
    return page.read(slotId, tuple, length) &&
        schema.formatVersion == DATA_FILE_FORMAT_VERSION &&
        length >= sizeof(HeapTupleHeaderData) &&
        dbms::isNull(castHeapHeader(tuple), static_cast<int>(column));
}

} // namespace dbms::detail
