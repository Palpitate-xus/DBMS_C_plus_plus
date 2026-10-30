#pragma once

#include "commands/TableManage.h"
#include "storage/HeapTupleHeader.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"

namespace dbms::detail {

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
