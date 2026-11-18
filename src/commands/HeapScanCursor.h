#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

// A row owns its bytes and physical NULL bitmap after the page lock and pin
// have been released. No page pointer escapes into expression evaluation.
struct HeapScanRow {
    int64_t rid = 0;
    std::string data;
    std::vector<bool> nulls;
};

struct OrderedPrimaryKeyNullTest {
    std::size_t column;
    bool expectsNull;
};

struct OrderedPrimaryKeyReadPage {
    std::vector<HeapScanRow> rows;
    std::size_t indexEntries = 0;
    std::size_t indexNodesVisited = 0;
    std::size_t heapRowsFetched = 0;
    bool orderedIndex = false;
};

class HeapScanCursor {
public:
    virtual ~HeapScanCursor() = default;
    // false means EOF; storage/visibility failures throw a typed DbError.
    virtual bool next(HeapScanRow& row) = 0;
};

} // namespace dbms
