#pragma once

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

class HeapScanCursor {
public:
    virtual ~HeapScanCursor() = default;
    // false means EOF; storage/visibility failures throw a typed DbError.
    virtual bool next(HeapScanRow& row) = 0;
};

} // namespace dbms
