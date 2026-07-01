#pragma once

#include "interfaces/index_am.h"
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace dbms {

// Simplified BRIN (Block Range Index).
// Stores one summary per block (minValue, maxValue). Useful for naturally
// ordered data (timestamps, sequences).
struct BrinSummary {
    std::string minVal;
    std::string maxVal;
    bool hasNull = false;
    size_t rowCount = 0;
};

class BrinIndex {
public:
    explicit BrinIndex(const std::filesystem::path& indexFile);
    ~BrinIndex();

    bool open();
    void close();

    void addValue(const std::string& value, RowId rowId);

    // Range scan: return rowIds in blocks whose range overlaps [low, high).
    std::vector<RowId> searchRange(const std::string& low, const std::string& high) const;

    void rebuild(const std::vector<std::pair<std::string, RowId>>& allEntries);

    const std::filesystem::path& path() const { return indexFile_; }

private:
    void persist() const;
    void load();

    std::filesystem::path indexFile_;
    static constexpr size_t BLOCK_SIZE = 128;
    std::vector<BrinSummary> blocks_;
    std::vector<RowId> rowOrder_;
    std::string pendingVal_;
    bool dirty_ = false;
};

} // namespace dbms
