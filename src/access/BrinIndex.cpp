#include "BrinIndex.h"

#include <algorithm>
#include <fstream>
#include <sstream>

namespace dbms {

BrinIndex::BrinIndex(const std::filesystem::path& indexFile) : indexFile_(indexFile) {}

BrinIndex::~BrinIndex() { close(); }

bool BrinIndex::open() {
    if (std::filesystem::exists(indexFile_)) load();
    return true;
}

void BrinIndex::close() {
    if (dirty_) persist();
}

void BrinIndex::addValue(const std::string& value, RowId rowId) {
    rowOrder_.push_back(rowId);
    size_t blockIdx = (rowOrder_.size() - 1) / BLOCK_SIZE;

    if (blockIdx >= blocks_.size()) {
        blocks_.push_back(BrinSummary{value, value, value.empty(), 1});
    } else {
        BrinSummary& s = blocks_[blockIdx];
        if (value.empty()) s.hasNull = true;
        if (value < s.minVal) s.minVal = value;
        if (value > s.maxVal) s.maxVal = value;
        s.rowCount++;
    }
    dirty_ = true;
}

std::vector<RowId> BrinIndex::searchRange(const std::string& low, const std::string& high) const {
    std::vector<RowId> result;
    for (size_t b = 0; b < blocks_.size(); ++b) {
        const auto& s = blocks_[b];
        // Block overlaps query range?
        bool overlap = false;
        if (low.empty() && high.empty()) {
            overlap = true;
        } else if (low.empty()) {
            overlap = (s.minVal <= high);
        } else if (high.empty()) {
            overlap = (s.maxVal >= low);
        } else {
            overlap = !(s.maxVal < low || s.minVal > high);
        }
        if (overlap) {
            size_t start = b * BLOCK_SIZE;
            size_t end = std::min(start + BLOCK_SIZE, rowOrder_.size());
            for (size_t i = start; i < end; ++i)
                result.push_back(rowOrder_[i]);
        }
    }
    return result;
}

void BrinIndex::rebuild(const std::vector<std::pair<std::string, RowId>>& allEntries) {
    blocks_.clear();
    rowOrder_.clear();
    for (const auto& [val, rid] : allEntries) {
        addValue(val, rid);
    }
}

void BrinIndex::persist() const {
    std::ofstream ofs(indexFile_);
    if (!ofs) return;
    ofs << rowOrder_.size() << "\n";
    for (auto rid : rowOrder_) ofs << rid << " ";
    ofs << "\n";
    for (const auto& s : blocks_) {
        ofs << s.minVal << "|" << s.maxVal << "|" << s.hasNull << "|" << s.rowCount << "\n";
    }
}

void BrinIndex::load() {
    std::ifstream ifs(indexFile_);
    if (!ifs) return;
    std::string line;
    std::getline(ifs, line);
    size_t n = 0;
    try { n = std::stoull(line); } catch (...) { return; }
    std::getline(ifs, line);
    std::istringstream rss(line);
    RowId rid;
    while (rss >> rid) rowOrder_.push_back(rid);
    while (std::getline(ifs, line)) {
        BrinSummary s;
        size_t p1 = line.find('|');
        if (p1 == std::string::npos) continue;
        s.minVal = line.substr(0, p1);
        size_t p2 = line.find('|', p1 + 1);
        if (p2 == std::string::npos) continue;
        s.maxVal = line.substr(p1 + 1, p2 - p1 - 1);
        size_t p3 = line.find('|', p2 + 1);
        if (p3 == std::string::npos) continue;
        s.hasNull = (line.substr(p2 + 1, p3 - p2 - 1) == "1");
        s.rowCount = std::stoull(line.substr(p3 + 1));
        blocks_.push_back(s);
    }
}

} // namespace dbms
