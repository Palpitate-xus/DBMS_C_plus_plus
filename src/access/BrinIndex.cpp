#include "BrinIndex.h"
#include "IndexFileUtil.h"

#include <algorithm>
#include <fstream>
#include <cstring>

namespace dbms {

namespace {
constexpr uint32_t BRIN_MAGIC = 0x4252494e; // BRIN
constexpr uint32_t BRIN_VERSION = 1;
constexpr size_t MAX_ROWS = 10'000'000;
constexpr size_t MAX_BLOCKS = (MAX_ROWS + 128 - 1) / 128;
constexpr size_t MAX_VALUE_LENGTH = 1'000'000;

template <typename T>
void appendBytes(std::string& out, const T& value) {
    out.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool readBytes(const std::string& data, size_t& offset, T& value) {
    if (data.size() - offset < sizeof(T)) return false;
    std::memcpy(&value, data.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

bool appendString(std::string& out, const std::string& value) {
    if (value.size() > MAX_VALUE_LENGTH) return false;
    const uint64_t length = value.size();
    appendBytes(out, length);
    out.append(value);
    return true;
}

bool readString(const std::string& data, size_t& offset, std::string& value) {
    uint64_t length = 0;
    if (!readBytes(data, offset, length) || length > MAX_VALUE_LENGTH ||
        data.size() - offset < length) return false;
    value.assign(data.data() + offset, static_cast<size_t>(length));
    offset += static_cast<size_t>(length);
    return true;
}
}

BrinIndex::BrinIndex(const std::filesystem::path& indexFile) : indexFile_(indexFile) {}

BrinIndex::~BrinIndex() { close(); }

bool BrinIndex::open() {
    blocks_.clear();
    rowOrder_.clear();
    if (std::filesystem::exists(indexFile_) && !load()) return false;
    dirty_ = false;
    return true;
}

bool BrinIndex::close() {
    if (dirty_ && !persist()) return false;
    dirty_ = false;
    return true;
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

bool BrinIndex::persist() const {
    if (rowOrder_.size() > MAX_ROWS || blocks_.size() > MAX_BLOCKS) return false;
    std::string data;
    appendBytes(data, BRIN_MAGIC);
    appendBytes(data, BRIN_VERSION);
    const uint64_t rowCount = rowOrder_.size();
    appendBytes(data, rowCount);
    for (auto rid : rowOrder_) appendBytes(data, rid);
    const uint64_t blockCount = blocks_.size();
    appendBytes(data, blockCount);
    for (const auto& s : blocks_) {
        if (s.rowCount > BLOCK_SIZE || !appendString(data, s.minVal) ||
            !appendString(data, s.maxVal)) return false;
        const uint8_t hasNull = s.hasNull ? 1 : 0;
        appendBytes(data, hasNull);
        const uint64_t count = s.rowCount;
        appendBytes(data, count);
    }
    return index_file::writeAtomically(indexFile_, data);
}

bool BrinIndex::load() {
    std::ifstream ifs(indexFile_, std::ios::binary);
    if (!ifs) return false;
    const std::string data((std::istreambuf_iterator<char>(ifs)),
                           std::istreambuf_iterator<char>());
    if (ifs.bad()) return false;

    size_t offset = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t rowCount = 0;
    uint64_t blockCount = 0;
    if (!readBytes(data, offset, magic) || !readBytes(data, offset, version) ||
        !readBytes(data, offset, rowCount) || magic != BRIN_MAGIC ||
        version != BRIN_VERSION || rowCount > MAX_ROWS) return false;
    rowOrder_.resize(static_cast<size_t>(rowCount));
    for (auto& rid : rowOrder_) {
        if (!readBytes(data, offset, rid)) return false;
    }
    if (!readBytes(data, offset, blockCount) || blockCount > MAX_BLOCKS ||
        blockCount != (rowCount + BLOCK_SIZE - 1) / BLOCK_SIZE) return false;
    blocks_.reserve(static_cast<size_t>(blockCount));
    for (uint64_t i = 0; i < blockCount; ++i) {
        BrinSummary s;
        uint8_t hasNull = 0;
        uint64_t count = 0;
        if (!readString(data, offset, s.minVal) || !readString(data, offset, s.maxVal) ||
            !readBytes(data, offset, hasNull) || !readBytes(data, offset, count) ||
            hasNull > 1 || count == 0 || count > BLOCK_SIZE) return false;
        s.hasNull = hasNull != 0;
        s.rowCount = static_cast<size_t>(count);
        blocks_.push_back(std::move(s));
    }
    if (offset != data.size()) return false;
    size_t total = 0;
    for (const auto& block : blocks_) total += block.rowCount;
    if (total != rowOrder_.size()) return false;
    return true;
}

} // namespace dbms
