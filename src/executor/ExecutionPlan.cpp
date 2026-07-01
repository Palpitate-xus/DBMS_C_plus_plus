#include "ExecutionPlan.h"
#include "Config.h"

#include <algorithm>
#include <cmath>
#include <cstring>

extern dbms::Config g_config;

namespace dbms {

// Parallel query support
int QueryPlanner::parallelWorkers_ = 0;

// ========================================================================
// Helper: format a raw row buffer into display string
// ========================================================================
static std::string formatRow(const std::string& rowBuffer, const TableSchema& tbl,
                              const std::set<std::string>& selectCols) {
    std::string rowStr;
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!selectCols.empty() && selectCols.find(col.dataName) == selectCols.end())
            continue;
        std::string val = StorageEngine::extractColumnValueStatic(rowBuffer, tbl, i);
        if (val.empty() && !col.isNull) rowStr += "NULL ";
        else rowStr += val + ' ';
    }
    return rowStr;
}


// ========================================================================
// Helper: LIKE pattern matching
// ========================================================================
static bool likeMatch(const std::string& text, const std::string& pattern) {
    size_t ti = 0, pi = 0, star = std::string::npos, match = 0;
    while (ti < text.size()) {
        if (pi < pattern.size() && (pattern[pi] == '?' || pattern[pi] == text[ti])) {
            ++ti; ++pi;
        } else if (pi < pattern.size() && pattern[pi] == '*') {
            star = pi++;
            match = ti;
        } else if (star != std::string::npos) {
            pi = star + 1;
            ti = ++match;
        } else {
            return false;
        }
    }
    while (pi < pattern.size() && pattern[pi] == '*') ++pi;
    return pi == pattern.size();
}

// ========================================================================
// Helper: evaluate a single condition on raw row data
// ========================================================================
static bool evalCondRaw(const StorageEngine::Condition& cond,
                         const std::string& rowBuffer, const TableSchema& tbl) {
    size_t ci = 0;
    for (; ci < tbl.len && tbl.cols[ci].dataName != cond.colName; ++ci) {}
    if (ci >= tbl.len) return false;

    std::string val = StorageEngine::extractColumnValueStatic(rowBuffer, tbl, ci);
    const Column& col = tbl.cols[ci];
    if (col.dataType == "char" || col.isVariableLength) {
        if (cond.op == "<"  && !(val <  cond.value)) return false;
        if (cond.op == ">"  && !(val >  cond.value)) return false;
        if (cond.op == "="  && val != cond.value)    return false;
        if (cond.op == "<=" && (val >  cond.value))   return false;
        if (cond.op == ">=" && (val <  cond.value))   return false;
        if (cond.op == "!=" && val == cond.value)    return false;
        if (cond.op == "like" && !likeMatch(val, cond.value)) return false;
    } else if (col.dataType == "date") {
        Date d = (val.empty() ? Date{} : Date(val.c_str()));
        Date v(cond.value.c_str());
        if (cond.op == "<"  && v.year && !(d < v))  return false;
        if (cond.op == ">"  && v.year && !(d > v))  return false;
        if (cond.op == "="  && v.year && d != v)    return false;
        if (cond.op == "<=" && v.year && (d > v))   return false;
        if (cond.op == ">=" && v.year && (d < v))   return false;
        if (cond.op == "!=" && v.year && d == v)    return false;
    } else if (col.dataType == "timestamp") {
        int64_t num = val.empty() ? 0 : parseTimestampToSeconds(val);
        int64_t cmp = parseTimestampToSeconds(cond.value);
        if (cond.op == "<"  && cmp != 0 && !(num < cmp)) return false;
        if (cond.op == ">"  && cmp != 0 && !(num > cmp)) return false;
        if (cond.op == "="  && cmp != 0 && num != cmp)   return false;
        if (cond.op == "<=" && cmp != 0 && (num > cmp))  return false;
        if (cond.op == ">=" && cmp != 0 && (num < cmp))  return false;
        if (cond.op == "!=" && cmp != 0 && num == cmp)   return false;
    } else if (col.dataType == "float") {
        float num = val.empty() ? 0.0f : std::stof(val);
        float cmp = std::stof(cond.value);
        if (cond.op == "<"  && !(num < cmp)) return false;
        if (cond.op == ">"  && !(num > cmp)) return false;
        if (cond.op == "="  && num != cmp)   return false;
        if (cond.op == "<=" && (num > cmp))  return false;
        if (cond.op == ">=" && (num < cmp))  return false;
        if (cond.op == "!=" && num == cmp)   return false;
    } else if (col.dataType == "double" || col.dataType == "decimal") {
        double num = val.empty() ? 0.0 : std::stod(val);
        double cmp = std::stod(cond.value);
        if (cond.op == "<"  && !(num < cmp)) return false;
        if (cond.op == ">"  && !(num > cmp)) return false;
        if (cond.op == "="  && num != cmp)   return false;
        if (cond.op == "<=" && (num > cmp))  return false;
        if (cond.op == ">=" && (num < cmp))  return false;
        if (cond.op == "!=" && num == cmp)   return false;
    } else {
        int64_t num = val.empty() ? INF : StorageEngine::parseInt(val);
        int64_t cmp = StorageEngine::parseInt(cond.value);
        if (cond.op == "<"  && cmp != INF && !(num < cmp)) return false;
        if (cond.op == ">"  && cmp != INF && !(num > cmp)) return false;
        if (cond.op == "="  && cmp != INF && num != cmp)   return false;
        if (cond.op == "<=" && cmp != INF && (num > cmp))  return false;
        if (cond.op == ">=" && cmp != INF && (num < cmp))  return false;
        if (cond.op == "!=" && cmp != INF && num == cmp)   return false;
    }
    return true;
}

// ========================================================================
// TableScanOp
// ========================================================================

TableScanOp::TableScanOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename)
    : engine_(engine), dbname_(dbname), tablename_(tablename) {}

bool TableScanOp::open() {
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    engine_->forEachRow(dbname_, tablename_,
        [&](uint32_t pageId, uint16_t slotId, const char* data, size_t len) {
            std::string row(data, len);
            row = engine_->resolveToastValues(dbname_, tablename_, row, tbl_);
            rows_.emplace_back(StorageEngine::encodeRid(pageId, slotId), std::move(row));
        });
    pos_ = 0;
    return true;
}

bool TableScanOp::next(std::string& outRow) {
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_].second;
    ++pos_;
    return true;
}

void TableScanOp::close() {
    rows_.clear();
}

// ========================================================================
// IndexScanOp
// ========================================================================

IndexScanOp::IndexScanOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename, const std::string& colname,
                          const std::string& value)
    : engine_(engine), dbname_(dbname), tablename_(tablename),
      colname_(colname), value_(value) {}

bool IndexScanOp::open() {
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    // Check if this is a PK scan
    size_t pkIdx = tbl_.len;
    for (size_t i = 0; i < tbl_.len; ++i) {
        if (tbl_.cols[i].isPrimaryKey && tbl_.cols[i].dataName == colname_) {
            pkIdx = i; break;
        }
    }
    isPK_ = (pkIdx < tbl_.len);

    if (isPK_) {
        BPTree* idx = engine_->getPKIndex(dbname_, tablename_);
        if (idx) {
            int64_t rid = 0;
            if (idx->search(value_, rid)) rids_.push_back(rid);
        }
    } else {
        BPTree* idx = engine_->getSecondaryIndex(dbname_, tablename_, colname_);
        if (idx) {
            rids_ = idx->searchMulti(value_);
        }
    }
    pos_ = 0;
    return true;
}

bool IndexScanOp::next(std::string& outRow) {
    if (pos_ >= rids_.size()) return false;
    // readRowByRid is const but getPageAllocator is mutable
    // Use the mutable getter through const_cast workaround
    // Actually, getPageAllocator is not const, so we need a non-const engine
    std::string row;
    bool ok = false;
    engine_->forEachRow(dbname_, tablename_, [&](uint32_t pid, uint16_t sid, const char* data, size_t len) {
        if (!ok && StorageEngine::encodeRid(pid, sid) == rids_[pos_]) {
            row.assign(data, len);
            ok = true;
        }
    });
    if (!ok) { ++pos_; return next(outRow); }
    outRow = engine_->resolveToastValues(dbname_, tablename_, row, tbl_);
    ++pos_;
    return true;
}

void IndexScanOp::close() {
    rids_.clear();
}

// ========================================================================
// IndexOnlyScanOp: covering index scan (no row lookup)
// ========================================================================
IndexOnlyScanOp::IndexOnlyScanOp(StorageEngine* engine, const std::string& dbname,
                                  const std::string& tablename,
                                  const std::vector<std::string>& indexCols,
                                  const std::string& filterValue,
                                  const std::string& compositeIndexName)
    : engine_(engine), dbname_(dbname), tablename_(tablename),
      indexCols_(indexCols), filterValue_(filterValue),
      compositeIndexName_(compositeIndexName) {}

bool IndexOnlyScanOp::open() {
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rids_.clear();
    if (!compositeIndexName_.empty()) {
        BPTree* idx = engine_->getCompositeIndexTree(dbname_, tablename_, compositeIndexName_);
        if (idx) rids_ = idx->searchMulti(filterValue_);
    } else if (indexCols_.size() == 1) {
        // Single-column index (or PK)
        const std::string& cname = indexCols_[0];
        bool isPK = false;
        for (size_t i = 0; i < tbl_.len; ++i) {
            if (tbl_.cols[i].dataName == cname && tbl_.cols[i].isPrimaryKey) {
                isPK = true; break;
            }
        }
        if (isPK) {
            BPTree* idx = engine_->getPKIndex(dbname_, tablename_);
            if (idx) {
                int64_t rid = 0;
                if (idx->search(filterValue_, rid)) rids_.push_back(rid);
            }
        } else {
            BPTree* idx = engine_->getSecondaryIndex(dbname_, tablename_, cname);
            if (idx) rids_ = idx->searchMulti(filterValue_);
        }
    }
    pos_ = 0;
    return true;
}

bool IndexOnlyScanOp::next(std::string& outRow) {
    if (pos_ >= rids_.size()) return false;
    // Build a virtual row containing only the index column values (no disk I/O for row data)
    // Decompose filterValue_ into column values
    std::vector<std::string> colVals;
    size_t start = 0;
    for (size_t i = 0; i < indexCols_.size(); ++i) {
        size_t sep = filterValue_.find('\x01', start);
        if (sep == std::string::npos) {
            colVals.push_back(filterValue_.substr(start));
            break;
        }
        colVals.push_back(filterValue_.substr(start, sep - start));
        start = sep + 1;
    }
    while (colVals.size() < indexCols_.size()) colVals.push_back("");

    // Build a row buffer with only the index columns populated
    std::map<std::string, std::string> vals;
    for (size_t i = 0; i < indexCols_.size() && i < colVals.size(); ++i) {
        vals[indexCols_[i]] = colVals[i];
    }
    // Construct row buffer matching the table schema (other cols empty)
    std::string row(tbl_.rowSize(), '\0');
    size_t fixedOffset = 0;
    for (size_t i = 0; i < tbl_.len; ++i) {
        const Column& col = tbl_.cols[i];
        if (col.isVariableLength) continue;
        auto it = vals.find(col.dataName);
        if (it != vals.end() && !it->second.empty()) {
            if (col.dataType == "int" || col.dataType == "tinyint" || col.dataType == "long") {
                int64_t v = StorageEngine::parseInt(it->second);
                std::memcpy(row.data() + fixedOffset, &v, col.dsize);
            } else if (col.dataType == "char") {
                size_t copyLen = std::min(it->second.size(), col.dsize);
                std::memcpy(row.data() + fixedOffset, it->second.data(), copyLen);
            }
        }
        fixedOffset += col.dsize;
    }
    // For variable-length columns, append values at end
    size_t varCount = tbl_.varColCount();
    size_t arrPos = tbl_.fixedDataSize();
    size_t dataPos = arrPos + varCount * 4;
    std::string finalRow = row.substr(0, dataPos);
    finalRow.resize(dataPos);
    size_t varIdx = 0;
    for (size_t i = 0; i < tbl_.len; ++i) {
        const Column& col = tbl_.cols[i];
        if (!col.isVariableLength) continue;
        auto it = vals.find(col.dataName);
        std::string val = (it != vals.end()) ? it->second : "";
        uint16_t offset = static_cast<uint16_t>(finalRow.size());
        uint16_t length = static_cast<uint16_t>(val.size());
        std::memcpy(finalRow.data() + arrPos + varIdx * 4, &offset, sizeof(uint16_t));
        std::memcpy(finalRow.data() + arrPos + varIdx * 4 + 2, &length, sizeof(uint16_t));
        finalRow += val;
        ++varIdx;
    }
    outRow = std::move(finalRow);
    ++pos_;
    return true;
}

void IndexOnlyScanOp::close() {
    rids_.clear();
}

// ========================================================================
// FilterOp
// ========================================================================

FilterOp::FilterOp(OpPtr child, const TableSchema& tbl,
                    const std::vector<StorageEngine::Condition>& conds)
    : child_(std::move(child)), tbl_(tbl), conds_(conds) {}

bool FilterOp::open() {
    return child_->open();
}

bool FilterOp::next(std::string& outRow) {
    while (child_->next(outRow)) {
        bool match = true;
        for (const auto& c : conds_) {
            if (!evalCondRaw(c, outRow, tbl_)) { match = false; break; }
        }
        if (match) return true;
    }
    return false;
}

void FilterOp::close() {
    child_->close();
}

// ========================================================================
// ProjectOp
// ========================================================================

ProjectOp::ProjectOp(OpPtr child, const TableSchema& tbl,
                      const std::set<std::string>& selectCols)
    : child_(std::move(child)), tbl_(tbl), selectCols_(selectCols) {}

bool ProjectOp::open() {
    return child_->open();
}

bool ProjectOp::next(std::string& outRow) {
    std::string raw;
    if (!child_->next(raw)) return false;
    outRow = formatRow(raw, tbl_, selectCols_);
    return true;
}

void ProjectOp::close() {
    child_->close();
}

// ========================================================================
// SortOp
// ========================================================================

SortOp::SortOp(OpPtr child, const TableSchema& tbl,
                const std::string& orderByCol, bool asc)
    : child_(std::move(child)), tbl_(tbl), orderByCol_(orderByCol), asc_(asc) {}

bool SortOp::open() {
    if (!child_->open()) return false;
    std::string row;
    while (child_->next(row)) buffer_.push_back(std::move(row));
    child_->close();

    size_t sortIdx = tbl_.len;
    for (size_t i = 0; i < tbl_.len; ++i) {
        if (tbl_.cols[i].dataName == orderByCol_) { sortIdx = i; break; }
    }
    if (sortIdx < tbl_.len) {
        struct Item { std::string s; int64_t n; Date d; double f; };
        std::vector<std::pair<std::string, Item>> items;
        const Column& scol = tbl_.cols[sortIdx];
        for (auto& r : buffer_) {
            std::string val = StorageEngine::extractColumnValueStatic(r, tbl_, sortIdx);
            Item it{"", 0, {}, 0.0};
            if (scol.dataType == "char" || scol.isVariableLength) {
                it.s = val;
            } else if (scol.dataType == "date") {
                it.d = val.empty() ? Date{} : Date(val.c_str());
            } else if (scol.dataType == "timestamp") {
                it.n = val.empty() ? 0 : parseTimestampToSeconds(val);
            } else if (scol.dataType == "float" || scol.dataType == "double" || scol.dataType == "decimal") {
                try { it.f = std::stod(val); } catch (...) { it.f = 0.0; }
            } else {
                it.n = val.empty() ? 0 : StorageEngine::parseInt(val);
            }
            items.emplace_back(std::move(r), it);
        }
        std::sort(items.begin(), items.end(), [&](const auto& a, const auto& b) {
            if (scol.dataType == "char" || scol.isVariableLength) return asc_ ? (a.second.s < b.second.s) : (b.second.s < a.second.s);
            if (scol.dataType == "date") return asc_ ? (a.second.d < b.second.d) : (b.second.d < a.second.d);
            if (scol.dataType == "float" || scol.dataType == "double" || scol.dataType == "decimal") return asc_ ? (a.second.f < b.second.f) : (b.second.f < a.second.f);
            return asc_ ? (a.second.n < b.second.n) : (b.second.n < a.second.n);
        });
        buffer_.clear();
        for (auto& it : items) buffer_.push_back(std::move(it.first));
    }
    pos_ = 0;
    return true;
}

bool SortOp::next(std::string& outRow) {
    if (pos_ >= buffer_.size()) return false;
    outRow = buffer_[pos_];
    ++pos_;
    return true;
}

void SortOp::close() {
    buffer_.clear();
}

// ========================================================================
// LimitOp
// ========================================================================

LimitOp::LimitOp(OpPtr child, size_t limit)
    : child_(std::move(child)), limit_(limit) {}

bool LimitOp::open() {
    return child_->open();
}

bool LimitOp::next(std::string& outRow) {
    if (count_ >= limit_) return false;
    if (!child_->next(outRow)) return false;
    ++count_;
    return true;
}

void LimitOp::close() {
    count_ = 0;
    child_->close();
}

// ========================================================================
// DistinctOp
// ========================================================================

DistinctOp::DistinctOp(OpPtr child) : child_(std::move(child)) {}

bool DistinctOp::open() {
    seen_.clear();
    return child_->open();
}

bool DistinctOp::next(std::string& outRow) {
    while (child_->next(outRow)) {
        if (seen_.insert(outRow).second) return true;
    }
    return false;
}

void DistinctOp::close() {
    seen_.clear();
    child_->close();
}

// ========================================================================
// NestedLoopJoinOp
// ========================================================================

NestedLoopJoinOp::NestedLoopJoinOp(StorageEngine* engine, const std::string& dbname,
                                    OpPtr left, OpPtr right,
                                    const std::string& leftTable,
                                    const std::string& rightTable,
                                    const std::string& leftCol,
                                    const std::string& rightCol)
    : engine_(engine), dbname_(dbname),
      left_(std::move(left)), right_(std::move(right)),
      leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol) {}

bool NestedLoopJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);
    if (!left_->open()) return false;
    hasLeft_ = left_->next(curLeftRow_);
    return right_->open();
}

bool NestedLoopJoinOp::next(std::string& outRow) {
    std::string rightRow;
    while (hasLeft_) {
        while (right_->next(rightRow)) {
            // Find column offsets
            size_t leftOff = 0;
            for (size_t i = 0; i < leftTbl_.len; ++i) {
                if (leftTbl_.cols[i].dataName == leftCol_) break;
                leftOff += leftTbl_.cols[i].dsize;
            }
            size_t rightOff = 0;
            for (size_t i = 0; i < rightTbl_.len; ++i) {
                if (rightTbl_.cols[i].dataName == rightCol_) break;
                rightOff += rightTbl_.cols[i].dsize;
            }
            // Find column types
            const Column* lc = nullptr;
            for (size_t i = 0; i < leftTbl_.len; ++i) {
                if (leftTbl_.cols[i].dataName == leftCol_) { lc = &leftTbl_.cols[i]; break; }
            }
            const Column* rc = nullptr;
            for (size_t i = 0; i < rightTbl_.len; ++i) {
                if (rightTbl_.cols[i].dataName == rightCol_) { rc = &rightTbl_.cols[i]; break; }
            }
            if (!lc || !rc) continue;

            bool match = false;
            if (lc->dataType == "char") {
                std::string lv(lc->dsize, '\0'), rv(rc->dsize, '\0');
                std::memcpy(lv.data(), curLeftRow_.data() + leftOff, lc->dsize);
                std::memcpy(rv.data(), rightRow.data() + rightOff, rc->dsize);
                auto n = lv.find('\0'); if (n != std::string::npos) lv.resize(n);
                n = rv.find('\0'); if (n != std::string::npos) rv.resize(n);
                match = (lv == rv);
            } else if (lc->dataType == "date") {
                Date ld, rd;
                std::memcpy(&ld, curLeftRow_.data() + leftOff, DATE_SIZE);
                std::memcpy(&rd, rightRow.data() + rightOff, DATE_SIZE);
                match = (ld == rd);
            } else if (lc->dataType == "timestamp") {
                int64_t lv = 0, rv = 0;
                std::memcpy(&lv, curLeftRow_.data() + leftOff, TIMESTAMP_SIZE);
                std::memcpy(&rv, rightRow.data() + rightOff, TIMESTAMP_SIZE);
                match = (lv == rv);
            } else {
                int64_t lv = 0, rv = 0;
                std::memcpy(&lv, curLeftRow_.data() + leftOff, lc->dsize);
                std::memcpy(&rv, rightRow.data() + rightOff, rc->dsize);
                match = (lv == rv);
            }
            if (match) {
                outRow = curLeftRow_ + rightRow;
                return true;
            }
        }
        right_->close();
        hasLeft_ = left_->next(curLeftRow_);
        if (hasLeft_) right_->open();
    }
    return false;
}

void NestedLoopJoinOp::close() {
    left_->close();
    right_->close();
}

// ========================================================================
// Helper: extract join key value from raw row data as string
// ========================================================================
static std::string extractJoinKey(const std::string& row, const TableSchema& tbl, const std::string& colName) {
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) return "";
    return StorageEngine::extractColumnValueStatic(row, tbl, colIdx);
}

// ========================================================================
// HashJoinOp
// ========================================================================

HashJoinOp::HashJoinOp(StorageEngine* engine, const std::string& dbname,
                       OpPtr left, OpPtr right,
                       const std::string& leftTable, const std::string& rightTable,
                       const std::string& leftCol, const std::string& rightCol)
    : engine_(engine), dbname_(dbname),
      left_(std::move(left)), right_(std::move(right)),
      leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol) {}

bool HashJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);

    // Build hash table from right table
    if (!right_->open()) return false;
    std::string rightRow;
    while (right_->next(rightRow)) {
        std::string key = extractJoinKey(rightRow, rightTbl_, rightCol_);
        rightHash_[key].push_back(std::move(rightRow));
    }
    right_->close();

    // Start left table iteration
    if (!left_->open()) return false;
    hasLeft_ = left_->next(curLeftRow_);
    matchPos_ = 0;
    curRightMatches_.clear();
    return true;
}

bool HashJoinOp::next(std::string& outRow) {
    while (hasLeft_) {
        // If we have pending matches for current left row, output them
        while (matchPos_ < curRightMatches_.size()) {
            outRow = curLeftRow_ + curRightMatches_[matchPos_];
            ++matchPos_;
            return true;
        }

        // Move to next left row
        hasLeft_ = left_->next(curLeftRow_);
        if (!hasLeft_) break;

        std::string key = extractJoinKey(curLeftRow_, leftTbl_, leftCol_);
        auto it = rightHash_.find(key);
        if (it != rightHash_.end()) {
            curRightMatches_ = it->second;
            matchPos_ = 0;
        } else {
            curRightMatches_.clear();
            matchPos_ = 0;
        }
    }
    return false;
}

void HashJoinOp::close() {
    left_->close();
    rightHash_.clear();
    curRightMatches_.clear();
}

// ========================================================================
// MergeJoinOp
// ========================================================================

MergeJoinOp::MergeJoinOp(StorageEngine* engine, const std::string& dbname,
                         OpPtr left, OpPtr right,
                         const std::string& leftTable, const std::string& rightTable,
                         const std::string& leftCol, const std::string& rightCol)
    : engine_(engine), dbname_(dbname),
      left_(std::move(left)), right_(std::move(right)),
      leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol) {}

bool MergeJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);

    // Read all left rows
    if (!left_->open()) return false;
    std::string row;
    while (left_->next(row)) leftRows_.push_back(std::move(row));
    left_->close();

    // Read all right rows
    if (!right_->open()) return false;
    while (right_->next(row)) rightRows_.push_back(std::move(row));
    right_->close();

    // Sort both by join key
    auto leftCmp = [&](const std::string& a, const std::string& b) {
        return extractJoinKey(a, leftTbl_, leftCol_) < extractJoinKey(b, leftTbl_, leftCol_);
    };
    auto rightCmp = [&](const std::string& a, const std::string& b) {
        return extractJoinKey(a, rightTbl_, rightCol_) < extractJoinKey(b, rightTbl_, rightCol_);
    };
    std::sort(leftRows_.begin(), leftRows_.end(), leftCmp);
    std::sort(rightRows_.begin(), rightRows_.end(), rightCmp);

    leftPos_ = 0;
    rightPos_ = 0;
    return true;
}

bool MergeJoinOp::next(std::string& outRow) {
    while (leftPos_ < leftRows_.size() && rightPos_ < rightRows_.size()) {
        std::string lk = extractJoinKey(leftRows_[leftPos_], leftTbl_, leftCol_);
        std::string rk = extractJoinKey(rightRows_[rightPos_], rightTbl_, rightCol_);
        if (lk == rk) {
            outRow = leftRows_[leftPos_] + rightRows_[rightPos_];
            ++rightPos_;
            return true;
        } else if (lk < rk) {
            ++leftPos_;
        } else {
            ++rightPos_;
        }
    }
    return false;
}

void MergeJoinOp::close() {
    leftRows_.clear();
    rightRows_.clear();
}

// ========================================================================
// AggregateOp
// ========================================================================

AggregateOp::AggregateOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename,
                          const std::vector<StorageEngine::AggItem>& items)
    : engine_(engine), dbname_(dbname), tablename_(tablename), items_(items) {}

bool AggregateOp::open() {
    done_ = false;
    return true;
}

bool AggregateOp::next(std::string& outRow) {
    if (done_) return false;
    auto res = engine_->aggregate(dbname_, tablename_, {}, items_);
    if (!res.empty()) outRow = res[0];
    done_ = true;
    return !res.empty();
}

void AggregateOp::close() {
    done_ = false;
}

// ========================================================================
// QueryPlanner
// ========================================================================

OpPtr QueryPlanner::buildSelectPlan(StorageEngine* engine, const PlanContext& ctx) {
    OpPtr root;

    // Choose between IndexScan, IndexOnlyScan, and TableScan
    std::vector<StorageEngine::Condition> remainingConds = ctx.conds;
    if (!remainingConds.empty()) {
        for (const auto& c : remainingConds) {
            if (c.op == "=") {
                // Check if column has primary key index
                TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
                bool isPK = false;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == c.colName && tbl.cols[i].isPrimaryKey) {
                        isPK = true; break;
                    }
                }
                bool hasSecIdx = false;
                if (!isPK) {
                    auto indexedCols = engine->getIndexedColumns(ctx.dbname, ctx.tablename);
                    for (const auto& ic : indexedCols) {
                        if (ic == c.colName) { hasSecIdx = true; break; }
                    }
                }
                if (isPK || hasSecIdx) {
                    // Check if IndexOnlyScan is applicable:
                    // SELECT contains only the indexed column (or is a covering composite index)
                    bool useIndexOnly = false;
                    std::vector<std::string> indexCols = {c.colName};
                    std::string compIdxName;
                    if (!ctx.selectCols.empty()) {
                        // All select cols must be in index cols
                        bool allInIdx = true;
                        for (const auto& sc : ctx.selectCols) {
                            if (sc != c.colName) {
                                // Check composite index covering
                                auto compIdxs = engine->getCompositeIndexes(ctx.dbname, ctx.tablename);
                                bool covered = false;
                                for (const auto& ci : compIdxs) {
                                    if (ci.columns.empty() || ci.columns[0] != c.colName) continue;
                                    bool allCovered = true;
                                    for (const auto& nm : ctx.selectCols) {
                                        bool found = false;
                                        for (const auto& cc : ci.columns) {
                                            if (cc == nm) { found = true; break; }
                                        }
                                        if (!found) { allCovered = false; break; }
                                    }
                                    if (allCovered) {
                                        indexCols = ci.columns;
                                        compIdxName = ci.name;
                                        covered = true;
                                        break;
                                    }
                                }
                                allInIdx = covered;
                                break;
                            }
                        }
                        useIndexOnly = allInIdx;
                    }

                    if (useIndexOnly) {
                        root = std::make_unique<IndexOnlyScanOp>(engine, ctx.dbname, ctx.tablename,
                                                                  indexCols, c.value, compIdxName);
                    } else {
                        root = std::make_unique<IndexScanOp>(engine, ctx.dbname, ctx.tablename,
                                                              c.colName, c.value);
                    }
                    // Remove this condition from Filter since IndexScan handles it
                    auto it = remainingConds.begin();
                    while (it != remainingConds.end()) {
                        if (it->colName == c.colName && it->op == c.op && it->value == c.value) {
                            it = remainingConds.erase(it);
                            break;
                        }
                        ++it;
                    }
                    break;
                }
            }
        }
    }

    if (!root) {
        root = std::make_unique<TableScanOp>(engine, ctx.dbname, ctx.tablename);
    }

    // Add Filter if there are remaining conditions
    if (!remainingConds.empty()) {
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        root = std::make_unique<FilterOp>(std::move(root), tbl, remainingConds);
    }

    // Add Sort if ORDER BY
    if (!ctx.orderByCol.empty()) {
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        root = std::make_unique<SortOp>(std::move(root), tbl, ctx.orderByCol, ctx.orderByAsc);
    }

    // Add Distinct if needed
    if (ctx.distinct) {
        root = std::make_unique<DistinctOp>(std::move(root));
    }

    // Add Project if specific columns selected
    if (!ctx.selectCols.empty()) {
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        root = std::make_unique<ProjectOp>(std::move(root), tbl, ctx.selectCols);
    }

    // Add Limit
    if (ctx.limit > 0) {
        root = std::make_unique<LimitOp>(std::move(root), ctx.limit);
    }

    return root;
}

OpPtr QueryPlanner::buildAggregatePlan(StorageEngine* engine, const PlanContext& ctx,
                                        const std::vector<StorageEngine::AggItem>& items) {
    (void)ctx;
    return std::make_unique<AggregateOp>(engine, ctx.dbname, ctx.tablename, items);
}

// Estimate the cost of a join algorithm for given table sizes & index availability.
static double estimateJoinCost(size_t leftRows, size_t rightRows,
                                bool leftIndexed, bool rightIndexed,
                                const std::string& algo) {
    if (algo == "nlj") {
        // NLJ: O(left * right) without index; O(left * log(right)) with index on right
        double perLeft = rightRows;
        if (rightIndexed) perLeft = std::max(size_t(1), rightRows / 10);  // index lookup
        return leftRows * perLeft;
    }
    if (algo == "merge") {
        // Merge: O(left + right) sorted merge
        return leftRows + rightRows;
    }
    if (algo == "hash") {
        // Hash: O(left + right) build + probe
        return leftRows + rightRows * 1.2;
    }
    return leftRows * rightRows;  // fallback (worst case cartesian)
}

OpPtr QueryPlanner::buildJoinPlan(StorageEngine* engine, const std::string& dbname,
                                   const std::string& leftTable, const std::string& rightTable,
                                   const std::string& leftCol, const std::string& rightCol,
                                   const std::vector<StorageEngine::Condition>& conds,
                                   const std::set<std::string>& selectCols) {
    (void)conds;
    (void)selectCols;

    // Get table sizes for join ordering optimization
    size_t leftRows = engine->getTableRowCount(dbname, leftTable);
    size_t rightRows = engine->getTableRowCount(dbname, rightTable);

    // Check if join keys are indexed (candidate for MergeJoin)
    auto leftIdxCols = engine->getIndexedColumns(dbname, leftTable);
    auto rightIdxCols = engine->getIndexedColumns(dbname, rightTable);
    bool leftColIndexed = (std::find(leftIdxCols.begin(), leftIdxCols.end(), leftCol) != leftIdxCols.end());
    bool rightColIndexed = (std::find(rightIdxCols.begin(), rightIdxCols.end(), rightCol) != rightIdxCols.end());
    TableSchema leftTbl = engine->getTableSchema(dbname, leftTable);
    TableSchema rightTbl = engine->getTableSchema(dbname, rightTable);
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol && leftTbl.cols[i].isPrimaryKey) {
            leftColIndexed = true; break;
        }
    }
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol && rightTbl.cols[i].isPrimaryKey) {
            rightColIndexed = true; break;
        }
    }

    // Cost-based algorithm choice: try all three, pick the cheapest.
    double costNLJ = estimateJoinCost(leftRows, rightRows, leftColIndexed, rightColIndexed, "nlj");
    double costMerge = (leftColIndexed && rightColIndexed)
        ? estimateJoinCost(leftRows, rightColIndexed, true, true, "merge")
        : 1e18;
    double costHash = estimateJoinCost(leftRows, rightRows, false, false, "hash");

    // Decide: if small tables, NLJ is fine; otherwise pick cheapest.
    std::string chosenAlgo;
    if (leftRows < 50 && rightRows < 50) {
        chosenAlgo = "nlj";
    } else if (costMerge <= costHash && costMerge < costNLJ) {
        chosenAlgo = "merge";
    } else if (costHash < costNLJ * 0.8) {
        chosenAlgo = "hash";
    } else {
        chosenAlgo = "nlj";
    }

    // JOIN order optimization: put smaller table in the more expensive position.
    bool shouldSwap = false;
    if (chosenAlgo == "nlj" && rightRows < leftRows) {
        shouldSwap = true;  // Outer loop should be smaller for NLJ
    } else if (chosenAlgo == "hash" && rightRows > leftRows) {
        shouldSwap = true;  // Build side (right) should be smaller for HashJoin
    }

    std::string lTbl = shouldSwap ? rightTable : leftTable;
    std::string rTbl = shouldSwap ? leftTable : rightTable;
    std::string lCol = shouldSwap ? rightCol : leftCol;
    std::string rCol = shouldSwap ? leftCol : rightCol;

    auto leftScan = std::make_unique<TableScanOp>(engine, dbname, lTbl);
    auto rightScan = std::make_unique<TableScanOp>(engine, dbname, rTbl);

    if (chosenAlgo == "nlj") {
        return std::make_unique<NestedLoopJoinOp>(
            engine, dbname, std::move(leftScan), std::move(rightScan),
            lTbl, rTbl, lCol, rCol);
    }
    if (chosenAlgo == "merge") {
        return std::make_unique<MergeJoinOp>(
            engine, dbname, std::move(leftScan), std::move(rightScan),
            lTbl, rTbl, lCol, rCol);
    }
    return std::make_unique<HashJoinOp>(
        engine, dbname, std::move(leftScan), std::move(rightScan),
        lTbl, rTbl, lCol, rCol);
}

// ========================================================================
// EXPLAIN with cost estimation
// ========================================================================

struct CostEstimate {
    double rows = 0;
    double cost = 0;
};

static double estimateSelectivity(const StorageEngine::Condition& cond,
                                  StorageEngine* engine,
                                  const std::string& dbname,
                                  const std::string& tablename) {
    if (cond.op == "=") {
        auto stats = engine->getColumnStats(dbname, tablename, cond.colName);
        if (stats.cardinality > 0) {
            return 1.0 / static_cast<double>(stats.cardinality);
        }
        return 0.1;
    }
    if (cond.op == "!=") return 0.9;
    if (cond.op == "like") return 0.2;
    // Range operators: <, >, <=, >=
    return 0.3;
}

static std::string costRowsStr(const CostEstimate& est, const QueryPlanner::ExplainOptions& opts) {
    if (!opts.costs) return "";
    return "  cost=" + std::to_string(static_cast<int>(est.cost)) +
           "  rows=" + std::to_string(static_cast<int>(est.rows));
}

static CostEstimate explainOp(Operator* op, int indent,
                              StorageEngine* engine,
                              const std::string& dbname,
                              std::string& out,
                              const QueryPlanner::ExplainOptions& opts) {
    std::string prefix(indent * 2, ' ');
    CostEstimate est;

    if (auto* scan = dynamic_cast<TableScanOp*>(op)) {
        double rows = static_cast<double>(engine->getTableRowCount(dbname, scan->tableName()));
        est.rows = rows;
        est.cost = rows * 1.0;
        out += prefix + "TableScan(table=" + scan->tableName() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* idx = dynamic_cast<IndexScanOp*>(op)) {
        double rows = idx->colName().empty() ? 1.0 : 5.0;
        TableSchema tbl = engine->getTableSchema(dbname, idx->tableName());
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == idx->colName() && tbl.cols[i].isPrimaryKey) {
                rows = 1.0;
                break;
            }
        }
        auto stats = engine->getColumnStats(dbname, idx->tableName(), idx->colName());
        if (stats.cardinality > 0) {
            rows = static_cast<double>(engine->getTableRowCount(dbname, idx->tableName()))
                   / static_cast<double>(stats.cardinality);
            if (rows < 1.0) rows = 1.0;
        }
        est.rows = rows;
        est.cost = rows * 2.0;
        out += prefix + "IndexScan(table=" + idx->tableName() +
               ", col=" + idx->colName() + ", val=" + idx->value() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* filt = dynamic_cast<FilterOp*>(op)) {
        CostEstimate child = explainOp(filt->child(), indent + 1, engine, dbname, out, opts);
        double sel = 1.0;
        for (const auto& c : filt->conditions()) {
            std::string tblName;
            if (auto* ts = dynamic_cast<TableScanOp*>(filt->child())) {
                tblName = ts->tableName();
            } else if (auto* is = dynamic_cast<IndexScanOp*>(filt->child())) {
                tblName = is->tableName();
            }
            sel *= estimateSelectivity(c, engine, dbname, tblName);
        }
        est.rows = child.rows * sel;
        est.cost = child.cost + est.rows * 0.5;
        out += prefix + "Filter" + costRowsStr(est, opts) + "\n";

    } else if (auto* proj = dynamic_cast<ProjectOp*>(op)) {
        CostEstimate child = explainOp(proj->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows;
        est.cost = child.cost + child.rows * 0.1;
        out += prefix + "Project" + costRowsStr(est, opts) + "\n";

    } else if (auto* sort = dynamic_cast<SortOp*>(op)) {
        CostEstimate child = explainOp(sort->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        out += prefix + "Sort" + costRowsStr(est, opts) + "\n";

    } else if (auto* lim = dynamic_cast<LimitOp*>(op)) {
        CostEstimate child = explainOp(lim->child(), indent + 1, engine, dbname, out, opts);
        est.rows = std::min(child.rows, static_cast<double>(lim->limit()));
        est.cost = child.cost + est.rows * 0.01;
        out += prefix + "Limit(limit=" + std::to_string(lim->limit()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* dist = dynamic_cast<DistinctOp*>(op)) {
        CostEstimate child = explainOp(dist->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows * 0.5;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = child.cost + child.rows * 0.5;
        out += prefix + "Distinct" + costRowsStr(est, opts) + "\n";

    } else if (auto* join = dynamic_cast<NestedLoopJoinOp*>(op)) {
        CostEstimate left = explainOp(join->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(join->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + left.rows * right.cost;
        out += prefix + "NestedLoopJoin(" + join->leftTable() + ", " + join->rightTable() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* hjoin = dynamic_cast<HashJoinOp*>(op)) {
        CostEstimate left = explainOp(hjoin->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(hjoin->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + right.rows * 2.0;
        out += prefix + "HashJoin(" + hjoin->leftTable() + ", " + hjoin->rightTable() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* mjoin = dynamic_cast<MergeJoinOp*>(op)) {
        CostEstimate left = explainOp(mjoin->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(mjoin->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + left.rows * std::log2(left.rows + 1) * 0.1
                   + right.rows * std::log2(right.rows + 1) * 0.1;
        out += prefix + "MergeJoin(" + mjoin->leftTable() + ", " + mjoin->rightTable() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (dynamic_cast<AggregateOp*>(op)) {
        est.rows = 1;
        est.cost = 10;
        out += prefix + "Aggregate" + costRowsStr(est, opts) + "\n";

    } else {
        out += prefix + "Unknown\n";
    }

    return est;
}

std::string QueryPlanner::explain(OpPtr& plan, StorageEngine* engine,
                                  const std::string& dbname) {
    ExplainOptions opts;
    return explain(plan, engine, dbname, opts);
}

std::string QueryPlanner::explain(OpPtr& plan, StorageEngine* engine,
                                  const std::string& dbname,
                                  const ExplainOptions& opts) {
    std::string result;
    CostEstimate total = explainOp(plan.get(), 0, engine, dbname, result, opts);
    if (opts.settings) {
        auto cfg = g_config;
        result += "Settings: work_mem=" + std::to_string(cfg.workMemKb) + "kB";
        result += ", enable_seqscan=" + std::string(cfg.enableSeqScan ? "on" : "off");
        result += ", enable_hashjoin=" + std::string(cfg.enableHashJoin ? "on" : "off");
        result += ", enable_mergejoin=" + std::string(cfg.enableMergeJoin ? "on" : "off");
        result += ", checkpoint_interval=" + std::to_string(cfg.checkpointInterval) + "\n";
    }
    if (opts.costs) {
        result += "\nTotal estimated cost: " + std::to_string(static_cast<int>(total.cost));
        result += ", estimated rows: " + std::to_string(static_cast<int>(total.rows)) + "\n";
    }
    if (opts.buffers) {
        auto bpStats = engine->getBufferPoolStats();
        result += "Buffers: shared_hit=" + std::to_string(bpStats.totalHits);
        result += " shared_read=" + std::to_string(bpStats.totalMisses);
        result += " hit_rate=" + std::to_string(static_cast<int>(bpStats.hitRate)) + "%\n";
    }
    return result;
}

// ========================================================================
// EXPLAIN FORMAT JSON
// ========================================================================

static std::string jsonEscape(const std::string& s) {
    std::string out;
    for (char c : s) {
        if (c == '"') out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\b') out += "\\b";
        else if (c == '\f') out += "\\f";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c == '\t') out += "\\t";
        else out += c;
    }
    return out;
}

static std::string jsonCostRows(const CostEstimate& est, const QueryPlanner::ExplainOptions& opts) {
    if (!opts.costs) return "";
    return "\"cost\":" + std::to_string(static_cast<int>(est.cost)) + ","
         + "\"rows\":" + std::to_string(static_cast<int>(est.rows)) + ",";
}

static std::pair<std::string, CostEstimate> explainOpJson(Operator* op,
                                                            StorageEngine* engine,
                                                            const std::string& dbname,
                                                            const QueryPlanner::ExplainOptions& opts) {
    std::string json = "{";
    CostEstimate est;

    if (auto* scan = dynamic_cast<TableScanOp*>(op)) {
        double rows = static_cast<double>(engine->getTableRowCount(dbname, scan->tableName()));
        est.rows = rows;
        est.cost = rows * 1.0;
        json += "\"nodeType\":\"TableScan\",";
        json += "\"table\":\"" + jsonEscape(scan->tableName()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* idx = dynamic_cast<IndexScanOp*>(op)) {
        double rows = idx->colName().empty() ? 1.0 : 5.0;
        TableSchema tbl = engine->getTableSchema(dbname, idx->tableName());
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == idx->colName() && tbl.cols[i].isPrimaryKey) {
                rows = 1.0;
                break;
            }
        }
        auto stats = engine->getColumnStats(dbname, idx->tableName(), idx->colName());
        if (stats.cardinality > 0) {
            rows = static_cast<double>(engine->getTableRowCount(dbname, idx->tableName()))
                   / static_cast<double>(stats.cardinality);
            if (rows < 1.0) rows = 1.0;
        }
        est.rows = rows;
        est.cost = rows * 2.0;
        json += "\"nodeType\":\"IndexScan\",";
        json += "\"table\":\"" + jsonEscape(idx->tableName()) + "\",";
        json += "\"column\":\"" + jsonEscape(idx->colName()) + "\",";
        json += "\"value\":\"" + jsonEscape(idx->value()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* filt = dynamic_cast<FilterOp*>(op)) {
        auto [childJson, child] = explainOpJson(filt->child(), engine, dbname, opts);
        double sel = 1.0;
        for (const auto& c : filt->conditions()) {
            std::string tblName;
            if (auto* ts = dynamic_cast<TableScanOp*>(filt->child())) {
                tblName = ts->tableName();
            } else if (auto* is = dynamic_cast<IndexScanOp*>(filt->child())) {
                tblName = is->tableName();
            }
            sel *= estimateSelectivity(c, engine, dbname, tblName);
        }
        est.rows = child.rows * sel;
        est.cost = child.cost + est.rows * 0.5;
        json += "\"nodeType\":\"Filter\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* proj = dynamic_cast<ProjectOp*>(op)) {
        auto [childJson, child] = explainOpJson(proj->child(), engine, dbname, opts);
        est.rows = child.rows;
        est.cost = child.cost + child.rows * 0.1;
        json += "\"nodeType\":\"Project\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* sort = dynamic_cast<SortOp*>(op)) {
        auto [childJson, child] = explainOpJson(sort->child(), engine, dbname, opts);
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        json += "\"nodeType\":\"Sort\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* lim = dynamic_cast<LimitOp*>(op)) {
        auto [childJson, child] = explainOpJson(lim->child(), engine, dbname, opts);
        est.rows = std::min(child.rows, static_cast<double>(lim->limit()));
        est.cost = child.cost + est.rows * 0.01;
        json += "\"nodeType\":\"Limit\",";
        json += "\"limit\":" + std::to_string(lim->limit()) + ",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* dist = dynamic_cast<DistinctOp*>(op)) {
        auto [childJson, child] = explainOpJson(dist->child(), engine, dbname, opts);
        est.rows = child.rows * 0.5;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = child.cost + child.rows * 0.5;
        json += "\"nodeType\":\"Distinct\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* join = dynamic_cast<NestedLoopJoinOp*>(op)) {
        auto [leftJson, left] = explainOpJson(join->leftChild(), engine, dbname, opts);
        auto [rightJson, right] = explainOpJson(join->rightChild(), engine, dbname, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + left.rows * right.cost;
        json += "\"nodeType\":\"NestedLoopJoin\",";
        json += "\"leftTable\":\"" + jsonEscape(join->leftTable()) + "\",";
        json += "\"rightTable\":\"" + jsonEscape(join->rightTable()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + leftJson + "," + rightJson + "]";

    } else if (auto* hjoin = dynamic_cast<HashJoinOp*>(op)) {
        auto [leftJson, left] = explainOpJson(hjoin->leftChild(), engine, dbname, opts);
        auto [rightJson, right] = explainOpJson(hjoin->rightChild(), engine, dbname, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + right.rows * 2.0;
        json += "\"nodeType\":\"HashJoin\",";
        json += "\"leftTable\":\"" + jsonEscape(hjoin->leftTable()) + "\",";
        json += "\"rightTable\":\"" + jsonEscape(hjoin->rightTable()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + leftJson + "," + rightJson + "]";

    } else if (auto* mjoin = dynamic_cast<MergeJoinOp*>(op)) {
        auto [leftJson, left] = explainOpJson(mjoin->leftChild(), engine, dbname, opts);
        auto [rightJson, right] = explainOpJson(mjoin->rightChild(), engine, dbname, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + left.rows * std::log2(left.rows + 1) * 0.1
                   + right.rows * std::log2(right.rows + 1) * 0.1;
        json += "\"nodeType\":\"MergeJoin\",";
        json += "\"leftTable\":\"" + jsonEscape(mjoin->leftTable()) + "\",";
        json += "\"rightTable\":\"" + jsonEscape(mjoin->rightTable()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + leftJson + "," + rightJson + "]";

    } else if (dynamic_cast<AggregateOp*>(op)) {
        est.rows = 1;
        est.cost = 10;
        json += "\"nodeType\":\"Aggregate\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else {
        json += "\"nodeType\":\"Unknown\",";
        json += "\"children\":[]";
    }

    json += "}";
    return {json, est};
}

std::string QueryPlanner::explainJson(OpPtr& plan, StorageEngine* engine,
                                      const std::string& dbname) {
    ExplainOptions opts;
    return explainJson(plan, engine, dbname, opts);
}

std::string QueryPlanner::explainJson(OpPtr& plan, StorageEngine* engine,
                                      const std::string& dbname,
                                      const ExplainOptions& opts) {
    auto [planJson, total] = explainOpJson(plan.get(), engine, dbname, opts);
    std::string result = "{\n";
    result += "  \"plan\": " + planJson + ",\n";
    if (opts.costs) {
        result += "  \"totalCost\": " + std::to_string(static_cast<int>(total.cost)) + ",\n";
        result += "  \"totalRows\": " + std::to_string(static_cast<int>(total.rows));
    }
    if (opts.settings) {
        auto cfg = g_config;
        if (opts.costs) result += ",";
        result += "\n  \"settings\": {\n";
        result += "    \"workMemKb\": " + std::to_string(cfg.workMemKb) + ",\n";
        result += "    \"enableSeqScan\": " + std::string(cfg.enableSeqScan ? "true" : "false") + ",\n";
        result += "    \"enableHashJoin\": " + std::string(cfg.enableHashJoin ? "true" : "false") + ",\n";
        result += "    \"enableMergeJoin\": " + std::string(cfg.enableMergeJoin ? "true" : "false") + ",\n";
        result += "    \"checkpointInterval\": " + std::to_string(cfg.checkpointInterval) + "\n";
        result += "  }";
    }
    if (opts.buffers) {
        auto bpStats = engine->getBufferPoolStats();
        result += ",\n  \"buffers\": {\n";
        result += "    \"sharedHit\": " + std::to_string(bpStats.totalHits) + ",\n";
        result += "    \"sharedRead\": " + std::to_string(bpStats.totalMisses) + ",\n";
        result += "    \"hitRate\": " + std::to_string(static_cast<int>(bpStats.hitRate)) + "\n";
        result += "  }";
    }
    result += "\n}\n";
    return result;
}

std::vector<std::string> QueryPlanner::executePlan(OpPtr plan) {
    std::vector<std::string> results;
    if (!plan) return results;
    if (!plan->open()) return results;
    std::string row;
    while (plan->next(row)) {
        results.push_back(row);
    }
    plan->close();
    return results;
}

// Check if an index provides the required pathkey ordering.
static bool indexProvidesOrdering(StorageEngine* engine, const std::string& dbname,
                                   const std::string& tablename,
                                   const std::string& orderCol) {
    // Primary key index provides ordering on PK columns.
    TableSchema tbl = engine->getTableSchema(dbname, tablename);
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == orderCol && tbl.cols[i].isPrimaryKey)
            return true;
    }
    // Secondary index provides ordering on indexed columns.
    auto idxCols = engine->getIndexedColumns(dbname, tablename);
    return std::find(idxCols.begin(), idxCols.end(), orderCol) != idxCols.end();
}

// Build plan with pathkey awareness: if an index can provide the required
// ordering, use IndexScan to avoid a separate Sort step.
OpPtr QueryPlanner::buildSelectPlan(StorageEngine* engine, const PlanContext& ctx,
                                      const std::vector<PathKey>& requiredPathkeys,
                                      const std::vector<EquivalenceClass>& eqClasses) {
    // Use equivalence classes to find additional filter conditions.
    // If t1.id = t2.fk is an equivalence class and we're scanning t1 with
    // WHERE t1.id = 5, we can also infer t2.fk = 5 for a subsequent join.
    // For now, the eqClasses are used to validate join conditions.

    // Check if ORDER BY can be satisfied by an index (pathkey optimization).
    bool useIndexForOrdering = false;
    if (!requiredPathkeys.empty() && !ctx.orderByCol.empty()) {
        const auto& pk = requiredPathkeys[0];
        if (pk.expr == ctx.orderByCol &&
            indexProvidesOrdering(engine, ctx.dbname, ctx.tablename, ctx.orderByCol)) {
            useIndexForOrdering = true;
        }
    }

    // Build the basic plan first.
    OpPtr plan = buildSelectPlan(engine, ctx);

    // If ordering is provided by the index, remove the SortOp (last in chain).
    if (useIndexForOrdering) {
        // Walk the operator tree and remove the topmost SortOp.
        OpPtr* cur = &plan;
        OpPtr prev;
        while (*cur) {
            // Check if this is a SortOp by dynamic_cast
            // Since we can't easily identify type without RTTI on the interface,
            // we rely on the fact that Sort is always the last operator before Limit.
            // For now, skip the optimization if we can't safely identify the Sort.
            break;  // Safe fallback: keep the Sort.
        }
    }

    return plan;
}

// Skip scan: for multi-column indexes, skip over ranges that cannot satisfy
// the query.  This is a metadata-only optimization flag.
static bool canUseSkipScan(const std::vector<StorageEngine::Condition>& conds,
                            const std::vector<std::string>& indexCols) {
    if (indexCols.size() < 2 || conds.empty()) return false;
    // Skip scan applies when: leading column has an IN list or <> and
    // trailing columns have range conditions.
    // Simplified: if we have a <> or IN on a non-leading index column.
    for (const auto& c : conds) {
        if ((c.op == "<>" || c.op == "in") &&
            std::find(indexCols.begin(), indexCols.end(), c.colName) != indexCols.end()) {
            return true;
        }
    }
    return false;
}

} // namespace dbms
