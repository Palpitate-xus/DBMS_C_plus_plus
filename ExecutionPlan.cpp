#include "ExecutionPlan.h"

#include <algorithm>
#include <cmath>
#include <cstring>

namespace dbms {

// ========================================================================
// Helper: format a raw row buffer into display string
// ========================================================================
static std::string formatRow(const char* rowData, const TableSchema& tbl,
                              const std::set<std::string>& selectCols) {
    std::string rowStr;
    size_t offset = 0;
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!selectCols.empty() && selectCols.find(col.dataName) == selectCols.end()) {
            offset += col.dsize;
            continue;
        }
        if (col.dataType == "char") {
            std::string buf(col.dsize, '\0');
            std::memcpy(buf.data(), rowData + offset, col.dsize);
            auto nul = buf.find('\0');
            if (nul != std::string::npos) buf.resize(nul);
            rowStr += buf + ' ';
        } else if (col.dataType == "date") {
            Date d;
            std::memcpy(&d, rowData + offset, DATE_SIZE);
            rowStr += str(d) + ' ';
        } else {
            int64_t val = 0;
            std::memcpy(&val, rowData + offset, col.dsize);
            if (val == INF) rowStr += "NULL ";
            else rowStr += transstr(val) + ' ';
        }
        offset += col.dsize;
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
                         const char* rowData, const TableSchema& tbl) {
    size_t offset = 0;
    size_t ci = 0;
    for (; ci < tbl.len && tbl.cols[ci].dataName != cond.colName; ++ci)
        offset += tbl.cols[ci].dsize;
    if (ci >= tbl.len) return false;

    const Column& col = tbl.cols[ci];
    if (col.dataType == "char") {
        std::string buf(col.dsize, '\0');
        std::memcpy(buf.data(), rowData + offset, col.dsize);
        auto nul = buf.find('\0');
        if (nul != std::string::npos) buf.resize(nul);
        if (cond.op == "<"  && !(buf <  cond.value)) return false;
        if (cond.op == ">"  && !(buf >  cond.value)) return false;
        if (cond.op == "="  && buf != cond.value)    return false;
        if (cond.op == "<=" && (buf >  cond.value))   return false;
        if (cond.op == ">=" && (buf <  cond.value))   return false;
        if (cond.op == "!=" && buf == cond.value)    return false;
        if (cond.op == "like" && !likeMatch(buf, cond.value)) return false;
    } else if (col.dataType == "date") {
        Date d;
        std::memcpy(&d, rowData + offset, DATE_SIZE);
        Date v(cond.value.c_str());
        if (cond.op == "<"  && v.year && !(d < v))  return false;
        if (cond.op == ">"  && v.year && !(d > v))  return false;
        if (cond.op == "="  && v.year && d != v)    return false;
        if (cond.op == "<=" && v.year && (d > v))   return false;
        if (cond.op == ">=" && v.year && (d < v))   return false;
        if (cond.op == "!=" && v.year && d == v)    return false;
    } else {
        int64_t val = 0;
        std::memcpy(&val, rowData + offset, col.dsize);
        int64_t cmp = StorageEngine::parseInt(cond.value);
        if (cond.op == "<"  && cmp != INF && !(val < cmp)) return false;
        if (cond.op == ">"  && cmp != INF && !(val > cmp)) return false;
        if (cond.op == "="  && cmp != INF && val != cmp)   return false;
        if (cond.op == "<=" && cmp != INF && (val > cmp))  return false;
        if (cond.op == ">=" && cmp != INF && (val < cmp))  return false;
        if (cond.op == "!=" && cmp != INF && val == cmp)   return false;
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
            rows_.emplace_back(StorageEngine::encodeRid(pageId, slotId),
                               std::string(data, len));
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
    auto* pa = const_cast<StorageEngine*>(engine_)->getPageAllocator(dbname_, tablename_);
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
    outRow = std::move(row);
    ++pos_;
    return true;
}

void IndexScanOp::close() {
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
            if (!evalCondRaw(c, outRow.data(), tbl_)) { match = false; break; }
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
    outRow = formatRow(raw.data(), tbl_, selectCols_);
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
        struct Item { std::string s; int64_t n; Date d; };
        std::vector<std::pair<std::string, Item>> items;
        const Column& scol = tbl_.cols[sortIdx];
        for (auto& r : buffer_) {
            size_t offset = 0;
            for (size_t c = 0; c < sortIdx; ++c) offset += tbl_.cols[c].dsize;
            Item it{"", 0, {}};
            if (scol.dataType == "char") {
                std::string buf(scol.dsize, '\0');
                std::memcpy(buf.data(), r.data() + offset, scol.dsize);
                auto nul = buf.find('\0');
                if (nul != std::string::npos) buf.resize(nul);
                it.s = buf;
            } else if (scol.dataType == "date") {
                std::memcpy(&it.d, r.data() + offset, DATE_SIZE);
            } else {
                std::memcpy(&it.n, r.data() + offset, scol.dsize);
            }
            items.emplace_back(std::move(r), it);
        }
        std::sort(items.begin(), items.end(), [&](const auto& a, const auto& b) {
            if (scol.dataType == "char") return asc_ ? (a.second.s < b.second.s) : (b.second.s < a.second.s);
            if (scol.dataType == "date") return asc_ ? (a.second.d < b.second.d) : (b.second.d < a.second.d);
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
// AggregateOp
// ========================================================================

AggregateOp::AggregateOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename,
                          const std::vector<std::pair<std::string, std::string>>& items)
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

    // Choose between IndexScan and TableScan
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
                    root = std::make_unique<IndexScanOp>(engine, ctx.dbname, ctx.tablename,
                                                          c.colName, c.value);
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
                                        const std::vector<std::pair<std::string, std::string>>& items) {
    (void)ctx;
    return std::make_unique<AggregateOp>(engine, ctx.dbname, ctx.tablename, items);
}

OpPtr QueryPlanner::buildJoinPlan(StorageEngine* engine, const std::string& dbname,
                                   const std::string& leftTable, const std::string& rightTable,
                                   const std::string& leftCol, const std::string& rightCol,
                                   const std::vector<StorageEngine::Condition>& conds,
                                   const std::set<std::string>& selectCols) {
    (void)conds;
    auto left = std::make_unique<TableScanOp>(engine, dbname, leftTable);
    auto right = std::make_unique<TableScanOp>(engine, dbname, rightTable);
    auto join = std::make_unique<NestedLoopJoinOp>(
        engine, dbname, std::move(left), std::move(right),
        leftTable, rightTable, leftCol, rightCol);
    // TODO: add filter for WHERE conditions, project for selectCols
    (void)selectCols;
    return join;
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

static CostEstimate explainOp(Operator* op, int indent,
                              StorageEngine* engine,
                              const std::string& dbname,
                              std::string& out) {
    std::string prefix(indent * 2, ' ');
    CostEstimate est;

    if (auto* scan = dynamic_cast<TableScanOp*>(op)) {
        double rows = static_cast<double>(engine->getTableRowCount(dbname, scan->tableName()));
        est.rows = rows;
        est.cost = rows * 1.0;
        out += prefix + "TableScan(table=" + scan->tableName() +
               ")  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* idx = dynamic_cast<IndexScanOp*>(op)) {
        double rows = idx->colName().empty() ? 1.0 : 5.0;
        // PK scan usually returns 1 row
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
        est.cost = rows * 2.0;  // index lookup + row fetch
        out += prefix + "IndexScan(table=" + idx->tableName() +
               ", col=" + idx->colName() + ", val=" + idx->value() +
               ")  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* filt = dynamic_cast<FilterOp*>(op)) {
        CostEstimate child = explainOp(filt->child(), indent + 1, engine, dbname, out);
        double sel = 1.0;
        for (const auto& c : filt->conditions()) {
            // Try to find the table name for this condition
            // We look at the child operator to determine the table
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
        out += prefix + "Filter  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* proj = dynamic_cast<ProjectOp*>(op)) {
        CostEstimate child = explainOp(proj->child(), indent + 1, engine, dbname, out);
        est.rows = child.rows;
        est.cost = child.cost + child.rows * 0.1;
        out += prefix + "Project  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* sort = dynamic_cast<SortOp*>(op)) {
        CostEstimate child = explainOp(sort->child(), indent + 1, engine, dbname, out);
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        out += prefix + "Sort  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* lim = dynamic_cast<LimitOp*>(op)) {
        CostEstimate child = explainOp(lim->child(), indent + 1, engine, dbname, out);
        est.rows = std::min(child.rows, static_cast<double>(lim->limit()));
        est.cost = child.cost + est.rows * 0.01;
        out += prefix + "Limit(limit=" + std::to_string(lim->limit()) +
               ")  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* dist = dynamic_cast<DistinctOp*>(op)) {
        CostEstimate child = explainOp(dist->child(), indent + 1, engine, dbname, out);
        est.rows = child.rows * 0.5;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = child.cost + child.rows * 0.5;
        out += prefix + "Distinct  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (auto* join = dynamic_cast<NestedLoopJoinOp*>(op)) {
        CostEstimate left = explainOp(join->leftChild(), indent + 1, engine, dbname, out);
        CostEstimate right = explainOp(join->rightChild(), indent + 1, engine, dbname, out);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + left.rows * right.cost;
        out += prefix + "NestedLoopJoin(" + join->leftTable() + ", " + join->rightTable() +
               ")  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else if (dynamic_cast<AggregateOp*>(op)) {
        est.rows = 1;
        est.cost = 10;  // base aggregate cost
        out += prefix + "Aggregate  cost=" + std::to_string(static_cast<int>(est.cost)) +
               "  rows=" + std::to_string(static_cast<int>(est.rows)) + "\n";

    } else {
        out += prefix + "Unknown\n";
    }

    return est;
}

std::string QueryPlanner::explain(OpPtr& plan, StorageEngine* engine,
                                  const std::string& dbname) {
    std::string result;
    CostEstimate total = explainOp(plan.get(), 0, engine, dbname, result);
    result += "\nTotal estimated cost: " + std::to_string(static_cast<int>(total.cost));
    result += ", estimated rows: " + std::to_string(static_cast<int>(total.rows)) + "\n";
    return result;
}

} // namespace dbms
