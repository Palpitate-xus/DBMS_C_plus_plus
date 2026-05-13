#include "TableManage.h"
#include "TxnIdGenerator.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

namespace dbms {

// Global active transaction tracking
std::mutex StorageEngine::globalTxnMutex_;
std::set<uint64_t> StorageEngine::activeTransactions_;

bool StorageEngine::ReadView::isVisible(uint64_t rowTxnId) const {
    if (rowTxnId == creatorTxnId) return true;
    if (rowTxnId < upLimitId) return true;
    if (rowTxnId >= lowLimitId) return false;
    if (activeTxnIds.count(rowTxnId)) return false;
    return true;
}

static std::string trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

// SQL LIKE pattern matching (% = any sequence, _ = single char), case-insensitive
static bool likeMatch(const std::string& text, const std::string& pattern) {
    std::string t = text;
    std::string p = pattern;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    for (char& c : p) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    size_t i = 0, j = 0;
    size_t starIdx = std::string::npos, matchIdx = 0;
    while (i < t.size()) {
        if (j < p.size() && (p[j] == t[i] || p[j] == '_')) {
            ++i; ++j;
        } else if (j < p.size() && p[j] == '%') {
            starIdx = j++;
            matchIdx = i;
        } else if (starIdx != std::string::npos) {
            j = starIdx + 1;
            i = ++matchIdx;
        } else {
            return false;
        }
    }
    while (j < p.size() && p[j] == '%') ++j;
    return j == p.size();
}

// ========================================================================
// Helper: fixed-length string IO (for backward-compatible binary format)
// ========================================================================
static void writeFixedString(std::ostream& out, const std::string& s, size_t len) {
    std::string buf = s;
    buf.resize(len, '\0');
    out.write(buf.data(), static_cast<std::streamsize>(len));
}

static std::string readFixedString(std::istream& in, size_t len) {
    std::string buf(len, '\0');
    in.read(buf.data(), static_cast<std::streamsize>(len));
    auto pos = buf.find('\0');
    if (pos != std::string::npos) buf.resize(pos);
    return buf;
}

// ========================================================================
// Column / TableSchema
// ========================================================================
void Column::print() const {
    std::cout << "ColumnName: " << dataName << '\n';
    std::cout << "hasNull: " << isNull << '\n';
    std::cout << "PrimaryKey: " << isPrimaryKey << '\n';
    std::cout << "DataType: " << dataType << '\n';
    std::cout << "DataSize: " << dsize << "\n\n";
}

void TableSchema::append(const Column& ncol) {
    if (len < MAX_COLUMNS) {
        cols[len++] = ncol;
    }
}

void TableSchema::appendFK(const ForeignKey& fk) {
    if (fkLen < MAX_COLUMNS) {
        fks[fkLen++] = fk;
    }
}

void TableSchema::print() const {
    std::cout << tablename << "\n\n";
    for (size_t i = 0; i < len; ++i) cols[i].print();
    if (fkLen > 0) {
        std::cout << "Foreign Keys:\n";
        for (size_t i = 0; i < fkLen; ++i) {
            std::cout << "  " << fks[i].colName << " -> " << fks[i].refTable
                      << "(" << fks[i].refCol << ") ON DELETE "
                      << fks[i].onDelete << "\n";
        }
    }
}

size_t TableSchema::rowSize() const {
    size_t total = 0;
    for (size_t i = 0; i < len; ++i) total += cols[i].dsize;
    return total + MVCC_HEADER_SIZE;
}

bool TableSchema::hasVariableLength() const {
    for (size_t i = 0; i < len; ++i) {
        if (cols[i].isVariableLength) return true;
    }
    return false;
}

size_t TableSchema::fixedDataSize() const {
    size_t total = 0;
    for (size_t i = 0; i < len; ++i) {
        if (!cols[i].isVariableLength) total += cols[i].dsize;
    }
    return total;
}

size_t TableSchema::varColCount() const {
    size_t count = 0;
    for (size_t i = 0; i < len; ++i) {
        if (cols[i].isVariableLength) ++count;
    }
    return count;
}

size_t TableSchema::getVarColIndex(size_t colIdx) const {
    size_t idx = 0;
    for (size_t i = 0; i < colIdx; ++i) {
        if (cols[i].isVariableLength) ++idx;
    }
    return idx;
}

size_t TableSchema::getFixedColOffset(size_t colIdx) const {
    size_t off = 0;
    for (size_t i = 0; i < colIdx; ++i) {
        if (!cols[i].isVariableLength) off += cols[i].dsize;
    }
    return off;
}

// ========================================================================
// Column constructors
// ========================================================================
Column makeIntColumn(const std::string& name, bool isNull, int scale, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    if (scale <= 1) {
        c.dataType = "tiny";
        c.dsize = 1;
    } else if (scale == 2) {
        c.dataType = "int";
        c.dsize = 4;
    } else {
        c.dataType = "long";
        c.dsize = 8;
    }
    return c;
}

Column makeStringColumn(const std::string& name, bool isNull, size_t length, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "char";
    c.dsize = std::max(size_t(1), std::min(length, size_t(1005)));
    return c;
}

Column makeDateColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "date";
    c.dsize = DATE_SIZE;
    return c;
}

Column makeVarCharColumn(const std::string& name, bool isNull, size_t maxLen, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "varchar";
    c.dsize = std::max(size_t(1), std::min(maxLen, size_t(65535)));
    return c;
}

// ========================================================================
// StorageEngine
// ========================================================================
StorageEngine::StorageEngine() {
    recoverAllDatabases();
    migrateAllDataFiles();
}

// ========================================================================
// Primary Key Index
// ========================================================================
std::filesystem::path StorageEngine::dbPath(const std::string& dbname) const {
    return std::filesystem::path(dbname);
}

std::filesystem::path StorageEngine::schemaPath(const std::string& dbname,
                                                 const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".stc");
}

std::filesystem::path StorageEngine::dataPath(const std::string& dbname,
                                               const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".dt");
}

std::filesystem::path StorageEngine::tableListPath(const std::string& dbname) const {
    return dbPath(dbname) / "tlist.lst";
}

std::filesystem::path StorageEngine::walPath(const std::string& dbname) const {
    return dbPath(dbname) / "wal.log";
}

std::filesystem::path StorageEngine::checkpointPath(const std::string& dbname) const {
    return dbPath(dbname) / "checkpoint";
}

std::filesystem::path StorageEngine::viewPath(const std::string& dbname,
                                               const std::string& viewname) const {
    return dbPath(dbname) / (viewname + ".view");
}

std::filesystem::path StorageEngine::viewsDir(const std::string& dbname) const {
    return dbPath(dbname) / ".views";
}

OpResult StorageEngine::createView(const std::string& dbname,
                                    const std::string& viewname,
                                    const std::string& sql) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto vdir = viewsDir(dbname);
    if (!std::filesystem::exists(vdir)) {
        std::filesystem::create_directories(vdir);
    }
    std::ofstream ofs(viewPath(dbname, viewname));
    if (!ofs) return OpResult::InvalidValue;
    ofs << sql;
    return OpResult::Success;
}

OpResult StorageEngine::dropView(const std::string& dbname,
                                  const std::string& viewname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = viewPath(dbname, viewname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::viewExists(const std::string& dbname,
                                const std::string& viewname) const {
    return std::filesystem::exists(viewPath(dbname, viewname));
}

std::string StorageEngine::getViewSQL(const std::string& dbname,
                                       const std::string& viewname) const {
    auto path = viewPath(dbname, viewname);
    if (!std::filesystem::exists(path)) return "";
    std::ifstream ifs(path);
    if (!ifs) return "";
    std::string sql((std::istreambuf_iterator<char>(ifs)),
                     std::istreambuf_iterator<char>());
    return sql;
}

std::vector<std::string> StorageEngine::getViewNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto vdir = viewsDir(dbname);
    if (!std::filesystem::exists(vdir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(vdir)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 5 && name.substr(name.size() - 5) == ".view") {
                result.push_back(name.substr(0, name.size() - 5));
            }
        }
    }
    return result;
}

// ========================================================================
// Statistics
// ========================================================================

std::filesystem::path StorageEngine::statsPath(const std::string& dbname) const {
    return dbPath(dbname) / ".stats";
}

void StorageEngine::analyzeTable(const std::string& dbname,
                                  const std::string& tablename) {
    if (!tableExists(dbname, tablename)) return;
    TableSchema tbl = getTableSchema(dbname, tablename);

    TableStats stats;
    stats.rowCount = 0;
    std::map<std::string, std::set<std::string>> distinctVals;
    std::map<std::string, std::string> minVals, maxVals;

    forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
        stats.rowCount++;
        std::string row(data, len);
        for (size_t i = 0; i < tbl.len; ++i) {
            std::string val = extractColumnValue(row, tbl, i);
            distinctVals[tbl.cols[i].dataName].insert(val);
            if (minVals.find(tbl.cols[i].dataName) == minVals.end() || val < minVals[tbl.cols[i].dataName]) {
                minVals[tbl.cols[i].dataName] = val;
            }
            if (maxVals.find(tbl.cols[i].dataName) == maxVals.end() || val > maxVals[tbl.cols[i].dataName]) {
                maxVals[tbl.cols[i].dataName] = val;
            }
        }
    });

    for (size_t i = 0; i < tbl.len; ++i) {
        const std::string& cname = tbl.cols[i].dataName;
        ColumnStats cs;
        cs.cardinality = distinctVals[cname].size();
        cs.minVal = minVals[cname];
        cs.maxVal = maxVals[cname];
        stats.colStats[cname] = cs;
    }

    // Load existing stats, update this table's entry
    std::map<std::string, TableStats> allStats;
    auto spath = statsPath(dbname);
    if (std::filesystem::exists(spath)) {
        std::ifstream ifs(spath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string tname, cname;
            size_t card;
            std::string minv, maxv;
            ss >> tname >> cname >> card;
            std::getline(ss, minv, '|');
            std::getline(ss, maxv, '|');
            allStats[tname].colStats[cname] = {card, minv, maxv};
        }
    }
    allStats[tablename] = stats;

    // Write back: include rowCount as a special entry
    std::ofstream ofs(spath);
    for (const auto& kv : allStats) {
        // Write row count as first line for this table
        ofs << kv.first << " __rows__ " << kv.second.rowCount << "||\n";
        for (const auto& cv : kv.second.colStats) {
            ofs << kv.first << " " << cv.first << " " << cv.second.cardinality
               << "|" << cv.second.minVal << "|" << cv.second.maxVal << "\n";
        }
    }
}

size_t StorageEngine::getTableRowCount(const std::string& dbname,
                                        const std::string& tablename) const {
    auto spath = statsPath(dbname);
    if (!std::filesystem::exists(spath)) return 0;
    std::ifstream ifs(spath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string tname, cname;
        size_t val;
        ss >> tname >> cname >> val;
        if (tname == tablename && cname == "__rows__") {
            return val;
        }
    }
    return 0;
}

StorageEngine::ColumnStats StorageEngine::getColumnStats(
    const std::string& dbname, const std::string& tablename,
    const std::string& colname) const {
    auto spath = statsPath(dbname);
    if (!std::filesystem::exists(spath)) return {};
    std::ifstream ifs(spath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string tname, cname;
        size_t card;
        std::string minv, maxv;
        ss >> tname >> cname >> card;
        std::getline(ss, minv, '|');
        std::getline(ss, maxv, '|');
        if (tname == tablename && cname == colname) {
            return {card, minv, maxv};
        }
    }
    return {};
}

// ========================================================================
// WAL helpers
// ========================================================================

static void syncFile(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
}

static void walAppend(const std::filesystem::path& walFile, const std::string& line) {
    std::ofstream ofs(walFile, std::ios::out | std::ios::app);
    if (ofs) {
        ofs << line << '\n';
        ofs.flush();
        ofs.close();
        syncFile(walFile);
    }
}

static std::vector<std::string> walReadAll(const std::filesystem::path& walFile) {
    std::vector<std::string> lines;
    std::ifstream ifs(walFile);
    if (!ifs) return lines;
    std::string line;
    while (std::getline(ifs, line)) {
        if (!line.empty()) lines.push_back(line);
    }
    return lines;
}

static void walClear(const std::filesystem::path& walFile) {
    std::filesystem::remove(walFile);
}

std::filesystem::path StorageEngine::indexPath(const std::string& dbname,
                                                const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".idx");
}

// ========================================================================
// Page Allocator
// ========================================================================

PageAllocator* StorageEngine::getPageAllocator(const std::string& dbname,
                                                const std::string& tablename) const {
    std::string key = dbname + "/" + tablename;
    auto it = pageAllocators_.find(key);
    if (it != pageAllocators_.end()) return it->second.get();

    std::filesystem::path dt = dataPath(dbname, tablename);
    // Migrate legacy file if needed
    if (std::filesystem::exists(dt)) {
        std::ifstream check(dt, std::ios::binary);
        if (check) {
            uint32_t magic = 0;
            check.read(reinterpret_cast<char*>(&magic), 4);
            if (magic != Page::MAGIC) {
                check.close();
                migrateToPageStorage(dbname, tablename);
            }
        }
    }

    TableSchema tbl = getTableSchema(dbname, tablename);
    auto pa = std::make_unique<PageAllocator>(dt.string(), tbl.rowSize());
    pa->open();
    PageAllocator* ptr = pa.get();
    pageAllocators_[key] = std::move(pa);
    return ptr;
}

void StorageEngine::closeAllPageAllocators() {
    pageAllocators_.clear();
}

void StorageEngine::migrateToPageStorage(const std::string& dbname,
                                          const std::string& tablename) const {
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();
    std::filesystem::path oldPath = dataPath(dbname, tablename);
    std::string tmpPath = oldPath.string() + ".new";

    {
        PageAllocator pa(tmpPath, rowSize);
        pa.open();
        std::ifstream in(oldPath, std::ios::binary);
        if (in && rowSize > 0) {
            in.seekg(0, std::ios::end);
            auto fs = static_cast<size_t>(in.tellg());
            in.seekg(0, std::ios::beg);
            size_t rowCount = fs / rowSize;
            uint32_t currentPageId = 0;
            for (size_t i = 0; i < rowCount; ++i) {
                std::string row(rowSize, '\0');
                in.read(row.data(), static_cast<std::streamsize>(rowSize));
                if (currentPageId == 0) {
                    currentPageId = pa.allocPage();
                }
                char* buf = pa.fetchPage(currentPageId);
                Page page(buf);
                uint16_t slotId = 0;
                if (!page.insert(row.data(), rowSize, slotId)) {
                    pa.markDirty(currentPageId);
                    pa.unpinPage(currentPageId);
                    currentPageId = pa.allocPage();
                    buf = pa.fetchPage(currentPageId);
                    page = Page(buf);
                    page.insert(row.data(), rowSize, slotId);
                }
                pa.markDirty(currentPageId);
                pa.unpinPage(currentPageId);
            }
        }
        pa.close();
    }
    std::filesystem::remove(oldPath);
    std::filesystem::rename(tmpPath, oldPath);
}

void StorageEngine::migrateAllDataFiles() {
    for (const auto& entry : std::filesystem::directory_iterator(".", std::filesystem::directory_options::skip_permission_denied)) {
        try { if (!entry.is_directory()) continue; } catch (...) { continue; }
        std::string dbname;
        try { dbname = entry.path().filename().string(); } catch (...) { continue; }
        try { if (!std::filesystem::exists(tableListPath(dbname))) continue; } catch (...) { continue; }
        auto tables = getTableNames(dbname);
        for (const auto& tname : tables) {
            std::filesystem::path dt = dataPath(dbname, tname);
            if (!std::filesystem::exists(dt)) continue;
            std::ifstream check(dt, std::ios::binary);
            if (!check) continue;
            uint32_t magic = 0;
            check.read(reinterpret_cast<char*>(&magic), 4);
            if (magic != Page::MAGIC) {
                std::cerr << "[MIGRATION] Migrating " << dbname << "/" << tname
                          << " to page-based storage..." << std::endl;
                migrateToPageStorage(dbname, tname);
            }
        }
    }
}

int64_t StorageEngine::encodeRid(uint32_t pageId, uint16_t slotId) {
    return static_cast<int64_t>((static_cast<uint64_t>(pageId) << 32) | static_cast<uint64_t>(slotId));
}

void StorageEngine::decodeRid(int64_t rid, uint32_t& pageId, uint16_t& slotId) {
    uint64_t urid = static_cast<uint64_t>(rid);
    pageId = static_cast<uint32_t>(urid >> 32);
    slotId = static_cast<uint16_t>(urid & 0xFFFF);
}

void StorageEngine::forEachRow(const std::string& dbname, const std::string& tablename,
                                const std::function<void(uint32_t, uint16_t, const char*, size_t)>& callback,
                                const ReadView* readView) const {
    const ReadView* rv = readView;
    if (!rv && inTransaction_) rv = &readView_;

    PageAllocator* pa = getPageAllocator(dbname, tablename);
    if (!pa) return;
    uint32_t np = pa->numPages();
    for (uint32_t pid = 1; pid < np; ++pid) {
        char* buf = pa->fetchPage(pid);
        Page page(buf);
        page.forEachLive([&callback, pid, rv](uint16_t sid, const char* data, size_t len) {
            if (len <= MVCC_HEADER_SIZE) return;
            if (rv) {
                uint64_t rowTxnId = 0;
                std::memcpy(&rowTxnId, data, sizeof(uint64_t));
                if (!rv->isVisible(rowTxnId)) return;
            }
            callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
        });
        pa->unpinPage(pid);
    }
}

bool StorageEngine::readRowByRid(PageAllocator* pa, int64_t rid, std::string& rowBuffer,
                                  const TableSchema& tbl) const {
    if (!pa) return false;
    uint32_t pageId = 0;
    uint16_t slotId = 0;
    decodeRid(rid, pageId, slotId);
    char* buf = pa->fetchPage(pageId);
    Page page(buf);
    const char* data = nullptr;
    size_t len = 0;
    bool ok = page.get(slotId, data, len);
    pa->unpinPage(pageId);
    if (!ok) return false;
    if (len > MVCC_HEADER_SIZE) {
        rowBuffer.assign(data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
    } else {
        rowBuffer.clear();
    }
    return true;
}

std::string StorageEngine::extractPKValue(const std::string& rowBuffer, const TableSchema& tbl) {
    size_t pkIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
    }
    if (pkIdx >= tbl.len) return "";

    const Column& col = tbl.cols[pkIdx];
    if (col.isVariableLength) {
        return extractColumnValue(rowBuffer, tbl, pkIdx);
    }

    size_t offset = 0;
    for (size_t i = 0; i < pkIdx; ++i) {
        if (!tbl.cols[i].isVariableLength) offset += tbl.cols[i].dsize;
    }
    if (offset + col.dsize > rowBuffer.size()) return "";
    if (col.dataType == "char") {
        std::string val = rowBuffer.substr(offset, col.dsize);
        auto nul = val.find('\0');
        if (nul != std::string::npos) val.resize(nul);
        return val;
    } else if (col.dataType == "date") {
        Date d;
        std::memcpy(&d, rowBuffer.data() + offset, DATE_SIZE);
        return str(d);
    } else {
        int64_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, col.dsize);
        return transstr(val);
    }
}

BPTree* StorageEngine::getPKIndex(const std::string& dbname, const std::string& tablename) const {
    std::string key = dbname + "/" + tablename;
    auto it = pkIndexCache_.find(key);
    if (it != pkIndexCache_.end()) {
        return it->second.get();
    }

    auto tree = std::make_unique<BPTree>(indexPath(dbname, tablename));
    if (tree->open()) {
        BPTree* ptr = tree.get();
        pkIndexCache_[key] = std::move(tree);
        return ptr;
    }
    return nullptr;
}

void StorageEngine::closeAllIndexes() {
    pkIndexCache_.clear();
    secondaryIndexCache_.clear();
}

// ========================================================================
// Secondary Index
// ========================================================================
std::filesystem::path StorageEngine::secondaryIndexPath(const std::string& dbname,
                                                         const std::string& tablename,
                                                         const std::string& colname) const {
    return dbPath(dbname) / (tablename + "_" + colname + ".idx");
}

std::filesystem::path StorageEngine::secondaryIndexMetaPath(const std::string& dbname,
                                                             const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".secidx");
}

std::vector<std::string> StorageEngine::getIndexedColumns(const std::string& dbname,
                                                           const std::string& tablename) const {
    std::vector<std::string> cols;
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return cols;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) cols.push_back(line);
    }
    return cols;
}

BPTree* StorageEngine::getSecondaryIndex(const std::string& dbname,
                                          const std::string& tablename,
                                          const std::string& colname) const {
    std::string key = dbname + "/" + tablename + "/" + colname;
    auto it = secondaryIndexCache_.find(key);
    if (it != secondaryIndexCache_.end()) return it->second.get();

    auto tree = std::make_unique<BPTree>(secondaryIndexPath(dbname, tablename, colname));
    if (tree->open()) {
        BPTree* ptr = tree.get();
        secondaryIndexCache_[key] = std::move(tree);
        return ptr;
    }
    return nullptr;
}

std::string StorageEngine::extractColumnValue(const std::string& rowBuffer,
                                               const TableSchema& tbl, size_t colIdx) {
    if (colIdx >= tbl.len) return "";
    const Column& col = tbl.cols[colIdx];

    if (col.isVariableLength) {
        // Variable-length column: look up in var offset array
        size_t varIdx = tbl.getVarColIndex(colIdx);
        size_t fixedSize = tbl.fixedDataSize();
        size_t arrPos = fixedSize + varIdx * 4;
        if (arrPos + 4 > rowBuffer.size()) return "";
        uint16_t dataOffset = 0, dataLen = 0;
        std::memcpy(&dataOffset, rowBuffer.data() + arrPos, sizeof(uint16_t));
        std::memcpy(&dataLen, rowBuffer.data() + arrPos + 2, sizeof(uint16_t));
        if (dataOffset + dataLen > rowBuffer.size()) return "";
        return rowBuffer.substr(dataOffset, dataLen);
    }

    // Fixed-length column
    size_t offset = 0;
    for (size_t i = 0; i < colIdx; ++i) {
        if (!tbl.cols[i].isVariableLength) offset += tbl.cols[i].dsize;
    }
    if (offset + col.dsize > rowBuffer.size()) return "";
    if (col.dataType == "char") {
        std::string val(col.dsize, '\0');
        std::memcpy(val.data(), rowBuffer.data() + offset, col.dsize);
        auto nul = val.find('\0');
        if (nul != std::string::npos) val.resize(nul);
        return val;
    } else if (col.dataType == "date") {
        Date d;
        std::memcpy(&d, rowBuffer.data() + offset, DATE_SIZE);
        return (d.year == 0) ? "" : str(d);
    } else {
        int64_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, col.dsize);
        return (val == INF) ? "" : transstr(val);
    }
}

OpResult StorageEngine::createIndex(const std::string& dbname, const std::string& tablename,
                                     const std::string& colname) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) return OpResult::InvalidValue;

    // Already indexed?
    auto existing = getIndexedColumns(dbname, tablename);
    for (const auto& c : existing) {
        if (c == colname) return OpResult::Success;
    }

    // Build index from existing data using page-based iteration
    BPTree* idx = getSecondaryIndex(dbname, tablename, colname);
    if (!idx) return OpResult::InvalidValue;

    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        std::string val = extractColumnValue(row, tbl, colIdx);
        if (!val.empty()) {
            idx->insertMulti(val, encodeRid(pageId, slotId));
        }
    });

    // Record in metadata
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ofstream out(meta, std::ios::out | std::ios::app);
    out << colname << '\n';
    return OpResult::Success;
}

OpResult StorageEngine::dropIndex(const std::string& dbname, const std::string& tablename,
                                   const std::string& colname) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    std::filesystem::remove(secondaryIndexPath(dbname, tablename, colname));

    // Update metadata
    auto cols = getIndexedColumns(dbname, tablename);
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    {
        std::ofstream out(meta, std::ios::out);
        for (const auto& c : cols) {
            if (c != colname) out << c << '\n';
        }
    }
    // Remove from cache
    std::string key = dbname + "/" + tablename + "/" + colname;
    secondaryIndexCache_.erase(key);
    return OpResult::Success;
}

bool StorageEngine::databaseExists(const std::string& dbname) const {
    return std::filesystem::exists(dbPath(dbname));
}

bool StorageEngine::tableExists(const std::string& dbname,
                                 const std::string& tablename) const {
    return std::filesystem::exists(schemaPath(dbname, tablename));
}

OpResult StorageEngine::createDatabase(const std::string& dbname) {
    if (databaseExists(dbname)) return OpResult::TableAlreadyExist;
    std::filesystem::create_directory(dbPath(dbname));
    {
        std::ofstream f(tableListPath(dbname), std::ios::binary);
    }
    return OpResult::Success;
}

OpResult StorageEngine::dropDatabase(const std::string& dbname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    std::filesystem::remove_all(dbPath(dbname));
    return OpResult::Success;
}

void StorageEngine::writeSchema(std::ostream& out, const TableSchema& tbl) {
    int32_t len = static_cast<int32_t>(tbl.len);
    out.write(reinterpret_cast<const char*>(&len), 4);
    for (size_t i = 0; i < tbl.len; ++i) {
        uint8_t flags = (tbl.cols[i].isNull ? 1 : 0) | (tbl.cols[i].isPrimaryKey ? 2 : 0) | (tbl.cols[i].isVariableLength ? 4 : 0) | (tbl.cols[i].isUnique ? 8 : 0) | (!tbl.cols[i].defaultValue.empty() ? 16 : 0) | (tbl.cols[i].isAutoIncrement ? 32 : 0) | (!tbl.cols[i].checkExpr.empty() ? 64 : 0);
        out.write(reinterpret_cast<const char*>(&flags), 1);
        writeFixedString(out, tbl.cols[i].dataType, MAX_TYPE_NAME_LEN);
        writeFixedString(out, tbl.cols[i].dataName, MAX_COL_NAME_LEN);
        int32_t dsize = static_cast<int32_t>(tbl.cols[i].dsize);
        out.write(reinterpret_cast<const char*>(&dsize), 4);
        if (!tbl.cols[i].defaultValue.empty()) {
            writeFixedString(out, tbl.cols[i].defaultValue, MAX_COL_NAME_LEN);
        }
        if (!tbl.cols[i].checkExpr.empty()) {
            uint16_t checkLen = static_cast<uint16_t>(tbl.cols[i].checkExpr.size());
            out.write(reinterpret_cast<const char*>(&checkLen), 2);
            out.write(tbl.cols[i].checkExpr.data(), checkLen);
        }
    }
    // Write foreign keys
    int32_t fkLen = static_cast<int32_t>(tbl.fkLen);
    out.write(reinterpret_cast<const char*>(&fkLen), 4);
    for (size_t i = 0; i < tbl.fkLen; ++i) {
        writeFixedString(out, tbl.fks[i].colName, MAX_COL_NAME_LEN);
        writeFixedString(out, tbl.fks[i].refTable, MAX_TABLE_NAME_LEN);
        writeFixedString(out, tbl.fks[i].refCol, MAX_COL_NAME_LEN);
        writeFixedString(out, tbl.fks[i].onDelete, 10);
    }
}

TableSchema StorageEngine::readSchema(std::istream& in, const std::string& tablename) const {
    TableSchema tbl;
    tbl.tablename = tablename;
    int32_t len = 0;
    in.read(reinterpret_cast<char*>(&len), 4);
    if (!in) return tbl;
    tbl.len = static_cast<size_t>(len);
    for (size_t i = 0; i < tbl.len; ++i) {
        uint8_t flags = 0;
        in.read(reinterpret_cast<char*>(&flags), 1);
        tbl.cols[i].isNull = (flags & 1) != 0;
        tbl.cols[i].isPrimaryKey = (flags & 2) != 0;
        tbl.cols[i].isVariableLength = (flags & 4) != 0;
        tbl.cols[i].isUnique = (flags & 8) != 0;
        bool hasDefault = (flags & 16) != 0;
        tbl.cols[i].isAutoIncrement = (flags & 32) != 0;
        bool hasCheck = (flags & 64) != 0;
        tbl.cols[i].dataType = readFixedString(in, MAX_TYPE_NAME_LEN);
        tbl.cols[i].dataName = readFixedString(in, MAX_COL_NAME_LEN);
        int32_t dsize = 0;
        in.read(reinterpret_cast<char*>(&dsize), 4);
        tbl.cols[i].dsize = static_cast<size_t>(dsize);
        if (hasDefault) {
            tbl.cols[i].defaultValue = readFixedString(in, MAX_COL_NAME_LEN);
        }
        if (hasCheck) {
            uint16_t checkLen = 0;
            in.read(reinterpret_cast<char*>(&checkLen), 2);
            if (in && checkLen > 0) {
                std::string checkExpr(checkLen, '\0');
                in.read(checkExpr.data(), checkLen);
                tbl.cols[i].checkExpr = checkExpr;
            }
        }
    }
    // Read foreign keys (if present)
    int32_t fkLen = 0;
    in.read(reinterpret_cast<char*>(&fkLen), 4);
    if (in && fkLen > 0 && fkLen <= static_cast<int32_t>(MAX_COLUMNS)) {
        tbl.fkLen = static_cast<size_t>(fkLen);
        for (size_t i = 0; i < tbl.fkLen; ++i) {
            tbl.fks[i].colName = readFixedString(in, MAX_COL_NAME_LEN);
            tbl.fks[i].refTable = readFixedString(in, MAX_TABLE_NAME_LEN);
            tbl.fks[i].refCol = readFixedString(in, MAX_COL_NAME_LEN);
            tbl.fks[i].onDelete = readFixedString(in, 10);
        }
    }
    return tbl;
}

OpResult StorageEngine::createTable(const std::string& dbname, const TableSchema& tbl) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (tableExists(dbname, tbl.tablename)) return OpResult::TableAlreadyExist;

    {
        std::ofstream out(schemaPath(dbname, tbl.tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    // Initialize page-based data file via PageAllocator
    {
        auto pa = std::make_unique<PageAllocator>(dataPath(dbname, tbl.tablename).string(), tbl.rowSize());
        pa->open();
        pa->close();
    }
    {
        std::ofstream out(tableListPath(dbname), std::ios::binary | std::ios::app);
        writeFixedString(out, tbl.tablename, MAX_TABLE_NAME_LEN);
    }
    // Create B+ tree index if table has primary key
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) {
            BPTree idx(indexPath(dbname, tbl.tablename));
            idx.open();
            idx.close();
            break;
        }
    }
    // Initialize auto-increment sequences
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isAutoIncrement) {
            writeNextSeq(dbname, tbl.tablename, tbl.cols[i].dataName, 1);
        }
    }
    return OpResult::Success;
}

OpResult StorageEngine::createTable(const std::string& dbname,
                                     const std::string& tablename,
                                     const TableSchema& tbl) {
    TableSchema t = tbl;
    t.tablename = tablename;
    return createTable(dbname, t);
}

OpResult StorageEngine::dropTable(const std::string& dbname,
                                   const std::string& tablename) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;

    std::filesystem::remove(schemaPath(dbname, tablename));
    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::remove(indexPath(dbname, tablename));
    removeSeq(dbname, tablename);

    // Remove from cache
    std::string key = dbname + "/" + tablename;
    pkIndexCache_.erase(key);

    auto names = getTableNames(dbname);
    {
        std::ofstream out(tableListPath(dbname), std::ios::binary);
        for (const auto& name : names) {
            if (name != tablename) {
                writeFixedString(out, name, MAX_TABLE_NAME_LEN);
            }
        }
    }
    return OpResult::Success;
}

OpResult StorageEngine::alterTableAddColumn(const std::string& dbname,
                                             const std::string& tablename,
                                             const Column& col) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;

    TableSchema tbl = getTableSchema(dbname, tablename);
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == col.dataName) return OpResult::TableAlreadyExist;
    }
    if (tbl.len >= MAX_COLUMNS) return OpResult::InvalidValue;

    size_t oldRowSize = tbl.rowSize();
    tbl.append(col);

    // Rewrite schema
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }

    // Migrate data: append default value for new column
    std::string tempPath = dataPath(dbname, tablename).string() + ".tmp";
    {
        std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
        std::ofstream out(tempPath, std::ios::binary);
        if (in) {
            in.seekg(0, std::ios::end);
            auto fileSize = static_cast<size_t>(in.tellg());
            in.seekg(0, std::ios::beg);
            size_t rowCount = (oldRowSize == 0) ? 0 : fileSize / oldRowSize;
            std::string defaultVal(col.dsize, '\0');
            if (col.dataType != "char") {
                int64_t nullVal = INF;
                std::memcpy(defaultVal.data(), &nullVal, col.dsize);
            }
            for (size_t i = 0; i < rowCount; ++i) {
                std::string row(oldRowSize, '\0');
                in.read(row.data(), static_cast<std::streamsize>(oldRowSize));
                out.write(row.data(), static_cast<std::streamsize>(oldRowSize));
                out.write(defaultVal.data(), static_cast<std::streamsize>(col.dsize));
            }
        }
    }
    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::rename(tempPath, dataPath(dbname, tablename));
    return OpResult::Success;
}

OpResult StorageEngine::alterTableDropColumn(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::string& colName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t dropIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) { dropIdx = i; break; }
    }
    if (dropIdx >= tbl.len) return OpResult::InvalidValue;

    size_t oldRowSize = tbl.rowSize();
    size_t dropSize = tbl.cols[dropIdx].dsize;
    size_t prefixSize = 0;
    for (size_t i = 0; i < dropIdx; ++i) prefixSize += tbl.cols[i].dsize;
    size_t suffixSize = oldRowSize - prefixSize - dropSize;

    // Shift columns left
    for (size_t i = dropIdx; i + 1 < tbl.len; ++i) tbl.cols[i] = tbl.cols[i + 1];
    tbl.len--;

    // Rewrite schema
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }

    // Migrate data: skip dropped column's data
    std::string tempPath = dataPath(dbname, tablename).string() + ".tmp";
    {
        std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
        std::ofstream out(tempPath, std::ios::binary);
        if (in) {
            in.seekg(0, std::ios::end);
            auto fileSize = static_cast<size_t>(in.tellg());
            in.seekg(0, std::ios::beg);
            size_t rowCount = (oldRowSize == 0) ? 0 : fileSize / oldRowSize;
            for (size_t i = 0; i < rowCount; ++i) {
                std::string prefix(prefixSize, '\0');
                std::string suffix(suffixSize, '\0');
                in.read(prefix.data(), static_cast<std::streamsize>(prefixSize));
                in.seekg(static_cast<std::streamsize>(dropSize), std::ios::cur);
                in.read(suffix.data(), static_cast<std::streamsize>(suffixSize));
                out.write(prefix.data(), static_cast<std::streamsize>(prefixSize));
                out.write(suffix.data(), static_cast<std::streamsize>(suffixSize));
            }
        }
    }
    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::rename(tempPath, dataPath(dbname, tablename));
    return OpResult::Success;
}

std::vector<std::string> StorageEngine::getTableNames(const std::string& dbname) const {
    std::vector<std::string> names;
    std::ifstream in(tableListPath(dbname), std::ios::binary);
    if (!in) return names;
    while (in) {
        std::string name = readFixedString(in, MAX_TABLE_NAME_LEN);
        if (!name.empty()) names.push_back(name);
    }
    return names;
}

TableSchema StorageEngine::getTableSchema(const std::string& dbname,
                                            const std::string& tablename) const {
    std::ifstream in(schemaPath(dbname, tablename), std::ios::binary);
    return readSchema(in, tablename);
}

int64_t StorageEngine::parseInt(const std::string& s) {
    if (s.empty() || s.length() > 19) return INF;
    int64_t val = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return INF;
        val = val * 10 + (c - '0');
    }
    return val;
}

bool StorageEngine::stringToBuffer(const std::string& src, char* dst, size_t len) {
    if (src.length() > len) return false;
    std::memset(dst, 0, len);
    std::memcpy(dst, src.data(), src.length());
    return true;
}

// ========================================================================
// Helper: evaluate a single condition against a row buffer (page-based)
// ========================================================================
bool StorageEngine::evalConditionOnRow(const Condition& cond,
                                        const std::string& rowBuffer, const TableSchema& tbl) {
    if (cond.colName == "__true__") return true;
    if (cond.colName == "__false__") return false;
    size_t ci = 0;
    for (; ci < tbl.len && tbl.cols[ci].dataName != cond.colName; ++ci) {}
    if (ci >= tbl.len) return false;

    std::string val = extractColumnValue(rowBuffer, tbl, ci);
    const Column& col = tbl.cols[ci];
    if (cond.op == "isnull") return val.empty();
    if (cond.op == "isnotnull") return !val.empty();
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
    } else {
        int64_t num = val.empty() ? INF : parseInt(val);
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
// Parse CHECK expression into Condition list
// e.g., "score>=0andscore<=100" -> [{"score",">=","0"}, {"score","<=","100"}]
// ========================================================================
static std::vector<StorageEngine::Condition> parseCheckConditions(const std::string& expr) {
    std::vector<StorageEngine::Condition> result;
    size_t pos = 0;
    while (pos < expr.size()) {
        size_t andPos = expr.find("and", pos);
        std::string condStr = (andPos == std::string::npos) ? expr.substr(pos) : expr.substr(pos, andPos - pos);
        if (!condStr.empty()) {
            size_t opStart = 0;
            while (opStart < condStr.size() && !strchr("<>=!", condStr[opStart])) ++opStart;
            if (opStart > 0 && opStart < condStr.size()) {
                StorageEngine::Condition c;
                c.colName = condStr.substr(0, opStart);
                size_t opEnd = opStart;
                while (opEnd < condStr.size() && strchr("<>=!", condStr[opEnd])) ++opEnd;
                c.op = condStr.substr(opStart, opEnd - opStart);
                c.value = condStr.substr(opEnd);
                if (c.value.size() >= 2 && c.value.front() == '\'' && c.value.back() == '\'')
                    c.value = c.value.substr(1, c.value.size() - 2);
                result.push_back(c);
            }
        }
        if (andPos == std::string::npos) break;
        pos = andPos + 3;
    }
    return result;
}

// ========================================================================
// Helper: build full row buffer (with MVCC header) from column values
// ========================================================================
static std::string buildRowBuffer(const TableSchema& tbl,
                                   const std::map<std::string, std::string>& values,
                                   uint64_t creatorTxnId) {
    std::string rowBuffer;
    uint64_t rollbackPtr = 0;
    rowBuffer.append(reinterpret_cast<const char*>(&creatorTxnId), sizeof(uint64_t));
    rowBuffer.append(reinterpret_cast<const char*>(&rollbackPtr), sizeof(uint64_t));

    if (!tbl.hasVariableLength()) {
        size_t dataSize = tbl.rowSize() - MVCC_HEADER_SIZE;
        rowBuffer.resize(tbl.rowSize(), '\0');
        size_t offset = MVCC_HEADER_SIZE;
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            auto it = values.find(col.dataName);
            std::string val = (it != values.end()) ? it->second : "";
            if (col.dataType == "char") {
                std::memset(&rowBuffer[offset], 0, col.dsize);
                if (!val.empty()) {
                    size_t copyLen = std::min(val.size(), col.dsize);
                    std::memcpy(&rowBuffer[offset], val.data(), copyLen);
                }
            } else if (col.dataType == "date") {
                if (val.empty()) {
                    Date d{};
                    std::memcpy(&rowBuffer[offset], &d, DATE_SIZE);
                } else {
                    Date d(val.c_str());
                    std::memcpy(&rowBuffer[offset], &d, DATE_SIZE);
                }
            } else {
                int64_t num = val.empty() ? INF : StorageEngine::parseInt(val);
                std::memcpy(&rowBuffer[offset], &num, col.dsize);
            }
            offset += col.dsize;
        }
    } else {
        size_t fixedSize = tbl.fixedDataSize();
        size_t nVar = tbl.varColCount();
        size_t varArraySize = nVar * 4;

        std::string fixedData(fixedSize, '\0');
        std::vector<std::string> varDataList;
        size_t fixedOff = 0;
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            auto it = values.find(col.dataName);
            std::string val = (it != values.end()) ? it->second : "";
            if (col.isVariableLength) {
                if (val.size() > col.dsize) val.resize(col.dsize);
                varDataList.push_back(val);
            } else {
                if (col.dataType == "char") {
                    std::memset(&fixedData[fixedOff], 0, col.dsize);
                    if (!val.empty()) {
                        size_t copyLen = std::min(val.size(), col.dsize);
                        std::memcpy(&fixedData[fixedOff], val.data(), copyLen);
                    }
                } else if (col.dataType == "date") {
                    if (val.empty()) {
                        Date d{};
                        std::memcpy(&fixedData[fixedOff], &d, DATE_SIZE);
                    } else {
                        Date d(val.c_str());
                        std::memcpy(&fixedData[fixedOff], &d, DATE_SIZE);
                    }
                } else {
                    int64_t num = val.empty() ? INF : StorageEngine::parseInt(val);
                    std::memcpy(&fixedData[fixedOff], &num, col.dsize);
                }
                fixedOff += col.dsize;
            }
        }

        std::string varOffsetArray(varArraySize, '\0');
        std::string varDataSection;
        uint16_t varDataOffset = static_cast<uint16_t>(fixedSize + varArraySize);
        for (size_t vi = 0; vi < varDataList.size(); ++vi) {
            uint16_t vlen = static_cast<uint16_t>(varDataList[vi].size());
            size_t arrPos = vi * 4;
            std::memcpy(&varOffsetArray[arrPos], &varDataOffset, sizeof(uint16_t));
            std::memcpy(&varOffsetArray[arrPos + 2], &vlen, sizeof(uint16_t));
            varDataSection += varDataList[vi];
            varDataOffset += vlen;
        }

        rowBuffer += fixedData;
        rowBuffer += varOffsetArray;
        rowBuffer += varDataSection;
    }
    return rowBuffer;
}

OpResult StorageEngine::insert(const std::string& dbname,
                                const std::string& tablename,
                                const std::map<std::string, std::string>& values) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockExclusive(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    // Apply DEFAULT values
    std::map<std::string, std::string> actualValues = values;
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        auto it = actualValues.find(col.dataName);
        if ((it == actualValues.end() || it->second.empty()) && !col.defaultValue.empty()) {
            actualValues[col.dataName] = col.defaultValue;
        }
    }

    // Apply AUTO_INCREMENT values
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!col.isAutoIncrement) continue;
        auto it = actualValues.find(col.dataName);
        if (it != actualValues.end() && !it->second.empty()) continue;
        int64_t nextVal = readNextSeq(dbname, tablename, col.dataName);
        actualValues[col.dataName] = std::to_string(nextVal);
        writeNextSeq(dbname, tablename, col.dataName, nextVal + 1);
    }

    // Check primary key uniqueness using B+ tree index
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) {
            auto it = actualValues.find(tbl.cols[i].dataName);
            if (it != actualValues.end() && !it->second.empty()) {
                BPTree* idx = getPKIndex(dbname, tablename);
                if (idx) {
                    int64_t dummy;
                    if (idx->search(it->second, dummy)) {
                        lockManager_.unlock(tablename);
                        return OpResult::DuplicateKey;
                    }
                }
            }
            break;
        }
    }

    // Check UNIQUE constraints
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!col.isUnique) continue;
        auto it = actualValues.find(col.dataName);
        if (it == actualValues.end() || it->second.empty()) continue;
        // Check if value already exists
        bool duplicate = false;
        forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
            if (duplicate) return;
            std::string row(data, len);
            std::string existingVal = extractColumnValue(row, tbl, i);
            if (existingVal == it->second) duplicate = true;
        });
        if (duplicate) {
            lockManager_.unlock(tablename);
            return OpResult::DuplicateKey;
        }
    }

    // Validate all values before building row buffer
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        auto it = actualValues.find(col.dataName);
        std::string val = (it != actualValues.end()) ? it->second : "";
        if (!col.isNull && val.empty()) {
            lockManager_.unlock(tablename);
            return OpResult::NullNotAllowed;
        }
        if (!col.isVariableLength && col.dataType == "date" && !val.empty()) {
            Date d(val.c_str());
            if (d.year == 0) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && col.dataType != "char" && col.dataType != "date" && !val.empty()) {
            int64_t num = parseInt(val);
            if (num == INF) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
    }

    // Build row buffer
    uint64_t creatorTxnId = inTransaction_ ? currentTxnId_ : 0;
    std::string rowBuffer = buildRowBuffer(tbl, actualValues, creatorTxnId);

    // Check CHECK constraints before writing
    std::string strippedRow(rowBuffer.data() + MVCC_HEADER_SIZE,
                            rowBuffer.size() - MVCC_HEADER_SIZE);
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (col.checkExpr.empty()) continue;
        auto checkConds = parseCheckConditions(col.checkExpr);
        for (const auto& cond : checkConds) {
            if (!evalConditionOnRow(cond, strippedRow, tbl)) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
    }

    // Check foreign key references
    for (size_t fi = 0; fi < tbl.fkLen; ++fi) {
        const ForeignKey& fk = tbl.fks[fi];
        auto it = actualValues.find(fk.colName);
        if (it == actualValues.end() || it->second.empty()) continue;
        if (!tableExists(dbname, fk.refTable)) {
            lockManager_.unlock(tablename);
            return OpResult::TableNotExist;
        }
        TableSchema refTbl = getTableSchema(dbname, fk.refTable);
        BPTree* refIdx = getPKIndex(dbname, fk.refTable);
        if (refIdx) {
            int64_t dummy;
            if (!refIdx->search(it->second, dummy)) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;  // referenced key not found
            }
        }
    }

    // Check if row fits in a page (page capacity = PAGE_SIZE - header - slot)
    constexpr size_t MAX_ROW_SIZE = Page::PAGE_SIZE - sizeof(Page::Header) - sizeof(Page::Slot);
    if (rowBuffer.size() > MAX_ROW_SIZE) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    // Write row into page-based storage
    PageAllocator* pa = getPageAllocator(dbname, tablename);
    uint32_t pageId = 0;
    uint16_t slotId = 0;
    {
        uint32_t numPages = pa->numPages();
        bool inserted = false;
        size_t actualRowSize = rowBuffer.size();
        for (uint32_t pid = 1; pid < numPages && !inserted; ++pid) {
            char* buf = pa->fetchPage(pid);
            Page page(buf);
            if (page.canFit(actualRowSize)) {
                if (page.insert(rowBuffer.data(), actualRowSize, slotId)) {
                    pageId = pid;
                    inserted = true;
                }
            }
            if (inserted) pa->markDirty(pid);
            pa->unpinPage(pid);
        }
        if (!inserted) {
            pageId = pa->allocPage();
            char* buf = pa->fetchPage(pageId);
            Page page(buf);
            if (!page.insert(rowBuffer.data(), actualRowSize, slotId)) {
                pa->unpinPage(pageId);
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
            pa->markDirty(pageId);
            pa->unpinPage(pageId);
        }
    }

    int64_t rid = encodeRid(pageId, slotId);

    // Log for transaction rollback
    if (inTransaction_ && dbname == txnDB_) {
        logTxnInsert(tablename, rid);
    }

    // Update B+ tree PK index
    {
        BPTree* idx = getPKIndex(dbname, tablename);
        if (idx) {
            std::string pkVal = extractPKValue(strippedRow, tbl);
            if (!pkVal.empty()) idx->insert(pkVal, rid);
        }
    }
    // Update secondary indexes
    {
        auto indexedCols = getIndexedColumns(dbname, tablename);
        for (const auto& colname : indexedCols) {
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx >= tbl.len) continue;
            BPTree* secIdx = getSecondaryIndex(dbname, tablename, colname);
            if (secIdx) {
                std::string val = extractColumnValue(strippedRow, tbl, colIdx);
                if (!val.empty()) secIdx->insertMulti(val, rid);
            }
        }
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

std::vector<StorageEngine::Condition> StorageEngine::parseConditions(
    const std::vector<std::string>& cstr) {
    std::vector<Condition> conds;
    for (const auto& s : cstr) {
        if (s.empty()) continue;
        Condition c;
        // Handle LIKE operator
        if (s.size() >= 4 && s.substr(0, 4) == "like") {
            c.op = "like";
            size_t sp = s.find(' ', 4);
            if (sp == std::string::npos) continue;
            c.colName = s.substr(4, sp - 4);
            c.value = s.substr(sp + 1);
            if (c.value.size() >= 2 && c.value.front() == '\'' && c.value.back() == '\'')
                c.value = c.value.substr(1, c.value.size() - 2);
            conds.push_back(c);
            continue;
        }
        // Handle IS NOT NULL operator
        if (s.size() >= 9 && s.substr(0, 9) == "isnotnull") {
            c.op = "isnotnull";
            c.colName = trim(s.substr(9));
            conds.push_back(c);
            continue;
        }
        // Handle IS NULL operator
        if (s.size() >= 6 && s.substr(0, 6) == "isnull") {
            c.op = "isnull";
            c.colName = trim(s.substr(6));
            conds.push_back(c);
            continue;
        }
        size_t opEnd = 0;
        while (opEnd < s.size() && (s[opEnd] == '<' || s[opEnd] == '>' || s[opEnd] == '=' || s[opEnd] == '!')) ++opEnd;
        if (opEnd == 0) continue;
        c.op = s.substr(0, opEnd);
        size_t sp = s.find(' ', opEnd);
        if (sp == std::string::npos) continue;
        c.colName = s.substr(opEnd, sp - opEnd);
        c.value = s.substr(sp + 1);
        if (c.value.size() >= 2 && c.value.front() == '\'' && c.value.back() == '\'')
            c.value = c.value.substr(1, c.value.size() - 2);
        conds.push_back(c);
    }
    return conds;
}

std::set<int64_t> StorageEngine::filterRows(const std::string& dbname,
                                             const std::string& tablename,
                                             const std::vector<Condition>& conds) {
    std::set<int64_t> ids;
    TableSchema tbl = getTableSchema(dbname, tablename);

    // Try B+ tree PK index for = conditions
    for (const auto& c : conds) {
        if (c.op == "=") {
            bool hasPK = false;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].isPrimaryKey && tbl.cols[i].dataName == c.colName) {
                    hasPK = true; break;
                }
            }
            if (hasPK) {
                BPTree* idx = getPKIndex(dbname, tablename);
                if (idx) {
                    int64_t val = -1;
                    if (idx->search(c.value, val)) ids.insert(val);
                }
            } else {
                BPTree* secIdx = getSecondaryIndex(dbname, tablename, c.colName);
                if (secIdx) {
                    auto vals = secIdx->searchMulti(c.value);
                    for (int64_t v : vals) ids.insert(v);
                }
            }
            if (!ids.empty()) {
                if (conds.size() > 1) {
                    PageAllocator* pa = getPageAllocator(dbname, tablename);
                    std::set<int64_t> toRemove;
                    for (int64_t rid : ids) {
                        std::string row;
                        if (!readRowByRid(pa, rid, row, tbl)) { toRemove.insert(rid); continue; }
                        bool match = true;
                        for (const auto& cond : conds) {
                            if (cond.op == "=" && cond.colName == c.colName) continue;
                            if (!evalConditionOnRow(cond, row, tbl)) { match = false; break; }
                        }
                        if (!match) toRemove.insert(rid);
                    }
                    for (auto r : toRemove) ids.erase(r);
                }
                return ids;
            }
        }
    }

    // Full table scan via page iterator
    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId, const char* data, size_t len) {
        int64_t rid = encodeRid(pageId, slotId);
        bool match = true;
        std::string row(data, len);
        for (const auto& c : conds) {
            if (!evalConditionOnRow(c, row, tbl)) { match = false; break; }
        }
        if (match) ids.insert(rid);
    });
    return ids;
}

OpResult StorageEngine::remove(const std::string& dbname,
                                const std::string& tablename,
                                const std::vector<std::string>& conditions) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockExclusive(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    PageAllocator* pa = getPageAllocator(dbname, tablename);

    auto conds = parseConditions(conditions);
    std::set<int64_t> toDelete = filterRows(dbname, tablename, conds);

    if (toDelete.empty()) {
        lockManager_.unlock(tablename);
        return OpResult::Success;
    }

    // Check foreign key references and apply ON DELETE actions
    {
        size_t pkIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
        }
        if (pkIdx < tbl.len) {
            std::set<std::string> deletedPKs;
            for (int64_t rid : toDelete) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                std::string pkVal = extractPKValue(row, tbl);
                if (!pkVal.empty()) deletedPKs.insert(pkVal);
            }

            if (!deletedPKs.empty()) {
                // Scan all other tables for FK references and collect actions
                struct CascadeAction { std::string table; int64_t rid; };
                struct SetNullAction { std::string table; int64_t rid; size_t colIdx; };
                std::vector<CascadeAction> cascadeActions;
                std::vector<SetNullAction> setNullActions;
                std::set<std::string> restrictTables;

                auto allTables = getTableNames(dbname);
                for (const auto& otherTable : allTables) {
                    if (otherTable == tablename) continue;
                    TableSchema otherTbl = getTableSchema(dbname, otherTable);
                    for (size_t fi = 0; fi < otherTbl.fkLen; ++fi) {
                        const ForeignKey& fk = otherTbl.fks[fi];
                        if (fk.refTable != tablename) continue;
                        size_t fkColIdx = otherTbl.len;
                        for (size_t ci = 0; ci < otherTbl.len; ++ci) {
                            if (otherTbl.cols[ci].dataName == fk.colName) { fkColIdx = ci; break; }
                        }
                        if (fkColIdx >= otherTbl.len) continue;

                        forEachRow(dbname, otherTable, [&](uint32_t opid, uint16_t osid, const char* data, size_t len) {
                            std::string row(data, len);
                            std::string fval = extractColumnValue(row, otherTbl, fkColIdx);
                            if (deletedPKs.find(fval) != deletedPKs.end()) {
                                int64_t orid = encodeRid(opid, osid);
                                if (fk.onDelete == "cascade") {
                                    cascadeActions.push_back({otherTable, orid});
                                } else if (fk.onDelete == "setnull") {
                                    setNullActions.push_back({otherTable, orid, fkColIdx});
                                } else {
                                    restrictTables.insert(otherTable);
                                }
                            }
                        });
                    }
                }

                if (!restrictTables.empty()) {
                    lockManager_.unlock(tablename);
                    return OpResult::InvalidValue;
                }

                // Collect tables that need locks for cascade/setnull
                std::set<std::string> cascadeTables;
                for (const auto& ca : cascadeActions) cascadeTables.insert(ca.table);
                for (const auto& sa : setNullActions) cascadeTables.insert(sa.table);

                // Acquire locks on referenced tables in alphabetical order
                std::vector<std::string> sortedTables(cascadeTables.begin(), cascadeTables.end());
                std::sort(sortedTables.begin(), sortedTables.end());
                for (const auto& t : sortedTables) {
                    lockManager_.lockExclusive(t);
                }

                // Apply SET NULL: set FK column to NULL
                for (const auto& sa : setNullActions) {
                    TableSchema otbl = getTableSchema(dbname, sa.table);
                    PageAllocator* opa = getPageAllocator(dbname, sa.table);
                    std::string row;
                    if (!readRowByRid(opa, sa.rid, row, otbl)) continue;

                    // Save old values for index update
                    std::string oldPK = extractPKValue(row, otbl);
                    std::map<std::string, std::string> oldIdxVals;
                    auto indexedCols = getIndexedColumns(dbname, sa.table);
                    for (const auto& ic : indexedCols) {
                        size_t ici = otbl.len;
                        for (size_t i = 0; i < otbl.len; ++i) {
                            if (otbl.cols[i].dataName == ic) { ici = i; break; }
                        }
                        if (ici < otbl.len) oldIdxVals[ic] = extractColumnValue(row, otbl, ici);
                    }

                    // Set FK column to NULL: rebuild row buffer
                    {
                        std::map<std::string, std::string> rowValues;
                        for (size_t i = 0; i < otbl.len; ++i) {
                            rowValues[otbl.cols[i].dataName] = extractColumnValue(row, otbl, i);
                        }
                        rowValues[otbl.cols[sa.colIdx].dataName] = "";
                        std::string newRow = buildRowBuffer(otbl, rowValues, 0);
                        uint32_t pid; uint16_t sid;
                        decodeRid(sa.rid, pid, sid);
                        char* pbuf = opa->fetchPage(pid);
                        if (pbuf) {
                            Page page(pbuf);
                            page.update(sid, newRow.data(), newRow.size());
                            pa->markDirty(pid);
                            opa->unpinPage(pid);
                        }
                    }

                    // Update PK index if PK changed (it didn't)
                    // Update secondary indexes if indexed column was changed
                    for (const auto& kv : oldIdxVals) {
                        const std::string& ic = kv.first;
                        size_t ici = otbl.len;
                        for (size_t i = 0; i < otbl.len; ++i) {
                            if (otbl.cols[i].dataName == ic) { ici = i; break; }
                        }
                        if (ici >= otbl.len || ici != sa.colIdx) continue;
                        BPTree* sidx = getSecondaryIndex(dbname, sa.table, ic);
                        if (!sidx) continue;
                        std::string newVal = extractColumnValue(row, otbl, ici);
                        if (kv.second != newVal) {
                            if (!kv.second.empty()) sidx->remove(kv.second);
                            if (!newVal.empty()) sidx->insertMulti(newVal, sa.rid);
                        }
                    }
                }

                // Apply CASCADE: delete referencing rows
                for (const auto& ca : cascadeActions) {
                    TableSchema otbl = getTableSchema(dbname, ca.table);
                    PageAllocator* opa = getPageAllocator(dbname, ca.table);

                    // Log for transaction rollback
                    if (inTransaction_ && dbname == txnDB_) {
                        std::string row;
                        if (readRowByRid(opa, ca.rid, row, otbl)) {
                            logTxnDelete(ca.table, ca.rid, row);
                        }
                    }

                    // Delete via tombstone
                    uint32_t pid; uint16_t sid;
                    decodeRid(ca.rid, pid, sid);
                    char* pbuf = opa->fetchPage(pid);
                    if (pbuf) {
                        Page page(pbuf);
                        page.remove(sid);
                        pa->markDirty(pid);
                        opa->unpinPage(pid);
                    }

                    // Remove from PK index
                    BPTree* pidx = getPKIndex(dbname, ca.table);
                    if (pidx) {
                        std::string row;
                        if (readRowByRid(opa, ca.rid, row, otbl)) {
                            std::string pkVal = extractPKValue(row, otbl);
                            if (!pkVal.empty()) pidx->remove(pkVal);
                        }
                    }

                    // Remove from secondary indexes
                    auto indexedCols = getIndexedColumns(dbname, ca.table);
                    for (const auto& ic : indexedCols) {
                        size_t ici = otbl.len;
                        for (size_t i = 0; i < otbl.len; ++i) {
                            if (otbl.cols[i].dataName == ic) { ici = i; break; }
                        }
                        if (ici >= otbl.len) continue;
                        BPTree* sidx = getSecondaryIndex(dbname, ca.table, ic);
                        if (!sidx) continue;
                        std::string row;
                        if (!readRowByRid(opa, ca.rid, row, otbl)) continue;
                        std::string val = extractColumnValue(row, otbl, ici);
                        if (!val.empty()) sidx->remove(val);
                    }
                }

                // Release locks on referenced tables
                for (const auto& t : sortedTables) {
                    lockManager_.unlock(t);
                }
            }
        }
    }

    // Delete rows via PageAllocator tombstones + update indexes
    for (int64_t rid : toDelete) {
        // Log for transaction rollback (before deletion)
        if (inTransaction_ && dbname == txnDB_) {
            std::string row;
            if (readRowByRid(pa, rid, row, tbl)) {
                logTxnDelete(tablename, rid, row);
            }
        }

        uint32_t pageId; uint16_t slotId;
        decodeRid(rid, pageId, slotId);
        char* pageBuf = pa->fetchPage(pageId);
        if (pageBuf) {
            Page page(pageBuf);
            page.remove(slotId);
            pa->markDirty(pageId);
            pa->unpinPage(pageId);
        }
    }

    // Remove from PK index
    BPTree* pkIdx = getPKIndex(dbname, tablename);
    if (pkIdx) {
        for (int64_t rid : toDelete) {
            std::string row;
            if (!readRowByRid(pa, rid, row, tbl)) continue;
            std::string pkVal = extractPKValue(row, tbl);
            if (!pkVal.empty()) pkIdx->remove(pkVal);
        }
    }

    // Remove from secondary indexes
    {
        auto indexedCols = getIndexedColumns(dbname, tablename);
        for (const auto& colname : indexedCols) {
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx >= tbl.len) continue;
            BPTree* secIdx = getSecondaryIndex(dbname, tablename, colname);
            if (!secIdx) continue;
            for (int64_t rid : toDelete) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) secIdx->remove(val);
            }
        }
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::update(const std::string& dbname,
                                const std::string& tablename,
                                const std::map<std::string, std::string>& updates,
                                const std::vector<std::string>& conditions) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    // Validate columns and pre-check values
    std::map<size_t, std::string> colUpdates;  // column index -> new value
    for (const auto& kv : updates) {
        bool found = false;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == kv.first) {
                found = true;
                const Column& col = tbl.cols[i];
                if (!col.isNull && kv.second.empty()) {
                    return OpResult::NullNotAllowed;
                }
                if (col.dataType == "date") {
                    Date d(kv.second.c_str());
                    if (d.year == 0) return OpResult::InvalidValue;
                } else if (!col.isVariableLength && col.dataType != "char") {
                    if (!kv.second.empty() && parseInt(kv.second) == INF) {
                        return OpResult::InvalidValue;
                    }
                }
                colUpdates[i] = kv.second;
                break;
            }
        }
        if (!found) return OpResult::InvalidValue;
    }

    lockManager_.lockExclusive(tablename);

    PageAllocator* pa = getPageAllocator(dbname, tablename);

    auto conds = parseConditions(conditions);
    std::set<int64_t> matchIds = conds.empty()
        ? [&](){ std::set<int64_t> s; forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char*, size_t) { s.insert(encodeRid(pid, sid)); }); return s; }()
        : filterRows(dbname, tablename, conds);

    if (matchIds.empty()) {
        lockManager_.unlock(tablename);
        return OpResult::Success;
    }

    // Pre-fetch indexed column list
    auto indexedCols = getIndexedColumns(dbname, tablename);

    // For each matching row, read old data, update, write back, update indexes
    for (int64_t rid : matchIds) {
        std::string row;
        if (!readRowByRid(pa, rid, row, tbl)) continue;

        // Log for transaction rollback (before modification)
        if (inTransaction_ && dbname == txnDB_) {
            logTxnUpdate(tablename, rid, row);
        }

        // Save old PK and indexed column values before modification
        std::string oldPK = extractPKValue(row, tbl);
        std::map<std::string, std::string> oldIdxVals;
        for (const auto& colname : indexedCols) {
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx < tbl.len) {
                oldIdxVals[colname] = extractColumnValue(row, tbl, colIdx);
            }
        }

        // Rebuild row buffer with updates
        std::map<std::string, std::string> rowValues;
        for (size_t i = 0; i < tbl.len; ++i) {
            rowValues[tbl.cols[i].dataName] = extractColumnValue(row, tbl, i);
        }
        for (const auto& kv : colUpdates) {
            rowValues[tbl.cols[kv.first].dataName] = kv.second;
        }
        std::string newRow = buildRowBuffer(tbl, rowValues, 0);

        // Write back via PageAllocator
        uint32_t pageId; uint16_t slotId;
        decodeRid(rid, pageId, slotId);

        // Check CHECK constraints before writing
        std::string strippedNewRow(newRow.data() + MVCC_HEADER_SIZE,
                                   newRow.size() - MVCC_HEADER_SIZE);
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            if (col.checkExpr.empty()) continue;
            auto checkConds = parseCheckConditions(col.checkExpr);
            for (const auto& cond : checkConds) {
                if (!evalConditionOnRow(cond, strippedNewRow, tbl)) {
                    lockManager_.unlock(tablename);
                    return OpResult::InvalidValue;
                }
            }
        }

        char* pageBuf = pa->fetchPage(pageId);
        int64_t actualRid = rid;
        if (pageBuf) {
            Page page(pageBuf);
            uint16_t newSlotId = slotId;
            if (!page.update(slotId, newRow.data(), newRow.size(), &newSlotId)) {
                pa->unpinPage(pageId);
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
            pa->markDirty(pageId);
            pa->unpinPage(pageId);
            if (newSlotId != slotId) {
                actualRid = encodeRid(pageId, newSlotId);
            }
        }

        // Update PK index if PK was updated or RID changed
        BPTree* pkIdx = getPKIndex(dbname, tablename);
        if (pkIdx) {
            std::string newPK = extractPKValue(strippedNewRow, tbl);
            if (oldPK != newPK) {
                if (!oldPK.empty()) pkIdx->remove(oldPK);
                if (!newPK.empty()) pkIdx->insert(newPK, actualRid);
            } else if (actualRid != rid && !newPK.empty()) {
                // RID changed but PK same: re-insert with new RID
                pkIdx->remove(newPK);
                pkIdx->insert(newPK, actualRid);
            }
        }

        // Update secondary indexes if indexed columns were updated or RID changed
        for (const auto& kv : oldIdxVals) {
            const std::string& colname = kv.first;
            const std::string& oldVal = kv.second;
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx >= tbl.len) continue;
            BPTree* secIdx = getSecondaryIndex(dbname, tablename, colname);
            if (!secIdx) continue;
            std::string newVal = extractColumnValue(strippedNewRow, tbl, colIdx);
            bool colChanged = (colUpdates.find(colIdx) != colUpdates.end());
            if (colChanged && oldVal != newVal) {
                if (!oldVal.empty()) secIdx->remove(oldVal);
                if (!newVal.empty()) secIdx->insert(newVal, actualRid);
            } else if (actualRid != rid && !newVal.empty()) {
                // RID changed: update index entry with new RID
                secIdx->remove(newVal);
                secIdx->insertMulti(newVal, actualRid);
            }
        }
    }

    lockManager_.unlock(tablename);
    return OpResult::Success;
}

std::vector<std::string> StorageEngine::query(const std::string& dbname,
                                               const std::string& tablename,
                                               const std::vector<std::string>& conditions,
                                               const std::set<std::string>& selectCols,
                                               const std::string& orderByCol,
                                               bool orderByAsc) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;
    lockManager_.lockShared(tablename);

    // READ COMMITTED: refresh snapshot before each query
    if (inTransaction_ && txnIsolationLevel_ == IsolationLevel::ReadCommitted) {
        refreshReadView();
    }

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();
    PageAllocator* pa = getPageAllocator(dbname, tablename);

    auto conds = parseConditions(conditions);
    std::vector<std::pair<int64_t, std::string>> matchRows;
    if (conds.empty()) {
        forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char* data, size_t) {
            matchRows.emplace_back(encodeRid(pid, sid), std::string(data, rowSize));
        });
    } else {
        auto ids = filterRows(dbname, tablename, conds);
        for (int64_t rid : ids) {
            std::string row;
            if (readRowByRid(pa, rid, row, tbl)) {
                matchRows.emplace_back(rid, std::move(row));
            }
        }
    }

    // ORDER BY
    if (!orderByCol.empty()) {
        size_t sortIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == orderByCol) { sortIdx = i; break; }
        }
        if (sortIdx < tbl.len) {
            struct Item { int64_t rid; std::string s; int64_t n; Date d; };
            std::vector<Item> items;
            const Column& scol = tbl.cols[sortIdx];
            for (auto& mr : matchRows) {
                std::string val = extractColumnValue(mr.second, tbl, sortIdx);
                Item it{mr.first, "", 0, {}};
                if (scol.dataType == "char" || scol.isVariableLength) {
                    it.s = val;
                } else if (scol.dataType == "date") {
                    it.d = val.empty() ? Date{} : Date(val.c_str());
                } else {
                    it.n = val.empty() ? 0 : parseInt(val);
                }
                items.push_back(std::move(it));
            }
            std::sort(items.begin(), items.end(), [&](const Item& a, const Item& b) {
                if (scol.dataType == "char" || scol.isVariableLength) return orderByAsc ? (a.s < b.s) : (b.s < a.s);
                if (scol.dataType == "date") return orderByAsc ? (a.d < b.d) : (b.d < a.d);
                return orderByAsc ? (a.n < b.n) : (b.n < a.n);
            });
            std::vector<std::pair<int64_t, std::string>> sorted;
            for (const auto& it : items) {
                for (auto& mr : matchRows) {
                    if (mr.first == it.rid) { sorted.push_back(std::move(mr)); break; }
                }
            }
            matchRows = std::move(sorted);
        }
    }

    for (auto& mr : matchRows) {
        std::string rowStr;
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            if (!selectCols.empty() && selectCols.find(col.dataName) == selectCols.end())
                continue;
            std::string val = extractColumnValue(mr.second, tbl, i);
            if (val.empty() && !col.isNull) rowStr += "NULL ";
            else rowStr += val + ' ';
        }
        result.push_back(rowStr);
    }
    lockManager_.unlock(tablename);
    return result;
}

// ========================================================================
// Scalar function helper
// ========================================================================
static std::string applyScalarFunc(const StorageEngine::SelectExpr& expr,
                                    const std::string& rowBuffer,
                                    const TableSchema& tbl) {
    auto getVal = [&](const std::string& arg) -> std::string {
        if (arg.size() >= 2 && arg.front() == '\'' && arg.back() == '\'')
            return arg.substr(1, arg.size() - 2);
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == arg)
                return StorageEngine::extractColumnValue(rowBuffer, tbl, i);
        }
        return arg;
    };

    if (expr.funcName == "length" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        return std::to_string(val.size());
    }
    if (expr.funcName == "upper" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        for (char& c : val) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        return val;
    }
    if (expr.funcName == "lower" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        for (char& c : val) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return val;
    }
    if (expr.funcName == "trim" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        size_t a = 0, b = val.size();
        while (a < b && val[a] == ' ') ++a;
        while (b > a && val[b - 1] == ' ') --b;
        return val.substr(a, b - a);
    }
    if (expr.funcName == "substring" && expr.funcArgs.size() >= 3) {
        std::string val = getVal(expr.funcArgs[0]);
        int start = std::stoi(expr.funcArgs[1]) - 1; // 1-based to 0-based
        int len = std::stoi(expr.funcArgs[2]);
        if (start < 0) start = 0;
        if (start >= static_cast<int>(val.size())) return "";
        return val.substr(start, len);
    }
    if (expr.funcName == "concat") {
        std::string result;
        for (const auto& arg : expr.funcArgs) result += getVal(arg);
        return result;
    }
    return "";
}

std::vector<std::string> StorageEngine::queryExpr(const std::string& dbname,
                                                   const std::string& tablename,
                                                   const std::vector<std::string>& conditions,
                                                   const std::vector<SelectExpr>& exprs,
                                                   const std::string& orderByCol,
                                                   bool orderByAsc) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;
    lockManager_.lockShared(tablename);

    if (inTransaction_ && txnIsolationLevel_ == IsolationLevel::ReadCommitted) {
        refreshReadView();
    }

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();
    PageAllocator* pa = getPageAllocator(dbname, tablename);

    auto conds = parseConditions(conditions);
    std::vector<std::pair<int64_t, std::string>> matchRows;
    if (conds.empty()) {
        forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char* data, size_t) {
            matchRows.emplace_back(encodeRid(pid, sid), std::string(data, rowSize));
        });
    } else {
        auto ids = filterRows(dbname, tablename, conds);
        for (int64_t rid : ids) {
            std::string row;
            if (readRowByRid(pa, rid, row, tbl)) {
                matchRows.emplace_back(rid, std::move(row));
            }
        }
    }

    // ORDER BY (only supports plain columns for now)
    if (!orderByCol.empty()) {
        size_t sortIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == orderByCol) { sortIdx = i; break; }
        }
        if (sortIdx < tbl.len) {
            struct Item { int64_t rid; std::string s; int64_t n; Date d; };
            std::vector<Item> items;
            const Column& scol = tbl.cols[sortIdx];
            for (auto& mr : matchRows) {
                std::string val = extractColumnValue(mr.second, tbl, sortIdx);
                Item it{mr.first, "", 0, {}};
                if (scol.dataType == "char" || scol.isVariableLength) {
                    it.s = val;
                } else if (scol.dataType == "date") {
                    it.d = val.empty() ? Date{} : Date(val.c_str());
                } else {
                    it.n = val.empty() ? 0 : parseInt(val);
                }
                items.push_back(std::move(it));
            }
            std::sort(items.begin(), items.end(), [&](const Item& a, const Item& b) {
                if (scol.dataType == "char" || scol.isVariableLength) return orderByAsc ? (a.s < b.s) : (b.s < a.s);
                if (scol.dataType == "date") return orderByAsc ? (a.d < b.d) : (b.d < a.d);
                return orderByAsc ? (a.n < b.n) : (b.n < a.n);
            });
            std::vector<std::pair<int64_t, std::string>> sorted;
            for (const auto& it : items) {
                for (auto& mr : matchRows) {
                    if (mr.first == it.rid) { sorted.push_back(std::move(mr)); break; }
                }
            }
            matchRows = std::move(sorted);
        }
    }

    for (auto& mr : matchRows) {
        std::string rowStr;
        for (const auto& expr : exprs) {
            std::string val;
            if (expr.isScalar) {
                val = applyScalarFunc(expr, mr.second, tbl);
            } else {
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == expr.colName) {
                        val = extractColumnValue(mr.second, tbl, i);
                        break;
                    }
                }
            }
            rowStr += val + ' ';
        }
        result.push_back(rowStr);
    }
    lockManager_.unlock(tablename);
    return result;
}

std::vector<std::string> StorageEngine::aggregate(
    const std::string& dbname, const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::vector<std::pair<std::string, std::string>>& items) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;
    lockManager_.lockShared(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    PageAllocator* pa = getPageAllocator(dbname, tablename);
    auto conds = parseConditions(conditions);
    auto ids = filterRows(dbname, tablename, conds);
    std::vector<int64_t> matchIds(ids.begin(), ids.end());

    std::string rowResult;
    for (const auto& item : items) {
        const std::string& func = item.first;
        const std::string& colName = item.second;
        int64_t count = 0, sum = 0;
        bool hasMax = false, hasMin = false;
        std::string maxStr, minStr;
        int64_t maxInt = 0, minInt = 0;
        Date maxDate, minDate;
        bool isInt = false, isDate = false, isChar = false;
        size_t colIdx = tbl.len;

        if (func != "count" || colName != "*") {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colName) {
                    colIdx = i;
                    isInt = (!tbl.cols[i].isVariableLength && tbl.cols[i].dataType != "char" && tbl.cols[i].dataType != "date");
                    isDate = (tbl.cols[i].dataType == "date");
                    isChar = (tbl.cols[i].dataType == "char" || tbl.cols[i].isVariableLength);
                    break;
                }
            }
        }

        for (int64_t rid : matchIds) {
            std::string row;
            if (!readRowByRid(pa, rid, row, tbl)) continue;
            if (func == "count") {
                if (colName == "*") { count++; continue; }
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) count++;
            } else {
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (isInt) {
                    int64_t num = val.empty() ? INF : parseInt(val);
                    if (num == INF) continue;
                    if (func == "sum") sum += num;
                    if (func == "avg") { sum += num; count++; }
                    if (func == "max") {
                        if (!hasMax || num > maxInt) { maxInt = num; hasMax = true; }
                    }
                    if (func == "min") {
                        if (!hasMin || num < minInt) { minInt = num; hasMin = true; }
                    }
                } else if (isDate) {
                    Date d = val.empty() ? Date{} : Date(val.c_str());
                    if (d.year == 0) continue;
                    if (func == "max") {
                        if (!hasMax || d > maxDate) { maxDate = d; hasMax = true; }
                    }
                    if (func == "min") {
                        if (!hasMin || d < minDate) { minDate = d; hasMin = true; }
                    }
                } else {
                    if (val.empty()) continue;
                    if (func == "max") {
                        if (!hasMax || val > maxStr) { maxStr = val; hasMax = true; }
                    }
                    if (func == "min") {
                        if (!hasMin || val < minStr) { minStr = val; hasMin = true; }
                    }
                }
            }
        }

        if (func == "count") rowResult += transstr(count) + ' ';
        else if (func == "sum") rowResult += transstr(sum) + ' ';
        else if (func == "avg") rowResult += (count == 0 ? "0" : std::to_string(static_cast<double>(sum) / count)) + ' ';
        else if (func == "max") {
            if (!hasMax) rowResult += "NULL ";
            else if (isInt) rowResult += transstr(maxInt) + ' ';
            else if (isDate) rowResult += str(maxDate) + ' ';
            else rowResult += maxStr + ' ';
        }
        else if (func == "min") {
            if (!hasMin) rowResult += "NULL ";
            else if (isInt) rowResult += transstr(minInt) + ' ';
            else if (isDate) rowResult += str(minDate) + ' ';
            else rowResult += minStr + ' ';
        }
    }
    if (!rowResult.empty()) result.push_back(rowResult);
    lockManager_.unlock(tablename);
    return result;
}

// ========================================================================
// Group aggregate: GROUP BY with HAVING
// ========================================================================

std::vector<std::string> StorageEngine::groupAggregate(
    const std::string& dbname, const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::vector<std::pair<std::string, std::string>>& items,
    const std::string& groupByCol,
    const std::vector<std::string>& havingConds) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;
    lockManager_.lockShared(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    // Find group-by column index
    size_t groupIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == groupByCol) { groupIdx = i; break; }
    }
    if (groupIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return result;
    }

    PageAllocator* pa = getPageAllocator(dbname, tablename);
    auto conds = parseConditions(conditions);
    std::vector<int64_t> matchIds;
    if (conds.empty()) {
        forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId, const char* data, size_t len) {
            matchIds.push_back(encodeRid(pageId, slotId));
        });
    } else {
        auto ids = filterRows(dbname, tablename, conds);
        matchIds.assign(ids.begin(), ids.end());
    }

    // Read group key for each matching row
    auto readGroupKey = [&](int64_t rid) -> std::string {
        std::string row;
        if (!readRowByRid(pa, rid, row, tbl)) return "";
        return extractColumnValue(row, tbl, groupIdx);
    };

    // Group rows by group key
    std::map<std::string, std::vector<int64_t>> groups;
    for (int64_t rowIdx : matchIds) {
        groups[readGroupKey(rowIdx)].push_back(rowIdx);
    }

    // Helper: compute aggregate for a group
    auto computeAgg = [&](const std::vector<int64_t>& gids,
                           const std::string& func, const std::string& colName) -> std::string {
        size_t colIdx = tbl.len;
        bool isInt = false, isDate = false, isChar = false;
        if (func != "count" || colName != "*") {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colName) {
                    colIdx = i;
                    isInt = (!tbl.cols[i].isVariableLength && tbl.cols[i].dataType != "char" && tbl.cols[i].dataType != "date");
                    isDate = (tbl.cols[i].dataType == "date");
                    isChar = (tbl.cols[i].dataType == "char" || tbl.cols[i].isVariableLength);
                    break;
                }
            }
        }
        int64_t count = 0, sum = 0;
        bool hasMax = false, hasMin = false;
        std::string maxStr, minStr;
        int64_t maxInt = 0, minInt = 0;
        Date maxDate, minDate;

        for (int64_t rid : gids) {
            std::string row;
            if (!readRowByRid(pa, rid, row, tbl)) continue;

            if (func == "count") {
                if (colName == "*") { count++; continue; }
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) count++;
            } else {
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (isInt) {
                    int64_t num = val.empty() ? INF : parseInt(val);
                    if (num == INF) continue;
                    if (func == "sum") sum += num;
                    if (func == "avg") { sum += num; count++; }
                    if (func == "max") { if (!hasMax || num > maxInt) { maxInt = num; hasMax = true; } }
                    if (func == "min") { if (!hasMin || num < minInt) { minInt = num; hasMin = true; } }
                } else if (isDate) {
                    Date d = val.empty() ? Date{} : Date(val.c_str());
                    if (d.year == 0) continue;
                    if (func == "max") { if (!hasMax || d > maxDate) { maxDate = d; hasMax = true; } }
                    if (func == "min") { if (!hasMin || d < minDate) { minDate = d; hasMin = true; } }
                } else {
                    if (val.empty()) continue;
                    if (func == "max") { if (!hasMax || val > maxStr) { maxStr = val; hasMax = true; } }
                    if (func == "min") { if (!hasMin || val < minStr) { minStr = val; hasMin = true; } }
                }
            }
        }
        if (func == "count") return transstr(count);
        if (func == "sum") return transstr(sum);
        if (func == "avg") return (count == 0 ? "0" : std::to_string(static_cast<double>(sum) / count));
        if (func == "max") {
            if (!hasMax) return "NULL";
            if (isInt) return transstr(maxInt);
            if (isDate) return str(maxDate);
            return maxStr;
        }
        if (func == "min") {
            if (!hasMin) return "NULL";
            if (isInt) return transstr(minInt);
            if (isDate) return str(minDate);
            return minStr;
        }
        return "";
    };

    // Parse HAVING conditions: support "aggFunc(col) op value"
    struct HavingCond {
        std::string func, colName, op, value;
    };
    std::vector<HavingCond> havings;
    for (const auto& hc : havingConds) {
        if (hc.empty()) continue;
        // Format: "func(col) op value" or "op col value" (if already modified)
        std::string s = hc;
        // Try parse aggFunc(col) op value
        size_t lp = s.find('(');
        size_t rp = s.find(')');
        if (lp != std::string::npos && rp != std::string::npos && rp > lp + 1) {
            HavingCond h;
            h.func = s.substr(0, lp);
            h.colName = s.substr(lp + 1, rp - lp - 1);
            std::string rest = trim(s.substr(rp + 1));
            // Parse operator and value: may be "> 1" or ">1" (no space)
            size_t opEnd = 0;
            while (opEnd < rest.size() && (rest[opEnd] == '<' || rest[opEnd] == '>' || rest[opEnd] == '=' || rest[opEnd] == '!')) ++opEnd;
            if (opEnd > 0) {
                h.op = rest.substr(0, opEnd);
                h.value = trim(rest.substr(opEnd));
                havings.push_back(h);
            }
        }
    }

    // Evaluate HAVING condition for a group
    auto evalHaving = [&](const HavingCond& h, const std::vector<int64_t>& gids) -> bool {
        std::string aggVal = computeAgg(gids, h.func, h.colName);
        if (h.op == "=") return aggVal == h.value;
        if (h.op == "!=") return aggVal != h.value;
        // Numeric comparison
        double a = 0, v = 0;
        try { a = std::stod(aggVal); } catch (...) {}
        try { v = std::stod(h.value); } catch (...) {}
        if (h.op == ">") return a > v;
        if (h.op == "<") return a < v;
        if (h.op == ">=") return a >= v;
        if (h.op == "<=") return a <= v;
        return true;
    };

    // Build result rows
    for (const auto& kv : groups) {
        const std::string& gkey = kv.first;
        const auto& gids = kv.second;

        // Apply HAVING
        bool pass = true;
        for (const auto& h : havings) {
            if (!evalHaving(h, gids)) { pass = false; break; }
        }
        if (!pass) continue;

        std::string row = gkey + ' ';
        for (const auto& item : items) {
            row += computeAgg(gids, item.first, item.second) + ' ';
        }
        result.push_back(row);
    }
    lockManager_.unlock(tablename);
    return result;
}

// ========================================================================
// JOIN implementation
// ========================================================================

std::vector<std::string> StorageEngine::join(
    const std::string& dbname,
    const std::string& leftTable,
    const std::string& rightTable,
    const std::string& leftCol,
    const std::string& rightCol,
    const std::vector<std::string>& conditions,
    const std::set<std::string>& selectCols) {
    std::vector<std::string> result;
    if (!tableExists(dbname, leftTable) || !tableExists(dbname, rightTable)) return result;

    // Lock both tables in alphabetical order to avoid deadlock
    if (leftTable < rightTable) {
        lockManager_.lockShared(leftTable);
        lockManager_.lockShared(rightTable);
    } else {
        lockManager_.lockShared(rightTable);
        lockManager_.lockShared(leftTable);
    }

    TableSchema leftTbl = getTableSchema(dbname, leftTable);
    TableSchema rightTbl = getTableSchema(dbname, rightTable);
    size_t leftRowSize = leftTbl.rowSize();
    size_t rightRowSize = rightTbl.rowSize();

    // Read all rows from left table
    std::vector<std::string> leftRows;
    forEachRow(dbname, leftTable, [&leftRows](uint32_t, uint16_t, const char* data, size_t len) {
        leftRows.emplace_back(data, len);
    });

    // Read all rows from right table
    std::vector<std::string> rightRows;
    forEachRow(dbname, rightTable, [&rightRows](uint32_t, uint16_t, const char* data, size_t len) {
        rightRows.emplace_back(data, len);
    });

    // Build column map for condition evaluation: colName -> {isLeft, colIdx}
    struct ColInfo { bool isLeft; size_t colIdx; };
    std::map<std::string, ColInfo> colMap;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        colMap[leftTbl.cols[i].dataName] = {true, i};
        colMap[leftTable + "." + leftTbl.cols[i].dataName] = {true, i};
    }
    for (size_t i = 0; i < rightTbl.len; ++i) {
        std::string simple = rightTbl.cols[i].dataName;
        if (colMap.find(simple) == colMap.end()) {
            colMap[simple] = {false, i};
        }
        colMap[rightTable + "." + simple] = {false, i};
    }

    // Evaluate a condition on left/right row pair
    auto evalCond = [&](const Condition& c, const std::string& leftRow, const std::string& rightRow) -> bool {
        auto it = colMap.find(c.colName);
        if (it == colMap.end()) return false;
        const TableSchema& tbl = it->second.isLeft ? leftTbl : rightTbl;
        const std::string& row = it->second.isLeft ? leftRow : rightRow;
        std::string val = extractColumnValue(row, tbl, it->second.colIdx);
        const Column& col = tbl.cols[it->second.colIdx];
        if (col.dataType == "char" || col.isVariableLength) {
            if (c.op == "<"  && !(val <  c.value)) return false;
            if (c.op == ">"  && !(val >  c.value)) return false;
            if (c.op == "="  && val != c.value)    return false;
            if (c.op == "<=" && (val >  c.value))   return false;
            if (c.op == ">=" && (val <  c.value))   return false;
            if (c.op == "!=" && val == c.value)    return false;
        } else if (col.dataType == "date") {
            Date d = val.empty() ? Date{} : Date(val.c_str());
            Date v(c.value.c_str());
            if (c.op == "<"  && v.year && !(d < v))  return false;
            if (c.op == ">"  && v.year && !(d > v))  return false;
            if (c.op == "="  && v.year && d != v)    return false;
            if (c.op == "<=" && v.year && (d > v))   return false;
            if (c.op == ">=" && v.year && (d < v))   return false;
            if (c.op == "!=" && v.year && d == v)    return false;
        } else {
            int64_t num = val.empty() ? INF : parseInt(val);
            int64_t cmp = parseInt(c.value);
            if (c.op == "<"  && cmp != INF && !(num < cmp)) return false;
            if (c.op == ">"  && cmp != INF && !(num > cmp)) return false;
            if (c.op == "="  && cmp != INF && num != cmp)   return false;
            if (c.op == "<=" && cmp != INF && (num > cmp))  return false;
            if (c.op == ">=" && cmp != INF && (num < cmp))  return false;
            if (c.op == "!=" && cmp != INF && num == cmp)   return false;
        }
        return true;
    };

    auto conds = parseConditions(conditions);

    // Find left and right column indices for ON condition
    size_t leftColIdx = leftTbl.len;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol) { leftColIdx = i; break; }
    }
    size_t rightColIdx = rightTbl.len;
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol) { rightColIdx = i; break; }
    }

    for (const auto& lr : leftRows) {
        for (const auto& rr : rightRows) {
            // ON condition
            if (leftColIdx >= leftTbl.len || rightColIdx >= rightTbl.len) continue;
            std::string lv = extractColumnValue(lr, leftTbl, leftColIdx);
            std::string rv = extractColumnValue(rr, rightTbl, rightColIdx);
            if (lv != rv) continue;

            // WHERE conditions
            bool whereMatch = true;
            for (const auto& c : conds) {
                if (!evalCond(c, lr, rr)) { whereMatch = false; break; }
            }
            if (!whereMatch) continue;

            // Format output with SELECT columns
            std::string rowStr;
            for (size_t i = 0; i < leftTbl.len; ++i) {
                std::string fullName = leftTable + "." + leftTbl.cols[i].dataName;
                bool include = selectCols.empty();
                if (!include) {
                    if (selectCols.find(leftTbl.cols[i].dataName) != selectCols.end() ||
                        selectCols.find(fullName) != selectCols.end()) include = true;
                }
                if (!include) continue;
                std::string val = extractColumnValue(lr, leftTbl, i);
                if (val.empty() && !leftTbl.cols[i].isNull) rowStr += "NULL ";
                else rowStr += val + ' ';
            }
            for (size_t i = 0; i < rightTbl.len; ++i) {
                std::string fullName = rightTable + "." + rightTbl.cols[i].dataName;
                bool include = selectCols.empty();
                if (!include) {
                    if (selectCols.find(rightTbl.cols[i].dataName) != selectCols.end() ||
                        selectCols.find(fullName) != selectCols.end()) include = true;
                }
                if (!include) continue;
                std::string val = extractColumnValue(rr, rightTbl, i);
                if (val.empty() && !rightTbl.cols[i].isNull) rowStr += "NULL ";
                else rowStr += val + ' ';
            }
            if (!rowStr.empty()) result.push_back(rowStr);
        }
    }
    lockManager_.unlock(leftTable);
    lockManager_.unlock(rightTable);
    return result;
}

// ========================================================================
// LEFT JOIN: preserve all left rows, NULL for non-matching right
// ========================================================================

std::vector<std::string> StorageEngine::leftJoin(
    const std::string& dbname,
    const std::string& leftTable,
    const std::string& rightTable,
    const std::string& leftCol,
    const std::string& rightCol,
    const std::vector<std::string>& conditions,
    const std::set<std::string>& selectCols) {
    std::vector<std::string> result;
    if (!tableExists(dbname, leftTable) || !tableExists(dbname, rightTable)) return result;

    if (leftTable < rightTable) {
        lockManager_.lockShared(leftTable);
        lockManager_.lockShared(rightTable);
    } else {
        lockManager_.lockShared(rightTable);
        lockManager_.lockShared(leftTable);
    }

    TableSchema leftTbl = getTableSchema(dbname, leftTable);
    TableSchema rightTbl = getTableSchema(dbname, rightTable);
    size_t leftRowSize = leftTbl.rowSize();
    size_t rightRowSize = rightTbl.rowSize();

    std::vector<std::string> leftRows;
    forEachRow(dbname, leftTable, [&leftRows](uint32_t, uint16_t, const char* data, size_t len) {
        leftRows.emplace_back(data, len);
    });
    std::vector<std::string> rightRows;
    forEachRow(dbname, rightTable, [&rightRows](uint32_t, uint16_t, const char* data, size_t len) {
        rightRows.emplace_back(data, len);
    });

    struct ColInfo { bool isLeft; size_t colIdx; };
    std::map<std::string, ColInfo> colMap;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        colMap[leftTbl.cols[i].dataName] = {true, i};
        colMap[leftTable + "." + leftTbl.cols[i].dataName] = {true, i};
    }
    for (size_t i = 0; i < rightTbl.len; ++i) {
        std::string simple = rightTbl.cols[i].dataName;
        if (colMap.find(simple) == colMap.end()) colMap[simple] = {false, i};
        colMap[rightTable + "." + simple] = {false, i};
    }

    auto evalCond = [&](const Condition& c, const std::string& leftRow, const std::string& rightRow) -> bool {
        auto it = colMap.find(c.colName);
        if (it == colMap.end()) return false;
        const TableSchema& tbl = it->second.isLeft ? leftTbl : rightTbl;
        const std::string& row = it->second.isLeft ? leftRow : rightRow;
        std::string val = extractColumnValue(row, tbl, it->second.colIdx);
        const Column& col = tbl.cols[it->second.colIdx];
        if (col.dataType == "char" || col.isVariableLength) {
            if (c.op == "<"  && !(val <  c.value)) return false;
            if (c.op == ">"  && !(val >  c.value)) return false;
            if (c.op == "="  && val != c.value)    return false;
            if (c.op == "<=" && (val >  c.value))   return false;
            if (c.op == ">=" && (val <  c.value))   return false;
            if (c.op == "!=" && val == c.value)    return false;
        } else if (col.dataType == "date") {
            Date d = val.empty() ? Date{} : Date(val.c_str());
            Date v(c.value.c_str());
            if (c.op == "<"  && v.year && !(d < v))  return false;
            if (c.op == ">"  && v.year && !(d > v))  return false;
            if (c.op == "="  && v.year && d != v)    return false;
            if (c.op == "<=" && v.year && (d > v))   return false;
            if (c.op == ">=" && v.year && (d < v))   return false;
            if (c.op == "!=" && v.year && d == v)    return false;
        } else {
            int64_t num = val.empty() ? INF : parseInt(val);
            int64_t cmp = parseInt(c.value);
            if (c.op == "<"  && cmp != INF && !(num < cmp)) return false;
            if (c.op == ">"  && cmp != INF && !(num > cmp)) return false;
            if (c.op == "="  && cmp != INF && num != cmp)   return false;
            if (c.op == "<=" && cmp != INF && (num > cmp))  return false;
            if (c.op == ">=" && cmp != INF && (num < cmp))  return false;
            if (c.op == "!=" && cmp != INF && num == cmp)   return false;
        }
        return true;
    };

    auto conds = parseConditions(conditions);

    size_t leftColIdx = leftTbl.len;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol) { leftColIdx = i; break; }
    }
    size_t rightColIdx = rightTbl.len;
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol) { rightColIdx = i; break; }
    }

    auto formatRow = [&](const std::string& lr, const std::string& rr, bool rightNull) -> std::string {
        std::string rowStr;
        for (size_t i = 0; i < leftTbl.len; ++i) {
            std::string fullName = leftTable + "." + leftTbl.cols[i].dataName;
            bool include = selectCols.empty() || selectCols.find(leftTbl.cols[i].dataName) != selectCols.end() || selectCols.find(fullName) != selectCols.end();
            if (!include) continue;
            std::string val = extractColumnValue(lr, leftTbl, i);
            if (val.empty() && !leftTbl.cols[i].isNull) rowStr += "NULL ";
            else rowStr += val + ' ';
        }
        for (size_t i = 0; i < rightTbl.len; ++i) {
            std::string fullName = rightTable + "." + rightTbl.cols[i].dataName;
            bool include = selectCols.empty() || selectCols.find(rightTbl.cols[i].dataName) != selectCols.end() || selectCols.find(fullName) != selectCols.end();
            if (!include) continue;
            if (rightNull) { rowStr += "NULL "; continue; }
            std::string val = extractColumnValue(rr, rightTbl, i);
            if (val.empty() && !rightTbl.cols[i].isNull) rowStr += "NULL ";
            else rowStr += val + ' ';
        }
        return rowStr;
    };

    for (const auto& lr : leftRows) {
        bool hasMatch = false;
        for (const auto& rr : rightRows) {
            if (leftColIdx >= leftTbl.len || rightColIdx >= rightTbl.len) continue;
            std::string lv = extractColumnValue(lr, leftTbl, leftColIdx);
            std::string rv = extractColumnValue(rr, rightTbl, rightColIdx);
            if (lv != rv) continue;
            bool whereMatch = true;
            for (const auto& c : conds) {
                if (!evalCond(c, lr, rr)) { whereMatch = false; break; }
            }
            if (!whereMatch) continue;
            hasMatch = true;
            std::string rowStr = formatRow(lr, rr, false);
            if (!rowStr.empty()) result.push_back(rowStr);
        }
        if (!hasMatch) {
            std::string rowStr = formatRow(lr, "", true);
            if (!rowStr.empty()) result.push_back(rowStr);
        }
    }
    lockManager_.unlock(leftTable);
    lockManager_.unlock(rightTable);
    return result;
}

// ========================================================================
// RIGHT JOIN: preserve all right rows, NULL for non-matching left
// ========================================================================

std::vector<std::string> StorageEngine::rightJoin(
    const std::string& dbname,
    const std::string& leftTable,
    const std::string& rightTable,
    const std::string& leftCol,
    const std::string& rightCol,
    const std::vector<std::string>& conditions,
    const std::set<std::string>& selectCols) {
    std::vector<std::string> result;
    if (!tableExists(dbname, leftTable) || !tableExists(dbname, rightTable)) return result;

    if (leftTable < rightTable) {
        lockManager_.lockShared(leftTable);
        lockManager_.lockShared(rightTable);
    } else {
        lockManager_.lockShared(rightTable);
        lockManager_.lockShared(leftTable);
    }

    TableSchema leftTbl = getTableSchema(dbname, leftTable);
    TableSchema rightTbl = getTableSchema(dbname, rightTable);
    size_t leftRowSize = leftTbl.rowSize();
    size_t rightRowSize = rightTbl.rowSize();

    std::vector<std::string> leftRows;
    forEachRow(dbname, leftTable, [&leftRows](uint32_t, uint16_t, const char* data, size_t len) {
        leftRows.emplace_back(data, len);
    });
    std::vector<std::string> rightRows;
    forEachRow(dbname, rightTable, [&rightRows](uint32_t, uint16_t, const char* data, size_t len) {
        rightRows.emplace_back(data, len);
    });

    struct ColInfo { bool isLeft; size_t colIdx; };
    std::map<std::string, ColInfo> colMap;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        colMap[leftTbl.cols[i].dataName] = {true, i};
        colMap[leftTable + "." + leftTbl.cols[i].dataName] = {true, i};
    }
    for (size_t i = 0; i < rightTbl.len; ++i) {
        std::string simple = rightTbl.cols[i].dataName;
        if (colMap.find(simple) == colMap.end()) colMap[simple] = {false, i};
        colMap[rightTable + "." + simple] = {false, i};
    }

    auto evalCond = [&](const Condition& c, const std::string& leftRow, const std::string& rightRow) -> bool {
        auto it = colMap.find(c.colName);
        if (it == colMap.end()) return false;
        const TableSchema& tbl = it->second.isLeft ? leftTbl : rightTbl;
        const std::string& row = it->second.isLeft ? leftRow : rightRow;
        std::string val = extractColumnValue(row, tbl, it->second.colIdx);
        const Column& col = tbl.cols[it->second.colIdx];
        if (col.dataType == "char" || col.isVariableLength) {
            if (c.op == "<"  && !(val <  c.value)) return false;
            if (c.op == ">"  && !(val >  c.value)) return false;
            if (c.op == "="  && val != c.value)    return false;
            if (c.op == "<=" && (val >  c.value))   return false;
            if (c.op == ">=" && (val <  c.value))   return false;
            if (c.op == "!=" && val == c.value)    return false;
        } else if (col.dataType == "date") {
            Date d = val.empty() ? Date{} : Date(val.c_str());
            Date v(c.value.c_str());
            if (c.op == "<"  && v.year && !(d < v))  return false;
            if (c.op == ">"  && v.year && !(d > v))  return false;
            if (c.op == "="  && v.year && d != v)    return false;
            if (c.op == "<=" && v.year && (d > v))   return false;
            if (c.op == ">=" && v.year && (d < v))   return false;
            if (c.op == "!=" && v.year && d == v)    return false;
        } else {
            int64_t num = val.empty() ? INF : parseInt(val);
            int64_t cmp = parseInt(c.value);
            if (c.op == "<"  && cmp != INF && !(num < cmp)) return false;
            if (c.op == ">"  && cmp != INF && !(num > cmp)) return false;
            if (c.op == "="  && cmp != INF && num != cmp)   return false;
            if (c.op == "<=" && cmp != INF && (num > cmp))  return false;
            if (c.op == ">=" && cmp != INF && (num < cmp))  return false;
            if (c.op == "!=" && cmp != INF && num == cmp)   return false;
        }
        return true;
    };

    auto conds = parseConditions(conditions);

    size_t leftColIdx = leftTbl.len;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol) { leftColIdx = i; break; }
    }
    size_t rightColIdx = rightTbl.len;
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol) { rightColIdx = i; break; }
    }

    auto formatRow = [&](const std::string& lr, const std::string& rr, bool leftNull) -> std::string {
        std::string rowStr;
        for (size_t i = 0; i < leftTbl.len; ++i) {
            std::string fullName = leftTable + "." + leftTbl.cols[i].dataName;
            bool include = selectCols.empty() || selectCols.find(leftTbl.cols[i].dataName) != selectCols.end() || selectCols.find(fullName) != selectCols.end();
            if (!include) continue;
            if (leftNull) { rowStr += "NULL "; continue; }
            std::string val = extractColumnValue(lr, leftTbl, i);
            if (val.empty() && !leftTbl.cols[i].isNull) rowStr += "NULL ";
            else rowStr += val + ' ';
        }
        for (size_t i = 0; i < rightTbl.len; ++i) {
            std::string fullName = rightTable + "." + rightTbl.cols[i].dataName;
            bool include = selectCols.empty() || selectCols.find(rightTbl.cols[i].dataName) != selectCols.end() || selectCols.find(fullName) != selectCols.end();
            if (!include) continue;
            std::string val = extractColumnValue(rr, rightTbl, i);
            if (val.empty() && !rightTbl.cols[i].isNull) rowStr += "NULL ";
            else rowStr += val + ' ';
        }
        return rowStr;
    };

    for (const auto& rr : rightRows) {
        bool hasMatch = false;
        for (const auto& lr : leftRows) {
            if (leftColIdx >= leftTbl.len || rightColIdx >= rightTbl.len) continue;
            std::string lv = extractColumnValue(lr, leftTbl, leftColIdx);
            std::string rv = extractColumnValue(rr, rightTbl, rightColIdx);
            if (lv != rv) continue;
            bool whereMatch = true;
            for (const auto& c : conds) {
                if (!evalCond(c, lr, rr)) { whereMatch = false; break; }
            }
            if (!whereMatch) continue;
            hasMatch = true;
            std::string rowStr = formatRow(lr, rr, false);
            if (!rowStr.empty()) result.push_back(rowStr);
        }
        if (!hasMatch) {
            std::string rowStr = formatRow("", rr, true);
            if (!rowStr.empty()) result.push_back(rowStr);
        }
    }
    lockManager_.unlock(leftTable);
    lockManager_.unlock(rightTable);
    return result;
}

// ========================================================================
// WAL crash recovery
// ========================================================================

void StorageEngine::recoverAllDatabases() {
    if (!std::filesystem::exists(".") || !std::filesystem::is_directory(".")) return;
    for (const auto& entry : std::filesystem::directory_iterator(".", std::filesystem::directory_options::skip_permission_denied)) {
        try {
            if (!entry.is_directory()) continue;
        } catch (...) { continue; }
        std::string dbname;
        try { dbname = entry.path().filename().string(); } catch (...) { continue; }
        // Skip non-database directories (simple heuristic: must have tlist.lst)
        try { if (!std::filesystem::exists(tableListPath(dbname))) continue; } catch (...) { continue; }

        std::filesystem::path walFile = walPath(dbname);
        if (!std::filesystem::exists(walFile)) continue;

        auto lines = walReadAll(walFile);
        if (lines.empty()) {
            walClear(walFile);
            continue;
        }

        bool hasCommit = false;
        bool hasRollback = false;
        for (const auto& l : lines) {
            if (l == "COMMIT") hasCommit = true;
            if (l == "ROLLBACK") hasRollback = true;
        }

        std::filesystem::path backup = dbPath(dbname);
        backup += ".txn_backup";

        if (hasCommit) {
            // Transaction was committed: WAL is just cleanup
            if (std::filesystem::exists(backup)) {
                std::filesystem::remove_all(backup);
            }
            walClear(walFile);
        } else if (hasRollback) {
            // Transaction was rolled back: cleanup
            if (std::filesystem::exists(backup)) {
                std::filesystem::remove_all(backup);
            }
            walClear(walFile);
        } else {
            // Incomplete transaction: restore from backup
            std::cerr << "[WAL RECOVERY] Incomplete transaction in " << dbname
                      << ". Restoring from backup..." << std::endl;
            if (std::filesystem::exists(backup)) {
                std::filesystem::path db = dbPath(dbname);
                std::filesystem::remove_all(db);
                std::filesystem::rename(backup, db);
            }
            walClear(walFile);
            // Clear stale index cache entries for this db
            pkIndexCache_.clear();
        }
    }
}

// ========================================================================
// ReadView refresh (for READ COMMITTED)
// ========================================================================
void StorageEngine::refreshReadView() {
    if (!inTransaction_ || txnIsolationLevel_ != IsolationLevel::ReadCommitted) return;
    std::lock_guard<std::mutex> lock(globalTxnMutex_);
    readView_.creatorTxnId = currentTxnId_;
    readView_.upLimitId = activeTransactions_.empty() ? currentTxnId_ : *activeTransactions_.begin();
    readView_.lowLimitId = TxnIdGenerator::instance().maxCommittedTxId() + 1;
    readView_.activeTxnIds = activeTransactions_;
    readView_.activeTxnIds.erase(currentTxnId_);
}

// ========================================================================
// Checkpoint: flush all dirty pages and truncate WAL
// ========================================================================
void StorageEngine::checkpoint(const std::string& dbname) {
    if (!databaseExists(dbname)) return;

    // Flush all page allocators for this database
    auto tables = getTableNames(dbname);
    for (const auto& tname : tables) {
        PageAllocator* pa = getPageAllocator(dbname, tname);
        if (pa) pa->flush();
    }

    // Write checkpoint record
    auto cpPath = checkpointPath(dbname);
    {
        std::ofstream cp(cpPath, std::ios::binary);
        uint64_t timestamp = static_cast<uint64_t>(std::time(nullptr));
        uint64_t maxTxId = TxnIdGenerator::instance().maxCommittedTxId();
        cp.write(reinterpret_cast<const char*>(&timestamp), sizeof(uint64_t));
        cp.write(reinterpret_cast<const char*>(&maxTxId), sizeof(uint64_t));
        cp.close();
    }
    syncFile(cpPath);

    // Truncate WAL after checkpoint
    walClear(walPath(dbname));
}

// ========================================================================
// VACUUM: reclaim space from deleted rows
// ========================================================================
size_t StorageEngine::vacuum(const std::string& dbname,
                             const std::string& tablename) {
    if (!databaseExists(dbname) || !tableExists(dbname, tablename)) return 0;
    lockManager_.lockExclusive(tablename);

    PageAllocator* pa = getPageAllocator(dbname, tablename);
    if (!pa) { lockManager_.unlock(tablename); return 0; }

    uint32_t np = pa->numPages();
    size_t freedPages = 0;

    for (uint32_t pid = 1; pid < np; ++pid) {
        char* buf = pa->fetchPage(pid);
        if (!buf) continue;
        Page page(buf);

        // Skip pages that are already empty or fully live
        uint16_t beforeLive = page.liveCount();
        uint16_t beforeSlots = page.slotCount();
        if (beforeSlots == 0 || beforeLive == beforeSlots) {
            pa->unpinPage(pid);
            continue;
        }

        // Compact: move live records together, keep slotIds stable
        page.compact();
        pa->markDirty(pid);

        // If page is now empty, return it to the free list
        if (page.liveCount() == 0) {
            pa->unpinPage(pid);
            pa->freePage(pid);
            freedPages++;
        } else {
            pa->unpinPage(pid);
        }
    }

    lockManager_.unlock(tablename);
    return freedPages;
}

// ========================================================================
// Transaction logging helpers
// ========================================================================

void StorageEngine::logTxnInsert(const std::string& tableName, int64_t rowIdx) {
    txnLog_.push_back({TxnLogEntry::Op::Insert, tableName, rowIdx, ""});
}

void StorageEngine::logTxnUpdate(const std::string& tableName, int64_t rowIdx,
                                  const std::string& oldRowData) {
    txnLog_.push_back({TxnLogEntry::Op::Update, tableName, rowIdx, oldRowData});
}

void StorageEngine::logTxnDelete(const std::string& tableName, int64_t rowIdx,
                                  const std::string& oldRowData) {
    txnLog_.push_back({TxnLogEntry::Op::Delete, tableName, rowIdx, oldRowData});
}

// ========================================================================
// Transaction support (Undo Log based rollback, no full-db snapshot)
// ========================================================================

OpResult StorageEngine::beginTransaction(const std::string& dbname) {
    if (inTransaction_) return OpResult::Success;  // already in txn
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;

    // Keep a backup for crash recovery (recoverAllDatabases)
    std::filesystem::path backup = dbPath(dbname);
    backup += ".txn_backup";
    if (std::filesystem::exists(backup)) {
        std::filesystem::remove_all(backup);
    }
    std::filesystem::create_directories(backup);
    for (const auto& entry : std::filesystem::directory_iterator(dbPath(dbname))) {
        std::filesystem::path dest = backup / entry.path().filename();
        if (entry.is_directory()) {
            std::filesystem::copy(entry.path(), dest, std::filesystem::copy_options::recursive);
        } else {
            std::filesystem::copy(entry.path(), dest);
        }
    }

    // Assign transaction ID and create ReadView (if needed)
    currentTxnId_ = TxnIdGenerator::instance().nextTxId();
    {
        std::lock_guard<std::mutex> lock(globalTxnMutex_);
        activeTransactions_.insert(currentTxnId_);
    }
    if (txnIsolationLevel_ != IsolationLevel::ReadUncommitted) {
        std::lock_guard<std::mutex> lock(globalTxnMutex_);
        readView_.creatorTxnId = currentTxnId_;
        readView_.upLimitId = activeTransactions_.empty() ? currentTxnId_ : *activeTransactions_.begin();
        readView_.lowLimitId = TxnIdGenerator::instance().maxCommittedTxId() + 1;
        readView_.activeTxnIds = activeTransactions_;
        readView_.activeTxnIds.erase(currentTxnId_);
    }

    txnLog_.clear();
    inTransaction_ = true;
    txnDB_ = dbname;
    // Write WAL BEGIN marker
    walClear(walPath(dbname));
    walAppend(walPath(dbname), "BEGIN " + std::to_string(currentTxnId_));
    return OpResult::Success;
}

OpResult StorageEngine::commitTransaction() {
    if (!inTransaction_) return OpResult::Success;
    // Update max committed txId
    TxnIdGenerator::instance().notifyCommit(currentTxnId_);
    // Remove from active set
    {
        std::lock_guard<std::mutex> lock(globalTxnMutex_);
        activeTransactions_.erase(currentTxnId_);
    }
    // Write WAL COMMIT marker
    walAppend(walPath(txnDB_), "COMMIT " + std::to_string(currentTxnId_));
    txnLog_.clear();
    walClear(walPath(txnDB_));
    lockManager_.unlockAll();
    currentTxnId_ = 0;
    inTransaction_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

OpResult StorageEngine::rollbackTransaction() {
    if (!inTransaction_) return OpResult::Success;
    // Remove from active set (aborted, not committed)
    {
        std::lock_guard<std::mutex> lock(globalTxnMutex_);
        activeTransactions_.erase(currentTxnId_);
    }
    walAppend(walPath(txnDB_), "ROLLBACK " + std::to_string(currentTxnId_));

    // Replay txnLog in reverse order to undo changes
    for (auto it = txnLog_.rbegin(); it != txnLog_.rend(); ++it) {
        PageAllocator* pa = getPageAllocator(txnDB_, it->tableName);
        TableSchema tbl = getTableSchema(txnDB_, it->tableName);

        if (it->op == TxnLogEntry::Op::Insert) {
            // Undo INSERT: remove the row
            uint32_t pageId; uint16_t slotId;
            decodeRid(it->rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                Page page(pageBuf);
                page.remove(slotId);
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            // Remove from PK index
            BPTree* pkIdx = getPKIndex(txnDB_, it->tableName);
            if (pkIdx) {
                std::string row;
                if (readRowByRid(pa, it->rowIdx, row, tbl)) {
                    std::string pkVal = extractPKValue(row, tbl);
                    if (!pkVal.empty()) pkIdx->remove(pkVal);
                }
            }
            // Remove from secondary indexes
            auto indexedCols = getIndexedColumns(txnDB_, it->tableName);
            for (const auto& colname : indexedCols) {
                size_t colIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
                }
                if (colIdx >= tbl.len) continue;
                BPTree* secIdx = getSecondaryIndex(txnDB_, it->tableName, colname);
                if (!secIdx) continue;
                std::string row;
                if (readRowByRid(pa, it->rowIdx, row, tbl)) {
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    if (!val.empty()) secIdx->remove(val);
                }
            }
        } else if (it->op == TxnLogEntry::Op::Update) {
            // Undo UPDATE: restore old row data
            uint32_t pageId; uint16_t slotId;
            decodeRid(it->rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                Page page(pageBuf);
                page.restore(slotId);  // clear tombstone if present
                page.update(slotId, it->rowData.data(), it->rowData.size());
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            // Rebuild indexes: oldPK -> newPK change needs index fix
            // Since we restored old data, we need to ensure PK index points to correct rid
            BPTree* pkIdx = getPKIndex(txnDB_, it->tableName);
            if (pkIdx) {
                std::string pkVal = extractPKValue(it->rowData, tbl);
                if (!pkVal.empty()) {
                    pkIdx->remove(pkVal);
                    pkIdx->insert(pkVal, it->rowIdx);
                }
            }
            auto indexedCols = getIndexedColumns(txnDB_, it->tableName);
            for (const auto& colname : indexedCols) {
                size_t colIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
                }
                if (colIdx >= tbl.len) continue;
                BPTree* secIdx = getSecondaryIndex(txnDB_, it->tableName, colname);
                if (!secIdx) continue;
                std::string val = extractColumnValue(it->rowData, tbl, colIdx);
                if (!val.empty()) {
                    secIdx->remove(val);
                    secIdx->insertMulti(val, it->rowIdx);
                }
            }
        } else if (it->op == TxnLogEntry::Op::Delete) {
            // Undo DELETE: restore the row by clearing tombstone
            uint32_t pageId; uint16_t slotId;
            decodeRid(it->rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                Page page(pageBuf);
                page.restore(slotId);
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            // Re-add to indexes
            BPTree* pkIdx = getPKIndex(txnDB_, it->tableName);
            if (pkIdx) {
                std::string pkVal = extractPKValue(it->rowData, tbl);
                if (!pkVal.empty()) pkIdx->insert(pkVal, it->rowIdx);
            }
            auto indexedCols = getIndexedColumns(txnDB_, it->tableName);
            for (const auto& colname : indexedCols) {
                size_t colIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
                }
                if (colIdx >= tbl.len) continue;
                BPTree* secIdx = getSecondaryIndex(txnDB_, it->tableName, colname);
                if (!secIdx) continue;
                std::string val = extractColumnValue(it->rowData, tbl, colIdx);
                if (!val.empty()) secIdx->insertMulti(val, it->rowIdx);
            }
        }
    }

    txnLog_.clear();
    walClear(walPath(txnDB_));
    lockManager_.unlockAll();
    currentTxnId_ = 0;
    inTransaction_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

// ========================================================================
// Table-level permissions
// ========================================================================

std::filesystem::path StorageEngine::permPath(const std::string& dbname) const {
    return dbPath(dbname) / ".permissions";
}

std::filesystem::path StorageEngine::seqPath(const std::string& dbname, const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".seq");
}

int64_t StorageEngine::readNextSeq(const std::string& dbname, const std::string& tablename, const std::string& colname) {
    auto path = seqPath(dbname, tablename);
    int64_t val = 1;
    if (std::filesystem::exists(path)) {
        std::ifstream ifs(path);
        if (ifs) {
            std::string line;
            while (std::getline(ifs, line)) {
                size_t sp = line.find(' ');
                if (sp == std::string::npos) continue;
                if (line.substr(0, sp) == colname) {
                    try {
                        val = std::stoll(line.substr(sp + 1));
                    } catch (...) { val = 1; }
                    break;
                }
            }
        }
    }
    return val;
}

void StorageEngine::writeNextSeq(const std::string& dbname, const std::string& tablename, const std::string& colname, int64_t val) {
    auto path = seqPath(dbname, tablename);
    // Read existing entries
    std::map<std::string, int64_t> seqs;
    if (std::filesystem::exists(path)) {
        std::ifstream ifs(path);
        if (ifs) {
            std::string line;
            while (std::getline(ifs, line)) {
                size_t sp = line.find(' ');
                if (sp == std::string::npos) continue;
                try {
                    seqs[line.substr(0, sp)] = std::stoll(line.substr(sp + 1));
                } catch (...) {}
            }
        }
    }
    seqs[colname] = val;
    std::ofstream ofs(path);
    for (const auto& p : seqs) {
        ofs << p.first << ' ' << p.second << '\n';
    }
}

void StorageEngine::removeSeq(const std::string& dbname, const std::string& tablename) {
    auto path = seqPath(dbname, tablename);
    if (std::filesystem::exists(path)) {
        std::filesystem::remove(path);
    }
}

static std::string privToStr(StorageEngine::TablePrivilege p) {
    switch (p) {
        case StorageEngine::TablePrivilege::Select: return "select";
        case StorageEngine::TablePrivilege::Insert: return "insert";
        case StorageEngine::TablePrivilege::Update: return "update";
        case StorageEngine::TablePrivilege::Delete: return "delete";
        case StorageEngine::TablePrivilege::All: return "all";
    }
    return "";
}

void StorageEngine::grant(const std::string& dbname, const std::string& tablename,
                          const std::string& username, TablePrivilege priv) {
    auto ppath = permPath(dbname);
    // Read existing permissions
    std::map<std::string, std::set<std::string>> perms; // user+table -> privileges
    if (std::filesystem::exists(ppath)) {
        std::ifstream ifs(ppath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string u, t, p;
            ss >> u >> t >> p;
            perms[u + "|" + t].insert(p);
        }
    }
    std::string key = username + "|" + tablename;
    perms[key].insert(privToStr(priv));
    // Write back
    std::ofstream ofs(ppath);
    for (const auto& kv : perms) {
        size_t pipe = kv.first.find('|');
        std::string u = kv.first.substr(0, pipe);
        std::string t = kv.first.substr(pipe + 1);
        for (const auto& p : kv.second) {
            ofs << u << " " << t << " " << p << "\n";
        }
    }
}

void StorageEngine::revoke(const std::string& dbname, const std::string& tablename,
                           const std::string& username, TablePrivilege priv) {
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return;
    std::map<std::string, std::set<std::string>> perms;
    {
        std::ifstream ifs(ppath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string u, t, p;
            ss >> u >> t >> p;
            perms[u + "|" + t].insert(p);
        }
    }
    std::string key = username + "|" + tablename;
    auto it = perms.find(key);
    if (it != perms.end()) {
        it->second.erase(privToStr(priv));
        if (it->second.empty()) perms.erase(it);
    }
    std::ofstream ofs(ppath);
    for (const auto& kv : perms) {
        size_t pipe = kv.first.find('|');
        std::string u = kv.first.substr(0, pipe);
        std::string t = kv.first.substr(pipe + 1);
        for (const auto& p : kv.second) {
            ofs << u << " " << t << " " << p << "\n";
        }
    }
}

bool StorageEngine::hasPermission(const std::string& dbname, const std::string& tablename,
                                  const std::string& username, TablePrivilege priv) const {
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return false;
    std::string target = privToStr(priv);
    std::ifstream ifs(ppath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string u, t, p;
        ss >> u >> t >> p;
        if (u == username && t == tablename) {
            if (p == "all" || p == target) return true;
        }
    }
    return false;
}

std::vector<std::string> StorageEngine::getUserPermissions(
    const std::string& dbname, const std::string& tablename,
    const std::string& username) const {
    std::vector<std::string> result;
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return result;
    std::ifstream ifs(ppath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string u, t, p;
        ss >> u >> t >> p;
        if (u == username && t == tablename) result.push_back(p);
    }
    return result;
}

} // namespace dbms
