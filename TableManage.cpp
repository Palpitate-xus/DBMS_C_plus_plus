#include "TableManage.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace dbms {

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
    return total;
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

// ========================================================================
// StorageEngine
// ========================================================================
StorageEngine::StorageEngine() = default;

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

std::filesystem::path StorageEngine::indexPath(const std::string& dbname,
                                                const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".idx");
}

std::string StorageEngine::extractPKValue(const std::string& rowBuffer, const TableSchema& tbl) {
    size_t pkIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
    }
    if (pkIdx >= tbl.len) return "";

    size_t offset = 0;
    for (size_t i = 0; i < pkIdx; ++i) offset += tbl.cols[i].dsize;

    const Column& col = tbl.cols[pkIdx];
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
    if (it != pkIndexCache_.end()) return it->second.get();

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
        uint8_t flags = (tbl.cols[i].isNull ? 1 : 0) | (tbl.cols[i].isPrimaryKey ? 2 : 0);
        out.write(reinterpret_cast<const char*>(&flags), 1);
        writeFixedString(out, tbl.cols[i].dataType, MAX_TYPE_NAME_LEN);
        writeFixedString(out, tbl.cols[i].dataName, MAX_COL_NAME_LEN);
        int32_t dsize = static_cast<int32_t>(tbl.cols[i].dsize);
        out.write(reinterpret_cast<const char*>(&dsize), 4);
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
        tbl.cols[i].dataType = readFixedString(in, MAX_TYPE_NAME_LEN);
        tbl.cols[i].dataName = readFixedString(in, MAX_COL_NAME_LEN);
        int32_t dsize = 0;
        in.read(reinterpret_cast<char*>(&dsize), 4);
        tbl.cols[i].dsize = static_cast<size_t>(dsize);
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
    {
        std::ofstream out(dataPath(dbname, tbl.tablename), std::ios::binary);
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

OpResult StorageEngine::insert(const std::string& dbname,
                                const std::string& tablename,
                                const std::map<std::string, std::string>& values) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    // Check primary key uniqueness using B+ tree index
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) {
            auto it = values.find(tbl.cols[i].dataName);
            if (it != values.end() && !it->second.empty()) {
                BPTree* idx = getPKIndex(dbname, tablename);
                if (idx) {
                    int64_t dummy;
                    if (idx->search(it->second, dummy)) return OpResult::DuplicateKey;
                }
            }
            break;
        }
    }

    // Build row buffer and validate all values before writing
    std::string rowBuffer(rowSize, '\0');
    size_t offset = 0;

    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        auto it = values.find(col.dataName);
        std::string val = (it != values.end()) ? it->second : "";

        if (!col.isNull && val.empty()) {
            return OpResult::NullNotAllowed;
        }

        if (col.dataType == "char") {
            if (!stringToBuffer(val, &rowBuffer[offset], col.dsize)) {
                return OpResult::InvalidValue;
            }
        } else if (col.dataType == "date") {
            Date d(val.c_str());
            if (d.year == 0) return OpResult::InvalidValue;
            std::memcpy(&rowBuffer[offset], &d, DATE_SIZE);
        } else {
            // integer types: tiny, int, long
            if (val.empty()) {
                int64_t nullVal = INF;
                std::memcpy(&rowBuffer[offset], &nullVal, col.dsize);
            } else {
                int64_t num = parseInt(val);
                if (num == INF) return OpResult::InvalidValue;
                std::memcpy(&rowBuffer[offset], &num, col.dsize);
            }
        }
        offset += col.dsize;
    }

    // Check foreign key references
    for (size_t fi = 0; fi < tbl.fkLen; ++fi) {
        const ForeignKey& fk = tbl.fks[fi];
        auto it = values.find(fk.colName);
        if (it == values.end() || it->second.empty()) continue;
        if (!tableExists(dbname, fk.refTable)) return OpResult::TableNotExist;
        TableSchema refTbl = getTableSchema(dbname, fk.refTable);
        BPTree* refIdx = getPKIndex(dbname, fk.refTable);
        if (refIdx) {
            int64_t dummy;
            if (!refIdx->search(it->second, dummy)) {
                return OpResult::InvalidValue;  // referenced key not found
            }
        }
    }

    {
        std::ofstream out(dataPath(dbname, tablename), std::ios::binary | std::ios::app);
        out.write(rowBuffer.data(), static_cast<std::streamsize>(rowSize));
    }
    // Update B+ tree PK index
    {
        std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
        in.seekg(0, std::ios::end);
        auto fs = static_cast<size_t>(in.tellg());
        int64_t rowIdx = (rowSize == 0) ? 0 : static_cast<int64_t>(fs / rowSize) - 1;
        if (rowIdx >= 0) {
            BPTree* idx = getPKIndex(dbname, tablename);
            if (idx) {
                std::string pkVal = extractPKValue(rowBuffer, tbl);
                if (!pkVal.empty()) idx->insert(pkVal, rowIdx);
            }
        }
    }
    return OpResult::Success;
}

std::vector<StorageEngine::Condition> StorageEngine::parseConditions(
    const std::vector<std::string>& cstr) {
    std::vector<Condition> conds;
    for (const auto& s : cstr) {
        if (s.empty()) continue;
        size_t opEnd = 0;
        while (opEnd < s.size() && (s[opEnd] == '<' || s[opEnd] == '>' || s[opEnd] == '=' || s[opEnd] == '!')) ++opEnd;
        if (opEnd == 0) continue;
        Condition c;
        c.op = s.substr(0, opEnd);
        size_t sp = s.find(' ', opEnd);
        if (sp == std::string::npos) continue;
        c.colName = s.substr(opEnd, sp - opEnd);
        c.value = s.substr(sp + 1);
        conds.push_back(c);
    }
    return conds;
}

std::set<int64_t> StorageEngine::filterRows(const std::string& dbname,
                                             const std::string& tablename,
                                             const std::vector<Condition>& conds) {
    std::set<int64_t> ids;
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

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
            }
            if (!ids.empty()) {
                // Verify remaining conditions by file scan on candidate rows
                if (conds.size() > 1) {
                    std::set<int64_t> toRemove;
                    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
                    for (int64_t rowIdx : ids) {
                        bool match = true;
                        for (const auto& cond : conds) {
                            if (cond.op == "=" && cond.colName == c.colName) continue;
                            size_t colOffset = 0;
                            size_t ci = 0;
                            for (; ci < tbl.len && tbl.cols[ci].dataName != cond.colName; ++ci)
                                colOffset += tbl.cols[ci].dsize;
                            if (ci >= tbl.len) { match = false; break; }
                            in.seekg(static_cast<std::streamoff>(colOffset + rowIdx * rowSize), std::ios::beg);
                            const Column& col = tbl.cols[ci];
                            if (col.dataType == "char") {
                                std::string buf(col.dsize, '\0');
                                in.read(buf.data(), static_cast<std::streamsize>(col.dsize));
                                auto nul = buf.find('\0');
                                if (nul != std::string::npos) buf.resize(nul);
                                if (cond.op == "<"  && !(buf <  cond.value)) match = false;
                                if (cond.op == ">"  && !(buf >  cond.value)) match = false;
                                if (cond.op == "="  && buf != cond.value)    match = false;
                                if (cond.op == "<=" && (buf >  cond.value))   match = false;
                                if (cond.op == ">=" && (buf <  cond.value))   match = false;
                                if (cond.op == "!=" && buf == cond.value)    match = false;
                            } else if (col.dataType == "date") {
                                Date d;
                                in.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                                Date v(cond.value.c_str());
                                if (cond.op == "<"  && v.year && !(d < v))  match = false;
                                if (cond.op == ">"  && v.year && !(d > v))  match = false;
                                if (cond.op == "="  && v.year && d != v)    match = false;
                                if (cond.op == "<=" && v.year && (d > v))   match = false;
                                if (cond.op == ">=" && v.year && (d < v))   match = false;
                                if (cond.op == "!=" && v.year && d == v)    match = false;
                            } else {
                                int64_t val = 0;
                                in.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(col.dsize));
                                int64_t cmp = parseInt(cond.value);
                                if (cond.op == "<"  && cmp != INF && !(val < cmp)) match = false;
                                if (cond.op == ">"  && cmp != INF && !(val > cmp)) match = false;
                                if (cond.op == "="  && cmp != INF && val != cmp)   match = false;
                                if (cond.op == "<=" && cmp != INF && (val > cmp))  match = false;
                                if (cond.op == ">=" && cmp != INF && (val < cmp))  match = false;
                                if (cond.op == "!=" && cmp != INF && val == cmp)   match = false;
                            }
                            if (!match) break;
                        }
                        if (!match) toRemove.insert(rowIdx);
                    }
                    for (auto r : toRemove) ids.erase(r);
                }
                return ids;
            }
        }
    }

    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
    if (!in) return ids;

    in.seekg(0, std::ios::end);
    auto fileSize = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    size_t rowCount = (rowSize == 0) ? 0 : fileSize / rowSize;

    for (int64_t i = 0; i < static_cast<int64_t>(rowCount); ++i) ids.insert(i);
    if (ids.empty() || conds.empty()) return ids;

    for (size_t colIdx = 0, colOffset = 0; colIdx < tbl.len;
         colOffset += tbl.cols[colIdx++].dsize) {
        const Column& col = tbl.cols[colIdx];

        // Collect conditions for this column
        std::vector<Condition> colConds;
        for (const auto& c : conds) {
            if (c.colName == col.dataName) colConds.push_back(c);
        }
        if (colConds.empty()) continue;

        std::set<int64_t> toRemove;
        for (int64_t rowIdx : ids) {
            in.seekg(static_cast<std::streamoff>(colOffset + rowIdx * rowSize), std::ios::beg);
            bool match = true;

            for (const auto& c : colConds) {
                if (col.dataType == "char") {
                    std::string buf(col.dsize, '\0');
                    in.read(buf.data(), static_cast<std::streamsize>(col.dsize));
                    auto nul = buf.find('\0');
                    if (nul != std::string::npos) buf.resize(nul);
                    if (c.op == "<"  && !(buf <  c.value)) match = false;
                    if (c.op == ">"  && !(buf >  c.value)) match = false;
                    if (c.op == "="  && buf != c.value)    match = false;
                    if (c.op == "<=" && (buf >  c.value))   match = false;
                    if (c.op == ">=" && (buf <  c.value))   match = false;
                    if (c.op == "!=" && buf == c.value)    match = false;
                } else if (col.dataType == "date") {
                    Date d;
                    in.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                    Date v(c.value.c_str());
                    if (c.op == "<"  && v.year && !(d < v))  match = false;
                    if (c.op == ">"  && v.year && !(d > v))  match = false;
                    if (c.op == "="  && v.year && d != v)    match = false;
                    if (c.op == "<=" && v.year && (d > v))   match = false;
                    if (c.op == ">=" && v.year && (d < v))   match = false;
                    if (c.op == "!=" && v.year && d == v)    match = false;
                } else {
                    int64_t val = 0;
                    in.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(col.dsize));
                    int64_t cmp = parseInt(c.value);
                    if (c.op == "<"  && cmp != INF && !(val < cmp)) match = false;
                    if (c.op == ">"  && cmp != INF && !(val > cmp)) match = false;
                    if (c.op == "="  && cmp != INF && val != cmp)   match = false;
                    if (c.op == "<=" && cmp != INF && (val > cmp))  match = false;
                    if (c.op == ">=" && cmp != INF && (val < cmp))  match = false;
                    if (c.op == "!=" && cmp != INF && val == cmp)   match = false;
                }
                if (!match) break;
            }
            if (!match) toRemove.insert(rowIdx);
        }
        for (auto r : toRemove) ids.erase(r);
        if (ids.empty()) break;
    }
    return ids;
}

OpResult StorageEngine::remove(const std::string& dbname,
                                const std::string& tablename,
                                const std::vector<std::string>& conditions) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
    if (!in) return OpResult::Success;  // empty file

    in.seekg(0, std::ios::end);
    auto fileSize = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    size_t rowCount = (rowSize == 0) ? 0 : fileSize / rowSize;

    auto conds = parseConditions(conditions);
    std::set<int64_t> toDelete = filterRows(dbname, tablename, conds);

    if (toDelete.empty()) return OpResult::Success;

    // Check foreign key RESTRICT: ensure no other table references rows being deleted
    {
        // Find PK column and offset of this table
        size_t pkIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
        }
        if (pkIdx < tbl.len) {
            // Read PK values of rows being deleted
            std::set<std::string> deletedPKs;
            for (int64_t rowIdx : toDelete) {
                in.seekg(static_cast<std::streamoff>(rowIdx * rowSize), std::ios::beg);
                size_t off = 0;
                for (size_t c = 0; c < pkIdx; ++c) {
                    in.seekg(static_cast<std::streamsize>(tbl.cols[c].dsize), std::ios::cur);
                    off += tbl.cols[c].dsize;
                }
                const Column& col = tbl.cols[pkIdx];
                std::string pkVal;
                if (col.dataType == "char") {
                    std::string buf(col.dsize, '\0');
                    in.read(buf.data(), static_cast<std::streamsize>(col.dsize));
                    auto nul = buf.find('\0');
                    if (nul != std::string::npos) buf.resize(nul);
                    pkVal = buf;
                } else if (col.dataType == "date") {
                    Date d;
                    in.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                    pkVal = str(d);
                } else {
                    int64_t val = 0;
                    in.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(col.dsize));
                    pkVal = transstr(val);
                }
                if (!pkVal.empty()) deletedPKs.insert(pkVal);
            }

            // Scan all other tables for FK references
            auto allTables = getTableNames(dbname);
            for (const auto& otherTable : allTables) {
                if (otherTable == tablename) continue;
                TableSchema otherTbl = getTableSchema(dbname, otherTable);
                for (size_t fi = 0; fi < otherTbl.fkLen; ++fi) {
                    const ForeignKey& fk = otherTbl.fks[fi];
                    if (fk.refTable != tablename) continue;
                    // Find FK column offset in other table
                    size_t fkColIdx = otherTbl.len;
                    for (size_t ci = 0; ci < otherTbl.len; ++ci) {
                        if (otherTbl.cols[ci].dataName == fk.colName) { fkColIdx = ci; break; }
                    }
                    if (fkColIdx >= otherTbl.len) continue;
                    size_t otherRowSize = otherTbl.rowSize();
                    std::ifstream oin(dataPath(dbname, otherTable), std::ios::binary);
                    if (!oin) continue;
                    oin.seekg(0, std::ios::end);
                    auto ofs = static_cast<size_t>(oin.tellg());
                    oin.seekg(0, std::ios::beg);
                    size_t orc = (otherRowSize == 0) ? 0 : ofs / otherRowSize;
                    for (size_t r = 0; r < orc; ++r) {
                        size_t coff = 0;
                        for (size_t c = 0; c < fkColIdx; ++c) {
                            oin.seekg(static_cast<std::streamsize>(otherTbl.cols[c].dsize), std::ios::cur);
                            coff += otherTbl.cols[c].dsize;
                        }
                        const Column& fcol = otherTbl.cols[fkColIdx];
                        std::string fval;
                        if (fcol.dataType == "char") {
                            std::string buf(fcol.dsize, '\0');
                            oin.read(buf.data(), static_cast<std::streamsize>(fcol.dsize));
                            auto nul = buf.find('\0');
                            if (nul != std::string::npos) buf.resize(nul);
                            fval = buf;
                        } else if (fcol.dataType == "date") {
                            Date d;
                            oin.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                            fval = str(d);
                        } else {
                            int64_t val = 0;
                            oin.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(fcol.dsize));
                            fval = transstr(val);
                        }
                        if (deletedPKs.find(fval) != deletedPKs.end()) {
                            return OpResult::InvalidValue;  // FK reference exists, cannot delete
                        }
                        for (size_t c = fkColIdx + 1; c < otherTbl.len; ++c)
                            oin.seekg(static_cast<std::streamsize>(otherTbl.cols[c].dsize), std::ios::cur);
                    }
                }
            }
        }
    }

    std::string tempPath = dataPath(dbname, tablename).string() + ".tmp";
    in.seekg(0, std::ios::beg);  // Reset stream position after FK check
    {
        std::ofstream out(tempPath, std::ios::binary);
        for (size_t i = 0; i < rowCount; ++i) {
            std::string row(rowSize, '\0');
            in.read(row.data(), static_cast<std::streamsize>(rowSize));
            if (toDelete.find(static_cast<int64_t>(i)) == toDelete.end()) {
                out.write(row.data(), static_cast<std::streamsize>(rowSize));
            }
        }
    }

    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::rename(tempPath, dataPath(dbname, tablename));
    // Rebuild B+ tree PK index
    std::filesystem::remove(indexPath(dbname, tablename));
    BPTree* idx = getPKIndex(dbname, tablename);
    if (idx) {
        size_t pkIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
        }
        if (pkIdx < tbl.len) {
            std::ifstream in2(dataPath(dbname, tablename), std::ios::binary);
            if (in2) {
                in2.seekg(0, std::ios::end);
                auto fs = static_cast<size_t>(in2.tellg());
                in2.seekg(0, std::ios::beg);
                size_t rc = (rowSize == 0) ? 0 : fs / rowSize;
                for (size_t r = 0; r < rc; ++r) {
                    std::string row(rowSize, '\0');
                    in2.read(row.data(), static_cast<std::streamsize>(rowSize));
                    std::string pkVal = extractPKValue(row, tbl);
                    if (!pkVal.empty()) idx->insert(pkVal, static_cast<int64_t>(r));
                }
            }
        }
    }
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
                } else if (col.dataType != "char") {
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

    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
    if (!in) return OpResult::Success;

    in.seekg(0, std::ios::end);
    auto fileSize = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    size_t rowCount = (rowSize == 0) ? 0 : fileSize / rowSize;

    auto conds = parseConditions(conditions);
    std::set<int64_t> matchIds = conds.empty()
        ? [&](){ std::set<int64_t> s; for (size_t i = 0; i < rowCount; ++i) s.insert(static_cast<int64_t>(i)); return s; }()
        : filterRows(dbname, tablename, conds);

    if (matchIds.empty()) return OpResult::Success;

    std::string tempPath = dataPath(dbname, tablename).string() + ".tmp";
    {
        std::ofstream out(tempPath, std::ios::binary);
        for (size_t i = 0; i < rowCount; ++i) {
            std::string row(rowSize, '\0');
            in.read(row.data(), static_cast<std::streamsize>(rowSize));
            if (matchIds.find(static_cast<int64_t>(i)) != matchIds.end()) {
                size_t offset = 0;
                for (size_t ci = 0; ci < tbl.len; ++ci) {
                    const Column& col = tbl.cols[ci];
                    auto it = colUpdates.find(ci);
                    if (it != colUpdates.end()) {
                        const std::string& val = it->second;
                        if (col.dataType == "char") {
                            stringToBuffer(val, &row[offset], col.dsize);
                        } else if (col.dataType == "date") {
                            Date d(val.c_str());
                            std::memcpy(&row[offset], &d, DATE_SIZE);
                        } else {
                            int64_t num = val.empty() ? INF : parseInt(val);
                            std::memcpy(&row[offset], &num, col.dsize);
                        }
                    }
                    offset += col.dsize;
                }
            }
            out.write(row.data(), static_cast<std::streamsize>(rowSize));
        }
    }

    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::rename(tempPath, dataPath(dbname, tablename));
    // Rebuild B+ tree PK index if PK column was updated
    bool pkUpdated = false;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey && colUpdates.find(i) != colUpdates.end()) {
            pkUpdated = true; break;
        }
    }
    if (pkUpdated) {
        std::filesystem::remove(indexPath(dbname, tablename));
        BPTree* idx = getPKIndex(dbname, tablename);
        if (idx) {
            size_t pkIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
            }
            if (pkIdx < tbl.len) {
                std::ifstream in2(dataPath(dbname, tablename), std::ios::binary);
                if (in2) {
                    in2.seekg(0, std::ios::end);
                    auto fs = static_cast<size_t>(in2.tellg());
                    in2.seekg(0, std::ios::beg);
                    size_t rc = (rowSize == 0) ? 0 : fs / rowSize;
                    for (size_t r = 0; r < rc; ++r) {
                        std::string row(rowSize, '\0');
                        in2.read(row.data(), static_cast<std::streamsize>(rowSize));
                        std::string pkVal = extractPKValue(row, tbl);
                        if (!pkVal.empty()) idx->insert(pkVal, static_cast<int64_t>(r));
                    }
                }
            }
        }
    }
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

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
    if (!in) return result;

    in.seekg(0, std::ios::end);
    auto fileSize = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    size_t rowCount = (rowSize == 0) ? 0 : fileSize / rowSize;

    auto conds = parseConditions(conditions);
    std::vector<int64_t> matchIds;
    if (conds.empty()) {
        for (size_t i = 0; i < rowCount; ++i) matchIds.push_back(static_cast<int64_t>(i));
    } else {
        auto ids = filterRows(dbname, tablename, conds);
        matchIds.assign(ids.begin(), ids.end());
    }

    // ORDER BY
    if (!orderByCol.empty()) {
        size_t sortIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == orderByCol) { sortIdx = i; break; }
        }
        if (sortIdx < tbl.len) {
            struct Item { int64_t rowIdx; std::string s; int64_t n; Date d; };
            std::vector<Item> items;
            const Column& scol = tbl.cols[sortIdx];
            for (int64_t rid : matchIds) {
                in.seekg(static_cast<std::streamoff>(rid * rowSize), std::ios::beg);
                for (size_t c = 0; c < sortIdx; ++c)
                    in.seekg(static_cast<std::streamsize>(tbl.cols[c].dsize), std::ios::cur);
                Item it{rid, "", 0, {}};
                if (scol.dataType == "char") {
                    std::string buf(scol.dsize, '\0');
                    in.read(buf.data(), static_cast<std::streamsize>(scol.dsize));
                    auto nul = buf.find('\0');
                    if (nul != std::string::npos) buf.resize(nul);
                    it.s = buf;
                } else if (scol.dataType == "date") {
                    in.read(reinterpret_cast<char*>(&it.d), DATE_SIZE);
                } else {
                    in.read(reinterpret_cast<char*>(&it.n), static_cast<std::streamsize>(scol.dsize));
                }
                items.push_back(std::move(it));
            }
            std::sort(items.begin(), items.end(), [&](const Item& a, const Item& b) {
                if (scol.dataType == "char") return orderByAsc ? (a.s < b.s) : (b.s < a.s);
                if (scol.dataType == "date") return orderByAsc ? (a.d < b.d) : (b.d < a.d);
                return orderByAsc ? (a.n < b.n) : (b.n < a.n);
            });
            matchIds.clear();
            for (const auto& it : items) matchIds.push_back(it.rowIdx);
        }
    }

    for (int64_t rowIdx : matchIds) {
        in.seekg(static_cast<std::streamoff>(rowIdx * rowSize), std::ios::beg);
        std::string rowStr;
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            if (!selectCols.empty() && selectCols.find(col.dataName) == selectCols.end()) {
                in.seekg(static_cast<std::streamsize>(col.dsize), std::ios::cur);
                continue;
            }
            if (col.dataType == "char") {
                std::string buf(col.dsize, '\0');
                in.read(buf.data(), static_cast<std::streamsize>(col.dsize));
                auto nul = buf.find('\0');
                if (nul != std::string::npos) buf.resize(nul);
                rowStr += buf + ' ';
            } else if (col.dataType == "date") {
                Date d;
                in.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                rowStr += str(d) + ' ';
            } else {
                int64_t val = 0;
                in.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(col.dsize));
                if (val == INF) rowStr += "NULL ";
                else rowStr += transstr(val) + ' ';
            }
        }
        result.push_back(rowStr);
    }
    return result;
}

std::vector<std::string> StorageEngine::aggregate(
    const std::string& dbname, const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::vector<std::pair<std::string, std::string>>& items) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
    if (!in) return result;

    in.seekg(0, std::ios::end);
    auto fileSize = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    size_t rowCount = (rowSize == 0) ? 0 : fileSize / rowSize;

    auto conds = parseConditions(conditions);
    std::vector<int64_t> matchIds;
    if (conds.empty()) {
        for (size_t i = 0; i < rowCount; ++i) matchIds.push_back(static_cast<int64_t>(i));
    } else {
        auto ids = filterRows(dbname, tablename, conds);
        matchIds.assign(ids.begin(), ids.end());
    }

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
                    isInt = (tbl.cols[i].dataType != "char" && tbl.cols[i].dataType != "date");
                    isDate = (tbl.cols[i].dataType == "date");
                    isChar = (tbl.cols[i].dataType == "char");
                    break;
                }
            }
        }

        for (int64_t rowIdx : matchIds) {
            if (func == "count") {
                if (colName == "*") { count++; continue; }
                if (colIdx >= tbl.len) continue;
                size_t offset = 0;
                for (size_t c = 0; c < colIdx; ++c) offset += tbl.cols[c].dsize;
                in.seekg(static_cast<std::streamoff>(rowIdx * rowSize + offset), std::ios::beg);
                if (isInt) {
                    int64_t val = 0;
                    in.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(tbl.cols[colIdx].dsize));
                    if (val != INF) count++;
                } else if (isDate) {
                    Date d;
                    in.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                    if (d.year != 0) count++;
                } else {
                    std::string buf(tbl.cols[colIdx].dsize, '\0');
                    in.read(buf.data(), static_cast<std::streamsize>(tbl.cols[colIdx].dsize));
                    auto nul = buf.find('\0');
                    if (nul != std::string::npos) buf.resize(nul);
                    if (!buf.empty()) count++;
                }
            } else {
                if (colIdx >= tbl.len) continue;
                size_t offset = 0;
                for (size_t c = 0; c < colIdx; ++c) offset += tbl.cols[c].dsize;
                in.seekg(static_cast<std::streamoff>(rowIdx * rowSize + offset), std::ios::beg);

                if (isInt) {
                    int64_t val = 0;
                    in.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(tbl.cols[colIdx].dsize));
                    if (val == INF) continue;
                    if (func == "sum") sum += val;
                    if (func == "avg") { sum += val; count++; }
                    if (func == "max") {
                        if (!hasMax || val > maxInt) { maxInt = val; hasMax = true; }
                    }
                    if (func == "min") {
                        if (!hasMin || val < minInt) { minInt = val; hasMin = true; }
                    }
                } else if (isDate) {
                    Date d;
                    in.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                    if (d.year == 0) continue;
                    if (func == "max") {
                        if (!hasMax || d > maxDate) { maxDate = d; hasMax = true; }
                    }
                    if (func == "min") {
                        if (!hasMin || d < minDate) { minDate = d; hasMin = true; }
                    }
                } else {
                    std::string buf(tbl.cols[colIdx].dsize, '\0');
                    in.read(buf.data(), static_cast<std::streamsize>(tbl.cols[colIdx].dsize));
                    auto nul = buf.find('\0');
                    if (nul != std::string::npos) buf.resize(nul);
                    if (buf.empty()) continue;
                    if (func == "max") {
                        if (!hasMax || buf > maxStr) { maxStr = buf; hasMax = true; }
                    }
                    if (func == "min") {
                        if (!hasMin || buf < minStr) { minStr = buf; hasMin = true; }
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

    TableSchema leftTbl = getTableSchema(dbname, leftTable);
    TableSchema rightTbl = getTableSchema(dbname, rightTable);
    size_t leftRowSize = leftTbl.rowSize();
    size_t rightRowSize = rightTbl.rowSize();

    // Read all rows from left table
    std::vector<std::string> leftRows;
    {
        std::ifstream in(dataPath(dbname, leftTable), std::ios::binary);
        if (in) {
            in.seekg(0, std::ios::end);
            auto fs = static_cast<size_t>(in.tellg());
            in.seekg(0, std::ios::beg);
            size_t cnt = (leftRowSize == 0) ? 0 : fs / leftRowSize;
            for (size_t i = 0; i < cnt; ++i) {
                std::string row(leftRowSize, '\0');
                in.read(row.data(), static_cast<std::streamsize>(leftRowSize));
                leftRows.push_back(std::move(row));
            }
        }
    }

    // Read all rows from right table
    std::vector<std::string> rightRows;
    {
        std::ifstream in(dataPath(dbname, rightTable), std::ios::binary);
        if (in) {
            in.seekg(0, std::ios::end);
            auto fs = static_cast<size_t>(in.tellg());
            in.seekg(0, std::ios::beg);
            size_t cnt = (rightRowSize == 0) ? 0 : fs / rightRowSize;
            for (size_t i = 0; i < cnt; ++i) {
                std::string row(rightRowSize, '\0');
                in.read(row.data(), static_cast<std::streamsize>(rightRowSize));
                rightRows.push_back(std::move(row));
            }
        }
    }

    // Build merged column layout for condition evaluation
    // Map: colName -> {offset, colInfo}
    // Also support "table.col" format
    struct ColInfo { size_t offset; const Column* col; };
    std::map<std::string, ColInfo> colMap;
    size_t off = 0;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        colMap[leftTbl.cols[i].dataName] = {off, &leftTbl.cols[i]};
        colMap[leftTable + "." + leftTbl.cols[i].dataName] = {off, &leftTbl.cols[i]};
        off += leftTbl.cols[i].dsize;
    }
    off = 0;
    for (size_t i = 0; i < rightTbl.len; ++i) {
        std::string simple = rightTbl.cols[i].dataName;
        if (colMap.find(simple) == colMap.end()) {
            colMap[simple] = {leftRowSize + off, &rightTbl.cols[i]};
        }
        colMap[rightTable + "." + simple] = {leftRowSize + off, &rightTbl.cols[i]};
        off += rightTbl.cols[i].dsize;
    }

    // Evaluate a condition on a merged row buffer
    auto evalCond = [&](const Condition& c, const std::string& merged) -> bool {
        auto it = colMap.find(c.colName);
        if (it == colMap.end()) return false;
        size_t offset = it->second.offset;
        const Column& col = *it->second.col;
        const char* buf = merged.data() + offset;
        if (col.dataType == "char") {
            std::string val(col.dsize, '\0');
            std::memcpy(val.data(), buf, col.dsize);
            auto nul = val.find('\0');
            if (nul != std::string::npos) val.resize(nul);
            if (c.op == "<"  && !(val <  c.value)) return false;
            if (c.op == ">"  && !(val >  c.value)) return false;
            if (c.op == "="  && val != c.value)    return false;
            if (c.op == "<=" && (val >  c.value))   return false;
            if (c.op == ">=" && (val <  c.value))   return false;
            if (c.op == "!=" && val == c.value)    return false;
        } else if (col.dataType == "date") {
            Date d;
            std::memcpy(&d, buf, DATE_SIZE);
            Date v(c.value.c_str());
            if (c.op == "<"  && v.year && !(d < v))  return false;
            if (c.op == ">"  && v.year && !(d > v))  return false;
            if (c.op == "="  && v.year && d != v)    return false;
            if (c.op == "<=" && v.year && (d > v))   return false;
            if (c.op == ">=" && v.year && (d < v))   return false;
            if (c.op == "!=" && v.year && d == v)    return false;
        } else {
            int64_t val = 0;
            std::memcpy(&val, buf, col.dsize);
            int64_t cmp = parseInt(c.value);
            if (c.op == "<"  && cmp != INF && !(val < cmp)) return false;
            if (c.op == ">"  && cmp != INF && !(val > cmp)) return false;
            if (c.op == "="  && cmp != INF && val != cmp)   return false;
            if (c.op == "<=" && cmp != INF && (val > cmp))  return false;
            if (c.op == ">=" && cmp != INF && (val < cmp))  return false;
            if (c.op == "!=" && cmp != INF && val == cmp)   return false;
        }
        return true;
    };

    auto conds = parseConditions(conditions);

    // Find left and right column offsets for ON condition
    size_t leftColOff = 0;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol) break;
        leftColOff += leftTbl.cols[i].dsize;
    }
    size_t rightColOff = 0;
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol) break;
        rightColOff += rightTbl.cols[i].dsize;
    }

    for (const auto& lr : leftRows) {
        for (const auto& rr : rightRows) {
            // ON condition
            bool onMatch = false;
            {
                const Column* lc = nullptr;
                for (size_t i = 0; i < leftTbl.len; ++i) {
                    if (leftTbl.cols[i].dataName == leftCol) { lc = &leftTbl.cols[i]; break; }
                }
                const Column* rc = nullptr;
                for (size_t i = 0; i < rightTbl.len; ++i) {
                    if (rightTbl.cols[i].dataName == rightCol) { rc = &rightTbl.cols[i]; break; }
                }
                if (!lc || !rc) continue;

                if (lc->dataType == "char") {
                    std::string lv(lc->dsize, '\0'), rv(rc->dsize, '\0');
                    std::memcpy(lv.data(), lr.data() + leftColOff, lc->dsize);
                    std::memcpy(rv.data(), rr.data() + rightColOff, rc->dsize);
                    auto n = lv.find('\0'); if (n != std::string::npos) lv.resize(n);
                    n = rv.find('\0'); if (n != std::string::npos) rv.resize(n);
                    onMatch = (lv == rv);
                } else if (lc->dataType == "date") {
                    Date ld, rd;
                    std::memcpy(&ld, lr.data() + leftColOff, DATE_SIZE);
                    std::memcpy(&rd, rr.data() + rightColOff, DATE_SIZE);
                    onMatch = (ld == rd);
                } else {
                    int64_t lv = 0, rv = 0;
                    std::memcpy(&lv, lr.data() + leftColOff, lc->dsize);
                    std::memcpy(&rv, rr.data() + rightColOff, rc->dsize);
                    onMatch = (lv == rv);
                }
            }
            if (!onMatch) continue;

            std::string merged = lr + rr;

            // WHERE conditions
            bool whereMatch = true;
            for (const auto& c : conds) {
                if (!evalCond(c, merged)) { whereMatch = false; break; }
            }
            if (!whereMatch) continue;

            // Format output with SELECT columns
            std::string rowStr;
            auto appendCol = [&](const std::string& prefix, const Column& col, const char* buf) {
                std::string fullName = prefix + "." + col.dataName;
                bool include = selectCols.empty();
                if (!include) {
                    if (selectCols.find(col.dataName) != selectCols.end() ||
                        selectCols.find(fullName) != selectCols.end()) {
                        include = true;
                    }
                }
                if (!include) return;
                if (col.dataType == "char") {
                    std::string val(col.dsize, '\0');
                    std::memcpy(val.data(), buf, col.dsize);
                    auto nul = val.find('\0');
                    if (nul != std::string::npos) val.resize(nul);
                    rowStr += val + ' ';
                } else if (col.dataType == "date") {
                    Date d;
                    std::memcpy(&d, buf, DATE_SIZE);
                    rowStr += str(d) + ' ';
                } else {
                    int64_t val = 0;
                    std::memcpy(&val, buf, col.dsize);
                    if (val == INF) rowStr += "NULL ";
                    else rowStr += transstr(val) + ' ';
                }
            };

            for (size_t i = 0; i < leftTbl.len; ++i)
                appendCol(leftTable, leftTbl.cols[i], lr.data() + [&](){ size_t o=0; for(size_t j=0;j<i;++j)o+=leftTbl.cols[j].dsize; return o; }());
            for (size_t i = 0; i < rightTbl.len; ++i)
                appendCol(rightTable, rightTbl.cols[i], rr.data() + [&](){ size_t o=0; for(size_t j=0;j<i;++j)o+=rightTbl.cols[j].dsize; return o; }());

            if (!rowStr.empty()) result.push_back(rowStr);
        }
    }
    return result;
}

// ========================================================================
// Transaction support (database-level snapshot)
// ========================================================================

OpResult StorageEngine::beginTransaction(const std::string& dbname) {
    if (inTransaction_) return OpResult::Success;  // already in txn
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;

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
    inTransaction_ = true;
    txnDB_ = dbname;
    return OpResult::Success;
}

OpResult StorageEngine::commitTransaction() {
    if (!inTransaction_) return OpResult::Success;
    std::filesystem::path backup = dbPath(txnDB_);
    backup += ".txn_backup";
    if (std::filesystem::exists(backup)) {
        std::filesystem::remove_all(backup);
    }
    inTransaction_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

OpResult StorageEngine::rollbackTransaction() {
    if (!inTransaction_) return OpResult::Success;
    std::filesystem::path backup = dbPath(txnDB_);
    backup += ".txn_backup";
    if (!std::filesystem::exists(backup)) {
        inTransaction_ = false;
        txnDB_.clear();
        return OpResult::Success;
    }
    std::filesystem::path db = dbPath(txnDB_);
    std::filesystem::remove_all(db);
    std::filesystem::rename(backup, db);
    // Clear index cache after rollback
    pkIndexCache_.clear();
    inTransaction_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

} // namespace dbms
