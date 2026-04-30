#include "TableManage.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
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

void TableSchema::print() const {
    std::cout << tablename << "\n\n";
    for (size_t i = 0; i < len; ++i) cols[i].print();
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
void StorageEngine::updatePKIndexOnInsert(const std::string& dbname,
                                           const std::string& tablename,
                                           int64_t rowIdx,
                                           const std::string& rowBuffer) {
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t pkIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
    }
    if (pkIdx >= tbl.len) return;

    size_t offset = 0;
    for (size_t i = 0; i < pkIdx; ++i) offset += tbl.cols[i].dsize;

    std::string pkVal;
    const Column& col = tbl.cols[pkIdx];
    if (col.dataType == "char") {
        pkVal = rowBuffer.substr(offset, col.dsize);
        auto nul = pkVal.find('\0');
        if (nul != std::string::npos) pkVal.resize(nul);
    } else if (col.dataType == "date") {
        Date d;
        std::memcpy(&d, rowBuffer.data() + offset, DATE_SIZE);
        pkVal = str(d);
    } else {
        int64_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, col.dsize);
        pkVal = transstr(val);
    }
    pkIndexes_[pkIndexKey(dbname, tablename)][pkVal] = rowIdx;
}

void StorageEngine::rebuildPKIndex(const std::string& dbname,
                                    const std::string& tablename) {
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t pkIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
    }

    std::string key = pkIndexKey(dbname, tablename);
    pkIndexes_[key].clear();
    if (pkIdx >= tbl.len) return;

    std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
    if (!in) return;

    size_t rowSize = tbl.rowSize();
    in.seekg(0, std::ios::end);
    auto fs = static_cast<size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    size_t rowCount = (rowSize == 0) ? 0 : fs / rowSize;

    for (size_t r = 0; r < rowCount; ++r) {
        for (size_t c = 0; c < pkIdx; ++c)
            in.seekg(static_cast<std::streamsize>(tbl.cols[c].dsize), std::ios::cur);

        std::string pkVal;
        const Column& col = tbl.cols[pkIdx];
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
        pkIndexes_[key][pkVal] = static_cast<int64_t>(r);

        for (size_t c = pkIdx + 1; c < tbl.len; ++c)
            in.seekg(static_cast<std::streamsize>(tbl.cols[c].dsize), std::ios::cur);
    }
}

std::set<int64_t> StorageEngine::lookupPKIndex(const std::string& dbname,
                                                const std::string& tablename,
                                                const TableSchema& tbl,
                                                const std::string& colName,
                                                const std::string& value) {
    std::set<int64_t> result;
    bool hasPK = false;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey && tbl.cols[i].dataName == colName) {
            hasPK = true; break;
        }
    }
    if (!hasPK) return result;

    std::string key = pkIndexKey(dbname, tablename);
    auto it = pkIndexes_.find(key);
    if (it == pkIndexes_.end()) return result;

    auto vit = it->second.find(value);
    if (vit != it->second.end()) result.insert(vit->second);
    return result;
}

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

    // Check primary key uniqueness
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isPrimaryKey) {
            auto it = values.find(tbl.cols[i].dataName);
            if (it != values.end() && !it->second.empty()) {
                std::ifstream checkIn(dataPath(dbname, tablename), std::ios::binary);
                if (checkIn) {
                    checkIn.seekg(0, std::ios::end);
                    auto fs = static_cast<size_t>(checkIn.tellg());
                    checkIn.seekg(0, std::ios::beg);
                    size_t rc = (rowSize == 0) ? 0 : fs / rowSize;
                    for (size_t r = 0; r < rc; ++r) {
                        size_t off = 0;
                        for (size_t j = 0; j < tbl.len; ++j) {
                            if (j == i) {
                                if (tbl.cols[j].dataType == "char") {
                                    std::string buf(tbl.cols[j].dsize, '\0');
                                    checkIn.read(buf.data(), static_cast<std::streamsize>(tbl.cols[j].dsize));
                                    auto nul = buf.find('\0');
                                    if (nul != std::string::npos) buf.resize(nul);
                                    if (buf == it->second) return OpResult::DuplicateKey;
                                } else if (tbl.cols[j].dataType == "date") {
                                    Date d;
                                    checkIn.read(reinterpret_cast<char*>(&d), DATE_SIZE);
                                    Date v(it->second.c_str());
                                    if (d == v) return OpResult::DuplicateKey;
                                } else {
                                    int64_t val = 0;
                                    checkIn.read(reinterpret_cast<char*>(&val), static_cast<std::streamsize>(tbl.cols[j].dsize));
                                    int64_t cmp = parseInt(it->second);
                                    if (val == cmp) return OpResult::DuplicateKey;
                                }
                            } else {
                                checkIn.seekg(static_cast<std::streamsize>(tbl.cols[j].dsize), std::ios::cur);
                            }
                            off += tbl.cols[j].dsize;
                        }
                    }
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

    {
        std::ofstream out(dataPath(dbname, tablename), std::ios::binary | std::ios::app);
        out.write(rowBuffer.data(), static_cast<std::streamsize>(rowSize));
    }
    // Update PK index
    {
        std::ifstream in(dataPath(dbname, tablename), std::ios::binary);
        in.seekg(0, std::ios::end);
        auto fs = static_cast<size_t>(in.tellg());
        int64_t rowIdx = (rowSize == 0) ? 0 : static_cast<int64_t>(fs / rowSize) - 1;
        if (rowIdx >= 0) updatePKIndexOnInsert(dbname, tablename, rowIdx, rowBuffer);
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

    // Try PK index for = conditions
    for (const auto& c : conds) {
        if (c.op == "=") {
            ids = lookupPKIndex(dbname, tablename, tbl, c.colName, c.value);
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

    std::string tempPath = dataPath(dbname, tablename).string() + ".tmp";
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
    rebuildPKIndex(dbname, tablename);
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
    rebuildPKIndex(dbname, tablename);
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

} // namespace dbms
