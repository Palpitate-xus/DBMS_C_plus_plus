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
Column makeIntColumn(const std::string& name, bool isNull, int scale) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
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

Column makeStringColumn(const std::string& name, bool isNull, size_t length) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.dataType = "char";
    c.dsize = std::max(size_t(1), std::min(length, size_t(1005)));
    return c;
}

Column makeDateColumn(const std::string& name, bool isNull) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.dataType = "date";
    c.dsize = DATE_SIZE;
    return c;
}

// ========================================================================
// StorageEngine
// ========================================================================
StorageEngine::StorageEngine() = default;

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
        uint8_t isNull = tbl.cols[i].isNull ? 1 : 0;
        out.write(reinterpret_cast<const char*>(&isNull), 1);
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
        uint8_t isNull = 0;
        in.read(reinterpret_cast<char*>(&isNull), 1);
        tbl.cols[i].isNull = (isNull != 0);
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
    return OpResult::Success;
}

std::vector<std::string> StorageEngine::query(const std::string& dbname,
                                               const std::string& tablename,
                                               const std::vector<std::string>& conditions,
                                               const std::set<std::string>& selectCols) {
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
    std::set<int64_t> matchIds;
    if (conds.empty()) {
        for (size_t i = 0; i < rowCount; ++i) matchIds.insert(static_cast<int64_t>(i));
    } else {
        matchIds = filterRows(dbname, tablename, conds);
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

} // namespace dbms
