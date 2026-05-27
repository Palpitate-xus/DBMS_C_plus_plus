#include "TableManage.h"
#include "TxnIdGenerator.h"
#include "Config.h"

extern dbms::Config g_config;

#include <algorithm>
#include <cmath>
#include <ctime>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
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
// Evaluate a simple generated-column expression: supports binary ops + - * /
// e.g. "age * 2", "salary + bonus", "price - discount"
static std::string evaluateGeneratedExpr(
    const std::string& expr,
    const std::map<std::string, std::string>& colValues) {
    // Find the operator
    char op = 0;
    size_t opPos = std::string::npos;
    for (size_t i = 0; i < expr.size(); ++i) {
        if (expr[i] == '+' || expr[i] == '-' || expr[i] == '*' || expr[i] == '/') {
            op = expr[i];
            opPos = i;
            break;
        }
    }
    if (opPos == std::string::npos || op == 0) return "";
    std::string left = trim(expr.substr(0, opPos));
    std::string right = trim(expr.substr(opPos + 1));
    if (left.empty() || right.empty()) return "";
    // Resolve operand values
    auto resolve = [&](const std::string& token) -> double {
        auto it = colValues.find(token);
        if (it != colValues.end() && !it->second.empty()) {
            try { return std::stod(it->second); } catch (...) {}
        }
        try { return std::stod(token); } catch (...) { return 0.0; }
    };
    double lv = resolve(left);
    double rv = resolve(right);
    double result = 0.0;
    switch (op) {
        case '+': result = lv + rv; break;
        case '-': result = lv - rv; break;
        case '*': result = lv * rv; break;
        case '/': result = (rv != 0.0) ? lv / rv : 0.0; break;
    }
    // Return as integer string if whole number, else decimal
    if (result == std::floor(result)) {
        return std::to_string(static_cast<int64_t>(result));
    }
    std::ostringstream oss;
    oss << result;
    return oss.str();
}

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

// REGEXP pattern matching (ECMAScript syntax), case-insensitive
static bool regexMatch(const std::string& text, const std::string& pattern) {
    try {
        std::regex re(pattern, std::regex::icase | std::regex::ECMAScript);
        return std::regex_search(text, re);
    } catch (...) {
        return false;
    }
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
            std::cout << "  ";
            for (size_t ci = 0; ci < fks[i].colNames.size(); ++ci) {
                if (ci > 0) std::cout << ",";
                std::cout << fks[i].colNames[ci];
            }
            std::cout << " -> " << fks[i].refTable << "(";
            for (size_t ci = 0; ci < fks[i].refCols.size(); ++ci) {
                if (ci > 0) std::cout << ",";
                std::cout << fks[i].refCols[ci];
            }
            std::cout << ") ON DELETE " << fks[i].onDelete << "\n";
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
Column makeIntColumn(const std::string& name, bool isNull, int scale, bool isPK, bool isUnsigned) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isUnsigned = isUnsigned;
    if (scale == 0) {
        c.dataType = isUnsigned ? "smallint unsigned" : "smallint";
        c.dsize = 2;
    } else if (scale == 1) {
        c.dataType = isUnsigned ? "tinyint unsigned" : "tinyint";
        c.dsize = 1;
    } else if (scale == 2) {
        c.dataType = isUnsigned ? "int unsigned" : "int";
        c.dsize = 4;
    } else {
        c.dataType = isUnsigned ? "bigint unsigned" : "bigint";
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

Column makeNCharColumn(const std::string& name, bool isNull, size_t length, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "nchar";
    c.dsize = std::max(size_t(1), std::min(length, size_t(4000)));
    return c;
}

Column makeNVarCharColumn(const std::string& name, bool isNull, size_t maxLen, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "nvarchar";
    c.dsize = std::max(size_t(1), std::min(maxLen, size_t(4000)));
    return c;
}

Column makeTimestampColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "timestamp";
    c.dsize = TIMESTAMP_SIZE;
    return c;
}

Column makeTimestamptzColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "timestamptz";
    c.dsize = TIMESTAMP_SIZE;
    return c;
}

Column makeTextColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "text";
    c.dsize = 65535;
    return c;
}

Column makeBlobColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "blob";
    c.dsize = 65535;
    return c;
}

Column makeBinaryColumn(const std::string& name, bool isNull, size_t length, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = false;
    c.dataType = "binary";
    c.dsize = std::max(size_t(1), std::min(length, size_t(1005)));
    return c;
}

Column makeVarBinaryColumn(const std::string& name, bool isNull, size_t length, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "varbinary";
    c.dsize = std::max(size_t(1), std::min(length, size_t(65535)));
    return c;
}

Column makeJsonColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "json";
    c.dsize = 65535;
    return c;
}

Column makeFloatColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "float";
    c.dsize = 4;
    return c;
}

Column makeDoubleColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "double";
    c.dsize = 8;
    return c;
}

Column makeDecimalColumn(const std::string& name, bool isNull, int precision, int scale, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "decimal";
    c.dsize = 8;  // stored as double internally
    return c;
}

Column makeBooleanColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "boolean";
    c.dsize = 1;
    return c;
}

Column makeUuidColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "uuid";
    c.dsize = 36;  // UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    return c;
}

Column makeTimeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "time";
    c.dsize = 4;  // int32_t seconds since 00:00:00
    return c;
}

Column makeDateTimeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "datetime";
    c.dsize = TIMESTAMP_SIZE;  // same as timestamp: int64_t seconds
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

std::filesystem::path StorageEngine::partitionDataPath(const std::string& dbname,
                                                        const std::string& tablename,
                                                        const std::string& partitionName) const {
    return dbPath(dbname) / (tablename + "#" + partitionName + ".dt");
}

std::filesystem::path StorageEngine::tableListPath(const std::string& dbname) const {
    return dbPath(dbname) / "tlist.lst";
}

std::string StorageEngine::getPartitionName(const TableSchema& tbl, const std::string& keyVal) const {
    if (tbl.partitionType == TableSchema::PartitionType::None) return "";
    if (tbl.partitionType == TableSchema::PartitionType::Range) {
        for (const auto& rp : tbl.rangePartitions) {
            if (keyVal < rp.second) return rp.first;
        }
        return tbl.rangePartitions.empty() ? "" : tbl.rangePartitions.back().first;
    }
    if (tbl.partitionType == TableSchema::PartitionType::List) {
        for (const auto& lp : tbl.listPartitions) {
            for (const auto& v : lp.second) {
                if (v == keyVal) return lp.first;
            }
        }
        return "";
    }
    if (tbl.partitionType == TableSchema::PartitionType::Hash) {
        if (tbl.hashPartitions == 0) return "";
        size_t h = std::hash<std::string>{}(keyVal) % tbl.hashPartitions;
        return "p" + std::to_string(h);
    }
    return "";
}

std::vector<std::string> StorageEngine::getTargetPartitions(
    const TableSchema& tbl, const std::vector<Condition>& conds) const {
    std::vector<std::string> result;
    if (tbl.partitionType == TableSchema::PartitionType::None) return result;
    if (tbl.partitionKey.empty()) return result;

    // Collect partition-key related conditions
    for (const auto& c : conds) {
        if (c.colName != tbl.partitionKey) continue;
        if (tbl.partitionType == TableSchema::PartitionType::Range) {
            if (c.op == "=") {
                result.push_back(getPartitionName(tbl, c.value));
            } else if (c.op == "<") {
                for (const auto& rp : tbl.rangePartitions) {
                    if (rp.second > c.value) result.push_back(rp.first);
                }
            } else if (c.op == "<=") {
                for (const auto& rp : tbl.rangePartitions) {
                    if (rp.second >= c.value) result.push_back(rp.first);
                }
            } else if (c.op == ">") {
                for (const auto& rp : tbl.rangePartitions) {
                    if (rp.second <= c.value) result.push_back(rp.first);
                }
                if (!tbl.rangePartitions.empty()) result.push_back(tbl.rangePartitions.back().first);
            } else if (c.op == ">=") {
                for (const auto& rp : tbl.rangePartitions) {
                    if (rp.second < c.value) result.push_back(rp.first);
                }
                if (!tbl.rangePartitions.empty()) result.push_back(tbl.rangePartitions.back().first);
            }
        } else if (tbl.partitionType == TableSchema::PartitionType::List) {
            if (c.op == "=") {
                result.push_back(getPartitionName(tbl, c.value));
            }
        } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
            if (c.op == "=") {
                result.push_back(getPartitionName(tbl, c.value));
            }
        }
    }
    // Deduplicate
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

std::filesystem::path StorageEngine::walPath(const std::string& dbname) const {
    return dbPath(dbname) / "wal.log";
}

std::filesystem::path StorageEngine::checkpointPath(const std::string& dbname) const {
    return dbPath(dbname) / "checkpoint";
}

std::filesystem::path StorageEngine::viewPath(const std::string& dbname,
                                               const std::string& viewname) const {
    return viewsDir(dbname) / (viewname + ".view");
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

std::string StorageEngine::getViewBaseTable(const std::string& dbname,
                                             const std::string& viewname) const {
    auto path = viewPath(dbname, viewname);
    if (!std::filesystem::exists(path)) return "";
    std::ifstream ifs(path);
    if (!ifs) return "";
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.substr(0, 11) == "BASE_TABLE:") {
            return line.substr(11);
        }
    }
    return "";
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
// Materialized views
// ========================================================================

static std::filesystem::path materializedViewPath(const std::string& dbname,
                                                   const std::string& viewname,
                                                   const dbms::StorageEngine* engine) {
    return engine->viewsDir(dbname) / (viewname + ".mview");
}

bool StorageEngine::isMaterializedView(const std::string& dbname,
                                       const std::string& viewname) const {
    return std::filesystem::exists(materializedViewPath(dbname, viewname, this));
}

std::string StorageEngine::getMaterializedViewSQL(const std::string& dbname,
                                                   const std::string& viewname) const {
    auto path = materializedViewPath(dbname, viewname, this);
    if (!std::filesystem::exists(path)) return "";
    std::ifstream ifs(path);
    if (!ifs) return "";
    std::string sql((std::istreambuf_iterator<char>(ifs)),
                     std::istreambuf_iterator<char>());
    return sql;
}

std::vector<std::string> StorageEngine::getMaterializedViewNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto vdir = viewsDir(dbname);
    if (!std::filesystem::exists(vdir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(vdir)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 6 && name.substr(name.size() - 6) == ".mview") {
                result.push_back(name.substr(0, name.size() - 6));
            }
        }
    }
    return result;
}

OpResult StorageEngine::dropMaterializedView(const std::string& dbname,
                                              const std::string& viewname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    // Drop backing table
    std::string backingTable = materializedViewPrefix(viewname);
    if (tableExists(dbname, backingTable)) {
        dropTable(dbname, backingTable);
    }
    // Remove mview metadata
    auto path = materializedViewPath(dbname, viewname, this);
    if (std::filesystem::exists(path)) {
        std::filesystem::remove(path);
    }
    return OpResult::Success;
}

// ========================================================================
// Stored procedures
// ========================================================================

static std::filesystem::path proceduresDir(const std::string& dbname) {
    return std::filesystem::path(dbname) / ".procs";
}

static std::filesystem::path procedurePath(const std::string& dbname,
                                           const std::string& procname) {
    return std::filesystem::path(dbname) / ".procs" / (procname + ".proc");
}

OpResult StorageEngine::createProcedure(const std::string& dbname,
                                         const std::string& procname,
                                         const std::vector<std::string>& statements) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto pdir = proceduresDir(dbname);
    if (!std::filesystem::exists(pdir)) {
        std::filesystem::create_directories(pdir);
    }
    std::ofstream ofs(procedurePath(dbname, procname));
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& stmt : statements) {
        ofs << stmt << '\n';
    }
    return OpResult::Success;
}

OpResult StorageEngine::dropProcedure(const std::string& dbname,
                                       const std::string& procname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = procedurePath(dbname, procname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::procedureExists(const std::string& dbname,
                                    const std::string& procname) const {
    return std::filesystem::exists(procedurePath(dbname, procname));
}

std::vector<std::string> StorageEngine::getProcedureStatements(
    const std::string& dbname, const std::string& procname) const {
    std::vector<std::string> result;
    auto path = procedurePath(dbname, procname);
    std::ifstream ifs(path);
    if (!ifs) return result;
    std::string line;
    while (std::getline(ifs, line)) {
        if (!line.empty()) result.push_back(line);
    }
    return result;
}

std::vector<std::string> StorageEngine::getProcedureNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto pdir = proceduresDir(dbname);
    if (!std::filesystem::exists(pdir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(pdir)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 5 && name.substr(name.size() - 5) == ".proc") {
                result.push_back(name.substr(0, name.size() - 5));
            }
        }
    }
    return result;
}

// ========================================================================
// User-defined functions (simple expression-based)
// ========================================================================

static std::filesystem::path udfDir(const std::string& dbname) {
    return std::filesystem::path(dbname) / ".funcs";
}

static std::filesystem::path udfPath(const std::string& dbname,
                                     const std::string& funcname) {
    return std::filesystem::path(dbname) / ".funcs" / (funcname + ".func");
}

OpResult StorageEngine::createUDF(const std::string& dbname,
                                   const std::string& funcname,
                                   const std::string& param,
                                   const std::string& expression) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto fdir = udfDir(dbname);
    if (!std::filesystem::exists(fdir)) {
        std::filesystem::create_directories(fdir);
    }
    std::ofstream ofs(udfPath(dbname, funcname));
    if (!ofs) return OpResult::InvalidValue;
    ofs << param << "\n" << expression << "\n";
    return OpResult::Success;
}

OpResult StorageEngine::dropUDF(const std::string& dbname,
                                 const std::string& funcname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = udfPath(dbname, funcname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::udfExists(const std::string& dbname,
                              const std::string& funcname) const {
    return std::filesystem::exists(udfPath(dbname, funcname));
}

StorageEngine::UDFInfo StorageEngine::getUDF(const std::string& dbname,
                                              const std::string& funcname) const {
    UDFInfo info;
    auto path = udfPath(dbname, funcname);
    std::ifstream ifs(path);
    if (!ifs) return info;
    std::getline(ifs, info.paramName);
    std::getline(ifs, info.expression);
    info.name = funcname;
    return info;
}

std::vector<std::string> StorageEngine::getUDFNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto fdir = udfDir(dbname);
    if (!std::filesystem::exists(fdir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(fdir)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 5 && name.substr(name.size() - 5) == ".func") {
                result.push_back(name.substr(0, name.size() - 5));
            }
        }
    }
    return result;
}

// ========================================================================
// Table-valued functions
// ========================================================================

static std::filesystem::path tvfDir(const std::string& dbname) {
    return std::filesystem::path(dbname) / ".tvf";
}

static std::filesystem::path tvfPath(const std::string& dbname,
                                     const std::string& funcname) {
    return std::filesystem::path(dbname) / ".tvf" / (funcname + ".tvf");
}

OpResult StorageEngine::createTVF(const std::string& dbname,
                                   const std::string& funcname,
                                   const std::string& param,
                                   const std::string& sql) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto tdir = tvfDir(dbname);
    if (!std::filesystem::exists(tdir)) {
        std::filesystem::create_directories(tdir);
    }
    std::ofstream ofs(tvfPath(dbname, funcname));
    if (!ofs) return OpResult::InvalidValue;
    ofs << param << "\n" << sql << "\n";
    return OpResult::Success;
}

OpResult StorageEngine::dropTVF(const std::string& dbname,
                                 const std::string& funcname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = tvfPath(dbname, funcname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::tvfExists(const std::string& dbname,
                              const std::string& funcname) const {
    return std::filesystem::exists(tvfPath(dbname, funcname));
}

std::string StorageEngine::getTVFSQL(const std::string& dbname,
                                      const std::string& funcname) const {
    auto path = tvfPath(dbname, funcname);
    if (!std::filesystem::exists(path)) return "";
    std::ifstream ifs(path);
    if (!ifs) return "";
    std::string param, sql;
    std::getline(ifs, param);
    std::getline(ifs, sql);
    return sql;
}

std::string StorageEngine::getTVFParam(const std::string& dbname,
                                        const std::string& funcname) const {
    auto path = tvfPath(dbname, funcname);
    if (!std::filesystem::exists(path)) return "";
    std::ifstream ifs(path);
    if (!ifs) return "";
    std::string param;
    std::getline(ifs, param);
    return param;
}

std::vector<std::string> StorageEngine::getTVFNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto tdir = tvfDir(dbname);
    if (!std::filesystem::exists(tdir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(tdir)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 4 && name.substr(name.size() - 4) == ".tvf") {
                result.push_back(name.substr(0, name.size() - 4));
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

// Helper: compute MCV (Most Common Values) from a list of values
static std::vector<std::pair<std::string, size_t>> computeMCV(
    const std::vector<std::string>& vals, size_t topN = 10) {
    std::map<std::string, size_t> freq;
    for (const auto& v : vals) freq[v]++;
    std::vector<std::pair<std::string, size_t>> result(freq.begin(), freq.end());
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    if (result.size() > topN) result.resize(topN);
    return result;
}

static std::string mcvToString(const std::vector<std::pair<std::string, size_t>>& mcv) {
    std::string s;
    for (size_t i = 0; i < mcv.size(); ++i) {
        if (i > 0) s += ";";
        s += std::to_string(mcv[i].second) + ":" + mcv[i].first;
    }
    return s;
}

static std::vector<std::pair<std::string, size_t>> mcvFromString(const std::string& s) {
    std::vector<std::pair<std::string, size_t>> result;
    size_t pos = 0;
    while (pos < s.size()) {
        size_t semi = s.find(';', pos);
        std::string item = (semi == std::string::npos) ? s.substr(pos) : s.substr(pos, semi - pos);
        size_t colon = item.find(':');
        if (colon != std::string::npos) {
            try {
                size_t freq = std::stoull(item.substr(0, colon));
                result.push_back({item.substr(colon + 1), freq});
            } catch (...) {}
        }
        if (semi == std::string::npos) break;
        pos = semi + 1;
    }
    return result;
}

// Parse a stats line into ColumnStats. Format:
// tname cname card|min|max|hist|mcv
static StorageEngine::ColumnStats parseStatsLine(const std::string& line) {
    std::stringstream ss(line);
    std::string tname, cname;
    ss >> tname >> cname;
    // Read remainder after cname; cardinality is at start, followed by |min|max|hist|mcv
    std::string rest;
    std::getline(ss, rest);
    rest = trim(rest);
    // Extract cardinality (number at start of rest)
    size_t card = 0;
    size_t numEnd = 0;
    while (numEnd < rest.size() && std::isdigit(static_cast<unsigned char>(rest[numEnd]))) ++numEnd;
    try { card = std::stoull(rest.substr(0, numEnd)); } catch (...) {}
    // The pipe-separated fields start at numEnd
    std::string pipePart = (numEnd < rest.size()) ? rest.substr(numEnd) : "";
    // Split pipePart by '|', skipping leading empty segments
    std::vector<std::string> parts;
    size_t p = 0;
    while (p < pipePart.size()) {
        size_t pipe = pipePart.find('|', p);
        std::string part = (pipe == std::string::npos) ? pipePart.substr(p) : pipePart.substr(p, pipe - p);
        parts.push_back(part);
        if (pipe == std::string::npos) break;
        p = pipe + 1;
    }
    // Skip leading empty parts (from the | immediately after cardinality)
    size_t startIdx = 0;
    while (startIdx < parts.size() && parts[startIdx].empty()) ++startIdx;
    std::vector<std::string> realParts;
    for (size_t i = startIdx; i < parts.size(); ++i) realParts.push_back(parts[i]);

    StorageEngine::ColumnStats cs;
    cs.cardinality = card;
    if (realParts.size() > 0) cs.minVal = realParts[0];
    if (realParts.size() > 1) cs.maxVal = realParts[1];
    // Histogram
    if (realParts.size() > 2 && !realParts[2].empty()) {
        const std::string& histStr = realParts[2];
        size_t pos = 0;
        while (pos < histStr.size()) {
            size_t semi = histStr.find(';', pos);
            std::string bucket = (semi == std::string::npos) ? histStr.substr(pos) : histStr.substr(pos, semi - pos);
            size_t comma = bucket.find(',');
            if (comma != std::string::npos) {
                cs.histogram.push_back({bucket.substr(0, comma), bucket.substr(comma + 1)});
            }
            if (semi == std::string::npos) break;
            pos = semi + 1;
        }
    }
    // MCV
    if (realParts.size() > 3 && !realParts[3].empty()) {
        cs.mcv = mcvFromString(realParts[3]);
    }
    return cs;
}

// Write a single ColumnStats entry
static void writeStatsEntry(std::ofstream& ofs, const std::string& tname,
                            const std::string& cname, const StorageEngine::ColumnStats& cs) {
    ofs << tname << " " << cname << " " << cs.cardinality
        << "|" << cs.minVal << "|" << cs.maxVal << "|";
    for (size_t i = 0; i < cs.histogram.size(); ++i) {
        if (i > 0) ofs << ";";
        ofs << cs.histogram[i].first << "," << cs.histogram[i].second;
    }
    ofs << "|" << mcvToString(cs.mcv) << "\n";
}

void StorageEngine::analyzeTable(const std::string& dbname,
                                  const std::string& tablename) {
    if (!tableExists(dbname, tablename)) return;
    TableSchema tbl = getTableSchema(dbname, tablename);

    TableStats stats;
    stats.rowCount = 0;
    std::map<std::string, std::set<std::string>> distinctVals;
    std::map<std::string, std::vector<std::string>> allVals;
    std::map<std::string, std::string> minVals, maxVals;

    forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
        stats.rowCount++;
        std::string row(data, len);
        for (size_t i = 0; i < tbl.len; ++i) {
            std::string val = extractColumnValue(row, tbl, i);
            distinctVals[tbl.cols[i].dataName].insert(val);
            allVals[tbl.cols[i].dataName].push_back(val);
            if (minVals.find(tbl.cols[i].dataName) == minVals.end() || val < minVals[tbl.cols[i].dataName]) {
                minVals[tbl.cols[i].dataName] = val;
            }
            if (maxVals.find(tbl.cols[i].dataName) == maxVals.end() || val > maxVals[tbl.cols[i].dataName]) {
                maxVals[tbl.cols[i].dataName] = val;
            }
        }
    });

    const size_t HIST_BUCKETS = 10;
    const size_t MCV_TOP_N = 10;
    for (size_t i = 0; i < tbl.len; ++i) {
        const std::string& cname = tbl.cols[i].dataName;
        StorageEngine::ColumnStats cs;
        cs.cardinality = distinctVals[cname].size();
        cs.minVal = minVals[cname];
        cs.maxVal = maxVals[cname];
        auto& vals = allVals[cname];
        // Build equi-depth histogram
        if (vals.size() >= HIST_BUCKETS * 2) {
            bool isNumeric = (!tbl.cols[i].isVariableLength &&
                              (tbl.cols[i].dataType == "int" || tbl.cols[i].dataType == "long" ||
                               tbl.cols[i].dataType == "tinyint" || tbl.cols[i].dataType == "float" ||
                               tbl.cols[i].dataType == "double" || tbl.cols[i].dataType == "decimal"));
            if (isNumeric) {
                std::vector<long long> nums;
                nums.reserve(vals.size());
                for (const auto& v : vals) {
                    try { nums.push_back(std::stoll(v)); } catch (...) { nums.push_back(0); }
                }
                std::sort(nums.begin(), nums.end());
                size_t perBucket = nums.size() / HIST_BUCKETS;
                for (size_t b = 0; b < HIST_BUCKETS; ++b) {
                    size_t start = b * perBucket;
                    size_t end = (b == HIST_BUCKETS - 1) ? nums.size() - 1 : (b + 1) * perBucket;
                    cs.histogram.push_back({std::to_string(nums[start]), std::to_string(nums[end])});
                }
            } else {
                std::sort(vals.begin(), vals.end());
                size_t perBucket = vals.size() / HIST_BUCKETS;
                for (size_t b = 0; b < HIST_BUCKETS; ++b) {
                    size_t start = b * perBucket;
                    size_t end = (b == HIST_BUCKETS - 1) ? vals.size() - 1 : (b + 1) * perBucket;
                    cs.histogram.push_back({vals[start], vals[end]});
                }
            }
        }
        // Compute MCV
        cs.mcv = computeMCV(vals, MCV_TOP_N);
        stats.colStats[cname] = cs;
    }

    // Load existing stats, update this table's entry (preserve multi-col stats)
    std::map<std::string, TableStats> allStats;
    auto spath = statsPath(dbname);
    if (std::filesystem::exists(spath)) {
        std::ifstream ifs(spath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string tname, cname;
            ss >> tname >> cname;
            if (cname == "__rows__") {
                std::string rc;
                std::getline(ss, rc);
                try { allStats[tname].rowCount = std::stoull(rc); } catch (...) {}
                continue;
            }
            if (cname == "__multi__") {
                std::string colKey;
                ss >> colKey;
                std::string rest;
                std::getline(ss, rest);
                // Reconstruct line for parseStatsLine
                allStats[tname].multiColStats[colKey] = parseStatsLine(tname + " " + colKey + " " + rest);
                continue;
            }
            allStats[tname].colStats[cname] = parseStatsLine(line);
        }
    }
    // Preserve multi-col stats from existing entry
    auto it = allStats.find(tablename);
    if (it != allStats.end()) {
        stats.multiColStats = it->second.multiColStats;
    }
    allStats[tablename] = stats;

    // Write back
    std::ofstream ofs(spath);
    for (const auto& kv : allStats) {
        ofs << kv.first << " __rows__ " << kv.second.rowCount << "||\n";
        for (const auto& cv : kv.second.colStats) {
            writeStatsEntry(ofs, kv.first, cv.first, cv.second);
        }
        for (const auto& mv : kv.second.multiColStats) {
            ofs << kv.first << " __multi__ " << mv.first << " " << mv.second.cardinality
                << "|" << mv.second.minVal << "|" << mv.second.maxVal << "|";
            for (size_t i = 0; i < mv.second.histogram.size(); ++i) {
                if (i > 0) ofs << ";";
                ofs << mv.second.histogram[i].first << "," << mv.second.histogram[i].second;
            }
            ofs << "|" << mcvToString(mv.second.mcv) << "\n";
        }
    }
}

void StorageEngine::analyzeMultiColumn(const std::string& dbname, const std::string& tablename,
                                        const std::vector<std::string>& colnames) {
    if (!tableExists(dbname, tablename)) return;
    if (colnames.size() < 2) return;
    TableSchema tbl = getTableSchema(dbname, tablename);

    // Build column index map
    std::map<std::string, size_t> colIdx;
    for (size_t i = 0; i < tbl.len; ++i) {
        colIdx[tbl.cols[i].dataName] = i;
    }
    for (const auto& c : colnames) {
        if (colIdx.find(c) == colIdx.end()) return;
    }

    std::string colKey;
    for (size_t i = 0; i < colnames.size(); ++i) {
        if (i > 0) colKey += ",";
        colKey += colnames[i];
    }

    std::map<std::string, size_t> distinctVals;
    std::map<std::string, std::string> minVals, maxVals;
    size_t rowCount = 0;

    forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
        rowCount++;
        std::string row(data, len);
        std::string combined;
        for (size_t i = 0; i < colnames.size(); ++i) {
            if (i > 0) combined += "#";
            combined += extractColumnValue(row, tbl, colIdx[colnames[i]]);
        }
        distinctVals[combined]++;
        if (minVals.empty() || combined < minVals.begin()->first) minVals[combined] = combined;
        if (maxVals.empty() || combined > maxVals.rbegin()->first) maxVals[combined] = combined;
    });

    StorageEngine::ColumnStats cs;
    cs.cardinality = distinctVals.size();
    if (!minVals.empty()) cs.minVal = minVals.begin()->first;
    if (!maxVals.empty()) cs.maxVal = maxVals.rbegin()->first;

    // MCV for combined values
    std::vector<std::pair<std::string, size_t>> freqVec(distinctVals.begin(), distinctVals.end());
    std::sort(freqVec.begin(), freqVec.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    if (freqVec.size() > 10) freqVec.resize(10);
    cs.mcv = freqVec;

    // Load existing stats
    std::map<std::string, TableStats> allStats;
    auto spath = statsPath(dbname);
    if (std::filesystem::exists(spath)) {
        std::ifstream ifs(spath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string tname, cname;
            ss >> tname >> cname;
            if (cname == "__rows__") {
                std::string rc; std::getline(ss, rc);
                try { allStats[tname].rowCount = std::stoull(rc); } catch (...) {}
                continue;
            }
            if (cname == "__multi__") {
                std::string colKey; ss >> colKey;
                std::string rest; std::getline(ss, rest);
                allStats[tname].multiColStats[colKey] = parseStatsLine(tname + " " + colKey + " " + rest);
                continue;
            }
            allStats[tname].colStats[cname] = parseStatsLine(line);
        }
    }
    allStats[tablename].multiColStats[colKey] = cs;

    // Write back
    std::ofstream ofs(spath);
    for (const auto& kv : allStats) {
        ofs << kv.first << " __rows__ " << kv.second.rowCount << "||\n";
        for (const auto& cv : kv.second.colStats) {
            writeStatsEntry(ofs, kv.first, cv.first, cv.second);
        }
        for (const auto& mv : kv.second.multiColStats) {
            ofs << kv.first << " __multi__ " << mv.first << " " << mv.second.cardinality
                << "|" << mv.second.minVal << "|" << mv.second.maxVal << "|";
            for (size_t i = 0; i < mv.second.histogram.size(); ++i) {
                if (i > 0) ofs << ";";
                ofs << mv.second.histogram[i].first << "," << mv.second.histogram[i].second;
            }
            ofs << "|" << mcvToString(mv.second.mcv) << "\n";
        }
    }
}

StorageEngine::BufferPoolStats StorageEngine::getBufferPoolStats() const {
    BufferPoolStats stats;
    // Page allocators (table data files)
    for (const auto& kv : pageAllocators_) {
        if (kv.second && kv.second->bufferPool()) {
            auto* bp = kv.second->bufferPool();
            stats.totalHits += bp->hits();
            stats.totalMisses += bp->misses();
        }
    }
    // Primary key indexes
    for (const auto& kv : pkIndexCache_) {
        if (kv.second) {
            stats.totalHits += kv.second->cacheHits();
            stats.totalMisses += kv.second->cacheMisses();
        }
    }
    // Secondary indexes
    for (const auto& kv : secondaryIndexCache_) {
        if (kv.second) {
            stats.totalHits += kv.second->cacheHits();
            stats.totalMisses += kv.second->cacheMisses();
        }
    }
    size_t total = stats.totalHits + stats.totalMisses;
    stats.hitRate = total == 0 ? 0.0 : 100.0 * static_cast<double>(stats.totalHits) / static_cast<double>(total);
    return stats;
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
        ss >> tname >> cname;
        if (tname == tablename && cname == "__rows__") {
            std::string rest;
            std::getline(ss, rest);
            try { return std::stoull(rest); } catch (...) { return 0; }
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
        ss >> tname >> cname;
        if (tname == tablename && cname == colname) {
            std::string rest;
            std::getline(ss, rest);
            return parseStatsLine(tname + " " + cname + " " + rest);
        }
    }
    return {};
}

StorageEngine::ColumnStats StorageEngine::getMultiColumnStats(
    const std::string& dbname, const std::string& tablename,
    const std::string& colKey) const {
    auto spath = statsPath(dbname);
    if (!std::filesystem::exists(spath)) return {};
    std::ifstream ifs(spath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string tname, tag, key;
        ss >> tname >> tag >> key;
        if (tname == tablename && tag == "__multi__" && key == colKey) {
            std::string rest;
            std::getline(ss, rest);
            return parseStatsLine(tname + " " + key + " " + rest);
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
                                const ReadView* readView,
                                const std::vector<std::string>& targetPartitions) const {
    const ReadView* rv = readView;
    if (!rv && inTransaction_) rv = &readView_;

    TableSchema tbl = getTableSchema(dbname, tablename);
    if (tbl.partitionType != TableSchema::PartitionType::None) {
        // Partitioned table: iterate over target partition files (or all if empty)
        std::vector<std::string> partNames = targetPartitions;
        if (partNames.empty()) {
            if (tbl.partitionType == TableSchema::PartitionType::Range) {
                for (const auto& rp : tbl.rangePartitions) partNames.push_back(rp.first);
            } else if (tbl.partitionType == TableSchema::PartitionType::List) {
                for (const auto& lp : tbl.listPartitions) partNames.push_back(lp.first);
            } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
                for (size_t i = 0; i < tbl.hashPartitions; ++i) partNames.push_back("p" + std::to_string(i));
            }
        }
        for (const auto& pname : partNames) {
            auto ppa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tablename, pname).string(), tbl.rowSize());
            if (!ppa->open()) continue;
            uint32_t np = ppa->numPages();
            for (uint32_t pid = 1; pid < np; ++pid) {
                char* buf = ppa->fetchPage(pid);
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
                ppa->unpinPage(pid);
            }
            ppa->close();
        }
        return;
    }

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
    return tbl.buildPKValue(rowBuffer);
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

// ========================================================================
// Hash Index
// ========================================================================
std::filesystem::path StorageEngine::hashIndexPath(const std::string& dbname,
                                                    const std::string& tablename,
                                                    const std::string& colname) const {
    return dbPath(dbname) / (tablename + "_" + colname + ".hidx");
}

std::filesystem::path StorageEngine::hashIndexMetaPath(const std::string& dbname,
                                                        const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".hashidx");
}

std::vector<std::string> StorageEngine::getHashIndexedColumns(const std::string& dbname,
                                                               const std::string& tablename) const {
    std::vector<std::string> cols;
    std::filesystem::path meta = hashIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return cols;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) cols.push_back(line);
    }
    return cols;
}

HashIndex* StorageEngine::getHashIndex(const std::string& dbname,
                                        const std::string& tablename,
                                        const std::string& colname) const {
    std::string key = dbname + "." + tablename + "." + colname;
    auto it = hashIndexCache_.find(key);
    if (it != hashIndexCache_.end()) return it->second.get();
    auto idx = std::make_unique<HashIndex>(hashIndexPath(dbname, tablename, colname));
    if (idx->open()) {
        HashIndex* ptr = idx.get();
        hashIndexCache_[key] = std::move(idx);
        return ptr;
    }
    return nullptr;
}

OpResult StorageEngine::createHashIndex(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& colname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    bool found = false;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colname) { found = true; break; }
    }
    if (!found) return OpResult::InvalidValue;

    std::filesystem::path meta = hashIndexMetaPath(dbname, tablename);
    std::set<std::string> existing;
    {
        std::ifstream in(meta);
        std::string line;
        while (std::getline(in, line)) if (!line.empty()) existing.insert(line);
    }
    if (existing.count(colname)) return OpResult::Success; // already exists
    existing.insert(colname);
    {
        std::ofstream out(meta, std::ios::trunc);
        for (const auto& c : existing) out << c << '\n';
    }

    // Build hash index from existing data
    HashIndex* hidx = getHashIndex(dbname, tablename, colname);
    if (!hidx) return OpResult::Success;
    hidx->clear();
    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == colname) {
                std::string val = extractColumnValue(row, tbl, i);
                if (!val.empty()) {
                    hidx->insert(val, encodeRid(pageId, slotId));
                }
                break;
            }
        }
    });
    return OpResult::Success;
}

OpResult StorageEngine::dropHashIndex(const std::string& dbname,
                                       const std::string& tablename,
                                       const std::string& colname) {
    std::filesystem::path meta = hashIndexMetaPath(dbname, tablename);
    std::set<std::string> existing;
    {
        std::ifstream in(meta);
        std::string line;
        while (std::getline(in, line)) if (!line.empty()) existing.insert(line);
    }
    existing.erase(colname);
    {
        std::ofstream out(meta, std::ios::trunc);
        for (const auto& c : existing) out << c << '\n';
    }
    std::string key = dbname + "." + tablename + "." + colname;
    hashIndexCache_.erase(key);
    std::filesystem::remove(hashIndexPath(dbname, tablename, colname));
    return OpResult::Success;
}

std::vector<std::string> StorageEngine::getIndexedColumns(const std::string& dbname,
                                                           const std::string& tablename) const {
    std::vector<std::string> cols;
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return cols;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (line.size() > 2 && line[0] == 'C' && line[1] == ':') {
            // Composite index: C:name:col1:col2:...[:INCLUDE:...]
            size_t pos = 2;
            size_t next = line.find(':', pos);
            if (next != std::string::npos) pos = next + 1;
            while (pos < line.size()) {
                next = line.find(':', pos);
                std::string cname = (next == std::string::npos) ? line.substr(pos) : line.substr(pos, next - pos);
                if (!cname.empty()) {
                    if (cname == "INCLUDE") break; // stop at INCLUDE marker
                    cols.push_back(cname);
                }
                if (next == std::string::npos) break;
                pos = next + 1;
            }
        } else if (line.substr(0, 5) == "EXPR:") {
            // Expression index: extract expression name
            size_t incPos = line.find(":INCLUDE:");
            size_t wherePos = line.find(":WHERE:");
            size_t endPos = std::min(
                incPos != std::string::npos ? incPos : line.size(),
                wherePos != std::string::npos ? wherePos : line.size());
            cols.push_back(line.substr(5, endPos - 5));
        } else {
            // Strip :DESC, :INCLUDE, and :WHERE suffixes
            size_t wherePos = line.find(":WHERE:");
            std::string base = (wherePos != std::string::npos) ? line.substr(0, wherePos) : line;
            size_t includePos = base.find(":INCLUDE");
            base = (includePos != std::string::npos) ? base.substr(0, includePos) : base;
            size_t descPos = base.find(":DESC");
            if (descPos != std::string::npos) {
                cols.push_back(base.substr(0, descPos));
            } else {
                cols.push_back(base);
            }
        }
    }
    return cols;
}

bool StorageEngine::isDescendingIndex(const std::string& dbname, const std::string& tablename,
                                      const std::string& colname) const {
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return false;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (line.substr(0, 5) == "EXPR:") continue;
        size_t wherePos = line.find(":WHERE:");
        std::string base = (wherePos != std::string::npos) ? line.substr(0, wherePos) : line;
        size_t incPos = base.find(":INCLUDE");
        base = (incPos != std::string::npos) ? base.substr(0, incPos) : base;
        if (base == colname + ":DESC") return true;
    }
    return false;
}

std::vector<std::string> StorageEngine::getIndexIncludeColumns(const std::string& dbname,
                                                               const std::string& tablename,
                                                               const std::string& colname) const {
    std::vector<std::string> result;
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        size_t incPos = line.find(":INCLUDE:");
        if (incPos == std::string::npos) continue;
        // For single-column index: colname[:DESC][:WHERE:...]:INCLUDE:...
        // For expression index: EXPR:expr[:WHERE:...]:INCLUDE:...
        std::string base = line.substr(0, incPos);
        std::string idxCol;
        if (base.substr(0, 5) == "EXPR:") {
            idxCol = base.substr(5);
            size_t wherePos = idxCol.find(":WHERE:");
            if (wherePos != std::string::npos) idxCol = idxCol.substr(0, wherePos);
        } else {
            size_t descPos = base.find(":DESC");
            idxCol = (descPos != std::string::npos) ? base.substr(0, descPos) : base;
        }
        if (idxCol == colname) {
            std::string incStr = line.substr(incPos + 9); // after ":INCLUDE:"
            size_t p = 0;
            while (p < incStr.size()) {
                size_t comma = incStr.find(',', p);
                std::string c = trim((comma == std::string::npos) ? incStr.substr(p) : incStr.substr(p, comma - p));
                if (!c.empty()) result.push_back(c);
                if (comma == std::string::npos) break;
                p = comma + 1;
            }
            return result;
        }
    }
    return result;
}

std::vector<StorageEngine::IndexMetadata> StorageEngine::getIndexMetadata(
    const std::string& dbname, const std::string& tablename) const {
    std::vector<IndexMetadata> result;
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        IndexMetadata info;
        // Composite index lines start with "C:"
        if (line.size() > 2 && line[0] == 'C' && line[1] == ':') continue;

        // Check for EXPR prefix
        if (line.substr(0, 5) == "EXPR:") {
            info.isExpression = true;
            size_t pos = 5;
            // Extract expression, stopping at :INCLUDE or :WHERE
            size_t incPos = line.find(":INCLUDE:", pos);
            size_t wherePos = line.find(":WHERE:", pos);
            size_t endPos = std::min(
                incPos != std::string::npos ? incPos : line.size(),
                wherePos != std::string::npos ? wherePos : line.size());
            info.name = line.substr(pos, endPos - pos);
            // Extract function from expression like "UPPER(col)"
            size_t lp = info.name.find('(');
            if (lp != std::string::npos) {
                info.exprFunc = info.name.substr(0, lp);
            }
            pos = endPos;
            if (incPos != std::string::npos && pos == incPos) {
                size_t wPos = line.find(":WHERE:", incPos + 9);
                std::string incStr = (wPos != std::string::npos)
                    ? line.substr(incPos + 9, wPos - incPos - 9)
                    : line.substr(incPos + 9);
                std::stringstream ss(incStr);
                std::string c;
                while (getline(ss, c, ',')) {
                    c = trim(c);
                    if (!c.empty()) info.includeCols.push_back(c);
                }
                if (wPos != std::string::npos) pos = wPos;
            }
            if (wherePos != std::string::npos && pos == wherePos) {
                info.whereCondition = line.substr(wherePos + 7);
            }
            result.push_back(std::move(info));
            continue;
        }

        // Regular single-column index: colname[:DESC][:INCLUDE:...][:WHERE:...]
        size_t wherePos = line.find(":WHERE:");
        size_t incPos = line.find(":INCLUDE:");
        size_t descPos = line.find(":DESC");

        std::string base;
        if (wherePos != std::string::npos) {
            base = line.substr(0, wherePos);
            info.whereCondition = line.substr(wherePos + 7);
        } else {
            base = line;
        }

        std::string colPart = base;
        if (incPos != std::string::npos && incPos < base.size()) {
            std::string incStr = base.substr(incPos + 9);
            colPart = base.substr(0, incPos);
            std::stringstream ss(incStr);
            std::string c;
            while (getline(ss, c, ',')) {
                c = trim(c);
                if (!c.empty()) info.includeCols.push_back(c);
            }
        }

        if (descPos != std::string::npos && descPos < colPart.size()) {
            info.name = colPart.substr(0, descPos);
            info.descending = true;
        } else {
            info.name = colPart;
        }
        result.push_back(std::move(info));
    }
    return result;
}

std::vector<StorageEngine::CompositeIndexInfo> StorageEngine::getCompositeIndexes(
    const std::string& dbname, const std::string& tablename) const {
    std::vector<CompositeIndexInfo> result;
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ifstream in(meta);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        if (line.size() > 2 && line[0] == 'C' && line[1] == ':') {
            size_t pos = 2;
            size_t next = line.find(':', pos);
            if (next == std::string::npos) continue;
            CompositeIndexInfo info;
            info.name = line.substr(pos, next - pos);
            pos = next + 1;
            while (pos < line.size()) {
                next = line.find(':', pos);
                std::string cname = (next == std::string::npos) ? line.substr(pos) : line.substr(pos, next - pos);
                if (!cname.empty()) {
                    if (cname == "INCLUDE") {
                        // Skip INCLUDE columns for composite index info
                        if (next != std::string::npos) pos = next + 1;
                        while (pos < line.size()) {
                            next = line.find(':', pos);
                            std::string inc = (next == std::string::npos) ? line.substr(pos) : line.substr(pos, next - pos);
                            if (inc.empty()) break;
                            if (next != std::string::npos) pos = next + 1;
                            else { pos = line.size(); break; }
                        }
                        continue;
                    }
                    if (cname == "WHERE") {
                        // Remainder is WHERE condition
                        if (next != std::string::npos) {
                            info.whereCondition = line.substr(next + 1);
                        }
                        break;
                    }
                    info.columns.push_back(cname);
                }
                if (next == std::string::npos) break;
                pos = next + 1;
            }
            if (!info.columns.empty()) result.push_back(std::move(info));
        }
    }
    return result;
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

BPTree* StorageEngine::getCompositeIndexTree(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::string& indexName) const {
    std::string key = dbname + "/" + tablename + "/C/" + indexName;
    auto it = secondaryIndexCache_.find(key);
    if (it != secondaryIndexCache_.end()) return it->second.get();

    std::filesystem::path p = dbPath(dbname) / (tablename + ".idx_" + indexName);
    auto tree = std::make_unique<BPTree>(p);
    if (tree->open()) {
        BPTree* ptr = tree.get();
        secondaryIndexCache_[key] = std::move(tree);
        return ptr;
    }
    return nullptr;
}

std::string StorageEngine::extractColumnValue(const std::string& rowBuffer,
                                               const TableSchema& tbl, size_t colIdx,
                                               const std::string& dbname) {
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
        std::string val = rowBuffer.substr(dataOffset, dataLen);
        // TOAST: if value is a toast marker, read from external storage
        uint64_t toastId = 0;
        if (!dbname.empty() && parseToastMarker(val, toastId)) {
            return readToast(dbname, tbl.tablename, toastId);
        }
        return val;
    }

    // Fixed-length column
    size_t offset = 0;
    for (size_t i = 0; i < colIdx; ++i) {
        if (!tbl.cols[i].isVariableLength) offset += tbl.cols[i].dsize;
    }
    if (offset + col.dsize > rowBuffer.size()) return "";
    if (col.dataType == "char" || col.dataType == "uuid") {
        std::string val(col.dsize, '\0');
        std::memcpy(val.data(), rowBuffer.data() + offset, col.dsize);
        auto nul = val.find('\0');
        if (nul != std::string::npos) val.resize(nul);
        return val;
    } else if (col.dataType == "date") {
        Date d;
        std::memcpy(&d, rowBuffer.data() + offset, DATE_SIZE);
        return (d.year == 0) ? "" : str(d);
    } else if (col.dataType == "timestamp" || col.dataType == "timestamptz" || col.dataType == "datetime") {
        int64_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, TIMESTAMP_SIZE);
        return (val == INF || val == 0) ? "" : formatTimestampSeconds(val);
    } else if (col.dataType == "time") {
        int32_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, sizeof(int32_t));
        return (val < 0) ? "" : formatTimeSeconds(val);
    } else if (col.dataType == "float") {
        float val = 0.0f;
        std::memcpy(&val, rowBuffer.data() + offset, sizeof(float));
        if (val == 0.0f) return "";
        std::ostringstream oss;
        oss << val;
        return oss.str();
    } else if (col.dataType == "double" || col.dataType == "decimal") {
        double val = 0.0;
        std::memcpy(&val, rowBuffer.data() + offset, sizeof(double));
        if (val == 0.0) return "";
        std::ostringstream oss;
        oss << val;
        return oss.str();
    } else if (col.dataType == "boolean") {
        int8_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, sizeof(int8_t));
        if (val == INT8_MIN) return "";
        return val ? "true" : "false";
    } else {
        int64_t val = 0;
        std::memcpy(&val, rowBuffer.data() + offset, col.dsize);
        return (val == INF) ? "" : transstr(val);
    }
}

std::string StorageEngine::buildCompositeKey(const std::string& rowBuffer,
                                               const TableSchema& tbl,
                                               const std::vector<std::string>& colNames) {
    std::string key;
    for (const auto& cname : colNames) {
        size_t colIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == cname) { colIdx = i; break; }
        }
        if (colIdx < tbl.len) {
            if (!key.empty()) key += '\x01';
            key += extractColumnValue(rowBuffer, tbl, colIdx);
        }
    }
    return key;
}

// ========================================================================
// Trigger support
// ========================================================================
std::filesystem::path StorageEngine::triggerPath(const std::string& dbname) const {
    return dbPath(dbname) / ".triggers";
}

void StorageEngine::writeTrigger(std::ostream& out, const Trigger& trg) const {
    size_t n = trg.name.size();
    out.write(reinterpret_cast<const char*>(&n), sizeof(size_t));
    out.write(trg.name.data(), static_cast<std::streamsize>(n));
    n = trg.timing.size();
    out.write(reinterpret_cast<const char*>(&n), sizeof(size_t));
    out.write(trg.timing.data(), static_cast<std::streamsize>(n));
    n = trg.event.size();
    out.write(reinterpret_cast<const char*>(&n), sizeof(size_t));
    out.write(trg.event.data(), static_cast<std::streamsize>(n));
    n = trg.tableName.size();
    out.write(reinterpret_cast<const char*>(&n), sizeof(size_t));
    out.write(trg.tableName.data(), static_cast<std::streamsize>(n));
    n = trg.action.size();
    out.write(reinterpret_cast<const char*>(&n), sizeof(size_t));
    out.write(trg.action.data(), static_cast<std::streamsize>(n));
    char forEachRow = trg.forEachRow ? 1 : 0;
    out.write(&forEachRow, sizeof(char));
}

StorageEngine::Trigger StorageEngine::readTrigger(std::istream& in) const {
    Trigger trg;
    auto readStr = [&](std::string& s) {
        size_t n = 0;
        in.read(reinterpret_cast<char*>(&n), sizeof(size_t));
        if (!in || n > 100000) return;
        s.resize(n);
        in.read(s.data(), static_cast<std::streamsize>(n));
    };
    readStr(trg.name);
    readStr(trg.timing);
    readStr(trg.event);
    readStr(trg.tableName);
    readStr(trg.action);
    // Backward compatibility: forEachRow flag may not exist in old files
    char forEachRow = 1;
    in.read(&forEachRow, sizeof(char));
    if (in) trg.forEachRow = (forEachRow != 0);
    return trg;
}

OpResult StorageEngine::createTrigger(const std::string& dbname, const Trigger& trg) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto existing = getAllTriggers(dbname);
    for (const auto& t : existing) {
        if (t.name == trg.name) return OpResult::Success; // already exists
    }
    existing.push_back(trg);
    std::ofstream out(triggerPath(dbname), std::ios::binary | std::ios::trunc);
    size_t count = existing.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
    for (const auto& t : existing) writeTrigger(out, t);
    return OpResult::Success;
}

OpResult StorageEngine::dropTrigger(const std::string& dbname, const std::string& trgName) {
    auto existing = getAllTriggers(dbname);
    bool found = false;
    {
        std::ofstream out(triggerPath(dbname), std::ios::binary | std::ios::trunc);
        size_t count = 0;
        for (const auto& t : existing) {
            if (t.name != trgName) ++count;
        }
        out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
        for (const auto& t : existing) {
            if (t.name != trgName) writeTrigger(out, t);
            else found = true;
        }
    }
    return found ? OpResult::Success : OpResult::TableNotExist;
}

std::vector<StorageEngine::Trigger> StorageEngine::getTriggers(
    const std::string& dbname, const std::string& tablename,
    const std::string& timing, const std::string& event) const {
    std::vector<Trigger> result;
    auto all = getAllTriggers(dbname);
    for (const auto& t : all) {
        if (t.tableName == tablename && t.timing == timing && t.event == event) {
            result.push_back(t);
        }
    }
    return result;
}

std::vector<StorageEngine::Trigger> StorageEngine::getAllTriggers(const std::string& dbname) const {
    std::vector<Trigger> result;
    std::ifstream in(triggerPath(dbname), std::ios::binary);
    if (!in) return result;
    size_t count = 0;
    in.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    if (!in || count > 10000) return result;
    for (size_t i = 0; i < count && in; ++i) {
        Trigger t = readTrigger(in);
        if (!t.name.empty()) result.push_back(std::move(t));
    }
    return result;
}

// TableSchema PK helpers (defined here because they use StorageEngine::extractColumnValue)
bool TableSchema::hasPrimaryKey() const {
    if (!pkColIndices.empty()) return true;
    for (size_t i = 0; i < len; ++i) if (cols[i].isPrimaryKey) return true;
    return false;
}

std::string TableSchema::buildPKValue(const std::string& rowBuffer) const {
    std::string key;
    if (!pkColIndices.empty()) {
        for (size_t idx : pkColIndices) {
            if (idx < len) key += StorageEngine::extractColumnValue(rowBuffer, *this, idx) + "\x01";
        }
    } else {
        // Collect all columns marked as primary key (composite PK support)
        bool first = true;
        for (size_t i = 0; i < len; ++i) {
            if (cols[i].isPrimaryKey) {
                if (!first) key += "\x01";
                key += StorageEngine::extractColumnValue(rowBuffer, *this, i);
                first = false;
            }
        }
    }
    return key;
}

std::string TableSchema::buildPKValue(const std::map<std::string, std::string>& values) const {
    std::string key;
    if (!pkColIndices.empty()) {
        for (size_t idx : pkColIndices) {
            if (idx < len) {
                auto it = values.find(cols[idx].dataName);
                key += (it != values.end() ? it->second : "") + "\x01";
            }
        }
    } else {
        bool first = true;
        for (size_t i = 0; i < len; ++i) {
            if (cols[i].isPrimaryKey) {
                if (!first) key += "\x01";
                auto it = values.find(cols[i].dataName);
                key += (it != values.end() ? it->second : "");
                first = false;
            }
        }
    }
    return key;
}

// Helper: evaluate simple expression (UPPER/LOWER) on a column value
static std::string evalExpr(const std::string& val, const std::string& exprFunc) {
    if (exprFunc == "UPPER") {
        std::string result = val;
        for (char& c : result) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        return result;
    }
    if (exprFunc == "LOWER") {
        std::string result = val;
        for (char& c : result) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return result;
    }
    return val;
}

OpResult StorageEngine::createIndex(const std::string& dbname, const std::string& tablename,
                                     const std::string& colname, bool ascending,
                                     const std::vector<std::string>& includeCols,
                                     const std::string& whereCondition,
                                     const std::string& expression) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);
    TableSchema tbl = getTableSchema(dbname, tablename);

    bool isExpression = !expression.empty();
    std::string actualColname = colname;
    std::string exprFunc;
    size_t colIdx = tbl.len;

    if (isExpression) {
        // Parse expression like "UPPER(name)"
        size_t lp = expression.find('(');
        size_t rp = expression.find(')');
        if (lp == std::string::npos || rp == std::string::npos || rp <= lp + 1) {
            return OpResult::InvalidValue;
        }
        exprFunc = expression.substr(0, lp);
        actualColname = expression.substr(lp + 1, rp - lp - 1);
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == actualColname) { colIdx = i; break; }
        }
    } else {
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
        }
    }
    if (colIdx >= tbl.len) return OpResult::InvalidValue;

    // Parse WHERE condition for partial index
    std::vector<Condition> whereConds;
    if (!whereCondition.empty()) {
        whereConds = parseConditions(std::vector<std::string>{whereCondition});
    }

    // Already indexed? If direction differs, drop and recreate
    auto existing = getIndexedColumns(dbname, tablename);
    bool alreadyIndexed = false;
    for (const auto& c : existing) {
        if (c == actualColname) { alreadyIndexed = true; break; }
    }
    if (alreadyIndexed && !isExpression) {
        bool currentDesc = isDescendingIndex(dbname, tablename, actualColname);
        if (currentDesc == !ascending) {
            return OpResult::Success; // same direction, nothing to do
        }
        dropIndex(dbname, tablename, actualColname);
    }

    // Build index from existing data using page-based iteration
    BPTree* idx = getSecondaryIndex(dbname, tablename,
        isExpression ? expression : actualColname);
    if (!idx) return OpResult::InvalidValue;

    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        // Partial index: skip rows not matching WHERE condition
        bool match = true;
        for (const auto& wc : whereConds) {
            if (!evalConditionOnRow(wc, row, tbl)) { match = false; break; }
        }
        if (!match) return;
        std::string val = extractColumnValue(row, tbl, colIdx);
        if (!val.empty()) {
            if (isExpression) val = evalExpr(val, exprFunc);
            idx->insertMulti(val, encodeRid(pageId, slotId));
        }
    });

    // Record in metadata
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ofstream out(meta, std::ios::out | std::ios::app);
    if (isExpression) {
        out << "EXPR:" << expression;
    } else {
        out << actualColname << (ascending ? "" : ":DESC");
    }
    if (!includeCols.empty()) {
        out << ":INCLUDE:";
        for (size_t i = 0; i < includeCols.size(); ++i) {
            if (i > 0) out << ",";
            out << includeCols[i];
        }
    }
    if (!whereCondition.empty()) {
        out << ":WHERE:" << whereCondition;
    }
    out << '\n';
    return OpResult::Success;
}

OpResult StorageEngine::dropIndex(const std::string& dbname, const std::string& tablename,
                                   const std::string& colname) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);
    std::filesystem::remove(secondaryIndexPath(dbname, tablename, colname));

    // Update metadata: remove lines matching this column/expression
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::vector<std::string> lines;
    {
        std::ifstream in(meta);
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            bool match = false;
            if (line.substr(0, 5) == "EXPR:") {
                // Expression index: match "EXPR:colname" or "EXPR:expression"
                size_t endPos = line.find(':');
                if (endPos == std::string::npos) endPos = line.size();
                // Check if the expression part matches colname
                size_t exprEnd = line.find(":INCLUDE:");
                if (exprEnd == std::string::npos) exprEnd = line.find(":WHERE:");
                if (exprEnd == std::string::npos) exprEnd = line.size();
                std::string expr = line.substr(5, exprEnd - 5);
                if (expr == colname) match = true;
            } else {
                // Regular index: extract column name (before :DESC, :INCLUDE, :WHERE)
                size_t wherePos = line.find(":WHERE:");
                std::string base = (wherePos != std::string::npos) ? line.substr(0, wherePos) : line;
                size_t includePos = base.find(":INCLUDE");
                base = (includePos != std::string::npos) ? base.substr(0, includePos) : base;
                size_t descPos = base.find(":DESC");
                std::string idxCol = (descPos != std::string::npos) ? base.substr(0, descPos) : base;
                if (idxCol == colname) match = true;
            }
            if (!match) lines.push_back(line);
        }
    }
    {
        std::ofstream out(meta, std::ios::out);
        for (const auto& l : lines) out << l << '\n';
    }
    // Remove from cache
    std::string key = dbname + "/" + tablename + "/" + colname;
    secondaryIndexCache_.erase(key);
    return OpResult::Success;
}

OpResult StorageEngine::createCompositeIndex(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::vector<std::string>& colnames,
                                              const std::string& indexName,
                                              const std::vector<std::string>& includeCols,
                                              const std::string& whereCondition) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);
    TableSchema tbl = getTableSchema(dbname, tablename);

    // Validate all columns exist
    for (const auto& cname : colnames) {
        bool found = false;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == cname) { found = true; break; }
        }
        if (!found) return OpResult::InvalidValue;
    }

    // Check name collision with existing composite indexes
    auto existingComp = getCompositeIndexes(dbname, tablename);
    for (const auto& ci : existingComp) {
        if (ci.name == indexName) return OpResult::Success;
    }

    // Parse WHERE condition for partial index
    std::vector<Condition> whereConds;
    if (!whereCondition.empty()) {
        whereConds = parseConditions(std::vector<std::string>{whereCondition});
    }

    // Build index from existing data
    BPTree* idx = getCompositeIndexTree(dbname, tablename, indexName);
    if (!idx) return OpResult::InvalidValue;

    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        // Partial index: skip rows not matching WHERE condition
        bool match = true;
        for (const auto& wc : whereConds) {
            if (!evalConditionOnRow(wc, row, tbl)) { match = false; break; }
        }
        if (!match) return;
        std::string key = buildCompositeKey(row, tbl, colnames);
        if (!key.empty()) {
            idx->insertMulti(key, encodeRid(pageId, slotId));
        }
    });

    // Record in metadata: C:indexName:col1:col2:...:INCLUDE:inc1,inc2[:WHERE:cond]
    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    std::ofstream out(meta, std::ios::out | std::ios::app);
    out << "C:" << indexName;
    for (const auto& c : colnames) out << ":" << c;
    if (!includeCols.empty()) {
        out << ":INCLUDE:";
        for (size_t i = 0; i < includeCols.size(); ++i) {
            if (i > 0) out << ",";
            out << includeCols[i];
        }
    }
    if (!whereCondition.empty()) {
        out << ":WHERE:" << whereCondition;
    }
    out << '\n';
    return OpResult::Success;
}

OpResult StorageEngine::dropCompositeIndex(const std::string& dbname,
                                            const std::string& tablename,
                                            const std::string& indexName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    std::filesystem::path p = dbPath(dbname) / (tablename + ".idx_" + indexName);
    std::filesystem::remove(p);

    // Rewrite metadata without this composite index
    auto singleCols = getIndexedColumns(dbname, tablename);
    auto compIdxs = getCompositeIndexes(dbname, tablename);

    std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
    {
        std::ofstream out(meta, std::ios::out);
        // Rewrite single-column entries (filter out those only in composite)
        for (const auto& c : singleCols) {
            // Check if this column is in any remaining composite index
            bool inComposite = false;
            for (const auto& ci : compIdxs) {
                if (ci.name == indexName) continue;
                for (const auto& cc : ci.columns) {
                    if (cc == c) { inComposite = true; break; }
                }
                if (inComposite) break;
            }
            // Also check if it was in the dropped composite
            bool inDropped = false;
            for (const auto& ci : compIdxs) {
                if (ci.name != indexName) continue;
                for (const auto& cc : ci.columns) {
                    if (cc == c) { inDropped = true; break; }
                }
            }
            if (!inDropped || inComposite) out << c << '\n';
        }
        // Rewrite composite entries
        for (const auto& ci : compIdxs) {
            if (ci.name == indexName) continue;
            out << "C:" << ci.name;
            for (const auto& c : ci.columns) out << ":" << c;
            out << '\n';
        }
    }
    // Remove from cache
    std::string key = dbname + "/" + tablename + "/C/" + indexName;
    secondaryIndexCache_.erase(key);
    return OpResult::Success;
}

OpResult StorageEngine::reindex(const std::string& dbname,
                                 const std::string& tablename) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);

    // 1. Rebuild primary key index
    std::filesystem::path pkPath = indexPath(dbname, tablename);
    std::string pkKey = dbname + "/" + tablename;
    {
        auto it = pkIndexCache_.find(pkKey);
        if (it != pkIndexCache_.end()) {
            it->second->close();
            pkIndexCache_.erase(it);
        }
    }
    std::filesystem::remove(pkPath);
    BPTree* pkIdx = getPKIndex(dbname, tablename);
    if (!pkIdx) return OpResult::InvalidValue;
    if (tbl.hasPrimaryKey()) {
        forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                           const char* data, size_t len) {
            std::string row(data, len);
            std::string pkVal = tbl.buildPKValue(row);
            if (!pkVal.empty()) {
                pkIdx->insert(pkVal, encodeRid(pageId, slotId));
            }
        });
    }

    // 2. Rebuild secondary indexes
    auto singleCols = getIndexedColumns(dbname, tablename);
    for (const auto& colname : singleCols) {
        std::filesystem::path idxPath = secondaryIndexPath(dbname, tablename, colname);
        std::string cacheKey = dbname + "/" + tablename + "/" + colname;
        {
            auto it = secondaryIndexCache_.find(cacheKey);
            if (it != secondaryIndexCache_.end()) {
                it->second->close();
                secondaryIndexCache_.erase(it);
            }
        }
        std::filesystem::remove(idxPath);
        BPTree* idx = getSecondaryIndex(dbname, tablename, colname);
        if (!idx) continue;
        size_t colIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
        }
        if (colIdx >= tbl.len) continue;
        forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                           const char* data, size_t len) {
            std::string row(data, len);
            std::string val = extractColumnValue(row, tbl, colIdx);
            if (!val.empty()) {
                idx->insertMulti(val, encodeRid(pageId, slotId));
            }
        });
    }

    // 3. Rebuild composite indexes
    auto compIdxs = getCompositeIndexes(dbname, tablename);
    for (const auto& ci : compIdxs) {
        std::filesystem::path p = dbPath(dbname) / (tablename + ".idx_" + ci.name);
        std::string cacheKey = dbname + "/" + tablename + "/C/" + ci.name;
        {
            auto it = secondaryIndexCache_.find(cacheKey);
            if (it != secondaryIndexCache_.end()) {
                it->second->close();
                secondaryIndexCache_.erase(it);
            }
        }
        std::filesystem::remove(p);
        BPTree* idx = getCompositeIndexTree(dbname, tablename, ci.name);
        if (!idx) continue;
        forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                           const char* data, size_t len) {
            std::string row(data, len);
            std::string key = buildCompositeKey(row, tbl, ci.columns);
            if (!key.empty()) {
                idx->insertMulti(key, encodeRid(pageId, slotId));
            }
        });
    }

    return OpResult::Success;
}

// ========================================================================
// Full-text index (simplified inverted index)
// ========================================================================

static std::filesystem::path fullTextIndexPath(const std::string& dbname,
                                                const std::string& tablename,
                                                const std::string& colname) {
    return std::filesystem::path(dbname) / (tablename + "_" + colname + ".fti");
}

static std::vector<std::string> tokenizeText(const std::string& text) {
    std::vector<std::string> tokens;
    std::string current;
    for (char c : text) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            current += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        } else {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
        }
    }
    if (!current.empty()) tokens.push_back(current);
    return tokens;
}

OpResult StorageEngine::createFullTextIndex(const std::string& dbname,
                                             const std::string& tablename,
                                             const std::string& colname) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) return OpResult::InvalidValue;

    std::map<std::string, std::set<int64_t>> inverted;
    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        std::string val = extractColumnValue(row, tbl, colIdx);
        auto tokens = tokenizeText(val);
        int64_t rid = encodeRid(pageId, slotId);
        for (const auto& tok : tokens) {
            inverted[tok].insert(rid);
        }
    });

    auto path = fullTextIndexPath(dbname, tablename, colname);
    std::ofstream out(path);
    if (!out) return OpResult::InvalidValue;
    for (const auto& kv : inverted) {
        out << kv.first;
        for (int64_t rid : kv.second) {
            out << ' ' << rid;
        }
        out << '\n';
    }
    return OpResult::Success;
}

OpResult StorageEngine::dropFullTextIndex(const std::string& dbname,
                                           const std::string& tablename,
                                           const std::string& colname) {
    auto path = fullTextIndexPath(dbname, tablename, colname);
    if (std::filesystem::exists(path)) {
        std::filesystem::remove(path);
    }
    return OpResult::Success;
}

bool StorageEngine::hasFullTextIndex(const std::string& dbname,
                                      const std::string& tablename,
                                      const std::string& colname) const {
    return std::filesystem::exists(fullTextIndexPath(dbname, tablename, colname));
}

std::vector<int64_t> StorageEngine::fullTextSearch(const std::string& dbname,
                                                     const std::string& tablename,
                                                     const std::string& colname,
                                                     const std::string& word) const {
    std::vector<int64_t> result;
    auto path = fullTextIndexPath(dbname, tablename, colname);
    std::ifstream in(path);
    if (!in) return result;
    std::string searchWord = word;
    for (char& c : searchWord) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    std::string line;
    while (std::getline(in, line)) {
        size_t sp = line.find(' ');
        std::string tok = (sp == std::string::npos) ? line : line.substr(0, sp);
        if (tok == searchWord) {
            if (sp != std::string::npos) {
                std::string rest = line.substr(sp + 1);
                std::stringstream ss(rest);
                int64_t rid;
                while (ss >> rid) result.push_back(rid);
            }
            break;
        }
    }
    return result;
}

std::vector<std::string> StorageEngine::getFullTextIndexedColumns(const std::string& dbname,
                                                                   const std::string& tablename) const {
    std::vector<std::string> result;
    auto dir = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dir)) return result;
    std::string prefix = tablename + "_";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(name.size() - 4) == ".fti" &&
            name.substr(0, prefix.size()) == prefix) {
            std::string colname = name.substr(prefix.size(), name.size() - prefix.size() - 4);
            result.push_back(colname);
        }
    }
    return result;
}

bool StorageEngine::databaseExists(const std::string& dbname) const {
    return std::filesystem::exists(dbPath(dbname));
}

std::vector<std::string> StorageEngine::getDatabaseNames() const {
    std::vector<std::string> result;
    if (!std::filesystem::exists(".") || !std::filesystem::is_directory(".")) return result;
    for (const auto& entry : std::filesystem::directory_iterator(".", std::filesystem::directory_options::skip_permission_denied)) {
        try {
            if (!entry.is_directory()) continue;
        } catch (...) { continue; }
        std::string dbname;
        try { dbname = entry.path().filename().string(); } catch (...) { continue; }
        try { if (!std::filesystem::exists(tableListPath(dbname))) continue; } catch (...) { continue; }
        result.push_back(dbname);
    }
    return result;
}

bool StorageEngine::tableExists(const std::string& dbname,
                                 const std::string& tablename) const {
    return std::filesystem::exists(schemaPath(dbname, tablename));
}

OpResult StorageEngine::createDatabase(const std::string& dbname, const std::string& charset) {
    if (databaseExists(dbname)) return OpResult::TableAlreadyExist;
    std::filesystem::create_directory(dbPath(dbname));
    {
        std::ofstream f(tableListPath(dbname), std::ios::binary);
    }
    {
        std::ofstream f(dbPath(dbname) / ".charset");
        f << charset;
    }
    return OpResult::Success;
}

std::string StorageEngine::getDatabaseCharset(const std::string& dbname) const {
    auto cpath = dbPath(dbname) / ".charset";
    if (!std::filesystem::exists(cpath)) return "utf8";
    std::ifstream f(cpath);
    std::string cs;
    if (f >> cs) return cs;
    return "utf8";
}

OpResult StorageEngine::dropDatabase(const std::string& dbname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    std::filesystem::remove_all(dbPath(dbname));
    return OpResult::Success;
}

constexpr int32_t SCHEMA_FORMAT_VERSION = 0x44420001;  // "DB" + version 1

void StorageEngine::writeSchema(std::ostream& out, const TableSchema& tbl) {
    // Write format version marker
    out.write(reinterpret_cast<const char*>(&SCHEMA_FORMAT_VERSION), 4);
    int32_t len = static_cast<int32_t>(tbl.len);
    out.write(reinterpret_cast<const char*>(&len), 4);
    for (size_t i = 0; i < tbl.len; ++i) {
        uint8_t flags = (tbl.cols[i].isNull ? 1 : 0) | (tbl.cols[i].isPrimaryKey ? 2 : 0) | (tbl.cols[i].isVariableLength ? 4 : 0) | (tbl.cols[i].isUnique ? 8 : 0) | (!tbl.cols[i].defaultValue.empty() ? 16 : 0) | (tbl.cols[i].isAutoIncrement ? 32 : 0) | (!tbl.cols[i].checkExpr.empty() ? 64 : 0) | (!tbl.cols[i].generatedExpr.empty() ? 128 : 0);
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
        if (!tbl.cols[i].generatedExpr.empty()) {
            uint16_t genLen = static_cast<uint16_t>(tbl.cols[i].generatedExpr.size());
            out.write(reinterpret_cast<const char*>(&genLen), 2);
            out.write(tbl.cols[i].generatedExpr.data(), genLen);
        }
    }
    // Write foreign keys (multi-column support: version 1 format)
    int32_t fkLen = static_cast<int32_t>(tbl.fkLen);
    out.write(reinterpret_cast<const char*>(&fkLen), 4);
    for (size_t i = 0; i < tbl.fkLen; ++i) {
        int32_t numCols = static_cast<int32_t>(tbl.fks[i].colNames.size());
        out.write(reinterpret_cast<const char*>(&numCols), 4);
        for (int32_t ci = 0; ci < numCols; ++ci) {
            writeFixedString(out, tbl.fks[i].colNames[ci], MAX_COL_NAME_LEN);
            writeFixedString(out, tbl.fks[i].refCols[ci], MAX_COL_NAME_LEN);
        }
        writeFixedString(out, tbl.fks[i].refTable, MAX_TABLE_NAME_LEN);
        writeFixedString(out, tbl.fks[i].onDelete, 10);
    }
    // Write composite primary key column indices (0 = use legacy isPrimaryKey)
    int32_t pkCount = static_cast<int32_t>(tbl.pkColIndices.size());
    out.write(reinterpret_cast<const char*>(&pkCount), 4);
    for (size_t idx : tbl.pkColIndices) {
        int32_t cidx = static_cast<int32_t>(idx);
        out.write(reinterpret_cast<const char*>(&cidx), 4);
    }
    // Write composite UNIQUE constraints
    int32_t ucCount = static_cast<int32_t>(tbl.uniqueConstraints.size());
    out.write(reinterpret_cast<const char*>(&ucCount), 4);
    for (const auto& uc : tbl.uniqueConstraints) {
        int32_t cc = static_cast<int32_t>(uc.size());
        out.write(reinterpret_cast<const char*>(&cc), 4);
        for (size_t idx : uc) {
            int32_t cidx = static_cast<int32_t>(idx);
            out.write(reinterpret_cast<const char*>(&cidx), 4);
        }
    }
    // Write partitioning info (new: backward-compatible)
    int32_t ptype = static_cast<int32_t>(tbl.partitionType);
    out.write(reinterpret_cast<const char*>(&ptype), 4);
    writeFixedString(out, tbl.partitionKey, MAX_COL_NAME_LEN);
    if (tbl.partitionType == TableSchema::PartitionType::Range) {
        int32_t rpCount = static_cast<int32_t>(tbl.rangePartitions.size());
        out.write(reinterpret_cast<const char*>(&rpCount), 4);
        for (const auto& rp : tbl.rangePartitions) {
            writeFixedString(out, rp.first, MAX_TABLE_NAME_LEN);
            writeFixedString(out, rp.second, MAX_COL_NAME_LEN);
        }
    } else if (tbl.partitionType == TableSchema::PartitionType::List) {
        int32_t lpCount = static_cast<int32_t>(tbl.listPartitions.size());
        out.write(reinterpret_cast<const char*>(&lpCount), 4);
        for (const auto& lp : tbl.listPartitions) {
            writeFixedString(out, lp.first, MAX_TABLE_NAME_LEN);
            int32_t vcount = static_cast<int32_t>(lp.second.size());
            out.write(reinterpret_cast<const char*>(&vcount), 4);
            for (const auto& v : lp.second) {
                writeFixedString(out, v, MAX_COL_NAME_LEN);
            }
        }
    } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
        int32_t hcount = static_cast<int32_t>(tbl.hashPartitions);
        out.write(reinterpret_cast<const char*>(&hcount), 4);
    }
}

TableSchema StorageEngine::readSchema(std::istream& in, const std::string& tablename) const {
    TableSchema tbl;
    tbl.tablename = tablename;
    int32_t firstInt = 0;
    in.read(reinterpret_cast<char*>(&firstInt), 4);
    if (!in) return tbl;

    bool isNewFormat = (firstInt == SCHEMA_FORMAT_VERSION);
    int32_t len = 0;
    if (isNewFormat) {
        in.read(reinterpret_cast<char*>(&len), 4);
    } else {
        // Legacy format: first 4 bytes are column count
        len = firstInt;
    }
    if (len < 0 || len > static_cast<int32_t>(MAX_COLUMNS)) return tbl;
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
        bool hasGenerated = (flags & 128) != 0;
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
        if (hasGenerated) {
            uint16_t genLen = 0;
            in.read(reinterpret_cast<char*>(&genLen), 2);
            if (in && genLen > 0) {
                std::string genExpr(genLen, '\0');
                in.read(genExpr.data(), genLen);
                tbl.cols[i].generatedExpr = genExpr;
            }
        }
    }
    // Read foreign keys
    int32_t fkLen = 0;
    in.read(reinterpret_cast<char*>(&fkLen), 4);
    if (in && fkLen > 0 && fkLen <= static_cast<int32_t>(MAX_COLUMNS)) {
        tbl.fkLen = static_cast<size_t>(fkLen);
        for (size_t i = 0; i < tbl.fkLen; ++i) {
            if (isNewFormat) {
                int32_t numCols = 0;
                in.read(reinterpret_cast<char*>(&numCols), 4);
                if (!in || numCols <= 0 || numCols > static_cast<int32_t>(MAX_COLUMNS)) break;
                tbl.fks[i].colNames.reserve(numCols);
                tbl.fks[i].refCols.reserve(numCols);
                for (int32_t ci = 0; ci < numCols; ++ci) {
                    tbl.fks[i].colNames.push_back(readFixedString(in, MAX_COL_NAME_LEN));
                    tbl.fks[i].refCols.push_back(readFixedString(in, MAX_COL_NAME_LEN));
                }
                tbl.fks[i].refTable = readFixedString(in, MAX_TABLE_NAME_LEN);
                tbl.fks[i].onDelete = readFixedString(in, 10);
            } else {
                // Legacy single-column format
                std::string colName = readFixedString(in, MAX_COL_NAME_LEN);
                tbl.fks[i].refTable = readFixedString(in, MAX_TABLE_NAME_LEN);
                std::string refCol = readFixedString(in, MAX_COL_NAME_LEN);
                tbl.fks[i].onDelete = readFixedString(in, 10);
                tbl.fks[i].colNames.push_back(colName);
                tbl.fks[i].refCols.push_back(refCol);
            }
        }
    }
    // Read composite primary key column indices (new format, ignore if EOF)
    int32_t pkCount = 0;
    in.read(reinterpret_cast<char*>(&pkCount), 4);
    if (in && pkCount > 0 && pkCount <= static_cast<int32_t>(MAX_COLUMNS)) {
        tbl.pkColIndices.reserve(pkCount);
        for (int32_t i = 0; i < pkCount; ++i) {
            int32_t cidx = 0;
            in.read(reinterpret_cast<char*>(&cidx), 4);
            if (in && cidx >= 0 && cidx < static_cast<int32_t>(tbl.len)) {
                tbl.pkColIndices.push_back(static_cast<size_t>(cidx));
            }
        }
    }
    // Read composite UNIQUE constraints (new format, ignore if EOF)
    int32_t ucCount = 0;
    in.read(reinterpret_cast<char*>(&ucCount), 4);
    if (in && ucCount > 0 && ucCount <= static_cast<int32_t>(MAX_COLUMNS)) {
        tbl.uniqueConstraints.reserve(ucCount);
        for (int32_t i = 0; i < ucCount; ++i) {
            int32_t cc = 0;
            in.read(reinterpret_cast<char*>(&cc), 4);
            if (!in || cc <= 0 || cc > static_cast<int32_t>(MAX_COLUMNS)) break;
            std::vector<size_t> constraint;
            constraint.reserve(cc);
            for (int32_t j = 0; j < cc; ++j) {
                int32_t cidx = 0;
                in.read(reinterpret_cast<char*>(&cidx), 4);
                if (in && cidx >= 0 && cidx < static_cast<int32_t>(tbl.len)) {
                    constraint.push_back(static_cast<size_t>(cidx));
                }
            }
            if (!constraint.empty()) tbl.uniqueConstraints.push_back(std::move(constraint));
        }
    }
    // Read partitioning info (new format, ignore if EOF for backward compatibility)
    int32_t ptype = 0;
    in.read(reinterpret_cast<char*>(&ptype), 4);
    if (in) {
        tbl.partitionType = static_cast<TableSchema::PartitionType>(ptype);
        tbl.partitionKey = readFixedString(in, MAX_COL_NAME_LEN);
        if (tbl.partitionType == TableSchema::PartitionType::Range) {
            int32_t rpCount = 0;
            in.read(reinterpret_cast<char*>(&rpCount), 4);
            if (in && rpCount > 0) {
                for (int32_t i = 0; i < rpCount; ++i) {
                    std::string pname = readFixedString(in, MAX_TABLE_NAME_LEN);
                    std::string bound = readFixedString(in, MAX_COL_NAME_LEN);
                    tbl.rangePartitions.push_back({pname, bound});
                }
            }
        } else if (tbl.partitionType == TableSchema::PartitionType::List) {
            int32_t lpCount = 0;
            in.read(reinterpret_cast<char*>(&lpCount), 4);
            if (in && lpCount > 0) {
                for (int32_t i = 0; i < lpCount; ++i) {
                    std::string pname = readFixedString(in, MAX_TABLE_NAME_LEN);
                    int32_t vcount = 0;
                    in.read(reinterpret_cast<char*>(&vcount), 4);
                    std::vector<std::string> values;
                    for (int32_t j = 0; j < vcount; ++j) {
                        values.push_back(readFixedString(in, MAX_COL_NAME_LEN));
                    }
                    tbl.listPartitions.push_back({pname, values});
                }
            }
        } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
            int32_t hcount = 0;
            in.read(reinterpret_cast<char*>(&hcount), 4);
            if (in && hcount > 0) tbl.hashPartitions = static_cast<size_t>(hcount);
        }
    }
    return tbl;
}

// ========================================================================
// TOAST (The Oversized-Attribute Storage Technique)
// ========================================================================

std::filesystem::path StorageEngine::toastDir(const std::string& dbname,
                                               const std::string& tablename) {
    return std::filesystem::path(dbname) / (tablename + ".toast");
}

std::filesystem::path StorageEngine::toastMetaPath(const std::string& dbname,
                                                    const std::string& tablename) {
    return std::filesystem::path(dbname) / (tablename + ".toastmeta");
}

uint64_t StorageEngine::allocToastId(const std::string& dbname, const std::string& tablename) {
    auto metaPath = toastMetaPath(dbname, tablename);
    uint64_t nextId = 1;
    if (std::filesystem::exists(metaPath)) {
        std::ifstream ifs(metaPath, std::ios::binary);
        if (ifs) ifs.read(reinterpret_cast<char*>(&nextId), sizeof(nextId));
    }
    uint64_t allocated = nextId;
    nextId++;
    std::ofstream ofs(metaPath, std::ios::binary);
    if (ofs) ofs.write(reinterpret_cast<const char*>(&nextId), sizeof(nextId));
    return allocated;
}

void StorageEngine::writeToast(const std::string& dbname, const std::string& tablename,
                                uint64_t toastId, const std::string& data) {
    auto tdir = toastDir(dbname, tablename);
    if (!std::filesystem::exists(tdir)) {
        std::filesystem::create_directories(tdir);
    }
    auto path = tdir / (std::to_string(toastId) + ".dat");
    std::ofstream ofs(path, std::ios::binary);
    if (!ofs) return;
    uint32_t len = static_cast<uint32_t>(data.size());
    ofs.write(reinterpret_cast<const char*>(&len), sizeof(len));
    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
}

std::string StorageEngine::readToast(const std::string& dbname, const std::string& tablename,
                                      uint64_t toastId) {
    auto path = toastDir(dbname, tablename) / (std::to_string(toastId) + ".dat");
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return "";
    uint32_t len = 0;
    ifs.read(reinterpret_cast<char*>(&len), sizeof(len));
    std::string data(len, '\0');
    ifs.read(data.data(), static_cast<std::streamsize>(len));
    return data;
}

void StorageEngine::deleteToast(const std::string& dbname, const std::string& tablename,
                                 uint64_t toastId) {
    auto path = toastDir(dbname, tablename) / (std::to_string(toastId) + ".dat");
    if (std::filesystem::exists(path)) std::filesystem::remove(path);
}

bool StorageEngine::parseToastMarker(const std::string& val, uint64_t& toastId) {
    size_t prefixLen = strlen(TOAST_PREFIX);
    if (val.size() <= prefixLen) return false;
    if (val.substr(0, prefixLen) != TOAST_PREFIX) return false;
    try {
        toastId = static_cast<uint64_t>(std::stoull(val.substr(prefixLen)));
        return true;
    } catch (...) {
        return false;
    }
}

void StorageEngine::deleteRowToast(const std::string& dbname, const std::string& tablename,
                                    int64_t rid) {
    // Read the row and find all toast markers
    TableSchema tbl = getTableSchema(dbname, tablename);
    PageAllocator* pa = getPageAllocator(dbname, tablename);
    std::string row;
    if (!readRowByRid(pa, rid, row, tbl)) return;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (!tbl.cols[i].isVariableLength) continue;
        std::string val = extractColumnValue(row, tbl, i);
        uint64_t toastId = 0;
        if (parseToastMarker(val, toastId)) {
            deleteToast(dbname, tablename, toastId);
        }
    }
}

void StorageEngine::prepareToastValues(const std::string& dbname, const std::string& tablename,
                                        const TableSchema& tbl,
                                        std::map<std::string, std::string>& values) {
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!col.isVariableLength) continue;
        auto it = values.find(col.dataName);
        if (it == values.end()) continue;
        if (it->second.size() > TOAST_THRESHOLD) {
            uint64_t toastId = allocToastId(dbname, tablename);
            writeToast(dbname, tablename, toastId, it->second);
            it->second = std::string(TOAST_PREFIX) + std::to_string(toastId);
        }
    }
}

OpResult StorageEngine::createTable(const std::string& dbname, const TableSchema& tbl) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (tableExists(dbname, tbl.tablename)) return OpResult::TableAlreadyExist;

    {
        std::ofstream out(schemaPath(dbname, tbl.tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    // Initialize page-based data file(s) via PageAllocator
    if (tbl.partitionType != TableSchema::PartitionType::None) {
        if (tbl.partitionType == TableSchema::PartitionType::Range) {
            for (const auto& rp : tbl.rangePartitions) {
                auto pa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tbl.tablename, rp.first).string(), tbl.rowSize());
                pa->open(); pa->close();
            }
        } else if (tbl.partitionType == TableSchema::PartitionType::List) {
            for (const auto& lp : tbl.listPartitions) {
                auto pa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tbl.tablename, lp.first).string(), tbl.rowSize());
                pa->open(); pa->close();
            }
        } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
            for (size_t i = 0; i < tbl.hashPartitions; ++i) {
                auto pa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tbl.tablename, "p" + std::to_string(i)).string(), tbl.rowSize());
                pa->open(); pa->close();
            }
        }
    } else {
        auto pa = std::make_unique<PageAllocator>(dataPath(dbname, tbl.tablename).string(), tbl.rowSize());
        pa->open();
        pa->close();
    }
    {
        std::ofstream out(tableListPath(dbname), std::ios::binary | std::ios::app);
        writeFixedString(out, tbl.tablename, MAX_TABLE_NAME_LEN);
    }
    // Create B+ tree index if table has primary key
    if (tbl.hasPrimaryKey()) {
        BPTree idx(indexPath(dbname, tbl.tablename));
        idx.open();
        idx.close();
    }
    // Initialize auto-increment sequences
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isAutoIncrement) {
            writeNextSeq(dbname, tbl.tablename, tbl.cols[i].dataName, 1);
        }
    }
    // Auto-create secondary index for UNIQUE columns
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isUnique) {
            createIndex(dbname, tbl.tablename, tbl.cols[i].dataName);
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
    lockManager_.lockMetadata(tablename);

    std::filesystem::remove(schemaPath(dbname, tablename));
    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::remove(indexPath(dbname, tablename));
    removeSeq(dbname, tablename);
    // Remove TOAST data
    auto tdir = toastDir(dbname, tablename);
    if (std::filesystem::exists(tdir)) {
        std::filesystem::remove_all(tdir);
    }
    std::filesystem::remove(toastMetaPath(dbname, tablename));

    // Remove from cache
    std::string key = dbname + "/" + tablename;
    pkIndexCache_.erase(key);
    pageAllocators_.erase(key);
    secondaryIndexCache_.erase(key);

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
    lockManager_.lockMetadata(tablename);

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
    lockManager_.lockMetadata(tablename);

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
    if (col.dataType == "char" || col.dataType == "uuid" || col.isVariableLength) {
        if (cond.op == "<"  && !(val <  cond.value)) return false;
        if (cond.op == ">"  && !(val >  cond.value)) return false;
        if (cond.op == "="  && val != cond.value)    return false;
        if (cond.op == "<=" && (val >  cond.value))   return false;
        if (cond.op == ">=" && (val <  cond.value))   return false;
        if (cond.op == "!=" && val == cond.value)    return false;
        if (cond.op == "like" && !likeMatch(val, cond.value)) return false;
        if (cond.op == "regexp" && !regexMatch(val, cond.value)) return false;
        if (cond.op == "contains") {
            auto tokens = tokenizeText(val);
            std::string searchWord = cond.value;
            for (char& c : searchWord) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            bool found = false;
            for (const auto& tok : tokens) {
                if (tok == searchWord) { found = true; break; }
            }
            if (!found) return false;
        }
    } else if (col.dataType == "date") {
        Date d = (val.empty() ? Date{} : Date(val.c_str()));
        Date v(cond.value.c_str());
        if (cond.op == "<"  && v.year && !(d < v))  return false;
        if (cond.op == ">"  && v.year && !(d > v))  return false;
        if (cond.op == "="  && v.year && d != v)    return false;
        if (cond.op == "<=" && v.year && (d > v))   return false;
        if (cond.op == ">=" && v.year && (d < v))   return false;
        if (cond.op == "!=" && v.year && d == v)    return false;
    } else if (col.dataType == "timestamp" || col.dataType == "timestamptz" || col.dataType == "datetime") {
        int64_t num = val.empty() ? 0 : parseTimestampToSeconds(val);
        int64_t cmp = parseTimestampToSeconds(cond.value);
        if (cond.op == "<"  && cmp != 0 && !(num < cmp)) return false;
        if (cond.op == ">"  && cmp != 0 && !(num > cmp)) return false;
        if (cond.op == "="  && cmp != 0 && num != cmp)   return false;
        if (cond.op == "<=" && cmp != 0 && (num > cmp))  return false;
        if (cond.op == ">=" && cmp != 0 && (num < cmp))  return false;
        if (cond.op == "!=" && cmp != 0 && num == cmp)   return false;
    } else if (col.dataType == "time") {
        int32_t num = val.empty() ? -1 : parseTimeToSeconds(val);
        int32_t cmp = parseTimeToSeconds(cond.value);
        if (cond.op == "<"  && cmp >= 0 && !(num < cmp)) return false;
        if (cond.op == ">"  && cmp >= 0 && !(num > cmp)) return false;
        if (cond.op == "="  && cmp >= 0 && num != cmp)   return false;
        if (cond.op == "<=" && cmp >= 0 && (num > cmp))  return false;
        if (cond.op == ">=" && cmp >= 0 && (num < cmp))  return false;
        if (cond.op == "!=" && cmp >= 0 && num == cmp)   return false;
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
    } else if (col.dataType == "boolean") {
        if (cond.op == "="  && val != cond.value) return false;
        if (cond.op == "!=" && val == cond.value) return false;
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
            if (col.dataType == "char" || col.dataType == "uuid") {
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
            } else if (col.dataType == "timestamp" || col.dataType == "timestamptz" || col.dataType == "datetime") {
                int64_t num = val.empty() ? INF : parseTimestampToSeconds(val);
                std::memcpy(&rowBuffer[offset], &num, TIMESTAMP_SIZE);
            } else if (col.dataType == "time") {
                int32_t num = val.empty() ? -1 : parseTimeToSeconds(val);
                std::memcpy(&rowBuffer[offset], &num, sizeof(int32_t));
            } else if (col.dataType == "float") {
                float num = val.empty() ? 0.0f : std::stof(val);
                std::memcpy(&rowBuffer[offset], &num, sizeof(float));
            } else if (col.dataType == "double" || col.dataType == "decimal") {
                double num = val.empty() ? 0.0 : std::stod(val);
                std::memcpy(&rowBuffer[offset], &num, sizeof(double));
            } else if (col.dataType == "boolean") {
                int8_t bval = val.empty() ? INT8_MIN :
                    (val == "1" || val == "true" || val == "TRUE") ? 1 :
                    (val == "0" || val == "false" || val == "FALSE") ? 0 : INT8_MIN;
                std::memcpy(&rowBuffer[offset], &bval, sizeof(int8_t));
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
                } else if (col.dataType == "timestamp" || col.dataType == "timestamptz" || col.dataType == "datetime") {
                    int64_t num = val.empty() ? INF : parseTimestampToSeconds(val);
                    std::memcpy(&fixedData[fixedOff], &num, TIMESTAMP_SIZE);
                } else if (col.dataType == "time") {
                    int32_t num = val.empty() ? -1 : parseTimeToSeconds(val);
                    std::memcpy(&fixedData[fixedOff], &num, sizeof(int32_t));
                } else if (col.dataType == "float") {
                    float num = val.empty() ? 0.0f : std::stof(val);
                    std::memcpy(&fixedData[fixedOff], &num, sizeof(float));
                } else if (col.dataType == "double" || col.dataType == "decimal") {
                    double num = val.empty() ? 0.0 : std::stod(val);
                    std::memcpy(&fixedData[fixedOff], &num, sizeof(double));
                } else if (col.dataType == "boolean") {
                    int8_t bval = val.empty() ? INT8_MIN :
                        (val == "1" || val == "true" || val == "TRUE") ? 1 :
                        (val == "0" || val == "false" || val == "FALSE") ? 0 : INT8_MIN;
                    std::memcpy(&fixedData[fixedOff], &bval, sizeof(int8_t));
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
    if (readOnly_) return OpResult::InvalidValue;
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
    if (tbl.hasPrimaryKey()) {
        std::string pkVal = tbl.buildPKValue(actualValues);
        if (!pkVal.empty()) {
            BPTree* idx = getPKIndex(dbname, tablename);
            if (idx) {
                int64_t dummy;
                if (idx->search(pkVal, dummy)) {
                    lockManager_.unlock(tablename);
                    return OpResult::DuplicateKey;
                }
            }
        }
    }

    // Check single-column UNIQUE constraints (use B+ tree index if available)
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!col.isUnique) continue;
        auto it = actualValues.find(col.dataName);
        if (it == actualValues.end() || it->second.empty()) continue;
        bool duplicate = false;
        // Try B+ tree secondary index first
        BPTree* secIdx = getSecondaryIndex(dbname, tablename, col.dataName);
        if (secIdx) {
            int64_t dummy;
            if (secIdx->search(it->second, dummy)) duplicate = true;
        } else {
            forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
                if (duplicate) return;
                std::string row(data, len);
                std::string existingVal = extractColumnValue(row, tbl, i);
                if (existingVal == it->second) duplicate = true;
            });
        }
        if (duplicate) {
            lockManager_.unlock(tablename);
            return OpResult::DuplicateKey;
        }
    }

    // Check composite UNIQUE constraints
    for (const auto& uc : tbl.uniqueConstraints) {
        if (uc.empty()) continue;
        std::string compositeKey;
        for (size_t idx : uc) {
            if (idx < tbl.len) {
                auto it = actualValues.find(tbl.cols[idx].dataName);
                compositeKey += (it != actualValues.end() ? it->second : "") + "\x01";
            }
        }
        bool duplicate = false;
        forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
            if (duplicate) return;
            std::string row(data, len);
            std::string existingKey;
            for (size_t idx : uc) {
                if (idx < tbl.len) existingKey += extractColumnValue(row, tbl, idx) + "\x01";
            }
            if (existingKey == compositeKey) duplicate = true;
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
        if (!col.isVariableLength && (col.dataType == "timestamp" || col.dataType == "timestamptz" || col.dataType == "datetime") && !val.empty()) {
            int64_t ts = parseTimestampToSeconds(val);
            if (ts == 0) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && col.dataType == "time" && !val.empty()) {
            int32_t ts = parseTimeToSeconds(val);
            if (ts < 0) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && col.dataType == "float" && !val.empty()) {
            try { std::stof(val); } catch (...) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && (col.dataType == "double" || col.dataType == "decimal") && !val.empty()) {
            try { std::stod(val); } catch (...) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && col.dataType == "boolean" && !val.empty()) {
            if (val != "1" && val != "0" && val != "true" && val != "false" && val != "TRUE" && val != "FALSE") {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && col.dataType != "char" && col.dataType != "binary" && col.dataType != "date" && col.dataType != "timestamp" && col.dataType != "timestamptz" && col.dataType != "datetime" && col.dataType != "time" && col.dataType != "float" && col.dataType != "double" && col.dataType != "decimal" && col.dataType != "boolean" && col.dataType != "uuid" && !val.empty()) {
            int64_t num = parseInt(val);
            if (num == INF) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
            if (col.isUnsigned && num < 0) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
    }

    // Compute generated columns
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (col.generatedExpr.empty()) continue;
        auto it = actualValues.find(col.dataName);
        if (it != actualValues.end() && !it->second.empty()) continue; // user provided value
        std::string computed = evaluateGeneratedExpr(col.generatedExpr, actualValues);
        if (!computed.empty()) {
            actualValues[col.dataName] = computed;
        }
    }

    // TOAST: offload large variable-length values to external storage
    prepareToastValues(dbname, tablename, tbl, actualValues);

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

    // Check gap lock conflict (if any transaction holds a gap lock covering PK)
    if (tbl.hasPrimaryKey()) {
        std::string pkVal = tbl.buildPKValue(actualValues);
        if (!pkVal.empty() && lockManager_.isGapLocked(tablename, pkVal)) {
            lockManager_.unlock(tablename);
            return OpResult::LockConflict;
        }
    }

    // Check foreign key references
    for (size_t fi = 0; fi < tbl.fkLen; ++fi) {
        const ForeignKey& fk = tbl.fks[fi];
        // Check if any FK column value is NULL (NULL is allowed in FKs)
        bool hasNull = false;
        for (const auto& colName : fk.colNames) {
            auto it = actualValues.find(colName);
            if (it == actualValues.end() || it->second.empty()) {
                hasNull = true;
                break;
            }
        }
        if (hasNull) continue;
        if (!tableExists(dbname, fk.refTable)) {
            lockManager_.unlock(tablename);
            return OpResult::TableNotExist;
        }
        TableSchema refTbl = getTableSchema(dbname, fk.refTable);
        if (fk.colNames.size() == 1) {
            // Single-column FK: use BPTree index for fast lookup
            BPTree* refIdx = getPKIndex(dbname, fk.refTable);
            if (refIdx) {
                auto it = actualValues.find(fk.colNames[0]);
                int64_t dummy;
                if (!refIdx->search(it->second, dummy)) {
                    lockManager_.unlock(tablename);
                    return OpResult::InvalidValue;  // referenced key not found
                }
            }
        } else {
            // Multi-column FK: scan reference table rows
            bool found = false;
            forEachRow(dbname, fk.refTable, [&](uint32_t, uint16_t, const char* data, size_t len) {
                if (found) return;
                std::string row(data, len);
                bool match = true;
                for (size_t ci = 0; ci < fk.refCols.size() && ci < fk.colNames.size(); ++ci) {
                    int refColIdx = -1;
                    for (size_t ri = 0; ri < refTbl.len; ++ri) {
                        if (refTbl.cols[ri].dataName == fk.refCols[ci]) {
                            refColIdx = static_cast<int>(ri);
                            break;
                        }
                    }
                    if (refColIdx < 0) { match = false; break; }
                    std::string refVal = extractColumnValue(row, refTbl, refColIdx);
                    auto it = actualValues.find(fk.colNames[ci]);
                    if (it == actualValues.end() || it->second != refVal) {
                        match = false;
                        break;
                    }
                }
                if (match) found = true;
            });
            if (!found) {
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

    // Determine target partition for partitioned tables
    std::string targetPartition;
    if (tbl.partitionType != TableSchema::PartitionType::None) {
        auto pit = actualValues.find(tbl.partitionKey);
        if (pit != actualValues.end() && !pit->second.empty()) {
            targetPartition = getPartitionName(tbl, pit->second);
        }
        if (targetPartition.empty()) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
    }

    // Write row into page-based storage
    PageAllocator* pa = nullptr;
    std::unique_ptr<PageAllocator> partPa;
    if (!targetPartition.empty()) {
        partPa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tablename, targetPartition).string(), tbl.rowSize());
        if (!partPa->open()) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
        pa = partPa.get();
    } else {
        pa = getPageAllocator(dbname, tablename);
    }
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
    // Update composite indexes
    {
        auto compIdxs = getCompositeIndexes(dbname, tablename);
        for (const auto& ci : compIdxs) {
            BPTree* cidx = getCompositeIndexTree(dbname, tablename, ci.name);
            if (cidx) {
                std::string key = buildCompositeKey(strippedRow, tbl, ci.columns);
                if (!key.empty()) cidx->insertMulti(key, rid);
            }
        }
    }
    // Update hash indexes
    {
        auto hashCols = getHashIndexedColumns(dbname, tablename);
        for (const auto& colname : hashCols) {
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx >= tbl.len) continue;
            HashIndex* hidx = getHashIndex(dbname, tablename, colname);
            if (hidx) {
                std::string val = extractColumnValue(strippedRow, tbl, colIdx);
                if (!val.empty()) hidx->insert(val, rid);
            }
        }
    }
    lockManager_.unlock(tablename);

    // Fire AFTER INSERT triggers
    if (triggerExecutor_) {
        auto triggers = getTriggers(dbname, tablename, "after", "insert");
        for (const auto& trg : triggers) {
            std::string action = trg.action;
            if (trg.forEachRow) {
                for (const auto& [col, val] : actualValues) {
                    std::string placeholder = "NEW." + col;
                    size_t pos = 0;
                    while ((pos = action.find(placeholder, pos)) != std::string::npos) {
                        action.replace(pos, placeholder.size(), val);
                        pos += val.size();
                    }
                }
            }
            triggerExecutor_(action);
        }
    }

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
        // Handle REGEXP operator
        if (s.size() >= 6 && s.substr(0, 6) == "regexp") {
            c.op = "regexp";
            size_t sp = s.find(' ', 6);
            if (sp == std::string::npos) continue;
            c.colName = s.substr(6, sp - 6);
            c.value = s.substr(sp + 1);
            if (c.value.size() >= 2 && c.value.front() == '\'' && c.value.back() == '\'')
                c.value = c.value.substr(1, c.value.size() - 2);
            conds.push_back(c);
            continue;
        }
        // Handle CONTAINS operator (full-text search)
        if (s.size() >= 8 && s.substr(0, 8) == "contains") {
            c.op = "contains";
            size_t sp = s.find(' ', 8);
            if (sp == std::string::npos) continue;
            c.colName = s.substr(8, sp - 8);
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

    // Try full-text index for CONTAINS conditions
    for (const auto& c : conds) {
        if (c.op == "contains") {
            if (hasFullTextIndex(dbname, tablename, c.colName)) {
                auto rids = fullTextSearch(dbname, tablename, c.colName, c.value);
                for (int64_t rid : rids) ids.insert(rid);
                if (!ids.empty()) {
                    if (conds.size() > 1) {
                        PageAllocator* pa = getPageAllocator(dbname, tablename);
                        std::set<int64_t> toRemove;
                        for (int64_t rid : ids) {
                            std::string row;
                            if (!readRowByRid(pa, rid, row, tbl)) { toRemove.insert(rid); continue; }
                            bool match = true;
                            for (const auto& cond : conds) {
                                if (cond.op == "contains" && cond.colName == c.colName) continue;
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
    }

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
                // Try hash index first (O(1) equality lookup)
                HashIndex* hidx = getHashIndex(dbname, tablename, c.colName);
                if (hidx) {
                    auto vals = hidx->search(c.value);
                    for (int64_t v : vals) ids.insert(v);
                }
                // Fallback to B+ tree secondary index
                if (ids.empty()) {
                    BPTree* secIdx = getSecondaryIndex(dbname, tablename, c.colName);
                    if (secIdx) {
                        auto vals = secIdx->searchMulti(c.value);
                        for (int64_t v : vals) ids.insert(v);
                    }
                }
                // Try expression index (e.g. UPPER(name) = 'FOO')
                if (ids.empty()) {
                    auto metaList = getIndexMetadata(dbname, tablename);
                    for (const auto& meta : metaList) {
                        if (!meta.isExpression) continue;
                        // Normalize: compare uppercase versions
                        std::string queryExpr = c.colName;
                        std::string idxExpr = meta.name;
                        // Convert function names to uppercase for comparison
                        size_t qlp = queryExpr.find('(');
                        size_t ilp = idxExpr.find('(');
                        if (qlp != std::string::npos && ilp != std::string::npos) {
                            std::string qfunc = queryExpr.substr(0, qlp);
                            std::string ifunc = idxExpr.substr(0, ilp);
                            for (char& ch : qfunc) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
                            for (char& ch : ifunc) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
                            if (qfunc == ifunc) {
                                // Functions match, check inner column
                                std::string qcol = queryExpr.substr(qlp + 1, queryExpr.find(')') - qlp - 1);
                                std::string icol = idxExpr.substr(ilp + 1, idxExpr.find(')') - ilp - 1);
                                if (qcol == icol) {
                                    BPTree* exprIdx = getSecondaryIndex(dbname, tablename, meta.name);
                                    if (exprIdx) {
                                        std::string searchVal = c.value;
                                        if (meta.exprFunc == "UPPER") {
                                            for (char& ch : searchVal) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
                                        } else if (meta.exprFunc == "LOWER") {
                                            for (char& ch : searchVal) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                                        }
                                        auto vals = exprIdx->searchMulti(searchVal);
                                        for (int64_t v : vals) ids.insert(v);
                                    }
                                    break;
                                }
                            }
                        }
                    }
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
    // Partition pruning: if conditions involve partition key, scan only matching partitions
    if (tbl.partitionType != TableSchema::PartitionType::None) {
        auto targetParts = getTargetPartitions(tbl, conds);
        if (!targetParts.empty()) {
            for (const auto& pname : targetParts) {
                auto ppa = std::make_unique<PageAllocator>(
                    partitionDataPath(dbname, tablename, pname).string(), tbl.rowSize());
                if (!ppa->open()) continue;
                uint32_t np = ppa->numPages();
                for (uint32_t pid = 1; pid < np; ++pid) {
                    char* buf = ppa->fetchPage(pid);
                    Page page(buf);
                    page.forEachLive([&](uint16_t sid, const char* data, size_t len) {
                        int64_t rid = encodeRid(pid, sid);
                        bool match = true;
                        std::string row(data, len);
                        for (const auto& c : conds) {
                            if (!evalConditionOnRow(c, row, tbl)) { match = false; break; }
                        }
                        if (match) ids.insert(rid);
                    });
                    ppa->unpinPage(pid);
                }
                ppa->close();
            }
            return ids;
        }
    }
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
    if (readOnly_) return OpResult::InvalidValue;
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockIntentExclusive(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    PageAllocator* pa = getPageAllocator(dbname, tablename);

    auto conds = parseConditions(conditions);
    std::set<int64_t> toDelete = filterRows(dbname, tablename, conds);

    if (toDelete.empty()) {
        lockManager_.unlock(tablename);
        return OpResult::Success;
    }

    // Acquire row-level exclusive locks on rows to be deleted
    for (int64_t rid : toDelete) {
        lockManager_.rowLockExclusive(tablename, rid);
    }

    // Check foreign key references and apply ON DELETE actions
    {
        size_t pkIdx = tbl.len;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
        }
        if (pkIdx < tbl.len) {
            // Collect deleted rows' primary key values (multi-column PK aware)
            std::vector<std::map<std::string, std::string>> deletedPKRows;
            for (int64_t rid : toDelete) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                std::map<std::string, std::string> pkVals;
                if (!tbl.pkColIndices.empty()) {
                    for (size_t pki : tbl.pkColIndices) {
                        pkVals[tbl.cols[pki].dataName] = extractColumnValue(row, tbl, pki);
                    }
                } else {
                    pkVals[tbl.cols[pkIdx].dataName] = extractPKValue(row, tbl);
                }
                if (!pkVals.empty()) deletedPKRows.push_back(std::move(pkVals));
            }

            if (!deletedPKRows.empty()) {
                // Scan all other tables for FK references and collect actions
                struct CascadeAction { std::string table; int64_t rid; };
                struct SetNullAction { std::string table; int64_t rid; std::vector<size_t> colIndices; };
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

                        // Build mapping from refCols to local col indices in otherTbl
                        std::vector<size_t> fkColIndices;
                        bool allFound = true;
                        for (const auto& colName : fk.colNames) {
                            size_t colIdx = otherTbl.len;
                            for (size_t ci = 0; ci < otherTbl.len; ++ci) {
                                if (otherTbl.cols[ci].dataName == colName) { colIdx = ci; break; }
                            }
                            if (colIdx >= otherTbl.len) { allFound = false; break; }
                            fkColIndices.push_back(colIdx);
                        }
                        if (!allFound || fkColIndices.empty()) continue;

                        forEachRow(dbname, otherTable, [&](uint32_t opid, uint16_t osid, const char* data, size_t len) {
                            std::string row(data, len);
                            // Build FK value map for this row
                            std::map<std::string, std::string> fkVals;
                            for (size_t ci = 0; ci < fk.colNames.size() && ci < fk.refCols.size(); ++ci) {
                                fkVals[fk.refCols[ci]] = extractColumnValue(row, otherTbl, fkColIndices[ci]);
                            }
                            // Check if any deleted PK row matches this FK
                            bool matched = false;
                            for (const auto& pkVals : deletedPKRows) {
                                bool allMatch = true;
                                for (const auto& [refCol, refVal] : pkVals) {
                                    auto it = fkVals.find(refCol);
                                    if (it == fkVals.end() || it->second != refVal) {
                                        allMatch = false; break;
                                    }
                                }
                                if (allMatch) { matched = true; break; }
                            }
                            if (matched) {
                                int64_t orid = encodeRid(opid, osid);
                                if (fk.onDelete == "cascade") {
                                    cascadeActions.push_back({otherTable, orid});
                                } else if (fk.onDelete == "setnull") {
                                    setNullActions.push_back({otherTable, orid, fkColIndices});
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
                    lockManager_.lockIntentExclusive(t);
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

                    // Set FK columns to NULL: rebuild row buffer
                    {
                        std::map<std::string, std::string> rowValues;
                        for (size_t i = 0; i < otbl.len; ++i) {
                            rowValues[otbl.cols[i].dataName] = extractColumnValue(row, otbl, i);
                        }
                        for (size_t ci : sa.colIndices) {
                            rowValues[otbl.cols[ci].dataName] = "";
                        }
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
                        if (ici >= otbl.len) continue;
                        bool isFkCol = false;
                        for (size_t ci : sa.colIndices) {
                            if (ci == ici) { isFkCol = true; break; }
                        }
                        if (!isFkCol) continue;
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

    // Pre-read row data for index removal (before tombstone)
    std::vector<std::string> rowsToDelete;
    rowsToDelete.reserve(toDelete.size());
    for (int64_t rid : toDelete) {
        std::string row;
        if (readRowByRid(pa, rid, row, tbl)) {
            rowsToDelete.push_back(row);
        } else {
            rowsToDelete.push_back("");
        }
    }

    // Delete TOAST entries for rows being deleted
    for (int64_t rid : toDelete) {
        deleteRowToast(dbname, tablename, rid);
    }

    // Delete rows via PageAllocator tombstones
    size_t delIdx = 0;
    for (int64_t rid : toDelete) {
        // Log for transaction rollback (before deletion)
        if (inTransaction_ && dbname == txnDB_ && !rowsToDelete[delIdx].empty()) {
            logTxnDelete(tablename, rid, rowsToDelete[delIdx]);
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
        ++delIdx;
    }

    // Remove from PK index
    BPTree* pkIdx = getPKIndex(dbname, tablename);
    if (pkIdx) {
        for (const auto& row : rowsToDelete) {
            if (row.empty()) continue;
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
            for (const auto& row : rowsToDelete) {
                if (row.empty()) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) secIdx->remove(val);
            }
        }
    }
    // Remove from composite indexes
    {
        auto compIdxs = getCompositeIndexes(dbname, tablename);
        for (const auto& ci : compIdxs) {
            BPTree* cidx = getCompositeIndexTree(dbname, tablename, ci.name);
            if (!cidx) continue;
            for (const auto& row : rowsToDelete) {
                if (row.empty()) continue;
                std::string key = buildCompositeKey(row, tbl, ci.columns);
                if (!key.empty()) cidx->remove(key);
            }
        }
    }
    // Remove from hash indexes
    {
        auto hashCols = getHashIndexedColumns(dbname, tablename);
        for (const auto& colname : hashCols) {
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx >= tbl.len) continue;
            HashIndex* hidx = getHashIndex(dbname, tablename, colname);
            if (!hidx) continue;
            size_t hidx_i = 0;
            for (int64_t rid : toDelete) {
                if (hidx_i < rowsToDelete.size() && !rowsToDelete[hidx_i].empty()) {
                    std::string val = extractColumnValue(rowsToDelete[hidx_i], tbl, colIdx);
                    if (!val.empty()) hidx->remove(val, rid);
                }
                ++hidx_i;
            }
        }
    }
    lockManager_.unlock(tablename);

    // Fire AFTER DELETE triggers
    if (triggerExecutor_) {
        auto triggers = getTriggers(dbname, tablename, "after", "delete");
        for (const auto& trg : triggers) {
            if (trg.forEachRow) {
                for (const auto& row : rowsToDelete) {
                    if (row.empty()) continue;
                    std::string action = trg.action;
                    for (size_t i = 0; i < tbl.len; ++i) {
                        std::string val = extractColumnValue(row, tbl, i);
                        std::string placeholder = "OLD." + tbl.cols[i].dataName;
                        size_t pos = 0;
                        while ((pos = action.find(placeholder, pos)) != std::string::npos) {
                            action.replace(pos, placeholder.size(), val);
                            pos += val.size();
                        }
                    }
                    triggerExecutor_(action);
                }
            } else {
                // Statement-level trigger: execute once
                triggerExecutor_(trg.action);
            }
        }
    }

    // Track dead tuples for auto-vacuum
    {
        auto key = std::make_pair(dbname, tablename);
        std::lock_guard<std::mutex> lock(deadTupleMutex_);
        deadTupleCounts_[key] += toDelete.size();
    }
    maybeAutoVacuum(dbname, tablename);

    return OpResult::Success;
}

OpResult StorageEngine::update(const std::string& dbname,
                                const std::string& tablename,
                                const std::map<std::string, std::string>& updates,
                                const std::vector<std::string>& conditions) {
    if (readOnly_) return OpResult::InvalidValue;
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
                    if (!kv.second.empty()) {
                        int64_t num = parseInt(kv.second);
                        if (num == INF) return OpResult::InvalidValue;
                        if (col.isUnsigned && num < 0) return OpResult::InvalidValue;
                    }
                }
                colUpdates[i] = kv.second;
                break;
            }
        }
        if (!found) return OpResult::InvalidValue;
    }

    lockManager_.lockIntentExclusive(tablename);

    PageAllocator* pa = getPageAllocator(dbname, tablename);

    auto conds = parseConditions(conditions);
    std::set<int64_t> matchIds = conds.empty()
        ? [&](){ std::set<int64_t> s; forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char*, size_t) { s.insert(encodeRid(pid, sid)); }); return s; }()
        : filterRows(dbname, tablename, conds);

    if (matchIds.empty()) {
        lockManager_.unlock(tablename);
        return OpResult::Success;
    }

    // Acquire row-level exclusive locks on rows to be updated
    for (int64_t rid : matchIds) {
        lockManager_.rowLockExclusive(tablename, rid);
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
        // Also save hash-indexed column values
        auto hashCols = getHashIndexedColumns(dbname, tablename);
        for (const auto& colname : hashCols) {
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
        // TOAST: delete old toast entries, create new ones for large values
        deleteRowToast(dbname, tablename, rid);
        prepareToastValues(dbname, tablename, tbl, rowValues);
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

        // Update composite indexes
        auto compIdxs = getCompositeIndexes(dbname, tablename);
        for (const auto& ci : compIdxs) {
            bool anyColChanged = false;
            for (const auto& cname : ci.columns) {
                size_t colIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == cname) { colIdx = i; break; }
                }
                if (colUpdates.find(colIdx) != colUpdates.end()) { anyColChanged = true; break; }
            }
            if (!anyColChanged && actualRid == rid) continue;
            BPTree* cidx = getCompositeIndexTree(dbname, tablename, ci.name);
            if (!cidx) continue;
            std::string newKey = buildCompositeKey(strippedNewRow, tbl, ci.columns);
            std::string oldKey = buildCompositeKey(row, tbl, ci.columns);
            if (oldKey != newKey) {
                if (!oldKey.empty()) cidx->remove(oldKey);
                if (!newKey.empty()) cidx->insertMulti(newKey, actualRid);
            } else if (actualRid != rid && !newKey.empty()) {
                cidx->remove(newKey);
                cidx->insertMulti(newKey, actualRid);
            }
        }

        // Update hash indexes
        auto hashIdxCols = getHashIndexedColumns(dbname, tablename);
        for (const auto& colname : hashIdxCols) {
            size_t colIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
            }
            if (colIdx >= tbl.len) continue;
            HashIndex* hidx = getHashIndex(dbname, tablename, colname);
            if (!hidx) continue;
            auto itOld = oldIdxVals.find(colname);
            std::string oldVal = (itOld != oldIdxVals.end()) ? itOld->second : "";
            std::string newVal = extractColumnValue(strippedNewRow, tbl, colIdx);
            bool colChanged = (colUpdates.find(colIdx) != colUpdates.end());
            if (colChanged && oldVal != newVal) {
                if (!oldVal.empty()) hidx->remove(oldVal, rid);
                if (!newVal.empty()) hidx->insert(newVal, actualRid);
            } else if (actualRid != rid && !newVal.empty()) {
                hidx->remove(newVal, rid);
                hidx->insert(newVal, actualRid);
            }
        }
    }

    lockManager_.unlock(tablename);

    // Fire AFTER UPDATE triggers
    if (triggerExecutor_) {
        auto triggers = getTriggers(dbname, tablename, "after", "update");
        for (const auto& trg : triggers) {
            if (trg.forEachRow) {
                for (int64_t rid : matchIds) {
                    std::string oldRow, newRow;
                    if (!readRowByRid(pa, rid, newRow, tbl)) continue;
                    std::string action = trg.action;
                    for (size_t i = 0; i < tbl.len; ++i) {
                        std::string val = extractColumnValue(newRow, tbl, i);
                        std::string placeholder = "NEW." + tbl.cols[i].dataName;
                        size_t pos = 0;
                        while ((pos = action.find(placeholder, pos)) != std::string::npos) {
                            action.replace(pos, placeholder.size(), val);
                            pos += val.size();
                        }
                    }
                    triggerExecutor_(action);
                }
            } else {
                // Statement-level trigger: execute once
                triggerExecutor_(trg.action);
            }
        }
    }

    return OpResult::Success;
}

// ========================================================================
// information_schema virtual tables
// ========================================================================
std::vector<std::string> StorageEngine::queryInformationSchema(
    const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::set<std::string>& selectCols,
    const std::vector<OrderBySpec>& orderBy) const {

    std::vector<std::string> result;
    auto conds = parseConditions(conditions);

    if (tablename == "tables" || tablename == "TABLES") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& tname : getTableNames(dbname)) {
                std::string row = dbname + " " + tname + " BASE_TABLE ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
            for (const auto& vname : getViewNames(dbname)) {
                std::string row = dbname + " " + vname + " VIEW ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "table_name" && c.op == "=" && vname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
        }
    } else if (tablename == "columns" || tablename == "COLUMNS") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& tname : getTableNames(dbname)) {
                TableSchema tbl = getTableSchema(dbname, tname);
                for (size_t i = 0; i < tbl.len; ++i) {
                    const Column& col = tbl.cols[i];
                    std::string nullable = col.isNull ? "YES" : "NO";
                    std::string row = dbname + " " + tname + " " + col.dataName + " " + col.dataType + " " + nullable + " ";
                    bool match = true;
                    for (const auto& c : conds) {
                        if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                        if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                        if (c.colName == "column_name" && c.op == "=" && col.dataName != c.value) { match = false; break; }
                    }
                    if (match) result.push_back(row);
                }
            }
        }
    } else if (tablename == "statistics" || tablename == "STATISTICS") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& tname : getTableNames(dbname)) {
                TableSchema tbl = getTableSchema(dbname, tname);
                // Single-column indexes
                auto idxCols = getIndexedColumns(dbname, tname);
                for (const auto& cname : idxCols) {
                    std::string row = dbname + " " + tname + " " + cname + "_idx " + cname + " ";
                    bool match = true;
                    for (const auto& c : conds) {
                        if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                        if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                    }
                    if (match) result.push_back(row);
                }
                // Composite indexes
                auto compIdxs = getCompositeIndexes(dbname, tname);
                for (const auto& ci : compIdxs) {
                    for (size_t seq = 0; seq < ci.columns.size(); ++seq) {
                        std::string row = dbname + " " + tname + " " + ci.name + " " + ci.columns[seq] + " " + std::to_string(seq + 1) + " ";
                        bool match = true;
                        for (const auto& c : conds) {
                            if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                            if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                        }
                        if (match) result.push_back(row);
                    }
                }
            }
        }
    } else if (tablename == "views" || tablename == "VIEWS") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& vname : getViewNames(dbname)) {
                std::string row = dbname + " " + vname + " ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "table_name" && c.op == "=" && vname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
        }
    }
    return result;
}

std::vector<std::string> StorageEngine::query(const std::string& dbname,
                                               const std::string& tablename,
                                               const std::vector<std::string>& conditions,
                                               const std::set<std::string>& selectCols,
                                               const std::vector<OrderBySpec>& orderBy,
                                               bool forUpdate,
                                               bool noWait,
                                               bool skipLocked,
                                               int timezoneOffsetMinutes) {
    std::vector<std::string> result;

    // information_schema virtual tables
    if (dbname == "information_schema") {
        return queryInformationSchema(tablename, conditions, selectCols, orderBy);
    }

    if (!tableExists(dbname, tablename)) return result;
    if (inTransaction_) {
        if (forUpdate) {
            lockManager_.lockIntentExclusive(tablename);
        } else {
            lockManager_.lockIntentShared(tablename);
        }
    } else {
        lockManager_.lockShared(tablename);
    }

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
        forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char* data, size_t len) {
            matchRows.emplace_back(encodeRid(pid, sid), std::string(data, len));
        });
    } else if (tbl.partitionType != TableSchema::PartitionType::None) {
        // Partitioned table: use forEachRow with partition pruning + in-callback filtering
        auto targetParts = getTargetPartitions(tbl, conds);
        const ReadView* rv = inTransaction_ ? &readView_ : nullptr;
        forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char* data, size_t len) {
            std::string row(data, len);
            bool match = true;
            for (const auto& c : conds) {
                if (!evalConditionOnRow(c, row, tbl)) { match = false; break; }
            }
            if (match) matchRows.emplace_back(encodeRid(pid, sid), std::move(row));
        }, rv, targetParts);
    } else {
        auto ids = filterRows(dbname, tablename, conds);
        for (int64_t rid : ids) {
            std::string row;
            if (readRowByRid(pa, rid, row, tbl)) {
                matchRows.emplace_back(rid, std::move(row));
            }
        }
    }

    // Row-level locks within transaction
    if (inTransaction_) {
        std::vector<std::pair<int64_t, std::string>> lockedRows;
        for (auto& mr : matchRows) {
            bool locked;
            if (noWait || skipLocked) {
                locked = forUpdate
                    ? lockManager_.rowLockExclusiveNoWait(tablename, mr.first)
                    : lockManager_.rowLockSharedNoWait(tablename, mr.first);
            } else {
                locked = forUpdate
                    ? lockManager_.rowLockExclusive(tablename, mr.first)
                    : lockManager_.rowLockShared(tablename, mr.first);
            }
            if (!locked) {
                if (skipLocked) {
                    continue; // skip locked rows
                }
                lockManager_.unlock(tablename);
                return result;
            }
            lockedRows.push_back(std::move(mr));
        }
        matchRows = std::move(lockedRows);
        // Gap locking for FOR UPDATE (simplified: lock gaps between matching rows)
        if (forUpdate && !matchRows.empty()) {
            std::vector<std::string> pkVals;
            for (auto& mr : matchRows) {
                std::string pk = extractPKValue(mr.second, tbl);
                if (!pk.empty()) pkVals.push_back(pk);
            }
            std::sort(pkVals.begin(), pkVals.end());
            for (size_t i = 1; i < pkVals.size(); ++i) {
                lockManager_.lockGap(tablename, pkVals[i-1], pkVals[i]);
            }
            if (!pkVals.empty()) {
                // Lock gap before first and after last
                lockManager_.lockGap(tablename, "", pkVals.front());
                lockManager_.lockGap(tablename, pkVals.back(), "~");
            }
        }
    }

    // ORDER BY (multi-column)
    if (!orderBy.empty()) {
        struct SortKey {
            int64_t rid;
            std::vector<std::tuple<std::string, int64_t, Date>> vals; // (str, num, date) per column
            std::vector<bool> isNulls; // true if value is NULL
        };
        std::vector<SortKey> keys;
        for (auto& mr : matchRows) {
            SortKey k{mr.first, {}, {}};
            for (const auto& spec : orderBy) {
                size_t sortIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == spec.colName) { sortIdx = i; break; }
                }
                if (sortIdx < tbl.len) {
                    std::string val = extractColumnValue(mr.second, tbl, sortIdx);
                    const Column& scol = tbl.cols[sortIdx];
                    k.isNulls.push_back(val.empty());
                    if (scol.dataType == "char" || scol.isVariableLength) {
                        k.vals.emplace_back(val, 0, Date{});
                    } else if (scol.dataType == "date") {
                        k.vals.emplace_back("", 0, val.empty() ? Date{} : Date(val.c_str()));
                    } else {
                        k.vals.emplace_back("", val.empty() ? 0 : parseInt(val), Date{});
                    }
                } else {
                    k.isNulls.push_back(true);
                    k.vals.emplace_back("", 0, Date{});
                }
            }
            keys.push_back(std::move(k));
        }
        std::sort(keys.begin(), keys.end(), [&](const SortKey& a, const SortKey& b) {
            for (size_t i = 0; i < orderBy.size(); ++i) {
                const auto& spec = orderBy[i];
                size_t sortIdx = tbl.len;
                for (size_t j = 0; j < tbl.len; ++j) {
                    if (tbl.cols[j].dataName == spec.colName) { sortIdx = j; break; }
                }
                if (sortIdx >= tbl.len) continue;
                // NULL handling: NULLS FIRST or NULLS LAST (default)
                bool aNull = a.isNulls[i];
                bool bNull = b.isNulls[i];
                if (aNull && bNull) continue;
                if (aNull) return spec.nullsFirst;
                if (bNull) return !spec.nullsFirst;
                const Column& scol = tbl.cols[sortIdx];
                bool less = false, greater = false;
                if (scol.dataType == "char" || scol.isVariableLength) {
                    std::string av = std::get<0>(a.vals[i]);
                    std::string bv = std::get<0>(b.vals[i]);
                    // Apply collation for string comparison
                    if (spec.collation == "nocase" || spec.collation == "NOCASE") {
                        std::string al = av, bl = bv;
                        for (char& c : al) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
                        for (char& c : bl) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
                        less = al < bl;
                        greater = bl < al;
                    } else if (spec.collation == "reverse" || spec.collation == "REVERSE") {
                        less = bv < av;
                        greater = av < bv;
                    } else {
                        // Default: binary collation
                        less = av < bv;
                        greater = bv < av;
                    }
                } else if (scol.dataType == "date") {
                    less = std::get<2>(a.vals[i]) < std::get<2>(b.vals[i]);
                    greater = std::get<2>(b.vals[i]) < std::get<2>(a.vals[i]);
                } else {
                    less = std::get<1>(a.vals[i]) < std::get<1>(b.vals[i]);
                    greater = std::get<1>(b.vals[i]) < std::get<1>(a.vals[i]);
                }
                if (less) return spec.ascending;
                if (greater) return !spec.ascending;
            }
            return false;
        });
        std::vector<std::pair<int64_t, std::string>> sorted;
        for (const auto& k : keys) {
            for (auto& mr : matchRows) {
                if (mr.first == k.rid) { sorted.push_back(std::move(mr)); break; }
            }
        }
        matchRows = std::move(sorted);
    }

    for (auto& mr : matchRows) {
        std::string rowStr;
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            if (!selectCols.empty() && selectCols.find(col.dataName) == selectCols.end())
                continue;
            std::string val = extractColumnValue(mr.second, tbl, i);
            // Apply session timezone for TIMESTAMPTZ columns
            if (timezoneOffsetMinutes != 0 && col.dataType == "timestamptz" && !val.empty()) {
                // Read raw seconds directly from row buffer (extractColumnValue already formats it)
                size_t offset = 0;
                for (size_t j = 0; j < i; ++j) {
                    if (!tbl.cols[j].isVariableLength) offset += tbl.cols[j].dsize;
                }
                if (offset + TIMESTAMP_SIZE <= mr.second.size()) {
                    int64_t utcSec = 0;
                    std::memcpy(&utcSec, mr.second.data() + offset, TIMESTAMP_SIZE);
                    if (utcSec != INF && utcSec != 0) {
                        val = formatTimestampWithTz(utcSec, timezoneOffsetMinutes);
                    }
                }
            }
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
                                    const TableSchema& tbl,
                                    StorageEngine* engine = nullptr,
                                    const std::string& dbname = "") {
    auto getVal = [&](const std::string& arg) -> std::string {
        if (arg.size() >= 2 && ((arg.front() == '\'' && arg.back() == '\'') || (arg.front() == '"' && arg.back() == '"')))
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
    if ((expr.funcName == "char_length" || expr.funcName == "character_length") && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        // Count UTF-8 code points (not bytes)
        size_t count = 0;
        for (size_t i = 0; i < val.size(); ) {
            unsigned char c = static_cast<unsigned char>(val[i]);
            if (c < 0x80) { i += 1; }
            else if ((c & 0xE0) == 0xC0) { i += 2; }
            else if ((c & 0xF0) == 0xE0) { i += 3; }
            else if ((c & 0xF8) == 0xF0) { i += 4; }
            else { i += 1; } // invalid byte, skip
            count++;
        }
        return std::to_string(count);
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
    if (expr.funcName == "abs" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        try {
            double d = std::stod(val);
            d = std::abs(d);
            if (d == std::floor(d)) return std::to_string(static_cast<int64_t>(d));
            return std::to_string(d);
        } catch (...) { return val; }
    }
    if (expr.funcName == "round" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        try {
            double d = std::stod(val);
            d = std::round(d);
            return std::to_string(static_cast<int64_t>(d));
        } catch (...) { return val; }
    }
    if (expr.funcName == "ceil" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        try {
            double d = std::stod(val);
            d = std::ceil(d);
            return std::to_string(static_cast<int64_t>(d));
        } catch (...) { return val; }
    }
    if (expr.funcName == "floor" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        try {
            double d = std::stod(val);
            d = std::floor(d);
            return std::to_string(static_cast<int64_t>(d));
        } catch (...) { return val; }
    }
    if (expr.funcName == "now" || expr.funcName == "current_timestamp") {
        std::time_t t = std::time(nullptr);
        std::tm* tm = std::localtime(&t);
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);
        return std::string(buf);
    }
    if (expr.funcName == "extract" && expr.funcArgs.size() >= 2) {
        std::string val = getVal(expr.funcArgs[1]);
        Date d(val.c_str());
        std::string field = expr.funcArgs[0];
        if (field == "year") return std::to_string(d.year);
        if (field == "month") return std::to_string(d.month);
        if (field == "day") return std::to_string(d.day);
        return "";
    }
    // YEAR / MONTH / DAY - date extraction functions
    if ((expr.funcName == "year" || expr.funcName == "month" || expr.funcName == "day") && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        Date d(val.c_str());
        if (d.year == 0) {
            // Try parsing as timestamp "YYYY-MM-DD HH:MM:SS"
            size_t sp = val.find(' ');
            if (sp != std::string::npos) d = Date(val.substr(0, sp).c_str());
        }
        if (d.year == 0) return "";
        if (expr.funcName == "year") return std::to_string(d.year);
        if (expr.funcName == "month") return std::to_string(d.month);
        return std::to_string(d.day);
    }
    // HOUR / MINUTE / SECOND - timestamp extraction functions
    if ((expr.funcName == "hour" || expr.funcName == "minute" || expr.funcName == "second") && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        // Parse time part: "YYYY-MM-DD HH:MM:SS" or "HH:MM:SS"
        std::string timePart;
        size_t sp = val.find(' ');
        if (sp != std::string::npos) timePart = val.substr(sp + 1);
        else timePart = val;
        // Parse HH:MM:SS
        int h = 0, m = 0, s = 0;
        int tpos[2] = {0, 0};
        int k = 0;
        for (size_t i = 0; i < timePart.size() && k < 2; i++) {
            if (timePart[i] == ':') tpos[k++] = static_cast<int>(i);
        }
        if (tpos[0] && tpos[1]) {
            for (int i = 0; i < tpos[0]; i++) if (timePart[i] >= '0' && timePart[i] <= '9') h = h * 10 + timePart[i] - '0';
            for (int i = tpos[0] + 1; i < tpos[1]; i++) if (timePart[i] >= '0' && timePart[i] <= '9') m = m * 10 + timePart[i] - '0';
            for (size_t i = tpos[1] + 1; i < timePart.size(); i++) if (timePart[i] >= '0' && timePart[i] <= '9') s = s * 10 + timePart[i] - '0';
        } else {
            return "";
        }
        if (expr.funcName == "hour") return std::to_string(h);
        if (expr.funcName == "minute") return std::to_string(m);
        return std::to_string(s);
    }
    if (expr.funcName == "case_when") {
        // Args: cond1, val1, cond2, val2, ..., default
        for (size_t i = 0; i + 1 < expr.funcArgs.size(); i += 2) {
            auto conds = StorageEngine::parseConditions({expr.funcArgs[i]});
            if (!conds.empty() && StorageEngine::evalConditionOnRow(conds[0], rowBuffer, tbl)) {
                return getVal(expr.funcArgs[i + 1]);
            }
        }
        if (!expr.funcArgs.empty()) return getVal(expr.funcArgs.back());
        return "";
    }
    if (expr.funcName == "cast" && expr.funcArgs.size() >= 2) {
        std::string val = getVal(expr.funcArgs[0]);
        std::string targetType = expr.funcArgs[1];
        if (targetType == "char" || targetType == "varchar" || targetType == "text" ||
            targetType == "binary" || targetType == "varbinary" || targetType == "blob") {
            return val;
        }
        if (targetType == "int" || targetType == "integer" || targetType == "tinyint" || targetType == "long") {
            try {
                int64_t num = std::stoll(val);
                return std::to_string(num);
            } catch (...) { return "0"; }
        }
        if (targetType == "date") {
            Date d(val.c_str());
            return (d.year == 0) ? "" : str(d);
        }
        if (targetType == "timestamp") {
            int64_t ts = parseTimestampToSeconds(val);
            return (ts == 0) ? "" : formatTimestampSeconds(ts);
        }
        if (targetType == "float") {
            try { return std::to_string(std::stof(val)); } catch (...) { return "0"; }
        }
        if (targetType == "double" || targetType == "decimal") {
            try { return std::to_string(std::stod(val)); } catch (...) { return "0"; }
        }
        return val;
    }
    if (expr.funcName == "convert" && expr.funcArgs.size() >= 2) {
        // CONVERT(val, type) - alias for CAST
        std::string val = getVal(expr.funcArgs[0]);
        std::string targetType = getVal(expr.funcArgs[1]);
        for (char& c : targetType) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (targetType == "int" || targetType == "integer") {
            try { return std::to_string(static_cast<int64_t>(std::stoll(val))); } catch (...) { return "0"; }
        }
        if (targetType == "float") {
            try { return std::to_string(std::stof(val)); } catch (...) { return "0"; }
        }
        if (targetType == "double") {
            try { return std::to_string(std::stod(val)); } catch (...) { return "0"; }
        }
        if (targetType == "char" || targetType == "varchar" || targetType == "text") {
            return val;
        }
        if (targetType == "date") {
            Date d(val.c_str());
            return (d.year == 0) ? "" : str(d);
        }
        return val;
    }
    if (expr.funcName == "to_number" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        try { return std::to_string(std::stod(val)); } catch (...) { return "0"; }
    }
    if (expr.funcName == "to_char" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        if (expr.funcArgs.size() >= 2) {
            // TO_CHAR(date, fmt) - reuse date_format logic
            std::string fmt = getVal(expr.funcArgs[1]);
            Date d(val.c_str());
            if (d.year == 0) return val;
            std::string out;
            for (size_t i = 0; i < fmt.size(); ++i) {
                if (fmt[i] == '%' && i + 1 < fmt.size()) {
                    char c = static_cast<char>(std::tolower(static_cast<unsigned char>(fmt[i + 1])));
                    char buf[16];
                    if (c == 'y') { std::snprintf(buf, sizeof(buf), "%04d", d.year); out += buf; }
                    else if (c == 'm') { std::snprintf(buf, sizeof(buf), "%02d", d.month); out += buf; }
                    else if (c == 'd') { std::snprintf(buf, sizeof(buf), "%02d", d.day); out += buf; }
                    else { out += fmt[i]; out += fmt[i + 1]; }
                    ++i;
                } else {
                    out += fmt[i];
                }
            }
            return out;
        }
        return val;
    }
    if (expr.funcName == "to_date" && expr.funcArgs.size() >= 1) {
        std::string val = getVal(expr.funcArgs[0]);
        Date d(val.c_str());
        return (d.year == 0) ? "" : str(d);
    }
    if (expr.funcName == "coalesce") {
        for (const auto& arg : expr.funcArgs) {
            std::string v = getVal(arg);
            if (!v.empty()) return v;
        }
        return "";
    }
    if (expr.funcName == "nullif" && expr.funcArgs.size() >= 2) {
        std::string v1 = getVal(expr.funcArgs[0]);
        std::string v2 = getVal(expr.funcArgs[1]);
        return (v1 == v2) ? "" : v1;
    }
    if (expr.funcName == "replace" && expr.funcArgs.size() >= 3) {
        std::string str = getVal(expr.funcArgs[0]);
        std::string from = getVal(expr.funcArgs[1]);
        std::string to = getVal(expr.funcArgs[2]);
        size_t pos = 0;
        while ((pos = str.find(from, pos)) != std::string::npos) {
            str.replace(pos, from.size(), to);
            pos += to.size();
        }
        return str;
    }
    if (expr.funcName == "position" && expr.funcArgs.size() >= 2) {
        std::string substr = getVal(expr.funcArgs[0]);
        std::string str = getVal(expr.funcArgs[1]);
        size_t pos = str.find(substr);
        return (pos == std::string::npos) ? "0" : std::to_string(pos + 1);
    }
    if (expr.funcName == "instr" && expr.funcArgs.size() >= 2) {
        std::string str = getVal(expr.funcArgs[0]);
        std::string substr = getVal(expr.funcArgs[1]);
        size_t pos = str.find(substr);
        return (pos == std::string::npos) ? "0" : std::to_string(pos + 1);
    }
    if (expr.funcName == "lpad" && expr.funcArgs.size() >= 2) {
        std::string str = getVal(expr.funcArgs[0]);
        try {
            size_t target = static_cast<size_t>(std::stoul(getVal(expr.funcArgs[1])));
            std::string pad = (expr.funcArgs.size() >= 3) ? getVal(expr.funcArgs[2]) : " ";
            if (pad.empty()) return str;
            if (str.size() >= target) return str.substr(0, target);
            std::string out;
            while (out.size() + str.size() < target) {
                out += pad;
            }
            out = out.substr(0, target - str.size());
            return out + str;
        } catch (...) { return str; }
    }
    if (expr.funcName == "rpad" && expr.funcArgs.size() >= 2) {
        std::string str = getVal(expr.funcArgs[0]);
        try {
            size_t target = static_cast<size_t>(std::stoul(getVal(expr.funcArgs[1])));
            std::string pad = (expr.funcArgs.size() >= 3) ? getVal(expr.funcArgs[2]) : " ";
            if (pad.empty()) return str;
            if (str.size() >= target) return str.substr(0, target);
            std::string out = str;
            while (out.size() < target) {
                out += pad;
            }
            return out.substr(0, target);
        } catch (...) { return str; }
    }
    if (expr.funcName == "reverse" && !expr.funcArgs.empty()) {
        std::string str = getVal(expr.funcArgs[0]);
        std::reverse(str.begin(), str.end());
        return str;
    }
    if (expr.funcName == "greatest" && !expr.funcArgs.empty()) {
        std::string best;
        bool isFirst = true;
        bool allNum = true;
        for (const auto& a : expr.funcArgs) {
            std::string v = getVal(a);
            if (v.empty()) continue;
            try { std::stod(v); } catch (...) { allNum = false; }
        }
        for (const auto& a : expr.funcArgs) {
            std::string v = getVal(a);
            if (v.empty()) continue;
            if (isFirst) { best = v; isFirst = false; continue; }
            if (allNum) {
                try {
                    if (std::stod(v) > std::stod(best)) best = v;
                } catch (...) {}
            } else {
                if (v > best) best = v;
            }
        }
        return best;
    }
    if (expr.funcName == "least" && !expr.funcArgs.empty()) {
        std::string best;
        bool isFirst = true;
        bool allNum = true;
        for (const auto& a : expr.funcArgs) {
            std::string v = getVal(a);
            if (v.empty()) continue;
            try { std::stod(v); } catch (...) { allNum = false; }
        }
        for (const auto& a : expr.funcArgs) {
            std::string v = getVal(a);
            if (v.empty()) continue;
            if (isFirst) { best = v; isFirst = false; continue; }
            if (allNum) {
                try {
                    if (std::stod(v) < std::stod(best)) best = v;
                } catch (...) {}
            } else {
                if (v < best) best = v;
            }
        }
        return best;
    }
    if ((expr.funcName == "if" || expr.funcName == "iif") && expr.funcArgs.size() >= 3) {
        std::string cond = getVal(expr.funcArgs[0]);
        std::string trueVal = getVal(expr.funcArgs[1]);
        std::string falseVal = getVal(expr.funcArgs[2]);
        bool isTrue = false;
        if (cond == "true" || cond == "1") isTrue = true;
        else if (cond == "false" || cond == "0" || cond.empty()) isTrue = false;
        else {
            try { isTrue = (std::stod(cond) != 0); } catch (...) { isTrue = !cond.empty(); }
        }
        return isTrue ? trueVal : falseVal;
    }
    if (expr.funcName == "power" && expr.funcArgs.size() >= 2) {
        try {
            double base = std::stod(getVal(expr.funcArgs[0]));
            double exp = std::stod(getVal(expr.funcArgs[1]));
            double r = std::pow(base, exp);
            std::ostringstream oss;
            oss << r;
            return oss.str();
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "sqrt" && expr.funcArgs.size() >= 1) {
        try {
            double v = std::stod(getVal(expr.funcArgs[0]));
            double r = std::sqrt(v);
            std::ostringstream oss;
            oss << r;
            return oss.str();
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "mod" && expr.funcArgs.size() >= 2) {
        try {
            int64_t a = std::stoll(getVal(expr.funcArgs[0]));
            int64_t b = std::stoll(getVal(expr.funcArgs[1]));
            if (b == 0) return "0";
            return std::to_string(a % b);
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "ln" && !expr.funcArgs.empty()) {
        try { return std::to_string(std::log(std::stod(getVal(expr.funcArgs[0])))); } catch (...) { return "0"; }
    }
    if (expr.funcName == "log" && !expr.funcArgs.empty()) {
        try {
            if (expr.funcArgs.size() >= 2) {
                double base = std::stod(getVal(expr.funcArgs[0]));
                double x = std::stod(getVal(expr.funcArgs[1]));
                return std::to_string(std::log(x) / std::log(base));
            }
            return std::to_string(std::log10(std::stod(getVal(expr.funcArgs[0]))));
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "exp" && !expr.funcArgs.empty()) {
        try { return std::to_string(std::exp(std::stod(getVal(expr.funcArgs[0])))); } catch (...) { return "0"; }
    }
    if (expr.funcName == "random" || expr.funcName == "rand") {
        return std::to_string(static_cast<double>(std::rand()) / RAND_MAX);
    }
    if (expr.funcName == "sin" && !expr.funcArgs.empty()) {
        try {
            double d = std::stod(getVal(expr.funcArgs[0]));
            std::ostringstream oss;
            oss << std::sin(d);
            return oss.str();
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "cos" && !expr.funcArgs.empty()) {
        try {
            double d = std::stod(getVal(expr.funcArgs[0]));
            std::ostringstream oss;
            oss << std::cos(d);
            return oss.str();
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "tan" && !expr.funcArgs.empty()) {
        try {
            double d = std::stod(getVal(expr.funcArgs[0]));
            std::ostringstream oss;
            oss << std::tan(d);
            return oss.str();
        } catch (...) { return "0"; }
    }
    if (expr.funcName == "split_part" && expr.funcArgs.size() >= 3) {
        std::string str = getVal(expr.funcArgs[0]);
        std::string delimiter = getVal(expr.funcArgs[1]);
        try {
            int part = std::stoi(getVal(expr.funcArgs[2]));
            if (part <= 0) return "";
            size_t pos = 0;
            int current = 1;
            while (current < part) {
                pos = str.find(delimiter, pos);
                if (pos == std::string::npos) return "";
                pos += delimiter.size();
                current++;
            }
            size_t end = str.find(delimiter, pos);
            if (end == std::string::npos) return str.substr(pos);
            return str.substr(pos, end - pos);
        } catch (...) { return ""; }
    }
    if (expr.funcName == "uuid_generate") {
        // Generate UUID v4: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
        // where y is one of {8, 9, a, b}
        const char hex[] = "0123456789abcdef";
        std::string uuid(36, '\0');
        for (size_t i = 0; i < 36; ++i) {
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                uuid[i] = '-';
            } else if (i == 14) {
                uuid[i] = '4';
            } else if (i == 19) {
                uuid[i] = hex[8 + (std::rand() & 3)];
            } else {
                uuid[i] = hex[std::rand() & 15];
            }
        }
        return uuid;
    }
    if (expr.funcName == "date_add" && expr.funcArgs.size() >= 3) {
        std::string dstr = getVal(expr.funcArgs[0]);
        Date d(dstr.c_str());
        if (d.year == 0) return "";
        try {
            int n = std::stoi(getVal(expr.funcArgs[1]));
            std::string unit = getVal(expr.funcArgs[2]);
            for (char& c : unit) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            Date result;
            if (unit == "day" || unit == "days") result = d + n;
            else if (unit == "month" || unit == "months") result = dateAddMonths(d, n);
            else if (unit == "year" || unit == "years") result = dateAddYears(d, n);
            else return "";
            return str(result);
        } catch (...) { return ""; }
    }
    if (expr.funcName == "date_sub" && expr.funcArgs.size() >= 3) {
        std::string dstr = getVal(expr.funcArgs[0]);
        Date d(dstr.c_str());
        if (d.year == 0) return "";
        try {
            int n = std::stoi(getVal(expr.funcArgs[1]));
            std::string unit = getVal(expr.funcArgs[2]);
            for (char& c : unit) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            Date result;
            if (unit == "day" || unit == "days") result = d - n;
            else if (unit == "month" || unit == "months") result = dateAddMonths(d, -n);
            else if (unit == "year" || unit == "years") result = dateAddYears(d, -n);
            else return "";
            return str(result);
        } catch (...) { return ""; }
    }
    if (expr.funcName == "datediff" && expr.funcArgs.size() >= 2) {
        std::string a = getVal(expr.funcArgs[0]);
        std::string b = getVal(expr.funcArgs[1]);
        Date d1(a.c_str()), d2(b.c_str());
        if (d1.year == 0 || d2.year == 0) return "";
        return std::to_string(d1 - d2);
    }
    if (expr.funcName == "age" && expr.funcArgs.size() >= 2) {
        std::string a = getVal(expr.funcArgs[0]);
        std::string b = getVal(expr.funcArgs[1]);
        Date d1(a.c_str()), d2(b.c_str());
        if (d1.year == 0 || d2.year == 0) return "";
        int years = d1.year - d2.year;
        if (d1.month < d2.month || (d1.month == d2.month && d1.day < d2.day)) {
            years--;
        }
        return std::to_string(years);
    }
    if (expr.funcName == "date_trunc" && expr.funcArgs.size() >= 2) {
        std::string unit = getVal(expr.funcArgs[0]);
        for (char& c : unit) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        std::string dstr = getVal(expr.funcArgs[1]);
        Date d(dstr.c_str());
        if (d.year == 0) return "";
        Date result = d;
        if (unit == "year") { result.month = 1; result.day = 1; }
        else if (unit == "month") { result.day = 1; }
        else if (unit == "day") { /* unchanged */ }
        else return "";
        return str(result);
    }
    if (expr.funcName == "date_format" && expr.funcArgs.size() >= 2) {
        std::string dstr = getVal(expr.funcArgs[0]);
        std::string fmt = getVal(expr.funcArgs[1]);
        Date d(dstr.c_str());
        if (d.year == 0) return "";
        std::string out;
        for (size_t i = 0; i < fmt.size(); ++i) {
            if (fmt[i] == '%' && i + 1 < fmt.size()) {
                char c = static_cast<char>(std::tolower(static_cast<unsigned char>(fmt[i + 1])));
                char buf[16];
                if (c == 'y') { std::snprintf(buf, sizeof(buf), "%04d", d.year); out += buf; }
                else if (c == 'm') { std::snprintf(buf, sizeof(buf), "%02d", d.month); out += buf; }
                else if (c == 'd') { std::snprintf(buf, sizeof(buf), "%02d", d.day); out += buf; }
                else { out += fmt[i]; out += fmt[i + 1]; }
                ++i;
            } else {
                out += fmt[i];
            }
        }
        return out;
    }
    if (expr.funcName == "json_extract" && expr.funcArgs.size() >= 2) {
        std::string jsonStr = getVal(expr.funcArgs[0]);
        std::string path = getVal(expr.funcArgs[1]);
        // Simple JSON path extractor: supports $.key or $['key'] or $.key1.key2
        // Trim whitespace and surrounding quotes from path
        path = trim(path);
        if (path.size() >= 2 && path.front() == '\'' && path.back() == '\'') {
            path = path.substr(1, path.size() - 2);
        }
        if (path.empty() || path[0] != '$') return jsonStr;
        // Remove leading $.
        std::string keyPath = (path.size() > 1 && path[1] == '.') ? path.substr(2) : path.substr(1);
        if (keyPath.empty()) return jsonStr;
        // Split keyPath by '.' or ['']
        std::vector<std::string> keys;
        {
            std::string cur;
            size_t i = 0;
            while (i < keyPath.size()) {
                if (keyPath[i] == '.') {
                    if (!cur.empty()) { keys.push_back(cur); cur.clear(); }
                    ++i;
                } else if (keyPath[i] == '[') {
                    if (!cur.empty()) { keys.push_back(cur); cur.clear(); }
                    ++i;
                    if (i < keyPath.size() && keyPath[i] == '\'') {
                        ++i;
                        while (i < keyPath.size() && keyPath[i] != '\'') { cur.push_back(keyPath[i]); ++i; }
                        if (i < keyPath.size()) ++i; // skip closing '
                    } else {
                        while (i < keyPath.size() && keyPath[i] != ']') { cur.push_back(keyPath[i]); ++i; }
                    }
                    if (i < keyPath.size() && keyPath[i] == ']') ++i;
                    if (!cur.empty()) { keys.push_back(cur); cur.clear(); }
                } else {
                    cur.push_back(keyPath[i]);
                    ++i;
                }
            }
            if (!cur.empty()) keys.push_back(cur);
        }
        // Navigate through JSON string levels
        std::string current = jsonStr;
        for (const std::string& key : keys) {
            if (key.empty()) continue;
            // Find the key in current JSON object
            std::string searchKey = "\"" + key + "\"";
            size_t keyPos = current.find(searchKey);
            if (keyPos == std::string::npos) return "";
            size_t colonPos = current.find(':', keyPos + searchKey.size());
            if (colonPos == std::string::npos) return "";
            size_t valStart = colonPos + 1;
            while (valStart < current.size() && std::isspace(static_cast<unsigned char>(current[valStart]))) ++valStart;
            if (valStart >= current.size()) return "";
            if (current[valStart] == '{') {
                // Object value - extract matching braces
                int depth = 1;
                size_t end = valStart + 1;
                while (end < current.size() && depth > 0) {
                    if (current[end] == '{') ++depth;
                    else if (current[end] == '}') --depth;
                    ++end;
                }
                current = current.substr(valStart, end - valStart);
            } else if (current[valStart] == '[') {
                // Array value - extract matching brackets
                int depth = 1;
                size_t end = valStart + 1;
                while (end < current.size() && depth > 0) {
                    if (current[end] == '[') ++depth;
                    else if (current[end] == ']') --depth;
                    ++end;
                }
                current = current.substr(valStart, end - valStart);
            } else if (current[valStart] == '\"') {
                // String value
                size_t end = valStart + 1;
                while (end < current.size() && current[end] != '\"') {
                    if (current[end] == '\\' && end + 1 < current.size()) end += 2;
                    else ++end;
                }
                current = current.substr(valStart + 1, end - valStart - 1);
            } else {
                // Number, boolean, null
                size_t end = valStart;
                while (end < current.size() && current[end] != ',' && current[end] != '}' && current[end] != ']') ++end;
                current = trim(current.substr(valStart, end - valStart));
            }
        }
        return current;
    }
    if (expr.funcName == "json_value" && expr.funcArgs.size() >= 2) {
        // json_value is alias for json_extract but returns unquoted string
        StorageEngine::SelectExpr subExpr;
        subExpr.isScalar = true;
        subExpr.funcName = "json_extract";
        subExpr.funcArgs = expr.funcArgs;
        return applyScalarFunc(subExpr, rowBuffer, tbl, engine, dbname);
    }
    if (expr.funcName == "subquery" && !expr.funcArgs.empty() && engine) {
        // Simplified scalar subquery: funcArgs[0] = "select col from table [where ...]"
        std::string subSql = expr.funcArgs[0];
        size_t fromPos = subSql.find("from");
        if (fromPos == std::string::npos) return "";
        std::string colsStr = trim(subSql.substr(6, fromPos - 6));
        size_t wherePos = subSql.find("where", fromPos);
        std::string subTname = trim(subSql.substr(fromPos + 4,
            (wherePos != std::string::npos) ? (wherePos - fromPos - 4)
            : (subSql.size() - fromPos - 4)));
        std::vector<std::string> subConds;
        if (wherePos != std::string::npos) {
            std::string condStr = trim(subSql.substr(wherePos + 5));
            // Correlated subquery: replace outer column refs with their literal values
            for (size_t i = 0; i < tbl.len; ++i) {
                const std::string& colName = tbl.cols[i].dataName;
                std::string colVal = StorageEngine::extractColumnValue(rowBuffer, tbl, i);
                // Quote string-type values
                bool needQuote = (tbl.cols[i].dataType == "char" ||
                                  tbl.cols[i].dataType == "varchar" ||
                                  tbl.cols[i].dataType == "text" ||
                                  tbl.cols[i].dataType == "binary" ||
                                  tbl.cols[i].dataType == "varbinary" ||
                                  tbl.cols[i].dataType == "blob" ||
                                  tbl.cols[i].dataType == "date" ||
                                  tbl.cols[i].dataType == "timestamp" ||
                                  tbl.cols[i].dataType == "datetime" ||
                                  tbl.cols[i].dataType == "time");
                std::string replacement = needQuote ? ("'" + colVal + "'") : colVal;
                // Replace whole-word occurrences of colName in condStr
                std::string newCond;
                size_t p = 0;
                while (p < condStr.size()) {
                    size_t found = condStr.find(colName, p);
                    if (found == std::string::npos) {
                        newCond += condStr.substr(p);
                        break;
                    }
                    bool leftOk = (found == 0) || !isalnum(static_cast<unsigned char>(condStr[found-1]));
                    bool rightOk = (found + colName.size() == condStr.size()) ||
                                   !isalnum(static_cast<unsigned char>(condStr[found+colName.size()]));
                    newCond += condStr.substr(p, found - p);
                    if (leftOk && rightOk) {
                        newCond += replacement;
                    } else {
                        newCond += condStr.substr(found, colName.size());
                    }
                    p = found + colName.size();
                }
                condStr = newCond;
            }
            // Transform col<op>value into <op>col value format expected by parseConditions
            if (!condStr.empty()) {
                size_t opStart = std::string::npos;
                size_t opLen = 0;
                for (size_t k = 0; k < condStr.size(); ++k) {
                    char c = condStr[k];
                    if (c == '>' || c == '<' || c == '=' || c == '!') {
                        opStart = k;
                        opLen = 1;
                        if (k + 1 < condStr.size() && (condStr[k+1] == '=' || condStr[k+1] == '>')) {
                            opLen = 2;
                        }
                        break;
                    }
                }
                if (opStart != std::string::npos) {
                    std::string opStr = condStr.substr(opStart, opLen);
                    std::string before = condStr.substr(0, opStart);
                    std::string after = condStr.substr(opStart + opLen);
                    condStr = opStr + before + " " + after;
                }
                subConds.push_back(condStr);
            }
        }
        std::set<std::string> subSelectCols;
        // Inline split colsStr by comma (avoid dependency on main.cpp's splitSelectColumns)
        {
            std::string cur;
            int depth = 0;
            for (char ch : colsStr) {
                if (ch == '(') depth++;
                else if (ch == ')') depth--;
                if (ch == ',' && depth == 0) {
                    subSelectCols.insert(trim(cur));
                    cur.clear();
                } else {
                    cur.push_back(ch);
                }
            }
            if (!trim(cur).empty()) subSelectCols.insert(trim(cur));
        }
        auto rows = engine->query(dbname, subTname, subConds, subSelectCols);
        if (rows.empty()) return "";
        std::string firstRow = trim(rows[0]);
        size_t sp = firstRow.find(' ');
        return (sp == std::string::npos) ? firstRow : trim(firstRow.substr(0, sp));
    }
    return "";
}

std::vector<std::string> StorageEngine::queryExpr(const std::string& dbname,
                                                   const std::string& tablename,
                                                   const std::vector<std::string>& conditions,
                                                   const std::vector<SelectExpr>& exprs,
                                                   const std::vector<OrderBySpec>& orderBy) {
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
        forEachRow(dbname, tablename, [&](uint32_t pid, uint16_t sid, const char* data, size_t len) {
            matchRows.emplace_back(encodeRid(pid, sid), std::string(data, len));
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

    // ORDER BY (multi-column)
    if (!orderBy.empty()) {
        struct SortKey {
            int64_t rid;
            std::vector<std::tuple<std::string, int64_t, Date>> vals;
            std::vector<bool> isNulls;
        };
        std::vector<SortKey> keys;
        for (auto& mr : matchRows) {
            SortKey k{mr.first, {}, {}};
            for (const auto& spec : orderBy) {
                size_t sortIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == spec.colName) { sortIdx = i; break; }
                }
                if (sortIdx < tbl.len) {
                    std::string val = extractColumnValue(mr.second, tbl, sortIdx);
                    const Column& scol = tbl.cols[sortIdx];
                    k.isNulls.push_back(val.empty());
                    if (scol.dataType == "char" || scol.isVariableLength) {
                        k.vals.push_back({val, 0, {}});
                    } else if (scol.dataType == "date") {
                        k.vals.push_back({"", 0, val.empty() ? Date{} : Date(val.c_str())});
                    } else {
                        k.vals.push_back({"", val.empty() ? 0 : parseInt(val), {}});
                    }
                } else {
                    k.isNulls.push_back(true);
                    k.vals.push_back({"", 0, {}});
                }
            }
            keys.push_back(std::move(k));
        }
        std::sort(keys.begin(), keys.end(), [&](const SortKey& a, const SortKey& b) {
            for (size_t i = 0; i < orderBy.size(); ++i) {
                const auto& spec = orderBy[i];
                size_t sortIdx = tbl.len;
                for (size_t j = 0; j < tbl.len; ++j) {
                    if (tbl.cols[j].dataName == spec.colName) { sortIdx = j; break; }
                }
                if (sortIdx >= tbl.len) continue;
                bool aNull = a.isNulls[i];
                bool bNull = b.isNulls[i];
                if (aNull && bNull) continue;
                if (aNull) return spec.nullsFirst;
                if (bNull) return !spec.nullsFirst;
                const Column& scol = tbl.cols[sortIdx];
                bool less = false, greater = false;
                if (scol.dataType == "char" || scol.isVariableLength) {
                    less = std::get<0>(a.vals[i]) < std::get<0>(b.vals[i]);
                    greater = std::get<0>(b.vals[i]) < std::get<0>(a.vals[i]);
                } else if (scol.dataType == "date") {
                    less = std::get<2>(a.vals[i]) < std::get<2>(b.vals[i]);
                    greater = std::get<2>(b.vals[i]) < std::get<2>(a.vals[i]);
                } else {
                    less = std::get<1>(a.vals[i]) < std::get<1>(b.vals[i]);
                    greater = std::get<1>(b.vals[i]) < std::get<1>(a.vals[i]);
                }
                if (less) return spec.ascending;
                if (greater) return !spec.ascending;
            }
            return false;
        });
        std::vector<std::pair<int64_t, std::string>> sorted;
        for (const auto& k : keys) {
            for (auto& mr : matchRows) {
                if (mr.first == k.rid) { sorted.push_back(std::move(mr)); break; }
            }
        }
        matchRows = std::move(sorted);
    }

    for (auto& mr : matchRows) {
        std::string rowStr;
        for (const auto& expr : exprs) {
            std::string val;
            if (expr.isScalar) {
                val = applyScalarFunc(expr, mr.second, tbl, this, dbname);
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
        std::string groupConcat;
        bool groupConcatFirst = true;
        std::vector<std::string> jsonAggVals;

        bool isDistinctCount = (func == "count" && colName.size() > 9 && colName.substr(0, 9) == "distinct ");
        std::string actualColName = isDistinctCount ? colName.substr(9) : colName;
        bool isJsonAgg = (func == "json_agg" || func == "jsonb_agg");

        if (func != "count" || actualColName != "*") {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == actualColName) {
                    colIdx = i;
                    isInt = (!tbl.cols[i].isVariableLength && tbl.cols[i].dataType != "char" && tbl.cols[i].dataType != "date");
                    isDate = (tbl.cols[i].dataType == "date");
                    isChar = (tbl.cols[i].dataType == "char" || tbl.cols[i].isVariableLength);
                    break;
                }
            }
        }

        if (isDistinctCount) {
            std::set<std::string> distinctVals;
            for (int64_t rid : matchIds) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) distinctVals.insert(val);
            }
            count = static_cast<int64_t>(distinctVals.size());
        } else {
            for (int64_t rid : matchIds) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                if (func == "count") {
                    if (colName == "*") { count++; continue; }
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    if (!val.empty()) count++;
                } else if (func == "group_concat" || func == "string_agg") {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    if (val.empty()) continue;
                    if (!groupConcatFirst) groupConcat += ",";
                    groupConcat += val;
                    groupConcatFirst = false;
                } else if (isJsonAgg) {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    jsonAggVals.push_back(val);
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
        }

        if (func == "count") rowResult += transstr(count) + ' ';
        else if (func == "sum") rowResult += transstr(sum) + ' ';
        else if (func == "avg") rowResult += (count == 0 ? "0" : std::to_string(static_cast<double>(sum) / count)) + ' ';
        else if (func == "group_concat" || func == "string_agg") {
            rowResult += (groupConcat.empty() ? "NULL" : groupConcat) + ' ';
        }
        else if (isJsonAgg) {
            std::string json = "[";
            bool first = true;
            for (const auto& v : jsonAggVals) {
                if (!first) json += ",";
                if (v.empty()) {
                    json += "null";
                } else if (isInt || v == "true" || v == "false" || v == "null") {
                    json += v;
                } else {
                    // String: escape quotes and wrap
                    std::string esc;
                    for (char c : v) {
                        if (c == '"' || c == '\\') esc += '\\';
                        esc += c;
                    }
                    json += '"' + esc + '"';
                }
                first = false;
            }
            json += "]";
            rowResult += json + ' ';
        }
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
    const std::vector<std::string>& groupByCols,
    const std::vector<std::string>& havingConds) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;
    lockManager_.lockShared(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();

    // Find group-by column indices
    std::vector<size_t> groupIdxs;
    for (const auto& gcol : groupByCols) {
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == gcol) { groupIdxs.push_back(i); break; }
        }
    }
    if (groupIdxs.size() != groupByCols.size()) {
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

    // Read composite group key for each matching row
    auto readGroupKey = [&](int64_t rid) -> std::string {
        std::string row;
        if (!readRowByRid(pa, rid, row, tbl)) return "";
        std::string key;
        for (size_t idx : groupIdxs) {
            if (!key.empty()) key += "\x01";
            key += extractColumnValue(row, tbl, idx);
        }
        return key;
    };

    // Group rows by group key
    std::map<std::string, std::vector<int64_t>> groups;
    for (int64_t rowIdx : matchIds) {
        groups[readGroupKey(rowIdx)].push_back(rowIdx);
    }

    // Helper: compute aggregate for a group
    auto computeAgg = [&](const std::vector<int64_t>& gids,
                           const std::string& func, const std::string& colName) -> std::string {
        bool isDistinctCount = (func == "count" && colName.size() > 9 && colName.substr(0, 9) == "distinct ");
        std::string actualColName = isDistinctCount ? colName.substr(9) : colName;
        bool isJsonAgg = (func == "json_agg" || func == "jsonb_agg");
        size_t colIdx = tbl.len;
        bool isInt = false, isDate = false, isChar = false;
        if (func != "count" || actualColName != "*") {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == actualColName) {
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
        std::string groupConcat;
        bool groupConcatFirst = true;
        std::vector<std::string> jsonAggVals;
        bool jsonAggFirst = true;

        if (isDistinctCount) {
            std::set<std::string> distinctVals;
            for (int64_t rid : gids) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) distinctVals.insert(val);
            }
            count = static_cast<int64_t>(distinctVals.size());
        } else {
            for (int64_t rid : gids) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;

                if (func == "count") {
                    if (colName == "*") { count++; continue; }
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    if (!val.empty()) count++;
                } else if (func == "group_concat" || func == "string_agg") {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    if (val.empty()) continue;
                    if (!groupConcatFirst) groupConcat += ",";
                    groupConcat += val;
                    groupConcatFirst = false;
                } else if (isJsonAgg) {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    jsonAggVals.push_back(val);
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
        }
        if (func == "count") return transstr(count);
        if (func == "sum") return transstr(sum);
        if (func == "avg") return (count == 0 ? "0" : std::to_string(static_cast<double>(sum) / count));
        if (func == "group_concat" || func == "string_agg") return groupConcat.empty() ? "NULL" : groupConcat;
        if (isJsonAgg) {
            std::string json = "[";
            bool first = true;
            for (const auto& v : jsonAggVals) {
                if (!first) json += ",";
                if (v.empty()) {
                    json += "null";
                } else if (isInt || v == "true" || v == "false" || v == "null") {
                    json += v;
                } else {
                    std::string esc;
                    for (char c : v) {
                        if (c == '"' || c == '\\') esc += '\\';
                        esc += c;
                    }
                    json += '"' + esc + '"';
                }
                first = false;
            }
            json += "]";
            return json;
        }
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

        std::string row;
        size_t kp = 0;
        while (kp < gkey.size()) {
            size_t sep = gkey.find('\x01', kp);
            std::string part = (sep == std::string::npos) ? gkey.substr(kp) : gkey.substr(kp, sep - kp);
            if (!row.empty()) row += ' ';
            row += part;
            if (sep == std::string::npos) break;
            kp = sep + 1;
        }
        row += ' ';
        for (const auto& item : items) {
            row += computeAgg(gids, item.first, item.second) + ' ';
        }
        result.push_back(row);
    }
    lockManager_.unlock(tablename);
    return result;
}

// Simple comma-split for function arguments (used by ORDER BY expression)
static std::vector<std::string> splitExprArgs(const std::string& s) {
    std::vector<std::string> args;
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && isspace(static_cast<unsigned char>(s[i]))) ++i;
        if (i >= s.size()) break;
        std::string arg;
        if (s[i] == '\'') {
            arg += s[i++];
            while (i < s.size() && s[i] != '\'') arg += s[i++];
            if (i < s.size()) arg += s[i++];
        } else {
            while (i < s.size() && s[i] != ',') arg += s[i++];
        }
        args.push_back(trim(arg));
        if (i < s.size() && s[i] == ',') ++i;
    }
    return args;
}

// ========================================================================
// Post-query ORDER BY expression sorting
// ========================================================================

std::vector<std::string> StorageEngine::sortByExpression(
    const std::string& dbname, const std::string& tablename,
    std::vector<std::string> rows,
    const std::vector<OrderBySpec>& exprSpecs) const {
    if (rows.empty() || exprSpecs.empty()) return rows;
    TableSchema tbl = getTableSchema(dbname, tablename);

    struct SortRow {
        std::string rowStr;
        std::vector<std::string> exprVals;
    };
    std::vector<SortRow> sortRows;
    for (const auto& rowStr : rows) {
        SortRow sr;
        sr.rowStr = rowStr;
        // Parse row into column values
        std::map<std::string, std::string> rowData;
        std::stringstream ss(rowStr);
        std::string val;
        for (size_t i = 0; i < tbl.len && ss >> val; ++i) {
            rowData[tbl.cols[i].dataName] = val;
        }
        for (const auto& spec : exprSpecs) {
            std::string ev;
            auto getCol = [&](const std::string& name) -> std::string {
                auto it = rowData.find(name);
                return (it != rowData.end()) ? it->second : "";
            };
            std::string argVal = getCol(spec.exprArg);
            if (spec.exprFunc == "length") {
                ev = std::to_string(argVal.size());
            } else if (spec.exprFunc == "upper") {
                ev = argVal;
                for (char& c : ev) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            } else if (spec.exprFunc == "lower") {
                ev = argVal;
                for (char& c : ev) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            } else if (spec.exprFunc == "abs") {
                try { ev = std::to_string(std::llabs(std::stoll(argVal))); } catch (...) { ev = "0"; }
            } else {
                ev = argVal;
            }
            sr.exprVals.push_back(ev);
        }
        sortRows.push_back(std::move(sr));
    }

    std::stable_sort(sortRows.begin(), sortRows.end(),
        [&](const SortRow& a, const SortRow& b) {
            for (size_t i = 0; i < exprSpecs.size(); ++i) {
                const auto& spec = exprSpecs[i];
                const std::string& av = a.exprVals[i];
                const std::string& bv = b.exprVals[i];
                bool aNull = av.empty();
                bool bNull = bv.empty();
                if (aNull && bNull) continue;
                if (aNull) return spec.nullsFirst;
                if (bNull) return !spec.nullsFirst;
                bool less = false, greater = false;
                // Try numeric comparison first
                try {
                    int64_t na = std::stoll(av);
                    int64_t nb = std::stoll(bv);
                    less = na < nb;
                    greater = na > nb;
                } catch (...) {
                    less = av < bv;
                    greater = av > bv;
                }
                if (less) return spec.ascending;
                if (greater) return !spec.ascending;
            }
            return false;
        });

    std::vector<std::string> result;
    for (auto& sr : sortRows) result.push_back(std::move(sr.rowStr));
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

    // Predicate pushdown: classify conditions by which table they reference
    std::vector<Condition> leftConds, rightConds, joinConds;
    for (const auto& c : conds) {
        bool onLeft = (colMap.find(c.colName) != colMap.end() && colMap.at(c.colName).isLeft);
        bool onRight = false;
        auto it = colMap.find(c.colName);
        if (it != colMap.end()) onRight = !it->second.isLeft;
        // Check if value references a column from the other table
        bool valIsCol = false;
        auto vit = colMap.find(c.value);
        if (vit != colMap.end()) valIsCol = true;
        if (onLeft && !valIsCol) leftConds.push_back(c);
        else if (onRight && !valIsCol) rightConds.push_back(c);
        else joinConds.push_back(c);
    }

    // Apply predicate pushdown: filter rows before JOIN
    auto evalSingleCond = [&](const Condition& c, const std::string& row,
                               const TableSchema& tbl) -> bool {
        auto it = colMap.find(c.colName);
        if (it == colMap.end()) return false;
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

    if (!leftConds.empty()) {
        leftRows.erase(std::remove_if(leftRows.begin(), leftRows.end(),
            [&](const std::string& row) {
                for (const auto& c : leftConds) if (!evalSingleCond(c, row, leftTbl)) return true;
                return false;
            }), leftRows.end());
    }
    if (!rightConds.empty()) {
        rightRows.erase(std::remove_if(rightRows.begin(), rightRows.end(),
            [&](const std::string& row) {
                for (const auto& c : rightConds) if (!evalSingleCond(c, row, rightTbl)) return true;
                return false;
            }), rightRows.end());
    }

    // Find left and right column indices for ON condition
    size_t leftColIdx = leftTbl.len;
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol) { leftColIdx = i; break; }
    }
    size_t rightColIdx = rightTbl.len;
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol) { rightColIdx = i; break; }
    }

    // JOIN optimization: build hash table on right table's join column
    std::unordered_map<std::string, std::vector<std::string>> rightHash;
    if (rightColIdx < rightTbl.len) {
        for (const auto& rr : rightRows) {
            std::string rv = extractColumnValue(rr, rightTbl, rightColIdx);
            rightHash[rv].push_back(rr);
        }
    }

    for (const auto& lr : leftRows) {
        // ON condition via hash probe
        if (leftColIdx >= leftTbl.len || rightColIdx >= rightTbl.len) continue;
        std::string lv = extractColumnValue(lr, leftTbl, leftColIdx);
        auto it = rightHash.find(lv);
        if (it == rightHash.end()) continue;

        for (const auto& rr : it->second) {
            // WHERE conditions (only cross-table conditions remain after pushdown)
            bool whereMatch = true;
            for (const auto& c : joinConds) {
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

std::vector<std::string> StorageEngine::fullOuterJoin(
    const std::string& dbname,
    const std::string& leftTable,
    const std::string& rightTable,
    const std::string& leftCol,
    const std::string& rightCol,
    const std::vector<std::string>& conditions,
    const std::set<std::string>& selectCols) {
    // FULL OUTER JOIN = LEFT JOIN UNION RIGHT JOIN, deduplicate inner rows
    auto leftResult = leftJoin(dbname, leftTable, rightTable, leftCol, rightCol, conditions, selectCols);
    auto rightResult = rightJoin(dbname, leftTable, rightTable, leftCol, rightCol, conditions, selectCols);
    std::set<std::string> seen(leftResult.begin(), leftResult.end());
    for (const auto& r : rightResult) {
        if (seen.insert(r).second) leftResult.push_back(r);
    }
    return leftResult;
}

std::vector<std::string> StorageEngine::crossJoin(
    const std::string& dbname,
    const std::string& leftTable,
    const std::string& rightTable,
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

    std::vector<std::string> leftRows;
    forEachRow(dbname, leftTable, [&leftRows](uint32_t, uint16_t, const char* data, size_t len) {
        leftRows.emplace_back(data, len);
    });
    std::vector<std::string> rightRows;
    forEachRow(dbname, rightTable, [&rightRows](uint32_t, uint16_t, const char* data, size_t len) {
        rightRows.emplace_back(data, len);
    });

    auto conds = parseConditions(conditions);

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

    for (const auto& lr : leftRows) {
        for (const auto& rr : rightRows) {
            bool whereMatch = true;
            for (const auto& c : conds) {
                if (!evalCond(c, lr, rr)) { whereMatch = false; break; }
            }
            if (!whereMatch) continue;
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

    // Archive WAL before truncation
    archiveWal(dbname);
    // Truncate WAL after checkpoint
    walClear(walPath(dbname));
}

// ========================================================================
// WAL archiving
// ========================================================================

void StorageEngine::archiveWal(const std::string& dbname) {
    auto wpath = walPath(dbname);
    if (!std::filesystem::exists(wpath)) return;
    auto archiveDir = walArchiveDir(dbname);
    if (!std::filesystem::exists(archiveDir)) {
        std::filesystem::create_directories(archiveDir);
    }
    // Copy WAL to archive with timestamp suffix
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&time_t_now);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d_%H%M%S");
    auto dest = archiveDir / ("wal_" + oss.str());
    try {
        std::filesystem::copy_file(wpath, dest, std::filesystem::copy_options::overwrite_existing);
    } catch (...) {}
}

// ========================================================================
// Physical backup / restore
// ========================================================================

bool StorageEngine::physicalBackup(const std::string& dbname, const std::string& backupPath) {
    if (!databaseExists(dbname)) return false;
    auto src = dbPath(dbname);
    auto dst = std::filesystem::path(backupPath);
    try {
        if (!std::filesystem::exists(dst)) {
            std::filesystem::create_directories(dst);
        }
        for (const auto& entry : std::filesystem::directory_iterator(src)) {
            auto destPath = dst / entry.path().filename();
            if (entry.is_directory()) {
                std::filesystem::copy(entry.path(), destPath,
                    std::filesystem::copy_options::overwrite_existing |
                    std::filesystem::copy_options::recursive);
            } else {
                std::filesystem::copy_file(entry.path(), destPath,
                    std::filesystem::copy_options::overwrite_existing);
            }
        }
        // Also backup WAL archive if exists
        auto archiveDir = walArchiveDir(dbname);
        if (std::filesystem::exists(archiveDir)) {
            auto destArchive = dst / "wal_archive";
            std::filesystem::copy(archiveDir, destArchive,
                std::filesystem::copy_options::overwrite_existing |
                std::filesystem::copy_options::recursive);
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool StorageEngine::physicalRestore(const std::string& dbname, const std::string& backupPath) {
    auto src = std::filesystem::path(backupPath);
    if (!std::filesystem::exists(src)) return false;
    auto dst = dbPath(dbname);
    try {
        // Remove existing database if present
        if (std::filesystem::exists(dst)) {
            std::filesystem::remove_all(dst);
        }
        std::filesystem::create_directories(dst);
        for (const auto& entry : std::filesystem::directory_iterator(src)) {
            auto destPath = dst / entry.path().filename();
            if (entry.is_directory() && entry.path().filename() == "wal_archive") {
                // Skip wal_archive in root, restore it separately
                continue;
            }
            if (entry.is_directory()) {
                std::filesystem::copy(entry.path(), destPath,
                    std::filesystem::copy_options::recursive);
            } else {
                std::filesystem::copy_file(entry.path(), destPath,
                    std::filesystem::copy_options::overwrite_existing);
            }
        }
        // Restore WAL archive
        auto srcArchive = src / "wal_archive";
        if (std::filesystem::exists(srcArchive)) {
            auto dstArchive = walArchiveDir(dbname);
            if (std::filesystem::exists(dstArchive)) {
                std::filesystem::remove_all(dstArchive);
            }
            std::filesystem::copy(srcArchive, dstArchive,
                std::filesystem::copy_options::recursive);
        }
        return true;
    } catch (...) {
        return false;
    }
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

    // Reset dead tuple count after successful vacuum
    if (freedPages > 0) {
        auto key = std::make_pair(dbname, tablename);
        std::lock_guard<std::mutex> lock(deadTupleMutex_);
        deadTupleCounts_[key] = 0;
    }

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
    savepoints_.clear();
    walClear(walPath(txnDB_));
    lockManager_.unlockAll();
    lockManager_.unlockAllGaps();
    currentTxnId_ = 0;
    inTransaction_ = false;
    readOnly_ = false;
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
    savepoints_.clear();
    walClear(walPath(txnDB_));
    lockManager_.unlockAll();
    lockManager_.unlockAllGaps();
    currentTxnId_ = 0;
    inTransaction_ = false;
    readOnly_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

// ========================================================================
// Savepoint support
// ========================================================================

OpResult StorageEngine::savepoint(const std::string& name) {
    if (!inTransaction_) return OpResult::InvalidValue;
    savepoints_[name] = txnLog_.size();
    return OpResult::Success;
}

OpResult StorageEngine::rollbackToSavepoint(const std::string& name) {
    if (!inTransaction_) return OpResult::InvalidValue;
    auto it = savepoints_.find(name);
    if (it == savepoints_.end()) return OpResult::InvalidValue;
    size_t spIdx = it->second;
    if (spIdx > txnLog_.size()) return OpResult::InvalidValue;

    // Undo entries from end back to savepoint
    for (size_t i = txnLog_.size(); i > spIdx; --i) {
        auto& entry = txnLog_[i - 1];
        PageAllocator* pa = getPageAllocator(txnDB_, entry.tableName);
        TableSchema tbl = getTableSchema(txnDB_, entry.tableName);
        if (entry.op == TxnLogEntry::Op::Insert) {
            // Remove from indexes first (row still exists)
            BPTree* pkIdx = getPKIndex(txnDB_, entry.tableName);
            if (pkIdx) {
                std::string row;
                if (readRowByRid(pa, entry.rowIdx, row, tbl)) {
                    std::string pkVal = extractPKValue(row, tbl);
                    if (!pkVal.empty()) pkIdx->remove(pkVal);
                }
            }
            auto indexedCols = getIndexedColumns(txnDB_, entry.tableName);
            for (const auto& colname : indexedCols) {
                size_t colIdx = tbl.len;
                for (size_t j = 0; j < tbl.len; ++j) {
                    if (tbl.cols[j].dataName == colname) { colIdx = j; break; }
                }
                if (colIdx >= tbl.len) continue;
                BPTree* secIdx = getSecondaryIndex(txnDB_, entry.tableName, colname);
                if (!secIdx) continue;
                std::string row;
                if (readRowByRid(pa, entry.rowIdx, row, tbl)) {
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    if (!val.empty()) secIdx->remove(val);
                }
            }
            // Then remove the row from page
            uint32_t pageId; uint16_t slotId;
            decodeRid(entry.rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                Page page(pageBuf);
                page.remove(slotId);
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
        } else if (entry.op == TxnLogEntry::Op::Update) {
            uint32_t pageId; uint16_t slotId;
            decodeRid(entry.rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                Page page(pageBuf);
                page.update(slotId, entry.rowData.data(), entry.rowData.size());
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            BPTree* pkIdx = getPKIndex(txnDB_, entry.tableName);
            if (pkIdx) {
                std::string newPk = extractPKValue(entry.rowData, tbl);
                if (!newPk.empty()) pkIdx->insert(newPk, entry.rowIdx);
            }
            auto indexedCols = getIndexedColumns(txnDB_, entry.tableName);
            for (const auto& colname : indexedCols) {
                size_t colIdx = tbl.len;
                for (size_t j = 0; j < tbl.len; ++j) {
                    if (tbl.cols[j].dataName == colname) { colIdx = j; break; }
                }
                if (colIdx >= tbl.len) continue;
                BPTree* secIdx = getSecondaryIndex(txnDB_, entry.tableName, colname);
                if (!secIdx) continue;
                std::string val = extractColumnValue(entry.rowData, tbl, colIdx);
                if (!val.empty()) secIdx->insertMulti(val, entry.rowIdx);
            }
        } else if (entry.op == TxnLogEntry::Op::Delete) {
            uint32_t pageId; uint16_t slotId;
            decodeRid(entry.rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                Page page(pageBuf);
                page.restore(slotId);
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            BPTree* pkIdx = getPKIndex(txnDB_, entry.tableName);
            if (pkIdx) {
                std::string pkVal = extractPKValue(entry.rowData, tbl);
                if (!pkVal.empty()) pkIdx->insert(pkVal, entry.rowIdx);
            }
            auto indexedCols = getIndexedColumns(txnDB_, entry.tableName);
            for (const auto& colname : indexedCols) {
                size_t colIdx = tbl.len;
                for (size_t j = 0; j < tbl.len; ++j) {
                    if (tbl.cols[j].dataName == colname) { colIdx = j; break; }
                }
                if (colIdx >= tbl.len) continue;
                BPTree* secIdx = getSecondaryIndex(txnDB_, entry.tableName, colname);
                if (!secIdx) continue;
                std::string val = extractColumnValue(entry.rowData, tbl, colIdx);
                if (!val.empty()) secIdx->insertMulti(val, entry.rowIdx);
            }
        }
    }
    txnLog_.resize(spIdx);

    // Remove all savepoints created after this one
    for (auto sit = savepoints_.begin(); sit != savepoints_.end(); ) {
        if (sit->second > spIdx) sit = savepoints_.erase(sit);
        else ++sit;
    }
    return OpResult::Success;
}

OpResult StorageEngine::releaseSavepoint(const std::string& name) {
    if (!inTransaction_) return OpResult::InvalidValue;
    auto it = savepoints_.find(name);
    if (it == savepoints_.end()) return OpResult::InvalidValue;
    savepoints_.erase(it);
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

// Auto-VACUUM: trigger vacuum when dead tuple count exceeds threshold
void StorageEngine::maybeAutoVacuum(const std::string& dbname,
                                    const std::string& tablename) {
    if (!g_config.autoVacuumEnabled) return;
    auto key = std::make_pair(dbname, tablename);
    size_t count = 0;
    {
        std::lock_guard<std::mutex> lock(deadTupleMutex_);
        auto it = deadTupleCounts_.find(key);
        if (it == deadTupleCounts_.end()) return;
        count = it->second;
    }
    if (count >= static_cast<size_t>(g_config.autoVacuumThreshold)) {
        vacuum(dbname, tablename);
        std::lock_guard<std::mutex> lock(deadTupleMutex_);
        deadTupleCounts_[key] = 0;
    }
}

size_t StorageEngine::getDeadTupleCount(const std::string& dbname,
                                        const std::string& tablename) const {
    auto key = std::make_pair(dbname, tablename);
    std::lock_guard<std::mutex> lock(deadTupleMutex_);
    auto it = deadTupleCounts_.find(key);
    return (it != deadTupleCounts_.end()) ? it->second : 0;
}

void StorageEngine::resetDeadTupleCount(const std::string& dbname,
                                        const std::string& tablename) {
    auto key = std::make_pair(dbname, tablename);
    std::lock_guard<std::mutex> lock(deadTupleMutex_);
    deadTupleCounts_[key] = 0;
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

// Permission entry: user|table|priv -> set of columns (empty = table-level)
static std::string permKey(const std::string& u, const std::string& t, const std::string& p) {
    return u + "|" + t + "|" + p;
}

static std::vector<std::string> parseColumns(const std::string& colsStr) {
    std::vector<std::string> cols;
    std::stringstream ss(colsStr);
    std::string col;
    while (std::getline(ss, col, ',')) {
        if (!col.empty()) cols.push_back(col);
    }
    return cols;
}

static std::string joinColumns(const std::vector<std::string>& cols) {
    std::string s;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i > 0) s += ",";
        s += cols[i];
    }
    return s;
}

void StorageEngine::grant(const std::string& dbname, const std::string& tablename,
                          const std::string& username, TablePrivilege priv,
                          const std::vector<std::string>& columns) {
    auto ppath = permPath(dbname);
    // Read: key -> set of columns (empty set = table-level)
    std::map<std::string, std::set<std::string>> perms;
    if (std::filesystem::exists(ppath)) {
        std::ifstream ifs(ppath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string u, t, p, cols;
            ss >> u >> t >> p;
            std::getline(ss, cols); // rest may be columns
            if (!cols.empty() && cols[0] == ' ') cols = cols.substr(1);
            auto key = permKey(u, t, p);
            if (!cols.empty()) {
                for (const auto& c : parseColumns(cols)) perms[key].insert(c);
            } else {
                perms[key] = {}; // table-level marker
            }
        }
    }
    auto key = permKey(username, tablename, privToStr(priv));
    if (columns.empty()) {
        perms[key] = {}; // table-level
    } else {
        if (perms[key].empty()) {
            // If transitioning from table-level to column-level, start fresh
            auto it = perms.find(key);
            if (it != perms.end() && it->second.empty()) {
                it->second.clear(); // was table-level
            }
        }
        for (const auto& c : columns) perms[key].insert(c);
    }
    // Write back
    std::ofstream ofs(ppath);
    for (const auto& kv : perms) {
        size_t p1 = kv.first.find('|');
        size_t p2 = kv.first.find('|', p1 + 1);
        std::string u = kv.first.substr(0, p1);
        std::string t = kv.first.substr(p1 + 1, p2 - p1 - 1);
        std::string p = kv.first.substr(p2 + 1);
        ofs << u << " " << t << " " << p;
        if (!kv.second.empty()) ofs << " " << joinColumns(std::vector<std::string>(kv.second.begin(), kv.second.end()));
        ofs << "\n";
    }
}

void StorageEngine::revoke(const std::string& dbname, const std::string& tablename,
                           const std::string& username, TablePrivilege priv,
                           const std::vector<std::string>& columns) {
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return;
    std::map<std::string, std::set<std::string>> perms;
    {
        std::ifstream ifs(ppath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string u, t, p, cols;
            ss >> u >> t >> p;
            std::getline(ss, cols);
            if (!cols.empty() && cols[0] == ' ') cols = cols.substr(1);
            auto key = permKey(u, t, p);
            if (!cols.empty()) {
                for (const auto& c : parseColumns(cols)) perms[key].insert(c);
            } else {
                perms[key] = {};
            }
        }
    }
    auto key = permKey(username, tablename, privToStr(priv));
    auto it = perms.find(key);
    if (it != perms.end()) {
        if (columns.empty()) {
            perms.erase(it); // revoke entire privilege
        } else {
            for (const auto& c : columns) it->second.erase(c);
            if (it->second.empty()) perms.erase(it);
        }
    }
    std::ofstream ofs(ppath);
    for (const auto& kv : perms) {
        size_t p1 = kv.first.find('|');
        size_t p2 = kv.first.find('|', p1 + 1);
        std::string u = kv.first.substr(0, p1);
        std::string t = kv.first.substr(p1 + 1, p2 - p1 - 1);
        std::string p = kv.first.substr(p2 + 1);
        ofs << u << " " << t << " " << p;
        if (!kv.second.empty()) ofs << " " << joinColumns(std::vector<std::string>(kv.second.begin(), kv.second.end()));
        ofs << "\n";
    }
}

bool StorageEngine::hasPermission(const std::string& dbname, const std::string& tablename,
                                  const std::string& username, TablePrivilege priv) const {
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return false;
    std::string target = privToStr(priv);
    std::ifstream ifs(ppath);
    std::string line;
    bool hasDbLevel = false;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string u, t, p, cols;
        ss >> u >> t >> p;
        std::getline(ss, cols);
        if (u != username) continue;
        // Table-level permission (no column restriction)
        if (t == tablename && (p == "all" || p == target)) {
            if (cols.empty() || (cols.size() == 1 && cols[0] == ' ')) return true;
        }
        // Database-level permission (wildcard table "*")
        if (t == "*" && (p == "all" || p == target)) hasDbLevel = true;
    }
    return hasDbLevel;
}

bool StorageEngine::hasColumnPermission(const std::string& dbname, const std::string& tablename,
                                        const std::string& username, TablePrivilege priv,
                                        const std::vector<std::string>& columns) const {
    if (columns.empty()) return hasPermission(dbname, tablename, username, priv);
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return false;
    std::string target = privToStr(priv);
    // Gather allowed columns for this user/table/priv
    std::set<std::string> allowedCols;
    bool hasTableLevel = false;
    bool hasDbLevel = false;
    {
        std::ifstream ifs(ppath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string u, t, p, cols;
            ss >> u >> t >> p;
            std::getline(ss, cols);
            if (!cols.empty() && cols[0] == ' ') cols = cols.substr(1);
            if (u != username) continue;
            if (t == "*" && (p == "all" || p == target)) hasDbLevel = true;
            if (t != tablename) continue;
            if (p == "all") hasTableLevel = true;
            if (p == target) {
                if (cols.empty()) hasTableLevel = true;
                else {
                    for (const auto& c : parseColumns(cols)) allowedCols.insert(c);
                }
            }
        }
    }
    if (hasTableLevel || hasDbLevel) return true;
    for (const auto& col : columns) {
        if (allowedCols.find(col) == allowedCols.end()) return false;
    }
    return true;
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
