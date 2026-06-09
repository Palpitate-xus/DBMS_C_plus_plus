#include "TableManage.h"
#include "TxnIdGenerator.h"
#include "Config.h"
#include <cmath>

extern dbms::Config g_config;

std::string dbms::sqlstateForOpResult(OpResult res) {
    switch (res) {
        case OpResult::Success: return "00000";
        case OpResult::TableNotExist: return "42P01";
        case OpResult::DatabaseNotExist: return "3D000";
        case OpResult::TableAlreadyExist: return "42P07";
        case OpResult::InvalidValue: return "22023";
        case OpResult::NullNotAllowed: return "23502";
        case OpResult::SyntaxError: return "42601";
        case OpResult::DuplicateKey: return "23505";
        case OpResult::LockConflict: return "55P03";
        case OpResult::SerializationFailure: return "40001";
    }
    return "XX000"; // internal_error
}

static std::string escapeString(const std::string& s) {
    std::string r;
    for (char c : s) {
        if (c == ' ') r += "\\s";
        else if (c == '\n') r += "\\n";
        else if (c == '\\') r += "\\\\";
        else r += c;
    }
    return r;
}

static std::string unescapeString(const std::string& s) {
    std::string r;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            if (s[i + 1] == 's') { r += ' '; ++i; }
            else if (s[i + 1] == 'n') { r += '\n'; ++i; }
            else if (s[i + 1] == '\\') { r += '\\'; ++i; }
            else r += s[i];
        } else {
            r += s[i];
        }
    }
    return r;
}

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
#include <locale>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>
#include <cwctype>

namespace dbms {

// Global active transaction tracking
std::mutex StorageEngine::globalTxnMutex_;
std::set<uint64_t> StorageEngine::activeTransactions_;
std::mutex StorageEngine::ssiMutex_;
std::map<uint64_t, std::set<int64_t>> StorageEngine::ssiReadSets_;
std::map<uint64_t, std::set<int64_t>> StorageEngine::ssiWriteSets_;
std::map<uint64_t, std::set<uint64_t>> StorageEngine::ssiOutEdges_;
std::map<uint64_t, std::set<uint64_t>> StorageEngine::ssiInEdges_;

// ========================================================================
// Row-Level Security helpers
// ========================================================================
static std::vector<std::string> buildRLSConditions(const StorageEngine* engine,
                                                    const std::string& dbname,
                                                    const std::string& tablename,
                                                    const std::string& cmd) {
    std::vector<std::string> extra;
    if (!engine) return extra;
    auto tbl = engine->getTableSchema(dbname, tablename);
    if (!tbl.rowLevelSecurity) return extra;
    std::string user = StorageEngine::getRLSUser();
    if (user.empty()) return extra;
    // Admin bypasses RLS unless FORCE
    if (user == "admin" && !tbl.forceRowLevelSecurity) return extra;
    auto policies = engine->getApplicablePolicies(dbname, tablename, cmd, user);
    for (const auto& p : policies) {
        if (!p.usingExpr.empty()) {
            std::string expr = p.usingExpr;
            // Strip surrounding parentheses if present
            if (expr.size() >= 2 && expr.front() == '(' && expr.back() == ')') {
                expr = expr.substr(1, expr.size() - 2);
            }
            extra.push_back(expr);
        }
    }
    return extra;
}

// ========================================================================
// Unicode string helpers
// ========================================================================
static std::string toUpperUtf8(const std::string& s) {
    std::wstring ws;
    size_t i = 0;
    while (i < s.size()) {
        unsigned char c = s[i];
        if (c < 0x80) { ws.push_back(static_cast<wchar_t>(c)); ++i; }
        else if ((c & 0xE0) == 0xC0 && i + 1 < s.size()) {
            ws.push_back(static_cast<wchar_t>(((c & 0x1F) << 6) | (s[i + 1] & 0x3F))); i += 2;
        } else if ((c & 0xF0) == 0xE0 && i + 2 < s.size()) {
            ws.push_back(static_cast<wchar_t>(((c & 0x0F) << 12) | ((s[i + 1] & 0x3F) << 6) | (s[i + 2] & 0x3F))); i += 3;
        } else if ((c & 0xF8) == 0xF0 && i + 3 < s.size()) {
            ws.push_back(static_cast<wchar_t>(((c & 0x07) << 18) | ((s[i + 1] & 0x3F) << 12) | ((s[i + 2] & 0x3F) << 6) | (s[i + 3] & 0x3F))); i += 4;
        } else { ws.push_back(static_cast<wchar_t>(c)); ++i; }
    }
    for (wchar_t& wc : ws) wc = std::towupper(wc);
    std::string out;
    for (wchar_t wc : ws) {
        if (wc < 0x80) out.push_back(static_cast<char>(wc));
        else if (wc < 0x800) { out.push_back(static_cast<char>(0xC0 | (wc >> 6))); out.push_back(static_cast<char>(0x80 | (wc & 0x3F))); }
        else { out.push_back(static_cast<char>(0xE0 | (wc >> 12))); out.push_back(static_cast<char>(0x80 | ((wc >> 6) & 0x3F))); out.push_back(static_cast<char>(0x80 | (wc & 0x3F))); }
    }
    return out;
}

static std::string toLowerUtf8(const std::string& s) {
    std::wstring ws;
    size_t i = 0;
    while (i < s.size()) {
        unsigned char c = s[i];
        if (c < 0x80) { ws.push_back(static_cast<wchar_t>(c)); ++i; }
        else if ((c & 0xE0) == 0xC0 && i + 1 < s.size()) {
            ws.push_back(static_cast<wchar_t>(((c & 0x1F) << 6) | (s[i + 1] & 0x3F))); i += 2;
        } else if ((c & 0xF0) == 0xE0 && i + 2 < s.size()) {
            ws.push_back(static_cast<wchar_t>(((c & 0x0F) << 12) | ((s[i + 1] & 0x3F) << 6) | (s[i + 2] & 0x3F))); i += 3;
        } else if ((c & 0xF8) == 0xF0 && i + 3 < s.size()) {
            ws.push_back(static_cast<wchar_t>(((c & 0x07) << 18) | ((s[i + 1] & 0x3F) << 12) | ((s[i + 2] & 0x3F) << 6) | (s[i + 3] & 0x3F))); i += 4;
        } else { ws.push_back(static_cast<wchar_t>(c)); ++i; }
    }
    for (wchar_t& wc : ws) wc = std::towlower(wc);
    std::string out;
    for (wchar_t wc : ws) {
        if (wc < 0x80) out.push_back(static_cast<char>(wc));
        else if (wc < 0x800) { out.push_back(static_cast<char>(0xC0 | (wc >> 6))); out.push_back(static_cast<char>(0x80 | (wc & 0x3F))); }
        else { out.push_back(static_cast<char>(0xE0 | (wc >> 12))); out.push_back(static_cast<char>(0x80 | ((wc >> 6) & 0x3F))); out.push_back(static_cast<char>(0x80 | (wc & 0x3F))); }
    }
    return out;
}

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
            std::cout << ") ON DELETE " << fks[i].onDelete;
            if (!fks[i].onUpdate.empty() && fks[i].onUpdate != "restrict")
                std::cout << " ON UPDATE " << fks[i].onUpdate;
            std::cout << "\n";
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

Column makeJsonbColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "jsonb";
    c.dsize = 65535;
    return c;
}

Column makeXmlColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "xml";
    c.dsize = 65535;
    return c;
}

Column makePgLsnColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "pg_lsn";
    c.dsize = 32; // e.g. "FFFFFFFF/FFFFFFFF" plus margin
    return c;
}

Column makeInt4RangeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "int4range";
    c.dsize = 64;
    return c;
}

Column makeInt8RangeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "int8range";
    c.dsize = 64;
    return c;
}

Column makeNumRangeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "numrange";
    c.dsize = 128;
    return c;
}

Column makeTsRangeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "tsrange";
    c.dsize = 64;
    return c;
}

Column makeTstzRangeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "tstzrange";
    c.dsize = 64;
    return c;
}

Column makeDateRangeColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "daterange";
    c.dsize = 64;
    return c;
}

Column makeTsVectorColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "tsvector";
    c.dsize = 65535;
    return c;
}

Column makeTsQueryColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.isVariableLength = true;
    c.dataType = "tsquery";
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

Column makePointColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "point";
    c.dsize = 16;  // two doubles: x, y
    return c;
}

Column makeINetColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "inet";
    c.dsize = 20;  // 1B family + 1B prefix_len + 1B is_cidr + 1B reserved + 16B addr
    return c;
}

Column makeCidrColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "cidr";
    c.dsize = 20;
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

Column makeIntervalColumn(const std::string& name, bool isNull, bool isPK) {
    Column c;
    c.dataName = name;
    c.isNull = isNull;
    c.isPrimaryKey = isPK;
    c.dataType = "interval";
    c.isVariableLength = true;
    c.dsize = 64;  // stores interval string like "1 day 2 hours"
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

std::filesystem::path StorageEngine::paramsPath(const std::string& dbname,
                                                 const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".params");
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

std::filesystem::path StorageEngine::partitionDataPath(const std::string& dbname,
                                                        const std::string& tablename,
                                                        const std::string& partitionName,
                                                        const std::string& subPartitionName) const {
    return dbPath(dbname) / (tablename + "#" + partitionName + "#" + subPartitionName + ".dt");
}

std::filesystem::path StorageEngine::fsmPath(const std::string& dbname,
                                               const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".fsm");
}

std::filesystem::path StorageEngine::vmPath(const std::string& dbname,
                                             const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".vm");
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
        return tbl.defaultPartitionName;
    }
    if (tbl.partitionType == TableSchema::PartitionType::Hash) {
        if (tbl.hashPartitions == 0) return "";
        size_t h = std::hash<std::string>{}(keyVal) % tbl.hashPartitions;
        return "p" + std::to_string(h);
    }
    return "";
}

std::string StorageEngine::getSubPartitionName(const TableSchema& tbl, const std::string& keyVal) const {
    if (tbl.subPartitionType == TableSchema::PartitionType::None) return "";
    if (tbl.subPartitionType == TableSchema::PartitionType::Hash) {
        if (tbl.subHashPartitions == 0) return "";
        size_t h = std::hash<std::string>{}(keyVal) % tbl.subHashPartitions;
        return "sp" + std::to_string(h);
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

OpResult StorageEngine::attachPartition(const std::string& dbname,
                                          const std::string& tablename,
                                          const std::string& partitionName,
                                          const std::string& partitionSpec) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    if (tbl.partitionType == TableSchema::PartitionType::None) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;  // table is not partitioned
    }

    // Check partition name doesn't already exist
    auto nameExists = [&]() -> bool {
        if (tbl.partitionType == TableSchema::PartitionType::Range) {
            for (const auto& rp : tbl.rangePartitions)
                if (rp.first == partitionName) return true;
        } else if (tbl.partitionType == TableSchema::PartitionType::List) {
            for (const auto& lp : tbl.listPartitions)
                if (lp.first == partitionName) return true;
        } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
            size_t hidx = std::stoull(partitionName.substr(1));
            if (hidx < tbl.hashPartitions) return true;
        }
        return false;
    };
    if (nameExists()) {
        lockManager_.unlock(tablename);
        return OpResult::TableAlreadyExist;
    }

    // Parse partition spec based on partition type
    std::string specLower = partitionSpec;
    std::transform(specLower.begin(), specLower.end(), specLower.begin(), ::tolower);

    if (tbl.partitionType == TableSchema::PartitionType::Range) {
        // Parse: FOR VALUES FROM (val) TO (val)
        size_t fromPos = specLower.find("from");
        size_t toPos = specLower.find(" to ");
        if (fromPos == std::string::npos || toPos == std::string::npos) {
            lockManager_.unlock(tablename);
            return OpResult::SyntaxError;
        }
        size_t val1lp = partitionSpec.find('(', fromPos);
        size_t val1rp = partitionSpec.find(')', val1lp);
        size_t val2lp = partitionSpec.find('(', toPos);
        size_t val2rp = partitionSpec.find(')', val2lp);
        if (val1lp == std::string::npos || val1rp == std::string::npos ||
            val2lp == std::string::npos || val2rp == std::string::npos) {
            lockManager_.unlock(tablename);
            return OpResult::SyntaxError;
        }
        std::string lowerBound = trim(partitionSpec.substr(val1lp + 1, val1rp - val1lp - 1));
        std::string upperBound = trim(partitionSpec.substr(val2lp + 1, val2rp - val2lp - 1));
        // Strip quotes
        auto unquote = [](std::string& s) {
            if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                                   (s.front() == '"' && s.back() == '"')))
                s = s.substr(1, s.size() - 2);
        };
        unquote(lowerBound);
        unquote(upperBound);
        // Insert in sorted position (lower bound ascending)
        auto it = tbl.rangePartitions.begin();
        while (it != tbl.rangePartitions.end() && it->second < lowerBound) ++it;
        tbl.rangePartitions.insert(it, {partitionName, upperBound});
    } else if (tbl.partitionType == TableSchema::PartitionType::List) {
        // Parse: FOR VALUES IN (v1, v2, ...)
        size_t inPos = specLower.find(" in ");
        if (inPos == std::string::npos) {
            lockManager_.unlock(tablename);
            return OpResult::SyntaxError;
        }
        size_t valLp = partitionSpec.find('(', inPos);
        size_t valRp = partitionSpec.find(')', valLp);
        if (valLp == std::string::npos || valRp == std::string::npos) {
            lockManager_.unlock(tablename);
            return OpResult::SyntaxError;
        }
        std::string valsStr = partitionSpec.substr(valLp + 1, valRp - valLp - 1);
        std::vector<std::string> values;
        size_t cp = 0;
        while (cp < valsStr.size()) {
            size_t c = valsStr.find(',', cp);
            std::string v = trim(valsStr.substr(cp, c - cp));
            if (v.size() >= 2 && ((v.front() == '\'' && v.back() == '\'') ||
                                   (v.front() == '"' && v.back() == '"')))
                v = v.substr(1, v.size() - 2);
            if (!v.empty()) values.push_back(v);
            if (c == std::string::npos) break;
            cp = c + 1;
        }
        tbl.listPartitions.push_back({partitionName, values});
    } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
        // Hash attach: increase hash partition count if partition name is "pN" where N == current count
        // e.g., ATTACH PARTITION p4 when hashPartitions is 4 → hashPartitions becomes 5
        if (partitionName.size() > 1 && partitionName[0] == 'p') {
            try {
                size_t idx = std::stoull(partitionName.substr(1));
                if (idx == tbl.hashPartitions) {
                    tbl.hashPartitions = idx + 1;
                } else {
                    lockManager_.unlock(tablename);
                    return OpResult::InvalidValue; // hash partitions must be attached in order
                }
            } catch (...) {
                lockManager_.unlock(tablename);
                return OpResult::SyntaxError;
            }
        } else {
            lockManager_.unlock(tablename);
            return OpResult::SyntaxError;
        }
    }

    // Create the partition data file
    {
        auto ppa = std::make_unique<PageAllocator>(
            partitionDataPath(dbname, tablename, partitionName).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
        ppa->open();
        ppa->close();
    }

    // Rewrite schema
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }

    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::detachPartition(const std::string& dbname,
                                          const std::string& tablename,
                                          const std::string& partitionName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    if (tbl.partitionType == TableSchema::PartitionType::None) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    bool found = false;
    if (tbl.partitionType == TableSchema::PartitionType::Range) {
        auto it = std::find_if(tbl.rangePartitions.begin(), tbl.rangePartitions.end(),
                                [&](const auto& rp) { return rp.first == partitionName; });
        if (it != tbl.rangePartitions.end()) {
            tbl.rangePartitions.erase(it);
            found = true;
        }
    } else if (tbl.partitionType == TableSchema::PartitionType::List) {
        auto it = std::find_if(tbl.listPartitions.begin(), tbl.listPartitions.end(),
                                [&](const auto& lp) { return lp.first == partitionName; });
        if (it != tbl.listPartitions.end()) {
            tbl.listPartitions.erase(it);
            found = true;
        }
    } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
        // Removing a hash partition reduces count; only the last partition can be detached
        if (partitionName == "p" + std::to_string(tbl.hashPartitions - 1) && tbl.hashPartitions > 0) {
            tbl.hashPartitions--;
            found = true;
        }
    }

    if (!found) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    // Rewrite schema
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }

    // Close any cached page allocator for the detached partition
    pageAllocators_.erase(dbname + "/" + tablename + "#" + partitionName);

    lockManager_.unlock(tablename);
    return OpResult::Success;
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

std::string StorageEngine::getViewCheckOption(const std::string& dbname,
                                               const std::string& viewname) const {
    auto path = viewPath(dbname, viewname);
    if (!std::filesystem::exists(path)) return "";
    std::ifstream ifs(path);
    if (!ifs) return "";
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.substr(0, 17) == "WITH_CHECK_OPTION") {
            size_t colon = line.find(':');
            if (colon != std::string::npos) return line.substr(colon + 1);
            return "CASCADED";
        }
    }
    return "";
}

// Forward declaration for validateViewCheckOption
static std::string buildRowBuffer(const TableSchema& tbl,
                                  const std::map<std::string, std::string>& values,
                                  uint64_t creatorTxnId);

// ========================================================================
// WITH CHECK OPTION helpers
// ========================================================================

static std::string extractViewWhereClause(const std::string& selectSql) {
    std::string lower = selectSql;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    size_t wherePos = lower.find(" where ");
    if (wherePos == std::string::npos) return "";
    size_t clauseStart = wherePos + 7;
    static const char* endMarkers[] = {" order by ", " group by ", " limit ", " having ", " union ", " intersect ", " except "};
    size_t clauseEnd = selectSql.size();
    for (const char* marker : endMarkers) {
        size_t pos = lower.find(marker, clauseStart);
        if (pos != std::string::npos && pos < clauseEnd) clauseEnd = pos;
    }
    return trim(selectSql.substr(clauseStart, clauseEnd - clauseStart));
}

static std::string normalizeViewCondStr(std::string s) {
    static const char* ops[] = {">=", "<=", "!=", "<>", ">", "<", "="};
    for (const char* op : ops) {
        size_t len = std::strlen(op);
        size_t pos = 0;
        while ((pos = s.find(op, pos)) != std::string::npos) {
            size_t before = pos;
            while (before > 0 && std::isspace(static_cast<unsigned char>(s[before - 1]))) before--;
            size_t after = pos + len;
            while (after < s.size() && std::isspace(static_cast<unsigned char>(s[after]))) after++;
            if (before != pos || after != pos + len) {
                s = s.substr(0, before) + op + s.substr(after);
                pos = before + len;
            } else {
                pos += len;
            }
        }
    }
    struct KW { const char* w; size_t l; const char* r; };
    static const KW kws[] = {
        {"like", 4, "like"}, {"regexp", 6, "regexp"}, {"contains", 8, "contains"},
        {"overlaps", 8, "overlaps"}, {"is not null", 11, "isnotnull"}, {"is null", 7, "isnull"}
    };
    for (const auto& kw : kws) {
        size_t pos = 0;
        while ((pos = s.find(kw.w, pos)) != std::string::npos) {
            size_t before = pos;
            while (before > 0 && std::isspace(static_cast<unsigned char>(s[before - 1]))) before--;
            size_t after = pos + kw.l;
            while (after < s.size() && std::isspace(static_cast<unsigned char>(s[after]))) after++;
            if (before != pos || after != pos + kw.l) {
                s = s.substr(0, before) + kw.r + s.substr(after);
                pos = before + std::strlen(kw.r);
            } else {
                pos += kw.l;
            }
        }
    }
    // overlaps parenthesis form
    size_t pos = 0;
    while ((pos = s.find("overlaps", pos)) != std::string::npos) {
        size_t leftStart = pos;
        while (leftStart > 0 && s[leftStart - 1] != '(') leftStart--;
        if (leftStart == 0 || s[leftStart - 1] != '(') { pos += 8; continue; }
        leftStart--;
        size_t rightEnd = pos + 8;
        while (rightEnd < s.size() && s[rightEnd] != ')') rightEnd++;
        if (rightEnd >= s.size()) { pos += 8; continue; }
        rightEnd++;
        std::string expr = s.substr(leftStart, rightEnd - leftStart);
        std::string inner = expr.substr(1, expr.size() - 2);
        size_t opPos = inner.find(")overlaps(");
        if (opPos != std::string::npos) {
            std::string repl = "overlaps:" + inner.substr(0, opPos) + "," + inner.substr(opPos + 10);
            s = s.substr(0, leftStart) + repl + s.substr(rightEnd);
            pos = leftStart + repl.size();
        } else { pos += 8; }
    }
    return s;
}

static std::string modifyViewLogic(const std::string& logic) {
    if (logic == "(" || logic == ")" || logic == "and" || logic == "or") return logic;
    size_t p = logic.find("like");
    if (p != std::string::npos)
        return "like" + logic.substr(0, p) + " " + logic.substr(p + 4);
    p = logic.find("regexp");
    if (p != std::string::npos)
        return "regexp" + logic.substr(0, p) + " " + logic.substr(p + 6);
    p = logic.find("contains");
    if (p != std::string::npos)
        return "contains" + logic.substr(0, p) + " " + logic.substr(p + 8);
    p = logic.find("overlaps");
    if (p != std::string::npos)
        return "overlaps" + logic.substr(0, p) + " " + logic.substr(p + 8);
    p = logic.find("isnotnull");
    if (p != std::string::npos && p + 9 == logic.size())
        return "isnotnull " + logic.substr(0, p);
    p = logic.find("isnull");
    if (p != std::string::npos && p + 6 == logic.size())
        return "isnull " + logic.substr(0, p);
    size_t opStart = std::string::npos, opLen = 0;
    for (size_t i = 0; i < logic.size(); ++i) {
        if (logic[i] == '>' || logic[i] == '<' || logic[i] == '=' || logic[i] == '!') {
            opStart = i; opLen = 1;
            if (i + 1 < logic.size() && (logic[i + 1] == '=' || logic[i + 1] == '>')) {
                if ((logic[i] == '<' && logic[i + 1] == '=') ||
                    (logic[i] == '>' && logic[i + 1] == '=') ||
                    (logic[i] == '!' && logic[i + 1] == '=') ||
                    (logic[i] == '<' && logic[i + 1] == '>')) opLen = 2;
            }
            break;
        }
    }
    if (opStart == std::string::npos) return "";
    std::string op = logic.substr(opStart, opLen);
    std::string before = logic.substr(0, opStart);
    std::string after = logic.substr(opStart + opLen);
    if (after.size() >= 2 && after.front() == '\'' && after.back() == '\'')
        after = after.substr(1, after.size() - 2);
    return op + before + " " + after;
}

static std::vector<std::string> tokenizeViewCond(const std::string& s) {
    std::vector<std::string> tokens;
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
        if (i >= s.size()) break;
        if (s[i] == '(' || s[i] == ')') {
            tokens.emplace_back(1, s[i]);
            ++i; continue;
        }
        size_t start = i;
        while (i < s.size() && !std::isspace(static_cast<unsigned char>(s[i])) && s[i] != '(' && s[i] != ')') ++i;
        tokens.push_back(s.substr(start, i - start));
    }
    return tokens;
}

// Simple DNF breakdown (AND-of-groups for OR semantics)
static std::vector<std::vector<std::string>> breakDownViewConds(const std::vector<std::string>& tokens) {
    if (tokens.size() == 3 && tokens[0] == "(" && tokens[2] == ")")
        return {{tokens[1]}};
    struct Frame { std::vector<std::vector<std::string>> groups; };
    std::vector<Frame> stack;
    stack.push_back(Frame());
    std::vector<std::string> operandStack;
    auto applyAnd = [&](const std::string& right) {
        Frame& cur = stack.back();
        if (!operandStack.empty()) {
            std::string left = operandStack.back(); operandStack.pop_back();
            if (cur.groups.empty()) cur.groups.push_back({left, right});
            else for (auto& g : cur.groups) g.push_back(right);
        } else {
            if (cur.groups.empty()) cur.groups.push_back({right});
            else for (auto& g : cur.groups) g.push_back(right);
        }
    };
    auto applyOr = [&](const std::string& right) {
        Frame& cur = stack.back();
        if (!operandStack.empty()) {
            std::string left = operandStack.back(); operandStack.pop_back();
            if (cur.groups.empty()) { cur.groups.push_back({left}); cur.groups.push_back({right}); }
            else {
                auto old = cur.groups;
                cur.groups.clear();
                for (auto& g : old) {
                    auto g2 = g;
                    g2.push_back(right);
                    cur.groups.push_back(std::move(g));
                    cur.groups.push_back(std::move(g2));
                }
            }
        } else {
            cur.groups.push_back({right});
        }
    };
    auto flushOp = [&](const std::string& op, const std::string& right) {
        if (op == "and") applyAnd(right);
        else if (op == "or") applyOr(right);
    };
    std::string pendingOp;
    for (size_t i = 0; i < tokens.size(); ++i) {
        const std::string& tok = tokens[i];
        if (tok == "(") {
            stack.push_back(Frame());
            pendingOp.clear();
        } else if (tok == ")") {
            if (!operandStack.empty()) {
                std::string last = operandStack.back(); operandStack.pop_back();
                if (!pendingOp.empty()) { flushOp(pendingOp, last); pendingOp.clear(); }
                else if (!stack.back().groups.empty()) {
                    for (auto& g : stack.back().groups) g.push_back(last);
                } else {
                    stack.back().groups.push_back({last});
                }
            }
            auto curGroups = std::move(stack.back().groups);
            stack.pop_back();
            if (!curGroups.empty()) {
                if (stack.back().groups.empty()) stack.back().groups = std::move(curGroups);
                else {
                    auto old = stack.back().groups;
                    stack.back().groups.clear();
                    for (auto& g1 : old)
                        for (auto& g2 : curGroups) {
                            auto merged = g1;
                            merged.insert(merged.end(), g2.begin(), g2.end());
                            stack.back().groups.push_back(std::move(merged));
                        }
                }
            }
        } else if (tok == "and" || tok == "or") {
            if (!operandStack.empty()) {
                std::string last = operandStack.back(); operandStack.pop_back();
                if (!pendingOp.empty()) flushOp(pendingOp, last);
                else operandStack.push_back(last);
            }
            pendingOp = tok;
        } else {
            operandStack.push_back(tok);
        }
    }
    if (!operandStack.empty()) {
        std::string last = operandStack.back(); operandStack.pop_back();
        if (!pendingOp.empty()) flushOp(pendingOp, last);
        else if (!stack.back().groups.empty()) {
            for (auto& g : stack.back().groups) g.push_back(last);
        } else {
            stack.back().groups.push_back({last});
        }
    }
    return stack.back().groups;
}

bool StorageEngine::validateViewCheckOption(
    const std::string& dbname,
    const std::string& viewname,
    const std::map<std::string, std::string>& colValues) const {

    std::string checkOpt = getViewCheckOption(dbname, viewname);
    if (checkOpt.empty()) return true;

    std::string baseTable = getViewBaseTable(dbname, viewname);
    if (baseTable.empty()) return true;

    TableSchema tbl = getTableSchema(dbname, baseTable);

    // Build simulated row buffer and strip MVCC header
    std::string rowBuffer = buildRowBuffer(tbl, colValues, 0);
    std::string dataOnly(rowBuffer.data() + MVCC_HEADER_SIZE,
                         rowBuffer.size() - MVCC_HEADER_SIZE);

    // Get view SQL and extract WHERE clause
    std::string viewSql = getViewSQL(dbname, viewname);
    std::string selectSql;
    {
        std::stringstream ss(viewSql);
        std::getline(ss, selectSql);
    }
    std::string whereClause = extractViewWhereClause(selectSql);
    if (whereClause.empty()) return true;

    // Parse WHERE conditions into DNF groups
    std::string normWhere = normalizeViewCondStr(whereClause);
    auto tokens = tokenizeViewCond(normWhere);
    auto groups = breakDownViewConds(tokens);

    // For each DNF group (OR branch), all conditions in the group must be satisfied
    // If ANY group is fully satisfied, the WHERE clause is satisfied
    for (const auto& group : groups) {
        std::vector<std::string> condStrs;
        for (const auto& tok : group) {
            if (tok == "and" || tok == "or" || tok == "(" || tok == ")") continue;
            std::string mc = modifyViewLogic(tok);
            if (!mc.empty()) condStrs.push_back(mc);
        }
        if (condStrs.empty()) continue;
        auto conds = parseConditions(condStrs);
        bool groupSatisfied = true;
        for (const auto& c : conds) {
            if (!evalConditionOnRow(c, dataOnly, tbl)) {
                groupSatisfied = false;
                break;
            }
        }
        if (groupSatisfied) return true;
    }
    return false;
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
                                         const std::vector<ProcParam>& params,
                                         const std::vector<std::string>& statements) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto pdir = proceduresDir(dbname);
    if (!std::filesystem::exists(pdir)) {
        std::filesystem::create_directories(pdir);
    }
    std::ofstream ofs(procedurePath(dbname, procname));
    if (!ofs) return OpResult::InvalidValue;
    // Write params metadata line first
    if (!params.empty()) {
        ofs << "PARAMS:";
        for (size_t i = 0; i < params.size(); ++i) {
            if (i > 0) ofs << ',';
            ofs << params[i].name << ':' << params[i].mode << ':' << params[i].type;
        }
        ofs << '\n';
    }
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
        if (line.empty()) continue;
        if (line.substr(0, 7) == "PARAMS:") continue;
        result.push_back(line);
    }
    return result;
}

std::vector<StorageEngine::ProcParam> StorageEngine::getProcedureParams(
    const std::string& dbname, const std::string& procname) const {
    std::vector<ProcParam> result;
    auto path = procedurePath(dbname, procname);
    std::ifstream ifs(path);
    if (!ifs) return result;
    std::string line;
    if (std::getline(ifs, line)) {
        if (line.substr(0, 7) == "PARAMS:") {
            std::string rest = line.substr(7);
            size_t p = 0;
            while (p < rest.size()) {
                size_t comma = rest.find(',', p);
                std::string part = rest.substr(p, comma - p);
                if (!part.empty()) {
                    size_t c1 = part.find(':');
                    size_t c2 = part.find(':', c1 + 1);
                    if (c1 != std::string::npos && c2 != std::string::npos) {
                        ProcParam pp;
                        pp.name = part.substr(0, c1);
                        pp.mode = part.substr(c1 + 1, c2 - c1 - 1);
                        pp.type = part.substr(c2 + 1);
                        result.push_back(pp);
                    }
                }
                if (comma == std::string::npos) break;
                p = comma + 1;
            }
        }
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
// Schema management
// ========================================================================

static std::filesystem::path schemaMarkerPath(const std::string& dbname,
                                              const std::string& schemaname) {
    return std::filesystem::path(dbname) / (".schema_" + schemaname);
}

OpResult StorageEngine::createSchema(const std::string& dbname,
                                     const std::string& schemaname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = schemaMarkerPath(dbname, schemaname);
    if (std::filesystem::exists(path)) return OpResult::Success; // already exists
    std::ofstream ofs(path);
    if (!ofs) return OpResult::InvalidValue;
    return OpResult::Success;
}

OpResult StorageEngine::dropSchema(const std::string& dbname,
                                   const std::string& schemaname,
                                   bool cascade) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = schemaMarkerPath(dbname, schemaname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    if (cascade) {
        std::string prefix = schemaname + "__";
        auto dbPath = std::filesystem::path(dbname);
        for (const auto& entry : std::filesystem::directory_iterator(dbPath)) {
            if (!entry.is_regular_file()) continue;
            std::string name = entry.path().filename().string();
            if (name.size() > 3 && name.substr(name.size() - 3) == ".dt") {
                std::string tname = name.substr(0, name.size() - 3);
                if (tname.size() > prefix.size() && tname.substr(0, prefix.size()) == prefix) {
                    dropTable(dbname, tname);
                }
            }
        }
    }
    std::filesystem::remove(path);
    return OpResult::Success;
}

OpResult StorageEngine::renameSchema(const std::string& dbname,
                                     const std::string& oldname,
                                     const std::string& newname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto oldPath = schemaMarkerPath(dbname, oldname);
    auto newPath = schemaMarkerPath(dbname, newname);
    if (!std::filesystem::exists(oldPath)) return OpResult::TableNotExist;
    if (std::filesystem::exists(newPath)) return OpResult::InvalidValue;
    std::filesystem::rename(oldPath, newPath);
    // Rename all tables in the old schema
    std::string oldPrefix = oldname + "__";
    std::string newPrefix = newname + "__";
    auto dbPath = std::filesystem::path(dbname);
    for (const auto& entry : std::filesystem::directory_iterator(dbPath)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > 3 && name.substr(name.size() - 3) == ".dt") {
            std::string tname = name.substr(0, name.size() - 3);
            if (tname.size() > oldPrefix.size() && tname.substr(0, oldPrefix.size()) == oldPrefix) {
                std::string rest = tname.substr(oldPrefix.size());
                std::string newTname = newPrefix + rest;
                // Rename .dt, .stc, .idx files
                for (const char* ext : {".dt", ".stc", ".idx"}) {
                    auto oldFile = dbPath / (tname + ext);
                    auto newFile = dbPath / (newTname + ext);
                    if (std::filesystem::exists(oldFile)) {
                        std::filesystem::rename(oldFile, newFile);
                    }
                }
                // Update tlist.lst
                auto tlistPath = dbPath / "tlist.lst";
                if (std::filesystem::exists(tlistPath)) {
                    std::ifstream ifs(tlistPath);
                    std::vector<std::string> lines;
                    std::string line;
                    while (std::getline(ifs, line)) lines.push_back(line);
                    std::ofstream ofs(tlistPath);
                    for (const auto& l : lines) {
                        if (l == tname) ofs << newTname << '\n';
                        else ofs << l << '\n';
                    }
                }
            }
        }
    }
    return OpResult::Success;
}

OpResult StorageEngine::alterTableSetSchema(const std::string& dbname,
                                            const std::string& tablename,
                                            const std::string& targetDbname) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (!databaseExists(targetDbname)) return OpResult::DatabaseNotExist;
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    if (tableExists(targetDbname, tablename)) return OpResult::TableAlreadyExist;

    auto srcPath = std::filesystem::path(dbname);
    auto dstPath = std::filesystem::path(targetDbname);

    // Move known table files (.dt, .idx, .stc, .seq)
    for (const char* ext : {".dt", ".idx", ".stc", ".seq"}) {
        auto oldFile = srcPath / (tablename + ext);
        auto newFile = dstPath / (tablename + ext);
        if (std::filesystem::exists(oldFile)) {
            std::filesystem::rename(oldFile, newFile);
        }
    }

    // Move index files: {tname}_{col}.{ext}
    for (const auto& entry : std::filesystem::directory_iterator(srcPath)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        // Match patterns like tname_col.fti, tname_col.gin, etc.
        size_t underscore = name.find('_');
        if (underscore != std::string::npos && name.substr(0, underscore) == tablename) {
            std::filesystem::rename(entry.path(), dstPath / name);
        }
    }

    // Also move composite index file if exists
    auto compIdx = srcPath / (tablename + "_composite.idx");
    if (std::filesystem::exists(compIdx)) {
        std::filesystem::rename(compIdx, dstPath / (tablename + "_composite.idx"));
    }

    // Update tlist.lst in source database: remove tablename
    {
        auto srcTlist = srcPath / "tlist.lst";
        if (std::filesystem::exists(srcTlist)) {
            std::ifstream ifs(srcTlist);
            std::vector<std::string> lines;
            std::string line;
            while (std::getline(ifs, line)) lines.push_back(line);
            std::ofstream ofs(srcTlist);
            for (const auto& l : lines) {
                if (l != tablename) ofs << l << '\n';
            }
        }
    }

    // Update tlist.lst in target database: add tablename
    {
        auto dstTlist = dstPath / "tlist.lst";
        std::vector<std::string> lines;
        if (std::filesystem::exists(dstTlist)) {
            std::ifstream ifs(dstTlist);
            std::string line;
            while (std::getline(ifs, line)) lines.push_back(line);
        }
        lines.push_back(tablename);
        std::ofstream ofs(dstTlist);
        for (const auto& l : lines) ofs << l << '\n';
    }

    // Move permissions from source to target database
    {
        auto permPath = dbPath("permissions.dat");
        if (std::filesystem::exists(permPath)) {
            std::ifstream ifs(permPath);
            std::vector<std::string> keepLines;
            std::vector<std::string> moveLines;
            std::string line;
            while (std::getline(ifs, line)) {
                // Format: dbname tablename username permission
                std::stringstream ss(line);
                std::string ldb, ltbl, luser, lperm;
                ss >> ldb >> ltbl >> luser >> lperm;
                if (ldb == dbname && ltbl == tablename) {
                    moveLines.push_back(targetDbname + " " + tablename + " " + luser + " " + lperm);
                } else {
                    keepLines.push_back(line);
                }
            }
            std::ofstream ofs(permPath);
            for (const auto& l : keepLines) ofs << l << '\n';
            std::ofstream ofsAppend(permPath, std::ios::app);
            for (const auto& l : moveLines) ofsAppend << l << '\n';
        }
    }

    return OpResult::Success;
}

bool StorageEngine::schemaExists(const std::string& dbname,
                                 const std::string& schemaname) const {
    return std::filesystem::exists(schemaMarkerPath(dbname, schemaname));
}

std::vector<std::string> StorageEngine::getSchemaNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto dbPath = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dbPath)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(dbPath)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 8 && name.substr(0, 8) == ".schema_") {
                result.push_back(name.substr(8));
            }
        }
    }
    return result;
}

std::vector<std::string> StorageEngine::getTablesInSchema(
    const std::string& dbname, const std::string& schemaname) const {
    std::vector<std::string> result;
    std::string prefix = schemaname + "__";
    auto dbPath = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dbPath)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(dbPath)) {
        if (entry.is_regular_file()) {
            std::string name = entry.path().filename().string();
            if (name.size() > 3 && name.substr(name.size() - 3) == ".dt") {
                std::string tname = name.substr(0, name.size() - 3);
                if (tname.size() > prefix.size() &&
                    tname.substr(0, prefix.size()) == prefix) {
                    result.push_back(tname.substr(prefix.size()));
                }
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

OpResult StorageEngine::createUDF(const std::string& dbname,
                                   const std::string& funcname,
                                   const std::vector<std::string>& params,
                                   const std::vector<std::string>& types,
                                   const std::string& expression) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto fdir = udfDir(dbname);
    if (!std::filesystem::exists(fdir)) {
        std::filesystem::create_directories(fdir);
    }
    std::ofstream ofs(udfPath(dbname, funcname));
    if (!ofs) return OpResult::InvalidValue;
    ofs << "PARAMS:";
    for (size_t i = 0; i < params.size(); ++i) {
        if (i > 0) ofs << ',';
        ofs << params[i] << ':' << (i < types.size() ? types[i] : "");
    }
    ofs << "\n" << expression << "\n";
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
    std::string line;
    if (std::getline(ifs, line)) {
        if (line.substr(0, 7) == "PARAMS:") {
            std::string rest = line.substr(7);
            size_t p = 0;
            while (p < rest.size()) {
                size_t comma = rest.find(',', p);
                std::string part = rest.substr(p, comma - p);
                if (!part.empty()) {
                    size_t colon = part.find(':');
                    if (colon != std::string::npos) {
                        info.paramNames.push_back(part.substr(0, colon));
                        info.paramTypes.push_back(part.substr(colon + 1));
                    } else {
                        info.paramNames.push_back(part);
                    }
                }
                if (comma == std::string::npos) break;
                p = comma + 1;
            }
            info.paramName = info.paramNames.empty() ? "" : info.paramNames[0];
        } else {
            info.paramName = line;
            info.paramNames.push_back(line);
        }
    }
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

std::vector<StorageEngine::BufferCacheEntry> StorageEngine::getBufferCacheEntries() const {
    std::vector<BufferCacheEntry> result;
    for (const auto& kv : pageAllocators_) {
        if (kv.second && kv.second->bufferPool()) {
            auto* bp = kv.second->bufferPool();
            auto frames = bp->getFrameInfo();
            for (const auto& f : frames) {
                if (f.valid) {
                    result.push_back({kv.first, f.pageId, f.dirty, f.pinCount});
                }
            }
        }
    }
    return result;
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
// Tablespace management
// ========================================================================
std::filesystem::path StorageEngine::tablespaceDir(const std::string& dbname,
                                                    const std::string& tablespaceName) const {
    if (tablespaceName.empty() || tablespaceName == "pg_default") {
        return dbPath(dbname);
    }
    auto tsPath = dbPath(dbname) / "pg_tblspc" / (tablespaceName + ".path");
    if (std::filesystem::exists(tsPath)) {
        std::ifstream ifs(tsPath);
        std::string path;
        if (std::getline(ifs, path) && !path.empty()) {
            return std::filesystem::path(path);
        }
    }
    // Fallback to default if tablespace file missing
    return dbPath(dbname);
}

OpResult StorageEngine::createTablespace(const std::string& dbname,
                                          const std::string& tsName,
                                          const std::string& physicalPath) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (tsName.empty() || tsName == "pg_default" || tsName == "pg_global") {
        return OpResult::InvalidValue; // reserved names
    }
    auto tsDir = dbPath(dbname) / "pg_tblspc";
    if (!std::filesystem::exists(tsDir)) {
        std::filesystem::create_directories(tsDir);
    }
    auto tsPath = tsDir / (tsName + ".path");
    if (std::filesystem::exists(tsPath)) return OpResult::TableAlreadyExist;

    // Ensure physical directory exists
    if (!std::filesystem::exists(physicalPath)) {
        std::filesystem::create_directories(physicalPath);
    }
    std::ofstream ofs(tsPath);
    if (!ofs) return OpResult::InvalidValue;
    ofs << physicalPath << "\n";
    return OpResult::Success;
}

OpResult StorageEngine::dropTablespace(const std::string& dbname,
                                        const std::string& tsName) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (tsName.empty() || tsName == "pg_default" || tsName == "pg_global") {
        return OpResult::InvalidValue;
    }
    auto tsPath = dbPath(dbname) / "pg_tblspc" / (tsName + ".path");
    if (!std::filesystem::exists(tsPath)) return OpResult::TableNotExist;

    // Check if any table uses this tablespace
    auto tables = getTableNames(dbname);
    for (const auto& tname : tables) {
        TableSchema tbl = getTableSchema(dbname, tname);
        if (tbl.tablespace == tsName) return OpResult::InvalidValue; // not empty
    }
    std::filesystem::remove(tsPath);
    return OpResult::Success;
}

std::vector<std::string> StorageEngine::listTablespaces(const std::string& dbname) const {
    std::vector<std::string> result;
    result.push_back("pg_default");
    auto tsDir = dbPath(dbname) / "pg_tblspc";
    if (!std::filesystem::exists(tsDir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(tsDir)) {
        std::string fname = entry.path().filename().string();
        if (fname.size() > 5 && fname.substr(fname.size() - 5) == ".path") {
            result.push_back(fname.substr(0, fname.size() - 5));
        }
    }
    return result;
}

// ========================================================================
// Page Allocator
// ========================================================================

PageAllocator* StorageEngine::getPageAllocator(const std::string& dbname,
                                                const std::string& tablename) const {
    std::string key = dbname + "/" + tablename;
    auto it = pageAllocators_.find(key);
    if (it != pageAllocators_.end()) return it->second.get();

    TableSchema tbl = getTableSchema(dbname, tablename);
    // Route data file according to tablespace
    std::filesystem::path baseDir = tablespaceDir(dbname, tbl.tablespace);
    std::filesystem::path dt = baseDir / (tablename + ".dt");

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

    auto pa = std::make_unique<PageAllocator>(dt.string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
    pa->open();
    PageAllocator* ptr = pa.get();
    pageAllocators_[key] = std::move(pa);
    return ptr;
}

void StorageEngine::closeAllPageAllocators() {
    pageAllocators_.clear();
}

FreeSpaceMap* StorageEngine::getFSM(const std::string& dbname,
                                     const std::string& tablename) const {
    std::string key = dbname + "/" + tablename;
    auto it = fsmCache_.find(key);
    if (it != fsmCache_.end()) return it->second.get();

    auto fsm = std::make_unique<FreeSpaceMap>(fsmPath(dbname, tablename).string());
    fsm->open();
    FreeSpaceMap* ptr = fsm.get();
    fsmCache_[key] = std::move(fsm);
    return ptr;
}

void StorageEngine::closeAllFSM() {
    fsmCache_.clear();
}

VisibilityMap* StorageEngine::getVM(const std::string& dbname,
                                     const std::string& tablename) const {
    std::string key = dbname + "/" + tablename;
    auto it = vmCache_.find(key);
    if (it != vmCache_.end()) return it->second.get();

    auto vm = std::make_unique<VisibilityMap>(vmPath(dbname, tablename).string());
    vm->open();
    VisibilityMap* ptr = vm.get();
    vmCache_[key] = std::move(vm);
    return ptr;
}

void StorageEngine::closeAllVM() {
    vmCache_.clear();
}

void StorageEngine::migrateToPageStorage(const std::string& dbname,
                                          const std::string& tablename) const {
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t rowSize = tbl.rowSize();
    std::filesystem::path oldPath = dataPath(dbname, tablename);
    std::string tmpPath = oldPath.string() + ".new";

    {
        PageAllocator pa(tmpPath, rowSize, pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
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
                PageWrapper page(buf, pa.pageSize(), tbl.formatVersion);
                uint16_t slotId = 0;
                if (!page.insert(row.data(), rowSize, slotId)) {
                    pa.markDirty(currentPageId);
                    pa.unpinPage(currentPageId);
                    currentPageId = pa.allocPage();
                    buf = pa.fetchPage(currentPageId);
                    page = PageWrapper(buf, pa.pageSize(), tbl.formatVersion);
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
                if (!tbl.defaultPartitionName.empty()) partNames.push_back(tbl.defaultPartitionName);
            } else if (tbl.partitionType == TableSchema::PartitionType::Hash) {
                for (size_t i = 0; i < tbl.hashPartitions; ++i) partNames.push_back("p" + std::to_string(i));
            }
        }
        for (const auto& pname : partNames) {
            if (tbl.subPartitionType != TableSchema::PartitionType::None) {
                std::vector<std::string> subNames;
                if (tbl.subPartitionType == TableSchema::PartitionType::Hash) {
                    for (size_t i = 0; i < tbl.subHashPartitions; ++i) subNames.push_back("sp" + std::to_string(i));
                }
                for (const auto& spname : subNames) {
                    auto ppa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tablename, pname, spname).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
                    if (!ppa->open()) continue;
                    uint32_t np = ppa->numPages();
                    VisibilityMap* vm = getVM(dbname, tablename);
                    for (uint32_t pid = 1; pid < np; ++pid) {
                        lockManager_.pageLockShared(dbname, tablename, pid);
                        char* buf = ppa->fetchPage(pid);
                        PageWrapper page(buf, ppa->pageSize(), tbl.formatVersion);
                        bool allVisible = vm->isAllVisible(pid);
                        if (allVisible && !rv) {
                            page.forEachLive([&callback, pid, this](uint16_t sid, const char* data, size_t len) {
                                if (len <= MVCC_HEADER_SIZE) return;
                                if (this->inTransaction_ && this->txnIsolationLevel_ == IsolationLevel::Serializable) {
                                    int64_t rid = this->encodeRid(pid, sid);
                                    this->txnReadRids_.insert(rid);
                                    std::lock_guard<std::mutex> lock(ssiMutex_);
                                    ssiReadSets_[this->currentTxnId_].insert(rid);
                                }
                                callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                            });
                        } else {
                            page.forEachLive([&callback, pid, rv, this](uint16_t sid, const char* data, size_t len) {
                                if (len <= MVCC_HEADER_SIZE) return;
                                if (rv) {
                                    uint64_t rowTxnId = 0;
                                    std::memcpy(&rowTxnId, data, sizeof(uint64_t));
                                    if (!rv->isVisible(rowTxnId)) return;
                                }
                                if (this->inTransaction_ && this->txnIsolationLevel_ == IsolationLevel::Serializable) {
                                    int64_t rid = this->encodeRid(pid, sid);
                                    this->txnReadRids_.insert(rid);
                                    std::lock_guard<std::mutex> lock(ssiMutex_);
                                    ssiReadSets_[this->currentTxnId_].insert(rid);
                                }
                                callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                            });
                        }
                        ppa->unpinPage(pid);
                        lockManager_.pageUnlock(dbname, tablename, pid);
                    }
                    ppa->close();
                }
            } else {
                auto ppa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tablename, pname).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
                if (!ppa->open()) continue;
                uint32_t np = ppa->numPages();
                VisibilityMap* vm = getVM(dbname, tablename);
                for (uint32_t pid = 1; pid < np; ++pid) {
                    lockManager_.pageLockShared(dbname, tablename, pid);
                    char* buf = ppa->fetchPage(pid);
                    PageWrapper page(buf, ppa->pageSize(), tbl.formatVersion);
                    bool allVisible = vm->isAllVisible(pid);
                    if (allVisible && !rv) {
                        page.forEachLive([&callback, pid, this](uint16_t sid, const char* data, size_t len) {
                            if (len <= MVCC_HEADER_SIZE) return;
                            if (this->inTransaction_ && this->txnIsolationLevel_ == IsolationLevel::Serializable) {
                                int64_t rid = this->encodeRid(pid, sid);
                                this->txnReadRids_.insert(rid);
                                std::lock_guard<std::mutex> lock(ssiMutex_);
                                ssiReadSets_[this->currentTxnId_].insert(rid);
                            }
                            callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                        });
                    } else {
                        page.forEachLive([&callback, pid, rv, this](uint16_t sid, const char* data, size_t len) {
                            if (len <= MVCC_HEADER_SIZE) return;
                            if (rv) {
                                uint64_t rowTxnId = 0;
                                std::memcpy(&rowTxnId, data, sizeof(uint64_t));
                                if (!rv->isVisible(rowTxnId)) return;
                            }
                            if (this->inTransaction_ && this->txnIsolationLevel_ == IsolationLevel::Serializable) {
                                int64_t rid = this->encodeRid(pid, sid);
                                this->txnReadRids_.insert(rid);
                                std::lock_guard<std::mutex> lock(ssiMutex_);
                                ssiReadSets_[this->currentTxnId_].insert(rid);
                            }
                            callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                        });
                    }
                    ppa->unpinPage(pid);
                    lockManager_.pageUnlock(dbname, tablename, pid);
                }
                ppa->close();
            }
        }
        return;
    }

    PageAllocator* pa = getPageAllocator(dbname, tablename);
    if (!pa) return;
    VisibilityMap* vm = getVM(dbname, tablename);
    uint32_t np = pa->numPages();
    for (uint32_t pid = 1; pid < np; ++pid) {
        lockManager_.pageLockShared(dbname, tablename, pid);
        char* buf = pa->fetchPage(pid);
        PageWrapper page(buf, pa->pageSize(), tbl.formatVersion);
        // VM optimization: skip MVCC check if page is AllVisible and no ReadView
        bool allVisible = vm->isAllVisible(pid);
        if (allVisible && !rv) {
            page.forEachLive([&callback, pid, this](uint16_t sid, const char* data, size_t len) {
                if (len <= MVCC_HEADER_SIZE) return;
                if (this->inTransaction_ && this->txnIsolationLevel_ == IsolationLevel::Serializable) {
                    int64_t rid = this->encodeRid(pid, sid);
                    this->txnReadRids_.insert(rid);
                    std::lock_guard<std::mutex> lock(ssiMutex_);
                    ssiReadSets_[this->currentTxnId_].insert(rid);
                }
                callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
            });
        } else {
            page.forEachLive([&callback, pid, rv, this](uint16_t sid, const char* data, size_t len) {
                if (len <= MVCC_HEADER_SIZE) return;
                if (rv) {
                    uint64_t rowTxnId = 0;
                    std::memcpy(&rowTxnId, data, sizeof(uint64_t));
                    if (!rv->isVisible(rowTxnId)) return;
                }
                if (this->inTransaction_ && this->txnIsolationLevel_ == IsolationLevel::Serializable) {
                    int64_t rid = this->encodeRid(pid, sid);
                    this->txnReadRids_.insert(rid);
                    std::lock_guard<std::mutex> lock(ssiMutex_);
                    ssiReadSets_[this->currentTxnId_].insert(rid);
                }
                callback(pid, sid, data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
            });
        }
        pa->unpinPage(pid);
        lockManager_.pageUnlock(dbname, tablename, pid);
    }
}

bool StorageEngine::readRowByRid(PageAllocator* pa, int64_t rid, std::string& rowBuffer,
                                  const TableSchema& tbl) const {
    if (!pa) return false;
    uint32_t pageId = 0;
    uint16_t slotId = 0;
    decodeRid(rid, pageId, slotId);
    char* buf = pa->fetchPage(pageId);
    PageWrapper page(buf, pa->pageSize(), tbl.formatVersion);
    const char* data = nullptr;
    size_t len = 0;
    bool ok = page.read(slotId, data, len);
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
                                         const std::string& colname,
                                         bool /*concurrently*/) {
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
    } else if (col.dataType == "point") {
        double x = 0.0, y = 0.0;
        std::memcpy(&x, rowBuffer.data() + offset, sizeof(double));
        std::memcpy(&y, rowBuffer.data() + offset + sizeof(double), sizeof(double));
        std::ostringstream oss;
        oss << x << "," << y;
        return oss.str();
    } else if (col.dataType == "inet" || col.dataType == "cidr") {
        // Format: 1B family | 1B prefix_len | 1B is_cidr | 1B reserved | 16B addr
        uint8_t family = static_cast<uint8_t>(rowBuffer[offset]);
        uint8_t prefix = static_cast<uint8_t>(rowBuffer[offset + 1]);
        std::string addrStr;
        if (family == 2) {  // IPv4
            uint8_t a = static_cast<uint8_t>(rowBuffer[offset + 4]);
            uint8_t b = static_cast<uint8_t>(rowBuffer[offset + 5]);
            uint8_t c = static_cast<uint8_t>(rowBuffer[offset + 6]);
            uint8_t d = static_cast<uint8_t>(rowBuffer[offset + 7]);
            std::ostringstream oss;
            oss << (int)a << "." << (int)b << "." << (int)c << "." << (int)d;
            addrStr = oss.str();
            if (col.dataType == "cidr" || prefix < 32)
                addrStr += "/" + std::to_string(prefix);
        } else {
            return "";  // IPv6 not yet supported
        }
        return addrStr;
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
    n = trg.whenCondition.size();
    out.write(reinterpret_cast<const char*>(&n), sizeof(size_t));
    out.write(trg.whenCondition.data(), static_cast<std::streamsize>(n));
    char forEachRow = trg.forEachRow ? 1 : 0;
    out.write(&forEachRow, sizeof(char));
    char enabled = trg.enabled ? 1 : 0;
    out.write(&enabled, sizeof(char));
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
    // Backward compatibility: whenCondition may not exist in old files
    readStr(trg.whenCondition);
    // Backward compatibility: forEachRow flag may not exist in old files
    char forEachRow = 1;
    in.read(&forEachRow, sizeof(char));
    if (in) trg.forEachRow = (forEachRow != 0);
    // Backward compatibility: enabled flag may not exist in old files
    char enabled = 1;
    in.read(&enabled, sizeof(char));
    if (in) trg.enabled = (enabled != 0);
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
        if (t.tableName == tablename && t.timing == timing && t.event == event && t.enabled) {
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

OpResult StorageEngine::enableTrigger(const std::string& dbname, const std::string& trgName) {
    auto existing = getAllTriggers(dbname);
    bool found = false;
    {
        std::ofstream out(triggerPath(dbname), std::ios::binary | std::ios::trunc);
        size_t count = existing.size();
        out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
        for (auto& t : existing) {
            if (t.name == trgName) { t.enabled = true; found = true; }
            writeTrigger(out, t);
        }
    }
    return found ? OpResult::Success : OpResult::TableNotExist;
}

OpResult StorageEngine::disableTrigger(const std::string& dbname, const std::string& trgName) {
    auto existing = getAllTriggers(dbname);
    bool found = false;
    {
        std::ofstream out(triggerPath(dbname), std::ios::binary | std::ios::trunc);
        size_t count = existing.size();
        out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
        for (auto& t : existing) {
            if (t.name == trgName) { t.enabled = false; found = true; }
            writeTrigger(out, t);
        }
    }
    return found ? OpResult::Success : OpResult::TableNotExist;
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
                                     const std::string& expression,
                                     bool concurrently) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    if (concurrently) {
        lockManager_.lockShared(tablename);
    } else {
        lockManager_.lockMetadata(tablename);
    }
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
            lockManager_.unlock(tablename);
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
    if (colIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

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
            lockManager_.unlock(tablename);
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
    lockManager_.unlock(tablename);
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
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::createCompositeIndex(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::vector<std::string>& colnames,
                                              const std::string& indexName,
                                              const std::vector<std::string>& includeCols,
                                              const std::string& whereCondition,
                                              bool concurrently) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    if (concurrently) {
        lockManager_.lockShared(tablename);
    } else {
        lockManager_.lockMetadata(tablename);
    }
    TableSchema tbl = getTableSchema(dbname, tablename);

    // Validate all columns exist
    for (const auto& cname : colnames) {
        bool found = false;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == cname) { found = true; break; }
        }
        if (!found) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
    }

    // Check name collision with existing composite indexes
    auto existingComp = getCompositeIndexes(dbname, tablename);
    for (const auto& ci : existingComp) {
        if (ci.name == indexName) {
            lockManager_.unlock(tablename);
            return OpResult::Success;
        }
    }

    // Parse WHERE condition for partial index
    std::vector<Condition> whereConds;
    if (!whereCondition.empty()) {
        whereConds = parseConditions(std::vector<std::string>{whereCondition});
    }

    // Build index from existing data
    BPTree* idx = getCompositeIndexTree(dbname, tablename, indexName);
    if (!idx) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

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
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::dropCompositeIndex(const std::string& dbname,
                                            const std::string& tablename,
                                            const std::string& indexName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    // Verify the composite index actually exists
    auto compIdxs = getCompositeIndexes(dbname, tablename);
    bool found = false;
    for (const auto& ci : compIdxs) {
        if (ci.name == indexName) { found = true; break; }
    }
    if (!found) return OpResult::TableNotExist;
    std::filesystem::path p = dbPath(dbname) / (tablename + ".idx_" + indexName);
    std::filesystem::remove(p);

    // Rewrite metadata without this composite index
    auto singleCols = getIndexedColumns(dbname, tablename);
    // compIdxs already fetched above; filter out the dropped one

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

// ========================================================================
// GIN index (Generalized Inverted Index)
// ========================================================================

static std::filesystem::path ginIndexPath(const std::string& dbname,
                                            const std::string& tablename,
                                            const std::string& colname) {
    return std::filesystem::path(dbname) / (tablename + "_" + colname + ".gin");
}

// Extract keys from a column value for GIN indexing
static std::vector<std::string> extractGinKeys(const std::string& value, const std::string& dataType) {
    std::vector<std::string> keys;
    if (dataType == "text" || dataType == "varchar" || dataType == "char") {
        // Text: tokenize (same as full-text)
        keys = tokenizeText(value);
    } else if (dataType == "json" || dataType == "jsonb") {
        // JSON: extract all string/number leaf values
        // Simple parser: find "..." or numeric values outside quotes
        size_t i = 0;
        while (i < value.size()) {
            // Skip whitespace and structural chars
            while (i < value.size() && (std::isspace(static_cast<unsigned char>(value[i])) ||
                                         value[i] == '{' || value[i] == '}' ||
                                         value[i] == '[' || value[i] == ']' ||
                                         value[i] == ':' || value[i] == ',')) ++i;
            if (i >= value.size()) break;
            if (value[i] == '"') {
                // String value
                size_t end = value.find('"', i + 1);
                if (end == std::string::npos) break;
                std::string s = value.substr(i + 1, end - i - 1);
                // Skip keys (strings followed by ':')
                size_t after = end + 1;
                while (after < value.size() && std::isspace(static_cast<unsigned char>(value[after]))) ++after;
                if (after < value.size() && value[after] == ':') {
                    // This is a key, skip it
                } else {
                    keys.push_back(s);
                }
                i = end + 1;
            } else if (std::isdigit(static_cast<unsigned char>(value[i])) || value[i] == '-') {
                // Number value
                size_t j = i + 1;
                while (j < value.size() && (std::isdigit(static_cast<unsigned char>(value[j])) ||
                                             value[j] == '.' || value[j] == 'e' || value[j] == 'E' ||
                                             value[j] == '+' || value[j] == '-')) ++j;
                keys.push_back(value.substr(i, j - i));
                i = j;
            } else {
                // true/false/null
                if (value.substr(i, 4) == "true") { keys.push_back("true"); i += 4; }
                else if (value.substr(i, 5) == "false") { keys.push_back("false"); i += 5; }
                else if (value.substr(i, 4) == "null") { keys.push_back("null"); i += 4; }
                else { ++i; }
            }
        }
    } else if (dataType.find("[") != std::string::npos || dataType == "array") {
        // Array: split by comma, strip brackets
        std::string trimmed = value;
        if (!trimmed.empty() && trimmed.front() == '{') trimmed = trimmed.substr(1);
        if (!trimmed.empty() && trimmed.back() == '}') trimmed.pop_back();
        size_t cp = 0;
        while (cp < trimmed.size()) {
            size_t c = trimmed.find(',', cp);
            std::string v = trim(trimmed.substr(cp, c - cp));
            if (!v.empty()) keys.push_back(v);
            if (c == std::string::npos) break;
            cp = c + 1;
        }
    } else {
        // Other types: value itself as single key
        keys.push_back(value);
    }
    return keys;
}

OpResult StorageEngine::createGinIndex(const std::string& dbname,
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
        auto keys = extractGinKeys(val, tbl.cols[colIdx].dataType);
        int64_t rid = encodeRid(pageId, slotId);
        for (const auto& k : keys) {
            if (!k.empty()) inverted[k].insert(rid);
        }
    });

    auto path = ginIndexPath(dbname, tablename, colname);
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

OpResult StorageEngine::dropGinIndex(const std::string& dbname,
                                      const std::string& tablename,
                                      const std::string& colname) {
    auto path = ginIndexPath(dbname, tablename, colname);
    if (std::filesystem::exists(path)) std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::hasGinIndex(const std::string& dbname,
                                 const std::string& tablename,
                                 const std::string& colname) const {
    return std::filesystem::exists(ginIndexPath(dbname, tablename, colname));
}

std::vector<int64_t> StorageEngine::ginSearch(const std::string& dbname,
                                               const std::string& tablename,
                                               const std::string& colname,
                                               const std::string& key) const {
    std::vector<int64_t> result;
    auto path = ginIndexPath(dbname, tablename, colname);
    std::ifstream in(path);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        size_t sp = line.find(' ');
        std::string tok = (sp == std::string::npos) ? line : line.substr(0, sp);
        if (tok == key) {
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

std::vector<std::string> StorageEngine::getGinIndexedColumns(const std::string& dbname,
                                                              const std::string& tablename) const {
    std::vector<std::string> result;
    auto dir = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dir)) return result;
    std::string prefix = tablename + "_";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(name.size() - 4) == ".gin" &&
            name.substr(0, prefix.size()) == prefix) {
            std::string colname = name.substr(prefix.size(), name.size() - prefix.size() - 4);
            result.push_back(colname);
        }
    }
    return result;
}

// ========================================================================
// GiST index (Generalized Search Tree) - simplified range/spatial index
// ========================================================================

static std::filesystem::path giSTIndexPath(const std::string& dbname,
                                             const std::string& tablename,
                                             const std::string& colname) {
    return std::filesystem::path(dbname) / (tablename + "_" + colname + ".gist");
}

static std::filesystem::path spGiSTIndexPath(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::string& colname) {
    return std::filesystem::path(dbname) / (tablename + "_" + colname + ".spgist");
}

OpResult StorageEngine::createGiSTIndex(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& colname) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) return OpResult::InvalidValue;

    auto path = giSTIndexPath(dbname, tablename, colname);
    std::ofstream out(path);
    if (!out) return OpResult::InvalidValue;

    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        std::string val = extractColumnValue(row, tbl, colIdx);
        int64_t rid = encodeRid(pageId, slotId);
        // For GiST we store each row's value range.
        // For single scalar values, min=max=val.
        // For multi-value (array), min=min element, max=max element.
        // For text, we store the value itself (prefix containment).
        out << rid << ' ' << val << ' ' << val << '\n';
    });
    return OpResult::Success;
}

OpResult StorageEngine::dropGiSTIndex(const std::string& dbname,
                                       const std::string& tablename,
                                       const std::string& colname) {
    auto path = giSTIndexPath(dbname, tablename, colname);
    if (std::filesystem::exists(path)) std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::hasGiSTIndex(const std::string& dbname,
                                  const std::string& tablename,
                                  const std::string& colname) const {
    return std::filesystem::exists(giSTIndexPath(dbname, tablename, colname));
}

std::vector<int64_t> StorageEngine::giSTSearchOverlap(const std::string& dbname,
                                                       const std::string& tablename,
                                                       const std::string& colname,
                                                       const std::string& low,
                                                       const std::string& high) const {
    std::vector<int64_t> result;
    auto path = giSTIndexPath(dbname, tablename, colname);
    std::ifstream in(path);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        int64_t rid;
        std::string entryLow, entryHigh;
        if (!(ss >> rid >> entryLow >> entryHigh)) continue;
        // Overlap: entry range [entryLow, entryHigh] intersects [low, high]
        // i.e., NOT (entryHigh < low OR entryLow > high)
        if (!(entryHigh < low || entryLow > high)) result.push_back(rid);
    }
    return result;
}

std::vector<int64_t> StorageEngine::giSTSearchContainedBy(const std::string& dbname,
                                                           const std::string& tablename,
                                                           const std::string& colname,
                                                           const std::string& low,
                                                           const std::string& high) const {
    std::vector<int64_t> result;
    auto path = giSTIndexPath(dbname, tablename, colname);
    std::ifstream in(path);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        int64_t rid;
        std::string entryLow, entryHigh;
        if (!(ss >> rid >> entryLow >> entryHigh)) continue;
        // Contained by: entry range is fully within [low, high]
        // i.e., low <= entryLow AND entryHigh <= high
        if (low <= entryLow && entryHigh <= high) result.push_back(rid);
    }
    return result;
}

std::vector<std::string> StorageEngine::getGiSTIndexedColumns(const std::string& dbname,
                                                               const std::string& tablename) const {
    std::vector<std::string> result;
    auto dir = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dir)) return result;
    std::string prefix = tablename + "_";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > 6 && name.substr(name.size() - 6) == ".gist" &&
            name.substr(0, prefix.size()) == prefix) {
            std::string colname = name.substr(prefix.size(), name.size() - prefix.size() - 6);
            result.push_back(colname);
        }
    }
    return result;
}

// ========================================================================
// SP-GiST index (Space-Partitioned GiST) - quadtree for POINT type
// ========================================================================

OpResult StorageEngine::createSPGiSTIndex(const std::string& dbname,
                                           const std::string& tablename,
                                           const std::string& colname) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) return OpResult::InvalidValue;

    auto path = spGiSTIndexPath(dbname, tablename, colname);
    std::ofstream out(path);
    if (!out) return OpResult::InvalidValue;

    forEachRow(dbname, tablename, [&](uint32_t pageId, uint16_t slotId,
                                       const char* data, size_t len) {
        std::string row(data, len);
        std::string val = extractColumnValue(row, tbl, colIdx);
        int64_t rid = encodeRid(pageId, slotId);
        out << rid << ' ' << val << '\n';
    });
    return OpResult::Success;
}

OpResult StorageEngine::dropSPGiSTIndex(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& colname) {
    auto path = spGiSTIndexPath(dbname, tablename, colname);
    if (std::filesystem::exists(path)) std::filesystem::remove(path);
    spGiSTCache_.erase(dbname + "/" + tablename + "/" + colname);
    return OpResult::Success;
}

bool StorageEngine::hasSPGiSTIndex(const std::string& dbname,
                                    const std::string& tablename,
                                    const std::string& colname) const {
    return std::filesystem::exists(spGiSTIndexPath(dbname, tablename, colname));
}

std::vector<int64_t> StorageEngine::spGiSTSearch(const std::string& dbname,
                                                  const std::string& tablename,
                                                  const std::string& colname,
                                                  const std::string& op,
                                                  const std::string& value) const {
    std::vector<int64_t> result;
    auto path = spGiSTIndexPath(dbname, tablename, colname);
    if (!std::filesystem::exists(path)) return result;

    // Build or retrieve cached quadtree
    std::string cacheKey = dbname + "/" + tablename + "/" + colname;
    SPGiSTIndex* idx = nullptr;
    {
        std::lock_guard<std::mutex> lock(spGiSTMutex_);
        auto it = spGiSTCache_.find(cacheKey);
        if (it != spGiSTCache_.end()) {
            idx = it->second.get();
        } else {
            auto newIdx = std::make_unique<SPGiSTIndex>(-1e9, -1e9, 1e9, 1e9);
            std::ifstream in(path);
            if (in) {
                std::string line;
                while (std::getline(in, line)) {
                    std::stringstream ss(line);
                    int64_t rid;
                    std::string pt;
                    if (!(ss >> rid >> pt)) continue;
                    double x = 0, y = 0;
                    size_t comma = pt.find(',');
                    if (comma != std::string::npos) {
                        try {
                            x = std::stod(pt.substr(0, comma));
                            y = std::stod(pt.substr(comma + 1));
                        } catch (...) { continue; }
                    }
                    newIdx->insert(x, y, rid);
                }
            }
            idx = newIdx.get();
            spGiSTCache_[cacheKey] = std::move(newIdx);
        }
    }

    if (!idx) return result;

    // Parse value as "x,y" for point, or "x" / "y" for directional queries
    if (op == "=") {
        double qx = 0, qy = 0;
        size_t comma = value.find(',');
        if (comma != std::string::npos) {
            try {
                qx = std::stod(value.substr(0, comma));
                qy = std::stod(value.substr(comma + 1));
            } catch (...) { return result; }
        }
        result = idx->searchEquals(qx, qy);
    } else if (op == "<<") {
        try { result = idx->searchLeftOf(std::stod(value)); } catch (...) {}
    } else if (op == ">>") {
        try { result = idx->searchRightOf(std::stod(value)); } catch (...) {}
    } else if (op == "<^") {
        try { result = idx->searchBelow(std::stod(value)); } catch (...) {}
    } else if (op == ">^") {
        try { result = idx->searchAbove(std::stod(value)); } catch (...) {}
    } else if (op == "<@") {
        // contained within circle: "cx,cy,radius"
        double cx = 0, cy = 0, r = 0;
        size_t c1 = value.find(',');
        size_t c2 = value.rfind(',');
        if (c1 != std::string::npos && c2 != std::string::npos && c2 > c1) {
            try {
                cx = std::stod(value.substr(0, c1));
                cy = std::stod(value.substr(c1 + 1, c2 - c1 - 1));
                r = std::stod(value.substr(c2 + 1));
            } catch (...) { return result; }
        }
        result = idx->searchWithin(cx, cy, r);
    }
    return result;
}

std::vector<std::string> StorageEngine::getSPGiSTIndexedColumns(const std::string& dbname,
                                                                 const std::string& tablename) const {
    std::vector<std::string> result;
    auto dir = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dir)) return result;
    std::string prefix = tablename + "_";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > 8 && name.substr(name.size() - 8) == ".spgist" &&
            name.substr(0, prefix.size()) == prefix) {
            std::string colname = name.substr(prefix.size(), name.size() - prefix.size() - 8);
            result.push_back(colname);
        }
    }
    return result;
}

// ========================================================================
// BRIN index (Block Range Index) - per-block min/max summary
// ========================================================================

static std::filesystem::path brinIndexPath(const std::string& dbname,
                                            const std::string& tablename,
                                            const std::string& colname) {
    return std::filesystem::path(dbname) / (tablename + "_" + colname + ".brin");
}

OpResult StorageEngine::createBrinIndex(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& colname,
                                         size_t pagesPerRange) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    if (pagesPerRange == 0) pagesPerRange = 64;
    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colname) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) return OpResult::InvalidValue;

    auto path = brinIndexPath(dbname, tablename, colname);
    std::ofstream out(path);
    if (!out) return OpResult::InvalidValue;

    // If table is partitioned, scan partition files; otherwise scan main data file
    auto scanTable = [&](const std::string& actualTableName) {
        TableSchema t = getTableSchema(dbname, actualTableName);
        if (t.partitionType != TableSchema::PartitionType::None) {
            std::vector<std::string> partNames;
            if (t.partitionType == TableSchema::PartitionType::Range) {
                for (const auto& rp : t.rangePartitions) partNames.push_back(rp.first);
            } else if (t.partitionType == TableSchema::PartitionType::List) {
                for (const auto& lp : t.listPartitions) partNames.push_back(lp.first);
            } else if (t.partitionType == TableSchema::PartitionType::Hash) {
                for (size_t i = 0; i < t.hashPartitions; ++i) partNames.push_back("p" + std::to_string(i));
            }
            for (const auto& pname : partNames) {
                auto ppa = std::make_unique<PageAllocator>(
                    partitionDataPath(dbname, actualTableName, pname).string(), t.rowSize(), pageSizeForFormatVersion(t.formatVersion), t.formatVersion);
                if (!ppa->open()) continue;
                uint32_t np = ppa->numPages();
                for (uint32_t blockStart = 1; blockStart < np; blockStart += static_cast<uint32_t>(pagesPerRange)) {
                    uint32_t blockEnd = std::min(blockStart + static_cast<uint32_t>(pagesPerRange) - 1, np - 1);
                    bool hasValue = false;
                    std::string rangeMin, rangeMax;
                    for (uint32_t pid = blockStart; pid <= blockEnd; ++pid) {
                        char* buf = ppa->fetchPage(pid);
                        PageWrapper page(buf, ppa->pageSize(), t.formatVersion);
                        page.forEachLive([&](uint16_t, const char* data, size_t len) {
                            if (len <= MVCC_HEADER_SIZE) return;
                            std::string row(data, len);
                            std::string val = extractColumnValue(row, t, colIdx);
                            if (!hasValue) { rangeMin = rangeMax = val; hasValue = true; }
                            else { if (val < rangeMin) rangeMin = val; if (val > rangeMax) rangeMax = val; }
                        });
                    }
                    if (hasValue) out << blockStart << ' ' << blockEnd << ' ' << rangeMin << ' ' << rangeMax << '\n';
                }
            }
        } else {
            auto pa = std::make_unique<PageAllocator>(dataPath(dbname, actualTableName).string(), t.rowSize(), pageSizeForFormatVersion(t.formatVersion), t.formatVersion);
            if (!pa->open()) return;
            uint32_t np = pa->numPages();
            for (uint32_t blockStart = 1; blockStart < np; blockStart += static_cast<uint32_t>(pagesPerRange)) {
                uint32_t blockEnd = std::min(blockStart + static_cast<uint32_t>(pagesPerRange) - 1, np - 1);
                bool hasValue = false;
                std::string rangeMin, rangeMax;
                for (uint32_t pid = blockStart; pid <= blockEnd; ++pid) {
                    char* buf = pa->fetchPage(pid);
                    PageWrapper page(buf, pa->pageSize(), t.formatVersion);
                    page.forEachLive([&](uint16_t, const char* data, size_t len) {
                        if (len <= MVCC_HEADER_SIZE) return;
                        std::string row(data, len);
                        std::string val = extractColumnValue(row, t, colIdx);
                        if (!hasValue) { rangeMin = rangeMax = val; hasValue = true; }
                        else { if (val < rangeMin) rangeMin = val; if (val > rangeMax) rangeMax = val; }
                    });
                }
                if (hasValue) out << blockStart << ' ' << blockEnd << ' ' << rangeMin << ' ' << rangeMax << '\n';
            }
        }
    };

    scanTable(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::dropBrinIndex(const std::string& dbname,
                                       const std::string& tablename,
                                       const std::string& colname) {
    auto path = brinIndexPath(dbname, tablename, colname);
    if (std::filesystem::exists(path)) std::filesystem::remove(path);
    return OpResult::Success;
}

bool StorageEngine::hasBrinIndex(const std::string& dbname,
                                  const std::string& tablename,
                                  const std::string& colname) const {
    return std::filesystem::exists(brinIndexPath(dbname, tablename, colname));
}

std::vector<std::pair<uint32_t, uint32_t>> StorageEngine::brinSearchRange(
    const std::string& dbname, const std::string& tablename, const std::string& colname,
    const std::string& op, const std::string& value) const {
    std::vector<std::pair<uint32_t, uint32_t>> result;
    auto path = brinIndexPath(dbname, tablename, colname);
    std::ifstream in(path);
    if (!in) return result;
    std::string line;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        uint32_t pstart, pend;
        std::string rangeMin, rangeMax;
        if (!(ss >> pstart >> pend >> rangeMin >> rangeMax)) continue;
        bool mayMatch = false;
        if (op == "=") {
            mayMatch = (rangeMin <= value && value <= rangeMax);
        } else if (op == "<") {
            mayMatch = (rangeMin < value);
        } else if (op == "<=") {
            mayMatch = (rangeMin <= value);
        } else if (op == ">") {
            mayMatch = (rangeMax > value);
        } else if (op == ">=") {
            mayMatch = (rangeMax >= value);
        }
        if (mayMatch) result.push_back({pstart, pend});
    }
    return result;
}

std::vector<std::string> StorageEngine::getBrinIndexedColumns(const std::string& dbname,
                                                               const std::string& tablename) const {
    std::vector<std::string> result;
    auto dir = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dir)) return result;
    std::string prefix = tablename + "_";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() > 6 && name.substr(name.size() - 6) == ".brin" &&
            name.substr(0, prefix.size()) == prefix) {
            std::string colname = name.substr(prefix.size(), name.size() - prefix.size() - 6);
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

constexpr int32_t SCHEMA_FORMAT_VERSION = 0x44420004;  // "DB" + version 4 (added formatVersion)

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
        // Write enum values
        uint16_t enumCount = static_cast<uint16_t>(tbl.cols[i].enumValues.size());
        out.write(reinterpret_cast<const char*>(&enumCount), 2);
        for (const auto& ev : tbl.cols[i].enumValues) {
            writeFixedString(out, ev, MAX_COL_NAME_LEN);
        }
        // Write array flag (new in version 2)
        uint8_t arrFlag = tbl.cols[i].isArray ? 1 : 0;
        out.write(reinterpret_cast<const char*>(&arrFlag), 1);
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
        writeFixedString(out, tbl.fks[i].onUpdate, 10);
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

    // Subpartitioning info (new, backward-compatible)
    int32_t sptype = static_cast<int32_t>(tbl.subPartitionType);
    out.write(reinterpret_cast<const char*>(&sptype), 4);
    if (tbl.subPartitionType != TableSchema::PartitionType::None) {
        writeFixedString(out, tbl.subPartitionKey, MAX_COL_NAME_LEN);
        if (tbl.subPartitionType == TableSchema::PartitionType::Hash) {
            int32_t shcount = static_cast<int32_t>(tbl.subHashPartitions);
            out.write(reinterpret_cast<const char*>(&shcount), 4);
        }
    }

    // Named constraint names (appended for backward compatibility)
    int32_t namedCheckCount = 0;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (!tbl.cols[i].checkConstraintName.empty()) namedCheckCount++;
    }
    out.write(reinterpret_cast<const char*>(&namedCheckCount), 4);
    for (size_t i = 0; i < tbl.len; ++i) {
        if (!tbl.cols[i].checkConstraintName.empty()) {
            int32_t cidx = static_cast<int32_t>(i);
            out.write(reinterpret_cast<const char*>(&cidx), 4);
            writeFixedString(out, tbl.cols[i].checkConstraintName, MAX_COL_NAME_LEN);
        }
    }

    int32_t namedUniqueCount = static_cast<int32_t>(tbl.uniqueConstraintNames.size());
    out.write(reinterpret_cast<const char*>(&namedUniqueCount), 4);
    for (const auto& name : tbl.uniqueConstraintNames) {
        writeFixedString(out, name, MAX_TABLE_NAME_LEN);
    }

    int32_t namedFkCount = 0;
    for (size_t i = 0; i < tbl.fkLen; ++i) {
        if (!tbl.fks[i].name.empty()) namedFkCount++;
    }
    out.write(reinterpret_cast<const char*>(&namedFkCount), 4);
    for (size_t i = 0; i < tbl.fkLen; ++i) {
        if (!tbl.fks[i].name.empty()) {
            int32_t fidx = static_cast<int32_t>(i);
            out.write(reinterpret_cast<const char*>(&fidx), 4);
            writeFixedString(out, tbl.fks[i].name, MAX_TABLE_NAME_LEN);
        }
    }
    // UNLOGGED flag (backward-compatible: new field at tail)
    uint8_t unloggedFlag = tbl.isUnlogged ? 1 : 0;
    out.write(reinterpret_cast<const char*>(&unloggedFlag), 1);
    // Row-Level Security flags (backward-compatible)
    uint8_t rlsFlags = (tbl.rowLevelSecurity ? 1 : 0) | (tbl.forceRowLevelSecurity ? 2 : 0);
    out.write(reinterpret_cast<const char*>(&rlsFlags), 1);
    // Default partition name (backward-compatible)
    writeFixedString(out, tbl.defaultPartitionName, MAX_TABLE_NAME_LEN);
    // Storage format version (new in version 4)
    out.write(reinterpret_cast<const char*>(&tbl.formatVersion), 4);
    // Tablespace (backward-compatible tail append)
    uint16_t tsLen = static_cast<uint16_t>(tbl.tablespace.size());
    out.write(reinterpret_cast<const char*>(&tsLen), 2);
    if (tsLen > 0) out.write(tbl.tablespace.data(), tsLen);
}

TableSchema StorageEngine::readSchema(std::istream& in, const std::string& tablename) const {
    TableSchema tbl;
    tbl.tablename = tablename;
    int32_t firstInt = 0;
    in.read(reinterpret_cast<char*>(&firstInt), 4);
    if (!in) return tbl;

    bool hasVersionHeader = (firstInt >= 0x44420001 && firstInt <= 0x44420004);
    bool hasArray = (firstInt >= 0x44420002);
    bool hasOnUpdate = (firstInt >= 0x44420003);
    bool hasFormatVersion = (firstInt >= 0x44420004);
    int32_t len = 0;
    if (hasVersionHeader) {
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
        // Read enum values (backward-compatible: may not exist in old files)
        uint16_t enumCount = 0;
        in.read(reinterpret_cast<char*>(&enumCount), 2);
        if (in && enumCount > 0 && enumCount <= 1000) {
            for (uint16_t ei = 0; ei < enumCount; ++ei) {
                std::string ev = readFixedString(in, MAX_COL_NAME_LEN);
                if (!ev.empty()) tbl.cols[i].enumValues.push_back(ev);
            }
        }
        // Read array flag (new in version 2)
        if (hasArray) {
            uint8_t arrFlag = 0;
            in.read(reinterpret_cast<char*>(&arrFlag), 1);
            if (in) tbl.cols[i].isArray = (arrFlag != 0);
        }
    }
    // Read foreign keys
    int32_t fkLen = 0;
    in.read(reinterpret_cast<char*>(&fkLen), 4);
    if (in && fkLen > 0 && fkLen <= static_cast<int32_t>(MAX_COLUMNS)) {
        tbl.fkLen = static_cast<size_t>(fkLen);
        for (size_t i = 0; i < tbl.fkLen; ++i) {
            if (hasVersionHeader) {
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
                if (hasOnUpdate) {
                    tbl.fks[i].onUpdate = readFixedString(in, 10);
                } else {
                    tbl.fks[i].onUpdate = "restrict";
                }
            } else {
                // Legacy single-column format
                std::string colName = readFixedString(in, MAX_COL_NAME_LEN);
                tbl.fks[i].refTable = readFixedString(in, MAX_TABLE_NAME_LEN);
                std::string refCol = readFixedString(in, MAX_COL_NAME_LEN);
                tbl.fks[i].onDelete = readFixedString(in, 10);
                tbl.fks[i].onUpdate = "restrict";
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

    // Read subpartitioning info (new, ignore if EOF for backward compatibility)
    int32_t sptype = 0;
    in.read(reinterpret_cast<char*>(&sptype), 4);
    if (in) {
        tbl.subPartitionType = static_cast<TableSchema::PartitionType>(sptype);
        if (tbl.subPartitionType != TableSchema::PartitionType::None) {
            tbl.subPartitionKey = readFixedString(in, MAX_COL_NAME_LEN);
            if (tbl.subPartitionType == TableSchema::PartitionType::Hash) {
                int32_t shcount = 0;
                in.read(reinterpret_cast<char*>(&shcount), 4);
                if (in && shcount > 0) tbl.subHashPartitions = static_cast<size_t>(shcount);
            }
        }
    }

    // Read named CHECK constraints (new, ignore if EOF for backward compatibility)
    int32_t namedCheckCount = 0;
    in.read(reinterpret_cast<char*>(&namedCheckCount), 4);
    if (in && namedCheckCount > 0 && namedCheckCount <= static_cast<int32_t>(MAX_COLUMNS)) {
        for (int32_t i = 0; i < namedCheckCount; ++i) {
            int32_t cidx = 0;
            in.read(reinterpret_cast<char*>(&cidx), 4);
            if (in && cidx >= 0 && cidx < static_cast<int32_t>(tbl.len)) {
                tbl.cols[cidx].checkConstraintName = readFixedString(in, MAX_COL_NAME_LEN);
            } else if (in) {
                readFixedString(in, MAX_COL_NAME_LEN); // skip
            }
        }
    }

    // Read named UNIQUE constraints (new, ignore if EOF)
    int32_t namedUniqueCount = 0;
    in.read(reinterpret_cast<char*>(&namedUniqueCount), 4);
    if (in && namedUniqueCount >= 0 && namedUniqueCount <= static_cast<int32_t>(MAX_COLUMNS)) {
        tbl.uniqueConstraintNames.reserve(namedUniqueCount);
        for (int32_t i = 0; i < namedUniqueCount; ++i) {
            tbl.uniqueConstraintNames.push_back(readFixedString(in, MAX_TABLE_NAME_LEN));
        }
    }

    // Read named FK constraints (new, ignore if EOF)
    int32_t namedFkCount = 0;
    in.read(reinterpret_cast<char*>(&namedFkCount), 4);
    if (in && namedFkCount > 0 && namedFkCount <= static_cast<int32_t>(MAX_COLUMNS)) {
        for (int32_t i = 0; i < namedFkCount; ++i) {
            int32_t fidx = 0;
            in.read(reinterpret_cast<char*>(&fidx), 4);
            if (in && fidx >= 0 && fidx < static_cast<int32_t>(tbl.fkLen)) {
                tbl.fks[fidx].name = readFixedString(in, MAX_TABLE_NAME_LEN);
            } else if (in) {
                readFixedString(in, MAX_TABLE_NAME_LEN); // skip
            }
        }
    }
    // UNLOGGED flag (backward-compatible: may not exist in old files)
    uint8_t unloggedFlag = 0;
    in.read(reinterpret_cast<char*>(&unloggedFlag), 1);
    if (in) tbl.isUnlogged = (unloggedFlag != 0);
    // Row-Level Security flags (backward-compatible)
    uint8_t rlsFlags = 0;
    in.read(reinterpret_cast<char*>(&rlsFlags), 1);
    if (in) {
        tbl.rowLevelSecurity = (rlsFlags & 1) != 0;
        tbl.forceRowLevelSecurity = (rlsFlags & 2) != 0;
    }
    // Default partition name (backward-compatible)
    std::string dpName = readFixedString(in, MAX_TABLE_NAME_LEN);
    if (in) tbl.defaultPartitionName = dpName;
    // Storage format version (new in version 4)
    if (hasFormatVersion) {
        uint32_t fv = 0;
        in.read(reinterpret_cast<char*>(&fv), 4);
        if (in) tbl.formatVersion = fv;
        // Tablespace (backward-compatible tail append)
        uint16_t tsLen = 0;
        in.read(reinterpret_cast<char*>(&tsLen), 2);
        if (in && tsLen > 0 && tsLen <= MAX_TABLE_NAME_LEN) {
            std::string ts(tsLen, '\0');
            in.read(ts.data(), tsLen);
            if (in) tbl.tablespace = std::move(ts);
        }
    }

    return tbl;
}

// ========================================================================
// Storage parameters (WITH clause)
// ========================================================================
std::map<std::string, std::string> StorageEngine::getStorageParams(
    const std::string& dbname, const std::string& tablename) const {
    std::map<std::string, std::string> params;
    auto pp = paramsPath(dbname, tablename);
    std::ifstream ifs(pp);
    if (!ifs) return params;
    std::string line;
    while (std::getline(ifs, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            params[trim(line.substr(0, eq))] = trim(line.substr(eq + 1));
        }
    }
    return params;
}

OpResult StorageEngine::setStorageParams(
    const std::string& dbname, const std::string& tablename,
    const std::map<std::string, std::string>& params) {
    auto pp = paramsPath(dbname, tablename);
    std::ofstream ofs(pp);
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& kv : params) {
        ofs << kv.first << "=" << kv.second << "\n";
    }
    return OpResult::Success;
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
    size_t threshold = toastThreshold(tbl.formatVersion);
    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        if (!col.isVariableLength) continue;
        auto it = values.find(col.dataName);
        if (it == values.end()) continue;
        if (it->second.size() > threshold) {
            uint64_t toastId = allocToastId(dbname, tablename);
            writeToast(dbname, tablename, toastId, it->second);
            it->second = std::string(TOAST_PREFIX) + std::to_string(toastId);
        }
    }
}

OpResult StorageEngine::createTable(const std::string& dbname, const TableSchema& tbl) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    if (tableExists(dbname, tbl.tablename)) return OpResult::TableAlreadyExist;

    TableSchema tblWithVersion = tbl;
    if (tblWithVersion.formatVersion == 0) {
        tblWithVersion.formatVersion = 2;  // Default to PostgreSQL 8KB format
    }
    size_t pageSize = pageSizeForFormatVersion(tblWithVersion.formatVersion);

    {
        std::ofstream out(schemaPath(dbname, tblWithVersion.tablename), std::ios::binary);
        writeSchema(out, tblWithVersion);
    }
    // Initialize page-based data file(s) via PageAllocator
    if (tblWithVersion.partitionType != TableSchema::PartitionType::None) {
        if (tblWithVersion.partitionType == TableSchema::PartitionType::Range) {
            for (const auto& rp : tblWithVersion.rangePartitions) {
                auto pa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tblWithVersion.tablename, rp.first).string(), tblWithVersion.rowSize(), pageSize, tblWithVersion.formatVersion);
                pa->open(); pa->close();
            }
        } else if (tblWithVersion.partitionType == TableSchema::PartitionType::List) {
            for (const auto& lp : tblWithVersion.listPartitions) {
                auto pa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tblWithVersion.tablename, lp.first).string(), tblWithVersion.rowSize(), pageSize, tblWithVersion.formatVersion);
                pa->open(); pa->close();
            }
        } else if (tblWithVersion.partitionType == TableSchema::PartitionType::Hash) {
            for (size_t i = 0; i < tblWithVersion.hashPartitions; ++i) {
                auto pa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tblWithVersion.tablename, "p" + std::to_string(i)).string(), tblWithVersion.rowSize(), pageSize, tblWithVersion.formatVersion);
                pa->open(); pa->close();
            }
        }
    } else {
        auto pa = std::make_unique<PageAllocator>(dataPath(dbname, tblWithVersion.tablename).string(), tblWithVersion.rowSize(), pageSize, tblWithVersion.formatVersion);
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
    std::filesystem::remove(fsmPath(dbname, tablename));
    std::filesystem::remove(vmPath(dbname, tablename));
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
    fsmCache_.erase(key);
    vmCache_.erase(key);
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
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::truncateTable(const std::string& dbname,
                                       const std::string& tablename) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);

    // Remove and recreate data file + forks
    std::filesystem::remove(dataPath(dbname, tablename));
    std::filesystem::remove(fsmPath(dbname, tablename));
    std::filesystem::remove(vmPath(dbname, tablename));
    {
        auto pa = std::make_unique<PageAllocator>(dataPath(dbname, tablename).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
        pa->open();
        pa->close();
    }

    // Remove and recreate primary key index
    if (tbl.hasPrimaryKey()) {
        std::filesystem::remove(indexPath(dbname, tablename));
        BPTree idx(indexPath(dbname, tablename));
        idx.open();
        idx.close();
    }

    // Remove and recreate secondary indexes
    auto idxMeta = getIndexMetadata(dbname, tablename);
    for (const auto& meta : idxMeta) {
        std::filesystem::remove(dbPath(dbname) / (tablename + ".idx_" + meta.name));
    }
    auto hashIdx = getHashIndexedColumns(dbname, tablename);
    for (const auto& col : hashIdx) {
        std::filesystem::remove(hashIndexPath(dbname, tablename, col));
    }
    auto compIdx = getCompositeIndexes(dbname, tablename);
    for (const auto& ci : compIdx) {
        std::filesystem::remove(dbPath(dbname) / (tablename + ".idx_" + ci.name));
    }
    auto ftCols = getFullTextIndexedColumns(dbname, tablename);
    for (const auto& col : ftCols) {
        std::filesystem::remove(fullTextIndexPath(dbname, tablename, col));
    }
    auto ginCols = getGinIndexedColumns(dbname, tablename);
    for (const auto& col : ginCols) {
        std::filesystem::remove(ginIndexPath(dbname, tablename, col));
    }
    auto gistCols = getGiSTIndexedColumns(dbname, tablename);
    for (const auto& col : gistCols) {
        std::filesystem::remove(giSTIndexPath(dbname, tablename, col));
    }
    auto brinCols = getBrinIndexedColumns(dbname, tablename);
    for (const auto& col : brinCols) {
        std::filesystem::remove(brinIndexPath(dbname, tablename, col));
    }

    // Clear caches
    std::string key = dbname + "/" + tablename;
    pkIndexCache_.erase(key);
    pageAllocators_.erase(key);
    fsmCache_.erase(key);
    vmCache_.erase(key);
    secondaryIndexCache_.erase(key);
    hashIndexCache_.erase(key);

    // Reset auto-increment sequences
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].isAutoIncrement) {
            writeNextSeq(dbname, tablename, tbl.cols[i].dataName, 1);
        }
    }

    // Clear statistics
    auto spath = statsPath(dbname);
    if (std::filesystem::exists(spath)) {
        std::ifstream ifs(spath);
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(ifs, line)) {
            if (!line.empty()) {
                std::stringstream ss(line);
                std::string t;
                ss >> t;
                if (t != tablename) lines.push_back(line);
            }
        }
        std::ofstream ofs(spath);
        for (size_t i = 0; i < lines.size(); ++i) {
            if (i > 0) ofs << '\n';
            ofs << lines[i];
        }
        if (!lines.empty()) ofs << '\n';
    }

    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableAddColumn(const std::string& dbname,
                                             const std::string& tablename,
                                             const Column& col) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == col.dataName) {
            lockManager_.unlock(tablename);
            return OpResult::TableAlreadyExist;
        }
    }
    if (tbl.len >= MAX_COLUMNS) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

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
    lockManager_.unlock(tablename);
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
    if (dropIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

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
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableRenameColumn(const std::string& dbname,
                                                const std::string& tablename,
                                                const std::string& oldName,
                                                const std::string& newName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    if (oldName == newName) return OpResult::Success;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == oldName) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == newName) {
            lockManager_.unlock(tablename);
            return OpResult::TableAlreadyExist;
        }
    }

    // Update schema
    tbl.cols[colIdx].dataName = newName;
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }

    auto dbDir = dbPath(dbname);

    // Update secondary index meta (.secidx)
    {
        std::filesystem::path meta = secondaryIndexMetaPath(dbname, tablename);
        if (std::filesystem::exists(meta)) {
            std::ifstream in(meta);
            std::vector<std::string> lines;
            std::string line;
            while (std::getline(in, line)) {
                if (!line.empty()) {
                    // Replace old column name with new in each line
                    // Simple token replacement by splitting on ':'
                    std::string updated;
                    size_t pos = 0;
                    while (pos < line.size()) {
                        size_t next = line.find(':', pos);
                        std::string token = (next == std::string::npos) ? line.substr(pos) : line.substr(pos, next - pos);
                        if (token == oldName) token = newName;
                        updated += token;
                        if (next == std::string::npos) break;
                        updated += ':';
                        pos = next + 1;
                    }
                    lines.push_back(updated);
                }
            }
            std::ofstream out(meta, std::ios::trunc);
            for (const auto& l : lines) out << l << '\n';
        }
    }

    // Update hash index meta (.hashidx)
    {
        std::filesystem::path meta = hashIndexMetaPath(dbname, tablename);
        if (std::filesystem::exists(meta)) {
            std::ifstream in(meta);
            std::vector<std::string> lines;
            std::string line;
            while (std::getline(in, line)) {
                if (!line.empty()) {
                    if (line == oldName) line = newName;
                    lines.push_back(line);
                }
            }
            std::ofstream out(meta, std::ios::trunc);
            for (const auto& l : lines) out << l << '\n';
        }
    }

    // Rename secondary index file if exists
    {
        std::filesystem::path oldIdx = secondaryIndexPath(dbname, tablename, oldName);
        std::filesystem::path newIdx = secondaryIndexPath(dbname, tablename, newName);
        if (std::filesystem::exists(oldIdx)) {
            std::filesystem::rename(oldIdx, newIdx);
            std::string oldKey = dbname + "/" + tablename + "/" + oldName;
            std::string newKey = dbname + "/" + tablename + "/" + newName;
            auto it = secondaryIndexCache_.find(oldKey);
            if (it != secondaryIndexCache_.end()) {
                secondaryIndexCache_[newKey] = std::move(it->second);
                secondaryIndexCache_.erase(it);
            }
        }
    }

    // Rename hash index file if exists
    {
        std::filesystem::path oldIdx = hashIndexPath(dbname, tablename, oldName);
        std::filesystem::path newIdx = hashIndexPath(dbname, tablename, newName);
        if (std::filesystem::exists(oldIdx)) {
            std::filesystem::rename(oldIdx, newIdx);
            std::string oldKey = dbname + "." + tablename + "." + oldName;
            auto it = hashIndexCache_.find(oldKey);
            if (it != hashIndexCache_.end()) {
                hashIndexCache_.erase(it);
            }
        }
    }

    // Rename fulltext index file if exists
    {
        std::filesystem::path oldIdx = fullTextIndexPath(dbname, tablename, oldName);
        std::filesystem::path newIdx = fullTextIndexPath(dbname, tablename, newName);
        if (std::filesystem::exists(oldIdx)) {
            std::filesystem::rename(oldIdx, newIdx);
        }
    }

    // Rename GIN/GiST/BRIN index files if exists
    {
        std::filesystem::path oldIdx = ginIndexPath(dbname, tablename, oldName);
        std::filesystem::path newIdx = ginIndexPath(dbname, tablename, newName);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }
    {
        std::filesystem::path oldIdx = giSTIndexPath(dbname, tablename, oldName);
        std::filesystem::path newIdx = giSTIndexPath(dbname, tablename, newName);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }
    {
        std::filesystem::path oldIdx = brinIndexPath(dbname, tablename, oldName);
        std::filesystem::path newIdx = brinIndexPath(dbname, tablename, newName);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }

    // Update sequence file (.seq)
    {
        std::filesystem::path sp = seqPath(dbname, tablename);
        if (std::filesystem::exists(sp)) {
            std::ifstream ifs(sp);
            std::map<std::string, int64_t> seqs;
            std::string line;
            while (std::getline(ifs, line)) {
                size_t p = line.find(' ');
                if (p == std::string::npos) continue;
                std::string cname = line.substr(0, p);
                try {
                    seqs[cname] = std::stoll(line.substr(p + 1));
                } catch (...) {}
            }
            auto it = seqs.find(oldName);
            if (it != seqs.end()) {
                int64_t val = it->second;
                seqs.erase(it);
                seqs[newName] = val;
                std::ofstream ofs(sp);
                for (const auto& p : seqs) ofs << p.first << ' ' << p.second << '\n';
            }
        }
    }

    // Update stats file (.stats)
    {
        std::filesystem::path sp = statsPath(dbname);
        if (std::filesystem::exists(sp)) {
            std::ifstream ifs(sp);
            std::vector<std::string> lines;
            std::string line;
            while (std::getline(ifs, line)) {
                if (!line.empty()) {
                    std::stringstream ss(line);
                    std::string t, c;
                    ss >> t >> c;
                    if (t == tablename && c == oldName) {
                        // Replace column name in line
                        size_t pos = line.find(oldName);
                        if (pos != std::string::npos) line.replace(pos, oldName.size(), newName);
                    }
                    lines.push_back(line);
                }
            }
            std::ofstream ofs(sp);
            for (size_t i = 0; i < lines.size(); ++i) {
                if (i > 0) ofs << '\n';
                ofs << lines[i];
            }
            if (!lines.empty()) ofs << '\n';
        }
    }

    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableRenameTable(const std::string& dbname,
                                               const std::string& oldName,
                                               const std::string& newName) {
    if (!tableExists(dbname, oldName)) return OpResult::TableNotExist;
    if (tableExists(dbname, newName)) return OpResult::TableAlreadyExist;
    lockManager_.lockMetadata(oldName);
    lockManager_.lockMetadata(newName);

    auto dbDir = dbPath(dbname);

    // Rename schema, data, PK index, sequence files
    std::filesystem::rename(schemaPath(dbname, oldName), schemaPath(dbname, newName));
    std::filesystem::rename(dataPath(dbname, oldName), dataPath(dbname, newName));
    if (std::filesystem::exists(indexPath(dbname, oldName))) {
        std::filesystem::rename(indexPath(dbname, oldName), indexPath(dbname, newName));
    }
    if (std::filesystem::exists(seqPath(dbname, oldName))) {
        std::filesystem::rename(seqPath(dbname, oldName), seqPath(dbname, newName));
    }

    // Rename secondary indexes and meta
    if (std::filesystem::exists(secondaryIndexMetaPath(dbname, oldName))) {
        auto cols = getIndexedColumns(dbname, oldName);
        for (const auto& c : cols) {
            std::filesystem::path oldIdx = secondaryIndexPath(dbname, oldName, c);
            std::filesystem::path newIdx = secondaryIndexPath(dbname, newName, c);
            if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
        }
        std::filesystem::rename(secondaryIndexMetaPath(dbname, oldName), secondaryIndexMetaPath(dbname, newName));
    }

    // Rename hash indexes and meta
    if (std::filesystem::exists(hashIndexMetaPath(dbname, oldName))) {
        auto cols = getHashIndexedColumns(dbname, oldName);
        for (const auto& c : cols) {
            std::filesystem::path oldIdx = hashIndexPath(dbname, oldName, c);
            std::filesystem::path newIdx = hashIndexPath(dbname, newName, c);
            if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
        }
        std::filesystem::rename(hashIndexMetaPath(dbname, oldName), hashIndexMetaPath(dbname, newName));
    }

    // Rename composite indexes
    auto compIdxs = getCompositeIndexes(dbname, oldName);
    for (const auto& ci : compIdxs) {
        std::filesystem::path oldIdx = dbDir / (oldName + ".idx_" + ci.name);
        std::filesystem::path newIdx = dbDir / (newName + ".idx_" + ci.name);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }

    // Rename fulltext indexes
    auto ftCols = getFullTextIndexedColumns(dbname, oldName);
    for (const auto& c : ftCols) {
        std::filesystem::path oldIdx = fullTextIndexPath(dbname, oldName, c);
        std::filesystem::path newIdx = fullTextIndexPath(dbname, newName, c);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }

    // Rename GIN/GiST/BRIN indexes
    auto ginCols = getGinIndexedColumns(dbname, oldName);
    for (const auto& c : ginCols) {
        std::filesystem::path oldIdx = ginIndexPath(dbname, oldName, c);
        std::filesystem::path newIdx = ginIndexPath(dbname, newName, c);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }
    auto gistCols = getGiSTIndexedColumns(dbname, oldName);
    for (const auto& c : gistCols) {
        std::filesystem::path oldIdx = giSTIndexPath(dbname, oldName, c);
        std::filesystem::path newIdx = giSTIndexPath(dbname, newName, c);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }
    auto brinCols = getBrinIndexedColumns(dbname, oldName);
    for (const auto& c : brinCols) {
        std::filesystem::path oldIdx = brinIndexPath(dbname, oldName, c);
        std::filesystem::path newIdx = brinIndexPath(dbname, newName, c);
        if (std::filesystem::exists(oldIdx)) std::filesystem::rename(oldIdx, newIdx);
    }

    // Rename TOAST directory and meta
    {
        std::filesystem::path oldToast = toastDir(dbname, oldName);
        std::filesystem::path newToast = toastDir(dbname, newName);
        if (std::filesystem::exists(oldToast)) std::filesystem::rename(oldToast, newToast);
    }
    {
        std::filesystem::path oldMeta = toastMetaPath(dbname, oldName);
        std::filesystem::path newMeta = toastMetaPath(dbname, newName);
        if (std::filesystem::exists(oldMeta)) std::filesystem::rename(oldMeta, newMeta);
    }

    // Update table list
    {
        auto names = getTableNames(dbname);
        std::ofstream out(tableListPath(dbname), std::ios::binary);
        for (const auto& name : names) {
            std::string writeName = (name == oldName) ? newName : name;
            writeFixedString(out, writeName, MAX_TABLE_NAME_LEN);
        }
    }

    // Update caches: remove old keys, keep new files on disk for lazy open
    std::string oldKey = dbname + "/" + oldName;
    pkIndexCache_.erase(oldKey);
    pageAllocators_.erase(oldKey);
    {
        std::vector<std::string> keysToRemove;
        for (const auto& p : secondaryIndexCache_) {
            if (p.first.rfind(oldKey + "/", 0) == 0) keysToRemove.push_back(p.first);
        }
        for (const auto& k : keysToRemove) secondaryIndexCache_.erase(k);
    }
    {
        std::vector<std::string> keysToRemove;
        for (const auto& p : hashIndexCache_) {
            if (p.first.rfind(dbname + "." + oldName + ".", 0) == 0) keysToRemove.push_back(p.first);
        }
        for (const auto& k : keysToRemove) hashIndexCache_.erase(k);
    }

    // Update permissions file
    {
        std::filesystem::path pp = permPath(dbname);
        if (std::filesystem::exists(pp)) {
            std::ifstream ifs(pp);
            std::vector<std::string> lines;
            std::string line;
            while (std::getline(ifs, line)) {
                if (!line.empty()) {
                    std::stringstream ss(line);
                    std::string user, t;
                    ss >> user >> t;
                    if (t == oldName) {
                        size_t pos = line.find(oldName);
                        if (pos != std::string::npos) line.replace(pos, oldName.size(), newName);
                    }
                    lines.push_back(line);
                }
            }
            std::ofstream ofs(pp);
            for (size_t i = 0; i < lines.size(); ++i) {
                if (i > 0) ofs << '\n';
                ofs << lines[i];
            }
            if (!lines.empty()) ofs << '\n';
        }
    }

    // Update stats file
    {
        std::filesystem::path sp = statsPath(dbname);
        if (std::filesystem::exists(sp)) {
            std::ifstream ifs(sp);
            std::vector<std::string> lines;
            std::string line;
            while (std::getline(ifs, line)) {
                if (!line.empty()) {
                    std::stringstream ss(line);
                    std::string t;
                    ss >> t;
                    if (t == oldName) {
                        size_t pos = line.find(oldName);
                        if (pos != std::string::npos) line.replace(pos, oldName.size(), newName);
                    }
                    lines.push_back(line);
                }
            }
            std::ofstream ofs(sp);
            for (size_t i = 0; i < lines.size(); ++i) {
                if (i > 0) ofs << '\n';
                ofs << lines[i];
            }
            if (!lines.empty()) ofs << '\n';
        }
    }

    // Update trigger file
    {
        std::filesystem::path tp = triggerPath(dbname);
        if (std::filesystem::exists(tp)) {
            auto triggers = getAllTriggers(dbname);
            bool changed = false;
            for (auto& t : triggers) {
                if (t.tableName == oldName) {
                    t.tableName = newName;
                    changed = true;
                }
            }
            if (changed) {
                std::ofstream out(tp, std::ios::binary | std::ios::trunc);
                size_t count = triggers.size();
                out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
                for (const auto& t : triggers) writeTrigger(out, t);
            }
        }
    }

    lockManager_.unlock(oldName);
    lockManager_.unlock(newName);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableSetDefault(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::string& colName,
                                              const std::string& defaultValue) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    tbl.cols[colIdx].defaultValue = defaultValue;
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableDropDefault(const std::string& dbname,
                                               const std::string& tablename,
                                               const std::string& colName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    tbl.cols[colIdx].defaultValue.clear();
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableSetNotNull(const std::string& dbname,
                                              const std::string& tablename,
                                              const std::string& colName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    tbl.cols[colIdx].isNull = false;
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableDropNotNull(const std::string& dbname,
                                               const std::string& tablename,
                                               const std::string& colName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    size_t colIdx = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) { colIdx = i; break; }
    }
    if (colIdx >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    tbl.cols[colIdx].isNull = true;
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableAddCheckConstraint(const std::string& dbname,
                                                        const std::string& tablename,
                                                        const std::string& name,
                                                        const std::string& expr) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    // Find the first column referenced in the expression
    size_t targetCol = tbl.len;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (expr.find(tbl.cols[i].dataName) != std::string::npos) {
            targetCol = i;
            break;
        }
    }
    if (targetCol >= tbl.len) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }
    // Ensure constraint name is unique
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].checkConstraintName == name) {
            lockManager_.unlock(tablename);
            return OpResult::TableAlreadyExist;
        }
    }

    tbl.cols[targetCol].checkExpr = expr;
    tbl.cols[targetCol].checkConstraintName = name;
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableAddUniqueConstraint(const std::string& dbname,
                                                         const std::string& tablename,
                                                         const std::string& name,
                                                         const std::vector<std::string>& colNames) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    std::vector<size_t> colIndices;
    for (const auto& cname : colNames) {
        bool found = false;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == cname) {
                colIndices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
    }
    // Check for duplicate constraint name
    for (const auto& n : tbl.uniqueConstraintNames) {
        if (n == name) {
            lockManager_.unlock(tablename);
            return OpResult::TableAlreadyExist;
        }
    }

    tbl.uniqueConstraints.push_back(colIndices);
    tbl.uniqueConstraintNames.push_back(name);
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableAddFKConstraint(const std::string& dbname,
                                                     const std::string& tablename,
                                                     const std::string& name,
                                                     const std::vector<std::string>& localCols,
                                                     const std::string& refTable,
                                                     const std::vector<std::string>& refCols,
                                                     const std::string& onDelete,
                                                     const std::string& onUpdate) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    if (!tableExists(dbname, refTable)) {
        // FK references a table in the same database
        if (!std::filesystem::exists(schemaPath(dbname, refTable))) {
            return OpResult::TableNotExist;
        }
    }
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    if (tbl.fkLen >= MAX_COLUMNS) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }
    // Verify local columns exist
    for (const auto& cname : localCols) {
        bool found = false;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == cname) { found = true; break; }
        }
        if (!found) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
    }
    // Check for duplicate constraint name
    for (size_t i = 0; i < tbl.fkLen; ++i) {
        if (tbl.fks[i].name == name) {
            lockManager_.unlock(tablename);
            return OpResult::TableAlreadyExist;
        }
    }

    ForeignKey fk;
    fk.name = name;
    fk.colNames = localCols;
    fk.refCols = refCols;
    fk.refTable = refTable;
    fk.onDelete = onDelete.empty() ? "restrict" : onDelete;
    fk.onUpdate = onUpdate.empty() ? "restrict" : onUpdate;
    tbl.appendFK(fk);
    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

OpResult StorageEngine::alterTableDropConstraint(const std::string& dbname,
                                                  const std::string& tablename,
                                                  const std::string& name) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    lockManager_.lockMetadata(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    bool found = false;

    // Search CHECK constraints
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].checkConstraintName == name) {
            tbl.cols[i].checkExpr.clear();
            tbl.cols[i].checkConstraintName.clear();
            found = true;
            break;
        }
    }

    // Search UNIQUE constraints
    if (!found) {
        for (size_t i = 0; i < tbl.uniqueConstraintNames.size(); ++i) {
            if (tbl.uniqueConstraintNames[i] == name) {
                tbl.uniqueConstraints.erase(tbl.uniqueConstraints.begin() + i);
                tbl.uniqueConstraintNames.erase(tbl.uniqueConstraintNames.begin() + i);
                found = true;
                break;
            }
        }
    }

    // Search FK constraints
    if (!found) {
        for (size_t i = 0; i < tbl.fkLen; ++i) {
            if (tbl.fks[i].name == name) {
                // Shift left
                for (size_t j = i; j + 1 < tbl.fkLen; ++j) {
                    tbl.fks[j] = tbl.fks[j + 1];
                }
                tbl.fkLen--;
                found = true;
                break;
            }
        }
    }

    if (!found) {
        lockManager_.unlock(tablename);
        return OpResult::InvalidValue;
    }

    {
        std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
        writeSchema(out, tbl);
    }
    lockManager_.unlock(tablename);
    return OpResult::Success;
}

static std::filesystem::path commentsPath(const std::string& dbname) {
    return std::filesystem::path(dbname) / ".comments";
}

OpResult StorageEngine::commentOnTable(const std::string& dbname,
                                        const std::string& tablename,
                                        const std::string& comment) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    auto path = commentsPath(dbname);
    std::vector<std::string> lines;
    {
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) lines.push_back(line);
        }
    }
    bool updated = false;
    std::string prefix = "T|" + tablename + "|";
    for (auto& line : lines) {
        if (line.substr(0, prefix.size()) == prefix) {
            if (comment.empty()) {
                line.clear();
            } else {
                line = prefix + comment;
            }
            updated = true;
            break;
        }
    }
    if (!updated && !comment.empty()) {
        lines.push_back(prefix + comment);
    }
    std::ofstream out(path, std::ios::trunc);
    for (const auto& line : lines) {
        if (!line.empty()) out << line << '\n';
    }
    return OpResult::Success;
}

OpResult StorageEngine::commentOnColumn(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& colname,
                                         const std::string& comment) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    auto path = commentsPath(dbname);
    std::vector<std::string> lines;
    {
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) lines.push_back(line);
        }
    }
    bool updated = false;
    std::string prefix = "C|" + tablename + "|" + colname + "|";
    for (auto& line : lines) {
        if (line.substr(0, prefix.size()) == prefix) {
            if (comment.empty()) {
                line.clear();
            } else {
                line = prefix + comment;
            }
            updated = true;
            break;
        }
    }
    if (!updated && !comment.empty()) {
        lines.push_back(prefix + comment);
    }
    std::ofstream out(path, std::ios::trunc);
    for (const auto& line : lines) {
        if (!line.empty()) out << line << '\n';
    }
    return OpResult::Success;
}

std::string StorageEngine::getTableComment(const std::string& dbname,
                                            const std::string& tablename) const {
    auto path = commentsPath(dbname);
    std::ifstream in(path);
    if (!in) return "";
    std::string prefix = "T|" + tablename + "|";
    std::string line;
    while (std::getline(in, line)) {
        if (line.substr(0, prefix.size()) == prefix) {
            return line.substr(prefix.size());
        }
    }
    return "";
}

std::string StorageEngine::getColumnComment(const std::string& dbname,
                                             const std::string& tablename,
                                             const std::string& colname) const {
    auto path = commentsPath(dbname);
    std::ifstream in(path);
    if (!in) return "";
    std::string prefix = "C|" + tablename + "|" + colname + "|";
    std::string line;
    while (std::getline(in, line)) {
        if (line.substr(0, prefix.size()) == prefix) {
            return line.substr(prefix.size());
        }
    }
    return "";
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

static std::filesystem::path sequencePath(const std::string& dbname, const std::string& seqname) {
    return std::filesystem::path(dbname) / (seqname + ".seq");
}

OpResult StorageEngine::createSequence(const std::string& dbname,
                                        const std::string& seqname,
                                        int64_t start, int64_t increment) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = sequencePath(dbname, seqname);
    if (std::filesystem::exists(path)) return OpResult::TableAlreadyExist;
    std::ofstream ofs(path);
    ofs << start << " " << increment << "\n";
    return OpResult::Success;
}

OpResult StorageEngine::alterSequence(const std::string& dbname,
                                       const std::string& seqname,
                                       bool hasRestart, int64_t restart,
                                       bool hasIncrement, int64_t increment) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = sequencePath(dbname, seqname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    int64_t current = 1;
    int64_t inc = 1;
    {
        std::ifstream ifs(path);
        if (ifs) ifs >> current >> inc;
    }
    if (hasRestart) current = restart;
    if (hasIncrement) inc = increment;
    std::ofstream ofs(path);
    ofs << current << " " << inc << "\n";
    return OpResult::Success;
}

OpResult StorageEngine::dropSequence(const std::string& dbname,
                                      const std::string& seqname) {
    auto path = sequencePath(dbname, seqname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::filesystem::remove(path);
    return OpResult::Success;
}

int64_t StorageEngine::nextval(const std::string& dbname,
                                const std::string& seqname) {
    auto path = sequencePath(dbname, seqname);
    int64_t val = 1, inc = 1;
    {
        std::ifstream ifs(path);
        if (ifs) ifs >> val >> inc;
    }
    int64_t result = val;
    val += inc;
    std::ofstream ofs(path);
    ofs << val << " " << inc << "\n";
    return result;
}

int64_t StorageEngine::currval(const std::string& dbname,
                                const std::string& seqname) {
    auto path = sequencePath(dbname, seqname);
    int64_t val = 1, inc = 1;
    {
        std::ifstream ifs(path);
        if (ifs) ifs >> val >> inc;
    }
    return val;
}

bool StorageEngine::sequenceExists(const std::string& dbname,
                                    const std::string& seqname) const {
    return std::filesystem::exists(sequencePath(dbname, seqname));
}

std::vector<std::string> StorageEngine::getSequenceNames(const std::string& dbname) const {
    std::vector<std::string> names;
    auto dir = dbPath(dbname);
    if (!std::filesystem::exists(dir)) return names;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            std::string fname = entry.path().filename().string();
            if (fname.size() > 4 && fname.substr(fname.size() - 4) == ".seq") {
                // Exclude table auto-increment sequences (tablename.seq)
                // Keep only sequences that don't match a table name
                std::string tname = fname.substr(0, fname.size() - 4);
                if (!std::filesystem::exists(schemaPath(dbname, tname))) {
                    names.push_back(tname);
                }
            }
        }
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

    // Handle OVERLAPS operator: overlaps:s1,e1,s2,e2
    if (cond.op == "overlaps") {
        std::string expr = cond.colName;
        if (!expr.empty() && expr.front() == ':') expr = expr.substr(1);
        std::vector<std::string> parts;
        std::stringstream ss(expr);
        std::string part;
        while (std::getline(ss, part, ',')) {
            // trim whitespace around each part
            size_t a = 0, b = part.size();
            while (a < b && std::isspace(static_cast<unsigned char>(part[a]))) ++a;
            while (b > a && std::isspace(static_cast<unsigned char>(part[b - 1]))) --b;
            if (a < b) parts.push_back(part.substr(a, b - a));
        }
        if (parts.size() == 4) {
            auto findCol = [&](const std::string& name) -> size_t {
                for (size_t i = 0; i < tbl.len; ++i)
                    if (tbl.cols[i].dataName == name) return i;
                return tbl.len;
            };
            size_t s1i = findCol(parts[0]);
            size_t e1i = findCol(parts[1]);
            size_t s2i = findCol(parts[2]);
            size_t e2i = findCol(parts[3]);
            if (s1i < tbl.len && e1i < tbl.len && s2i < tbl.len && e2i < tbl.len) {
                std::string s1 = extractColumnValue(rowBuffer, tbl, s1i);
                std::string e1 = extractColumnValue(rowBuffer, tbl, e1i);
                std::string s2 = extractColumnValue(rowBuffer, tbl, s2i);
                std::string e2 = extractColumnValue(rowBuffer, tbl, e2i);
                return (s1 < e2) && (s2 < e1);
            }
        }
        return false;
    }

    size_t ci = 0;
    for (; ci < tbl.len && tbl.cols[ci].dataName != cond.colName; ++ci) {}
    if (ci >= tbl.len) return false;

    std::string val = extractColumnValue(rowBuffer, tbl, ci);
    const Column& col = tbl.cols[ci];
    if (cond.op == "isnull") return val.empty();
    if (cond.op == "isnotnull") return !val.empty();

    // Three-valued logic: any comparison with NULL yields UNKNOWN (FALSE in WHERE)
    bool valIsNull = val.empty();
    bool condIsNull = cond.value.empty();
    if (valIsNull || condIsNull) return false;

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
        auto normalizeDateStr = [](const std::string& s) -> std::string {
            if (s.size() < 8) return s;
            // YYYY/MM/DD or YYYY.MM.DD -> YYYY-MM-DD
            std::string r = s;
            for (char& c : r) if (c == '/' || c == '.') c = '-';
            return r;
        };
        std::string nv = normalizeDateStr(val);
        std::string nc = normalizeDateStr(cond.value);
        Date d = (nv.empty() ? Date{} : Date(nv.c_str()));
        Date v(nc.c_str());
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
        float num = 0.0f, cmp = 0.0f;
        try {
            num = val.empty() ? 0.0f : std::stof(val);
            cmp = std::stof(cond.value);
        } catch (...) {
            return false;  // Invalid comparison value → UNKNOWN → FALSE in WHERE
        }
        if (cond.op == "<"  && !(num < cmp)) return false;
        if (cond.op == ">"  && !(num > cmp)) return false;
        if (cond.op == "="  && num != cmp)   return false;
        if (cond.op == "<=" && (num > cmp))  return false;
        if (cond.op == ">=" && (num < cmp))  return false;
        if (cond.op == "!=" && num == cmp)   return false;
    } else if (col.dataType == "double" || col.dataType == "decimal") {
        double num = 0.0, cmp = 0.0;
        try {
            num = val.empty() ? 0.0 : std::stod(val);
            cmp = std::stod(cond.value);
        } catch (...) {
            return false;  // Invalid comparison value → UNKNOWN → FALSE in WHERE
        }
        if (cond.op == "<"  && !(num < cmp)) return false;
        if (cond.op == ">"  && !(num > cmp)) return false;
        if (cond.op == "="  && num != cmp)   return false;
        if (cond.op == "<=" && (num > cmp))  return false;
        if (cond.op == ">=" && (num < cmp))  return false;
        if (cond.op == "!=" && num == cmp)   return false;
    } else if (col.dataType == "boolean") {
        auto normalizeBool = [](const std::string& s) -> std::string {
            if (s == "1" || s == "true" || s == "yes" || s == "on") return "true";
            if (s == "0" || s == "false" || s == "no" || s == "off") return "false";
            return s;
        };
        std::string nv = normalizeBool(val);
        std::string nc = normalizeBool(cond.value);
        if (cond.op == "="  && nv != nc) return false;
        if (cond.op == "!=" && nv == nc) return false;
    } else if (col.dataType == "point") {
        // Parse POINT value format: "x,y"
        auto parsePoint = [](const std::string& s) -> std::pair<double, double> {
            double x = 0.0, y = 0.0;
            if (!s.empty()) {
                size_t comma = s.find(',');
                if (comma != std::string::npos) {
                    try { x = std::stod(s.substr(0, comma)); } catch (...) {}
                    try { y = std::stod(s.substr(comma + 1)); } catch (...) {}
                }
            }
            return {x, y};
        };
        auto [px, py] = parsePoint(val);
        auto [cx, cy] = parsePoint(cond.value);
        if (cond.op == "=") {
            if (px != cx || py != cy) return false;
        } else if (cond.op == "!=") {
            if (px == cx && py == cy) return false;
        } else if (cond.op == "<<") {
            if (!(px < cx)) return false;
        } else if (cond.op == ">>") {
            if (!(px > cx)) return false;
        } else if (cond.op == "<^") {
            if (!(py < cy)) return false;
        } else if (cond.op == ">^") {
            if (!(py > cy)) return false;
        } else if (cond.op == "<@") {
            // contained within circle: "cx,cy,radius"
            double cx2 = 0, cy2 = 0, radius = 0;
            std::string v = cond.value;
            if (v.size() >= 2 && v.front() == '\'' && v.back() == '\'')
                v = v.substr(1, v.size() - 2);
            size_t c1 = v.find(',');
            size_t c2 = v.rfind(',');
            if (c1 != std::string::npos && c2 != std::string::npos && c2 > c1) {
                try {
                    cx2 = std::stod(v.substr(0, c1));
                    cy2 = std::stod(v.substr(c1 + 1, c2 - c1 - 1));
                    radius = std::stod(v.substr(c2 + 1));
                } catch (...) {}
            }
            double dx = px - cx2, dy = py - cy2;
            double dist = std::sqrt(dx * dx + dy * dy);
            if (!(dist <= radius)) return false;
        } else {
            // Fallback to string comparison for other operators
            if (cond.op == "<"  && !(val <  cond.value)) return false;
            if (cond.op == ">"  && !(val >  cond.value)) return false;
        }
    } else if (col.dataType == "inet" || col.dataType == "cidr") {
        // Parse INET from string representation: "192.168.1.1" or "192.168.1.0/24"
        auto parseINet = [](const std::string& s) -> std::tuple<uint8_t, uint8_t, uint32_t> {
            uint8_t family = 0, prefix = 32;
            uint32_t addr = 0;
            std::string clean = s;
            if (clean.size() >= 2 && clean.front() == '\'' && clean.back() == '\'')
                clean = clean.substr(1, clean.size() - 2);
            if (clean.empty()) return {family, prefix, addr};
            size_t slash = clean.find('/');
            std::string addrPart = clean;
            if (slash != std::string::npos) {
                try { prefix = static_cast<uint8_t>(std::stoi(clean.substr(slash + 1))); }
                catch (...) { prefix = 32; }
                addrPart = clean.substr(0, slash);
            }
            int a = 0, b = 0, c = 0, d = 0;
            if (sscanf(addrPart.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d) == 4) {
                family = 2;
                addr = (static_cast<uint32_t>(a) << 24) |
                       (static_cast<uint32_t>(b) << 16) |
                       (static_cast<uint32_t>(c) << 8) |
                        static_cast<uint32_t>(d);
            }
            return {family, prefix, addr};
        };
        auto [vfam, vpref, vaddr] = parseINet(val);
        auto [cfam, cpref, caddr] = parseINet(cond.value);
        if (vfam == 0 || cfam == 0) return false;
        if (cond.op == "=") {
            if (vaddr != caddr || vpref != cpref) return false;
        } else if (cond.op == "!=") {
            if (vaddr == caddr && vpref == cpref) return false;
        } else if (cond.op == "<<") {
            // subnet of: this network is contained within the given network
            // Given network must have smaller or equal prefix (be a supernet)
            if (cpref > vpref) return false;
            uint32_t mask = (cpref == 0) ? 0 : (0xFFFFFFFF << (32 - cpref));
            if ((vaddr & mask) != (caddr & mask)) return false;
        } else if (cond.op == ">>") {
            // contains: this network contains the given network
            if (vpref > cpref) return false;
            uint32_t mask = (vpref == 0) ? 0 : (0xFFFFFFFF << (32 - vpref));
            if ((vaddr & mask) != (caddr & mask)) return false;
        } else if (cond.op == "&&") {
            // overlap: two networks share any addresses
            uint32_t vmask = (vpref == 0) ? 0 : (0xFFFFFFFF << (32 - vpref));
            uint32_t cmask = (cpref == 0) ? 0 : (0xFFFFFFFF << (32 - cpref));
            uint32_t vnet = vaddr & vmask;
            uint32_t cnet = caddr & cmask;
            uint32_t vbroad = vaddr | ~vmask;
            uint32_t cbroad = caddr | ~cmask;
            if (vnet > cbroad || cnet > vbroad) return false;
        } else {
            return false;
        }
    } else {
        int64_t num = val.empty() ? INF : parseInt(val);
        int64_t cmp = StorageEngine::parseInt(cond.value);
        if (cmp == INF) return false;  // Invalid comparison value → UNKNOWN → FALSE in WHERE
        if (cond.op == "<"  && !(num < cmp)) return false;
        if (cond.op == ">"  && !(num > cmp)) return false;
        if (cond.op == "="  && num != cmp)   return false;
        if (cond.op == "<=" && (num > cmp))  return false;
        if (cond.op == ">=" && (num < cmp))  return false;
        if (cond.op == "!=" && num == cmp)   return false;
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
                c.colName = trim(condStr.substr(0, opStart));
                size_t opEnd = opStart;
                while (opEnd < condStr.size() && strchr("<>=!", condStr[opEnd])) ++opEnd;
                c.op = trim(condStr.substr(opStart, opEnd - opStart));
                c.value = trim(condStr.substr(opEnd));
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
            } else if (col.dataType == "point") {
                double x = 0.0, y = 0.0;
                if (!val.empty()) {
                    std::string clean = val;
                    // Strip POINT( prefix and ) suffix
                    if (clean.size() > 6 && clean.substr(0, 6) == "point(") {
                        clean = clean.substr(6);
                        if (!clean.empty() && clean.back() == ')') clean.pop_back();
                    } else if (clean.front() == '(' && clean.back() == ')') {
                        clean = clean.substr(1, clean.size() - 2);
                    }
                    size_t comma = clean.find(',');
                    if (comma != std::string::npos) {
                        try { x = std::stod(trim(clean.substr(0, comma))); } catch (...) {}
                        try { y = std::stod(trim(clean.substr(comma + 1))); } catch (...) {}
                    }
                }
                std::memcpy(&rowBuffer[offset], &x, sizeof(double));
                std::memcpy(&rowBuffer[offset + sizeof(double)], &y, sizeof(double));
            } else if (col.dataType == "inet" || col.dataType == "cidr") {
                // Parse IPv4 address with optional prefix: "192.168.1.1" or "192.168.1.0/24"
                uint8_t family = 0, prefix = 32;
                uint8_t addr[16] = {0};
                if (!val.empty()) {
                    std::string addrPart = val;
                    // Extract prefix if present
                    size_t slash = addrPart.find('/');
                    if (slash != std::string::npos) {
                        try { prefix = static_cast<uint8_t>(std::stoi(addrPart.substr(slash + 1))); }
                        catch (...) { prefix = 32; }
                        addrPart = addrPart.substr(0, slash);
                    }
                    // Parse IPv4 dotted-decimal
                    int a = 0, b = 0, c = 0, d = 0;
                    if (sscanf(addrPart.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d) == 4) {
                        family = 2;  // IPv4
                        addr[0] = static_cast<uint8_t>(a);
                        addr[1] = static_cast<uint8_t>(b);
                        addr[2] = static_cast<uint8_t>(c);
                        addr[3] = static_cast<uint8_t>(d);
                    }
                }
                rowBuffer[offset] = static_cast<char>(family);
                rowBuffer[offset + 1] = static_cast<char>(prefix);
                rowBuffer[offset + 2] = static_cast<char>(col.dataType == "cidr" ? 1 : 0);
                rowBuffer[offset + 3] = 0;  // reserved
                std::memcpy(&rowBuffer[offset + 4], addr, 16);
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
                size_t maxLen = col.isArray ? 1024 : col.dsize;
                if (val.size() > maxLen) val.resize(maxLen);
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
                } else if (col.dataType == "inet" || col.dataType == "cidr") {
                    uint8_t family = 0, prefix = 32;
                    uint8_t addr[16] = {0};
                    if (!val.empty()) {
                        std::string addrPart = val;
                        size_t slash = addrPart.find('/');
                        if (slash != std::string::npos) {
                            try { prefix = static_cast<uint8_t>(std::stoi(addrPart.substr(slash + 1))); }
                            catch (...) { prefix = 32; }
                            addrPart = addrPart.substr(0, slash);
                        }
                        int a = 0, b = 0, c = 0, d = 0;
                        if (sscanf(addrPart.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d) == 4) {
                            family = 2;
                            addr[0] = static_cast<uint8_t>(a);
                            addr[1] = static_cast<uint8_t>(b);
                            addr[2] = static_cast<uint8_t>(c);
                            addr[3] = static_cast<uint8_t>(d);
                        }
                    }
                    fixedData[fixedOff] = static_cast<char>(family);
                    fixedData[fixedOff + 1] = static_cast<char>(prefix);
                    fixedData[fixedOff + 2] = static_cast<char>(col.dataType == "cidr" ? 1 : 0);
                    fixedData[fixedOff + 3] = 0;
                    std::memcpy(&fixedData[fixedOff + 4], addr, 16);
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

// ========================================================================
// JSON validator: structural + token-level validation
// ========================================================================
static bool isValidJson(const std::string& s) {
    if (s.empty()) return true;  // empty = NULL
    struct Token {
        enum Type { LBrace, RBrace, LBracket, RBracket, Colon, Comma,
                    String, Number, True, False, Null, End } type;
        std::string text;
    };
    size_t i = 0;
    auto skipSpace = [&]() { while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i; };
    auto nextToken = [&]() -> Token {
        skipSpace();
        if (i >= s.size()) return {Token::End, ""};
        char c = s[i];
        if (c == '{') { ++i; return {Token::LBrace, "{"}; }
        if (c == '}') { ++i; return {Token::RBrace, "}"}; }
        if (c == '[') { ++i; return {Token::LBracket, "["}; }
        if (c == ']') { ++i; return {Token::RBracket, "]"}; }
        if (c == ':') { ++i; return {Token::Colon, ":"}; }
        if (c == ',') { ++i; return {Token::Comma, ","}; }
        if (c == '"') {
            ++i;
            std::string str;
            while (i < s.size() && s[i] != '"') {
                if (s[i] == '\\' && i + 1 < s.size()) { str += s[i]; str += s[i+1]; i += 2; }
                else { str += s[i]; ++i; }
            }
            if (i >= s.size()) return {Token::End, ""};  // unclosed string
            ++i; // skip closing "
            return {Token::String, str};
        }
        // Number
        if (c == '-' || (c >= '0' && c <= '9')) {
            size_t start = i;
            if (c == '-') ++i;
            while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
            if (i < s.size() && s[i] == '.') {
                ++i;
                while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
            }
            if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
                ++i;
                if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
                while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
            }
            return {Token::Number, s.substr(start, i - start)};
        }
        // Literals: true, false, null
        if (i + 4 <= s.size() && s.substr(i, 4) == "true") { i += 4; return {Token::True, "true"}; }
        if (i + 5 <= s.size() && s.substr(i, 5) == "false") { i += 5; return {Token::False, "false"}; }
        if (i + 4 <= s.size() && s.substr(i, 4) == "null") { i += 4; return {Token::Null, "null"}; }
        return {Token::End, ""};  // invalid token
    };

    // Parse value
    std::function<bool()> parseValue;
    parseValue = [&]() -> bool {
        Token t = nextToken();
        if (t.type == Token::String || t.type == Token::Number ||
            t.type == Token::True || t.type == Token::False || t.type == Token::Null) {
            return true;
        }
        if (t.type == Token::LBrace) {
            Token peek = nextToken();
            if (peek.type == Token::RBrace) return true;  // empty object
            if (peek.type != Token::String) return false;  // key must be string
            Token colon = nextToken();
            if (colon.type != Token::Colon) return false;
            if (!parseValue()) return false;
            while (true) {
                Token commaOrEnd = nextToken();
                if (commaOrEnd.type == Token::RBrace) return true;
                if (commaOrEnd.type != Token::Comma) return false;
                Token key = nextToken();
                if (key.type != Token::String) return false;
                Token c = nextToken();
                if (c.type != Token::Colon) return false;
                if (!parseValue()) return false;
            }
        }
        if (t.type == Token::LBracket) {
            Token peek = nextToken();
            if (peek.type == Token::RBracket) return true;  // empty array
            // Put back peek token (simplified: we already consumed it, so parse as value)
            // Actually we consumed it. We need to handle this differently.
            // Since we can't un-get, we'll restructure.
            return false;  // Will handle differently below
        }
        return false;
    };

    // Array parsing (separate to handle the consumed first token issue)
    std::function<bool()> parseArray = [&]() -> bool {
        Token peek = nextToken();
        if (peek.type == Token::RBracket) return true;
        // We consumed the first token of the first element. Need to re-parse from there.
        // Since the tokenizer is linear and we can't un-get, we'll use a different approach:
        // Count tokens and re-parse the whole thing. This is a simplified validator.
        return false;
    };

    // Simpler approach: just validate the full string recursively with index tracking
    i = 0;
    skipSpace();
    if (i >= s.size()) return true;

    std::function<bool()> parseVal;
    parseVal = [&]() -> bool {
        skipSpace();
        if (i >= s.size()) return false;
        char c = s[i];
        if (c == '"') {
            ++i;
            while (i < s.size() && s[i] != '"') {
                if (s[i] == '\\' && i + 1 < s.size()) i += 2;
                else ++i;
            }
            if (i >= s.size()) return false;
            ++i;
            return true;
        }
        if (c == '{') {
            ++i;
            skipSpace();
            if (i < s.size() && s[i] == '}') { ++i; return true; }
            while (true) {
                skipSpace();
                if (i >= s.size() || s[i] != '"') return false;
                if (!parseVal()) return false;  // key string
                skipSpace();
                if (i >= s.size() || s[i] != ':') return false;
                ++i;
                if (!parseVal()) return false;  // value
                skipSpace();
                if (i < s.size() && s[i] == ',') { ++i; continue; }
                if (i < s.size() && s[i] == '}') { ++i; return true; }
                return false;
            }
        }
        if (c == '[') {
            ++i;
            skipSpace();
            if (i < s.size() && s[i] == ']') { ++i; return true; }
            while (true) {
                if (!parseVal()) return false;
                skipSpace();
                if (i < s.size() && s[i] == ',') { ++i; continue; }
                if (i < s.size() && s[i] == ']') { ++i; return true; }
                return false;
            }
        }
        // Number
        if (c == '-' || (c >= '0' && c <= '9')) {
            if (c == '-') ++i;
            bool hasDigits = false;
            while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; hasDigits = true; }
            if (!hasDigits) return false;
            if (i < s.size() && s[i] == '.') {
                ++i;
                hasDigits = false;
                while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; hasDigits = true; }
            }
            if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
                ++i;
                if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
                hasDigits = false;
                while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; hasDigits = true; }
                if (!hasDigits) return false;
            }
            return true;
        }
        // true, false, null
        if (i + 4 <= s.size() && s.substr(i, 4) == "true") { i += 4; return true; }
        if (i + 5 <= s.size() && s.substr(i, 5) == "false") { i += 5; return true; }
        if (i + 4 <= s.size() && s.substr(i, 4) == "null") { i += 4; return true; }
        return false;
    };

    bool ok = parseVal();
    if (!ok) return false;
    skipSpace();
    return i == s.size();
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
        // Validate JSON / JSONB columns
        if ((col.dataType == "json" || col.dataType == "jsonb") && !val.empty()) {
            if (!isValidJson(val)) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
        }
        if (!col.isVariableLength && col.dataType != "char" && col.dataType != "binary" && col.dataType != "date" && col.dataType != "timestamp" && col.dataType != "timestamptz" && col.dataType != "datetime" && col.dataType != "time" && col.dataType != "float" && col.dataType != "double" && col.dataType != "decimal" && col.dataType != "boolean" && col.dataType != "uuid" && col.dataType != "point" && col.dataType != "inet" && col.dataType != "cidr" && !val.empty()) {
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
    std::string targetSubPartition;
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
    if (tbl.subPartitionType != TableSchema::PartitionType::None) {
        auto spit = actualValues.find(tbl.subPartitionKey);
        if (spit != actualValues.end() && !spit->second.empty()) {
            targetSubPartition = getSubPartitionName(tbl, spit->second);
        }
        if (targetSubPartition.empty()) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
    }

    // Write row into page-based storage
    PageAllocator* pa = nullptr;
    std::unique_ptr<PageAllocator> partPa;
    if (!targetPartition.empty() && !targetSubPartition.empty()) {
        partPa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tablename, targetPartition, targetSubPartition).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
        if (!partPa->open()) {
            lockManager_.unlock(tablename);
            return OpResult::InvalidValue;
        }
        pa = partPa.get();
    } else if (!targetPartition.empty()) {
        partPa = std::make_unique<PageAllocator>(partitionDataPath(dbname, tablename, targetPartition).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
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
        uint8_t minPercent = static_cast<uint8_t>(std::min(size_t(100), actualRowSize * 100 / pa->pageSize() + 1));

        // Try FSM first for fast page lookup
        if (numPages > 1) {
            FreeSpaceMap* fsm = getFSM(dbname, tablename);
            uint32_t candidate = fsm->findPage(minPercent, 1);
            if (candidate > 0 && candidate < numPages) {
                lockManager_.pageLockExclusive(dbname, tablename, candidate);
                char* buf = pa->fetchPage(candidate);
                PageWrapper page(buf, pa->pageSize(), tbl.formatVersion);
                if (page.canFit(actualRowSize)) {
                    if (page.insert(rowBuffer.data(), actualRowSize, slotId)) {
                        pageId = candidate;
                        inserted = true;
                        pa->markDirty(candidate);
                        size_t freePct = page.freeSpace() * 100 / pa->pageSize();
                        fsm->setFreePercent(candidate, static_cast<uint8_t>(freePct));
                        getVM(dbname, tablename)->setAllVisible(candidate, false);
                    }
                }
                pa->unpinPage(candidate);
                lockManager_.pageUnlock(dbname, tablename, candidate);
            }
        }

        // Fallback: sequential scan
        for (uint32_t pid = 1; pid < numPages && !inserted; ++pid) {
            lockManager_.pageLockExclusive(dbname, tablename, pid);
            char* buf = pa->fetchPage(pid);
            PageWrapper page(buf, pa->pageSize(), tbl.formatVersion);
            if (page.canFit(actualRowSize)) {
                if (page.insert(rowBuffer.data(), actualRowSize, slotId)) {
                    pageId = pid;
                    inserted = true;
                }
            }
            if (inserted) {
                pa->markDirty(pid);
                size_t freePct = page.freeSpace() * 100 / pa->pageSize();
                getFSM(dbname, tablename)->setFreePercent(pid, static_cast<uint8_t>(freePct));
                getVM(dbname, tablename)->setAllVisible(pid, false);
            }
            pa->unpinPage(pid);
            lockManager_.pageUnlock(dbname, tablename, pid);
        }

        if (!inserted) {
            pageId = pa->allocPage();
            lockManager_.pageLockExclusive(dbname, tablename, pageId);
            char* buf = pa->fetchPage(pageId);
            PageWrapper page(buf, pa->pageSize(), tbl.formatVersion);
            if (!page.insert(rowBuffer.data(), actualRowSize, slotId)) {
                pa->unpinPage(pageId);
                lockManager_.pageUnlock(dbname, tablename, pageId);
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
            pa->markDirty(pageId);
            size_t freePct = page.freeSpace() * 100 / pa->pageSize();
            getFSM(dbname, tablename)->setFreePercent(pageId, static_cast<uint8_t>(freePct));
            getVM(dbname, tablename)->setAllVisible(pageId, false);
            pa->unpinPage(pageId);
            lockManager_.pageUnlock(dbname, tablename, pageId);
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
            // Evaluate WHEN condition if present
            if (!trg.whenCondition.empty() && whenEvaluator_) {
                std::string cond = trg.whenCondition;
                if (trg.forEachRow) {
                    for (const auto& [col, val] : actualValues) {
                        std::string placeholder = "NEW." + col;
                        size_t pos = 0;
                        while ((pos = cond.find(placeholder, pos)) != std::string::npos) {
                            cond.replace(pos, placeholder.size(), val);
                            pos += val.size();
                        }
                    }
                }
                if (!whenEvaluator_(cond, actualValues, {})) continue;
            }
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
            // Trigger diagnostic variables
            {
                auto replaceVar = [&](const std::string& var, const std::string& val) {
                    size_t pos = 0;
                    while ((pos = action.find(var, pos)) != std::string::npos) {
                        action.replace(pos, var.size(), val);
                        pos += val.size();
                    }
                };
                replaceVar("tg_name", trg.name);
                replaceVar("tg_when", trg.timing);
                replaceVar("tg_level", trg.forEachRow ? "ROW" : "STATEMENT");
                replaceVar("tg_op", "INSERT");
                replaceVar("tg_relname", trg.tableName);
            }
            triggerExecutor_(action);
        }
    }

    recordModification(dbname, tablename, 1);
    maybeAutoAnalyze(dbname, tablename);
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
        // Handle OVERLAPS operator
        if (s.size() >= 8 && s.substr(0, 8) == "overlaps") {
            c.op = "overlaps";
            c.colName = trim(s.substr(8));
            conds.push_back(c);
            continue;
        }
        // Handle 2-char operators: spatial << >> <^ >^ <@ and network &&
        // Format: operator_colname value (e.g., "<<loc 5.0,0.0" or "&&ip '192.168.1.0/24'")
        if (s.size() >= 4 && (s.substr(0, 2) == "<<" || s.substr(0, 2) == ">>" ||
                               s.substr(0, 2) == "<^" || s.substr(0, 2) == ">^" ||
                               s.substr(0, 2) == "<@" || s.substr(0, 2) == "&&")) {
            c.op = s.substr(0, 2);
            size_t sp = s.find(' ', 2);
            if (sp == std::string::npos) continue;
            c.colName = s.substr(2, sp - 2);
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

    // Try GIN index for = conditions (supports text/json/array multi-key lookup)
    for (const auto& c : conds) {
        if (c.op == "=") {
            if (hasGinIndex(dbname, tablename, c.colName)) {
                auto rids = ginSearch(dbname, tablename, c.colName, c.value);
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
    }

    // Try GiST index for range conditions (overlap/containment)
    for (const auto& c : conds) {
        if (hasGiSTIndex(dbname, tablename, c.colName)) {
            if (c.op == "=") {
                // GiST overlap with [val, val] is equivalent to equality for scalars
                auto rids = giSTSearchOverlap(dbname, tablename, c.colName, c.value, c.value);
                for (int64_t rid : rids) ids.insert(rid);
            } else if (c.op == ">=" || c.op == ">") {
                // Search for values that overlap with [query_val, +inf)
                auto rids = giSTSearchOverlap(dbname, tablename, c.colName, c.value, "");
                for (int64_t rid : rids) ids.insert(rid);
            } else if (c.op == "<=" || c.op == "<") {
                // Search for values that overlap with (-inf, query_val]
                auto rids = giSTSearchOverlap(dbname, tablename, c.colName, "", c.value);
                for (int64_t rid : rids) ids.insert(rid);
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
                            if (cond.colName == c.colName) continue;
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

    // Collect BRIN-suggested page ranges to narrow full scan
    std::vector<std::pair<uint32_t, uint32_t>> brinRanges;
    for (const auto& c : conds) {
        if (hasBrinIndex(dbname, tablename, c.colName)) {
            auto ranges = brinSearchRange(dbname, tablename, c.colName, c.op, c.value);
            if (!ranges.empty()) {
                brinRanges = ranges;
                break; // Use first applicable BRIN index
            }
        }
    }

    // Full table scan via page iterator (with optional BRIN page-range filtering)
    // Partition pruning: if conditions involve partition key, scan only matching partitions
    if (tbl.partitionType != TableSchema::PartitionType::None) {
        auto targetParts = getTargetPartitions(tbl, conds);
        if (!targetParts.empty()) {
            for (const auto& pname : targetParts) {
                auto ppa = std::make_unique<PageAllocator>(
                    partitionDataPath(dbname, tablename, pname).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
                if (!ppa->open()) continue;
                uint32_t np = ppa->numPages();
                for (uint32_t pid = 1; pid < np; ++pid) {
                    char* buf = ppa->fetchPage(pid);
                    PageWrapper page(buf, ppa->pageSize(), tbl.formatVersion);
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

    // Apply Row-Level Security policies
    std::vector<std::string> allConditions = conditions;
    auto rlsConds = buildRLSConditions(this, dbname, tablename, "DELETE");
    allConditions.insert(allConditions.end(), rlsConds.begin(), rlsConds.end());

    auto conds = parseConditions(allConditions);
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
                            PageWrapper page(pbuf, opa->pageSize(), otbl.formatVersion);
                            uint16_t newSlotId = sid;
                            page.update(sid, newRow.data(), newRow.size(), newSlotId);
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
                        PageWrapper page(pbuf, opa->pageSize(), otbl.formatVersion);
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
        lockManager_.pageLockExclusive(dbname, tablename, pageId);
        char* pageBuf = pa->fetchPage(pageId);
        if (pageBuf) {
            PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
            page.remove(slotId);
            pa->markDirty(pageId);
            size_t freePct = page.freeSpace() * 100 / pa->pageSize();
            getFSM(dbname, tablename)->setFreePercent(pageId, static_cast<uint8_t>(freePct));
            getVM(dbname, tablename)->setAllVisible(pageId, false);
            pa->unpinPage(pageId);
        }
        lockManager_.pageUnlock(dbname, tablename, pageId);
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
                    std::map<std::string, std::string> oldValues;
                    for (size_t i = 0; i < tbl.len; ++i) {
                        oldValues[tbl.cols[i].dataName] = extractColumnValue(row, tbl, i);
                    }
                    // Evaluate WHEN condition if present
                    if (!trg.whenCondition.empty() && whenEvaluator_) {
                        std::string cond = trg.whenCondition;
                        for (const auto& [col, val] : oldValues) {
                            std::string placeholder = "OLD." + col;
                            size_t pos = 0;
                            while ((pos = cond.find(placeholder, pos)) != std::string::npos) {
                                cond.replace(pos, placeholder.size(), val);
                                pos += val.size();
                            }
                        }
                        if (!whenEvaluator_(cond, {}, oldValues)) continue;
                    }
                    std::string action = trg.action;
                    for (const auto& [col, val] : oldValues) {
                        std::string placeholder = "OLD." + col;
                        size_t pos = 0;
                        while ((pos = action.find(placeholder, pos)) != std::string::npos) {
                            action.replace(pos, placeholder.size(), val);
                            pos += val.size();
                        }
                    }
                    // Trigger diagnostic variables
                    {
                        auto replaceVar = [&](const std::string& var, const std::string& val) {
                            size_t pos = 0;
                            while ((pos = action.find(var, pos)) != std::string::npos) {
                                action.replace(pos, var.size(), val);
                                pos += val.size();
                            }
                        };
                        replaceVar("tg_name", trg.name);
                        replaceVar("tg_when", trg.timing);
                        replaceVar("tg_level", trg.forEachRow ? "ROW" : "STATEMENT");
                        replaceVar("tg_op", "DELETE");
                        replaceVar("tg_relname", trg.tableName);
                    }
                    triggerExecutor_(action);
                }
            } else {
                // Statement-level trigger: execute once
                if (!trg.whenCondition.empty() && whenEvaluator_) {
                    if (!whenEvaluator_(trg.whenCondition, {}, {})) continue;
                }
                std::string action = trg.action;
                {
                    auto replaceVar = [&](const std::string& var, const std::string& val) {
                        size_t pos = 0;
                        while ((pos = action.find(var, pos)) != std::string::npos) {
                            action.replace(pos, var.size(), val);
                            pos += val.size();
                        }
                    };
                    replaceVar("tg_name", trg.name);
                    replaceVar("tg_when", trg.timing);
                    replaceVar("tg_level", trg.forEachRow ? "ROW" : "STATEMENT");
                    replaceVar("tg_op", "DELETE");
                    replaceVar("tg_relname", trg.tableName);
                }
                triggerExecutor_(action);
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
    recordModification(dbname, tablename, toDelete.size());
    maybeAutoAnalyze(dbname, tablename);

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

    // Apply Row-Level Security policies
    std::vector<std::string> allConditions = conditions;
    auto rlsConds = buildRLSConditions(this, dbname, tablename, "UPDATE");
    allConditions.insert(allConditions.end(), rlsConds.begin(), rlsConds.end());

    auto conds = parseConditions(allConditions);
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

        // Check if PK changed and apply ON UPDATE foreign key actions
        std::string newPK = extractPKValue(strippedNewRow, tbl);
        if (!oldPK.empty() && oldPK != newPK) {
            // Collect all referencing rows and their ON UPDATE actions
            struct UpdateCascadeAction { std::string table; int64_t rid; std::map<std::string, std::string> newFkVals; };
            struct UpdateSetNullAction { std::string table; int64_t rid; std::vector<size_t> colIndices; };
            std::vector<UpdateCascadeAction> updateCascadeActions;
            std::vector<UpdateSetNullAction> updateSetNullActions;
            std::set<std::string> restrictTables;

            // Get the old PK values as a map
            std::map<std::string, std::string> oldPKVals;
            if (!tbl.pkColIndices.empty()) {
                for (size_t pki : tbl.pkColIndices) {
                    oldPKVals[tbl.cols[pki].dataName] = extractColumnValue(row, tbl, pki);
                }
            } else {
                size_t pkIdxCol = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].isPrimaryKey) { pkIdxCol = i; break; }
                }
                if (pkIdxCol < tbl.len) oldPKVals[tbl.cols[pkIdxCol].dataName] = extractColumnValue(row, tbl, pkIdxCol);
            }

            if (!oldPKVals.empty()) {
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
                            std::string oRow(data, len);
                            // Build FK value map for this row
                            std::map<std::string, std::string> fkVals;
                            for (size_t ci = 0; ci < fk.colNames.size() && ci < fk.refCols.size(); ++ci) {
                                fkVals[fk.refCols[ci]] = extractColumnValue(oRow, otherTbl, fkColIndices[ci]);
                            }
                            // Check if old PK row matches this FK
                            bool matched = true;
                            for (const auto& [refCol, refVal] : oldPKVals) {
                                auto it = fkVals.find(refCol);
                                if (it == fkVals.end() || it->second != refVal) {
                                    matched = false; break;
                                }
                            }
                            if (matched) {
                                int64_t orid = encodeRid(opid, osid);
                                if (fk.onUpdate == "cascade") {
                                    UpdateCascadeAction ca;
                                    ca.table = otherTable;
                                    ca.rid = orid;
                                    // Build new FK values: map refCols to new PK values
                                    for (size_t ci = 0; ci < fk.colNames.size() && ci < fk.refCols.size(); ++ci) {
                                        auto it = rowValues.find(fk.refCols[ci]);
                                        if (it != rowValues.end()) {
                                            ca.newFkVals[fk.colNames[ci]] = it->second;
                                        }
                                    }
                                    if (!ca.newFkVals.empty()) updateCascadeActions.push_back(std::move(ca));
                                } else if (fk.onUpdate == "setnull") {
                                    updateSetNullActions.push_back({otherTable, orid, fkColIndices});
                                } else {
                                    restrictTables.insert(otherTable);
                                }
                            }
                        });
                    }
                }
            }

            if (!restrictTables.empty()) {
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }

            // Acquire locks on referenced tables in alphabetical order
            std::set<std::string> affectedTables;
            for (const auto& ca : updateCascadeActions) affectedTables.insert(ca.table);
            for (const auto& sa : updateSetNullActions) affectedTables.insert(sa.table);
            std::vector<std::string> sortedTables(affectedTables.begin(), affectedTables.end());
            std::sort(sortedTables.begin(), sortedTables.end());
            for (const auto& t : sortedTables) {
                lockManager_.lockIntentExclusive(t);
            }

            // Apply ON UPDATE SET NULL
            for (const auto& sa : updateSetNullActions) {
                TableSchema otbl = getTableSchema(dbname, sa.table);
                PageAllocator* opa = getPageAllocator(dbname, sa.table);
                std::string oRow;
                if (!readRowByRid(opa, sa.rid, oRow, otbl)) continue;

                // Transaction log
                if (inTransaction_ && dbname == txnDB_) {
                    logTxnUpdate(sa.table, sa.rid, oRow);
                }

                // Rebuild row with FK columns set to NULL
                std::map<std::string, std::string> oRowVals;
                for (size_t i = 0; i < otbl.len; ++i) {
                    oRowVals[otbl.cols[i].dataName] = extractColumnValue(oRow, otbl, i);
                }
                for (size_t ci : sa.colIndices) {
                    oRowVals[otbl.cols[ci].dataName] = "";
                }
                std::string newORow = buildRowBuffer(otbl, oRowVals, 0);
                uint32_t opid; uint16_t osid;
                decodeRid(sa.rid, opid, osid);
                char* obuf = opa->fetchPage(opid);
                if (obuf) {
                    Page opage(obuf, opa->pageSize());
                    opage.update(osid, newORow.data(), newORow.size());
                    pa->markDirty(opid);
                    opa->unpinPage(opid);
                }
            }

            // Apply ON UPDATE CASCADE: update referencing rows' FK values
            for (const auto& ca : updateCascadeActions) {
                TableSchema otbl = getTableSchema(dbname, ca.table);
                PageAllocator* opa = getPageAllocator(dbname, ca.table);
                std::string oRow;
                if (!readRowByRid(opa, ca.rid, oRow, otbl)) continue;

                // Transaction log
                if (inTransaction_ && dbname == txnDB_) {
                    logTxnUpdate(ca.table, ca.rid, oRow);
                }

                std::map<std::string, std::string> oRowVals;
                for (size_t i = 0; i < otbl.len; ++i) {
                    oRowVals[otbl.cols[i].dataName] = extractColumnValue(oRow, otbl, i);
                }
                for (const auto& kv : ca.newFkVals) {
                    oRowVals[kv.first] = kv.second;
                }
                std::string newORow = buildRowBuffer(otbl, oRowVals, 0);
                uint32_t opid; uint16_t osid;
                decodeRid(ca.rid, opid, osid);
                char* obuf = opa->fetchPage(opid);
                if (obuf) {
                    Page opage(obuf, opa->pageSize());
                    opage.update(osid, newORow.data(), newORow.size());
                    pa->markDirty(opid);
                    opa->unpinPage(opid);
                }
            }

            for (const auto& t : sortedTables) {
                lockManager_.unlock(t);
            }
        }

        char* pageBuf = pa->fetchPage(pageId);
        int64_t actualRid = rid;
        if (pageBuf) {
            lockManager_.pageLockExclusive(dbname, tablename, pageId);
            PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
            uint16_t newSlotId = slotId;
            if (!page.update(slotId, newRow.data(), newRow.size(), newSlotId)) {
                pa->unpinPage(pageId);
                lockManager_.pageUnlock(dbname, tablename, pageId);
                lockManager_.unlock(tablename);
                return OpResult::InvalidValue;
            }
            pa->markDirty(pageId);
            size_t freePct = page.freeSpace() * 100 / pa->pageSize();
            getFSM(dbname, tablename)->setFreePercent(pageId, static_cast<uint8_t>(freePct));
            getVM(dbname, tablename)->setAllVisible(pageId, false);
            pa->unpinPage(pageId);
            lockManager_.pageUnlock(dbname, tablename, pageId);
            if (newSlotId != slotId) {
                actualRid = encodeRid(pageId, newSlotId);
                // Update the last txnLog entry with the new rid since the row moved
                if (inTransaction_ && dbname == txnDB_ && !txnLog_.empty()) {
                    txnLog_.back().rowIdx = actualRid;
                }
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
                    std::map<std::string, std::string> newValues;
                    for (size_t i = 0; i < tbl.len; ++i) {
                        newValues[tbl.cols[i].dataName] = extractColumnValue(newRow, tbl, i);
                    }
                    // Evaluate WHEN condition if present
                    if (!trg.whenCondition.empty() && whenEvaluator_) {
                        std::string cond = trg.whenCondition;
                        for (const auto& [col, val] : newValues) {
                            std::string placeholder = "NEW." + col;
                            size_t pos = 0;
                            while ((pos = cond.find(placeholder, pos)) != std::string::npos) {
                                cond.replace(pos, placeholder.size(), val);
                                pos += val.size();
                            }
                        }
                        if (!whenEvaluator_(cond, newValues, {})) continue;
                    }
                    std::string action = trg.action;
                    for (const auto& [col, val] : newValues) {
                        std::string placeholder = "NEW." + col;
                        size_t pos = 0;
                        while ((pos = action.find(placeholder, pos)) != std::string::npos) {
                            action.replace(pos, placeholder.size(), val);
                            pos += val.size();
                        }
                    }
                    // Trigger diagnostic variables
                    {
                        auto replaceVar = [&](const std::string& var, const std::string& val) {
                            size_t pos = 0;
                            while ((pos = action.find(var, pos)) != std::string::npos) {
                                action.replace(pos, var.size(), val);
                                pos += val.size();
                            }
                        };
                        replaceVar("tg_name", trg.name);
                        replaceVar("tg_when", trg.timing);
                        replaceVar("tg_level", trg.forEachRow ? "ROW" : "STATEMENT");
                        replaceVar("tg_op", "UPDATE");
                        replaceVar("tg_relname", trg.tableName);
                    }
                    triggerExecutor_(action);
                }
            } else {
                // Statement-level trigger: execute once
                if (!trg.whenCondition.empty() && whenEvaluator_) {
                    if (!whenEvaluator_(trg.whenCondition, {}, {})) continue;
                }
                std::string action = trg.action;
                {
                    auto replaceVar = [&](const std::string& var, const std::string& val) {
                        size_t pos = 0;
                        while ((pos = action.find(var, pos)) != std::string::npos) {
                            action.replace(pos, var.size(), val);
                            pos += val.size();
                        }
                    };
                    replaceVar("tg_name", trg.name);
                    replaceVar("tg_when", trg.timing);
                    replaceVar("tg_level", trg.forEachRow ? "ROW" : "STATEMENT");
                    replaceVar("tg_op", "UPDATE");
                    replaceVar("tg_relname", trg.tableName);
                }
                triggerExecutor_(action);
            }
        }
    }

    recordModification(dbname, tablename, matchIds.size());
    maybeAutoAnalyze(dbname, tablename);
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
    } else if (tablename == "routines" || tablename == "ROUTINES") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& pname : getProcedureNames(dbname)) {
                std::string row = dbname + " " + pname + " PROCEDURE ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "routine_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "routine_name" && c.op == "=" && pname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
            for (const auto& fname : getUDFNames(dbname)) {
                std::string row = dbname + " " + fname + " FUNCTION ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "routine_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "routine_name" && c.op == "=" && fname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
            for (const auto& tname : getTVFNames(dbname)) {
                std::string row = dbname + " " + tname + " FUNCTION ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "routine_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "routine_name" && c.op == "=" && tname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
            for (const auto& trg : getAllTriggers(dbname)) {
                std::string row = dbname + " " + trg.name + " TRIGGER ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "routine_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "routine_name" && c.op == "=" && trg.name != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
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
    } else if (tablename == "triggers" || tablename == "TRIGGERS") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& trg : getAllTriggers(dbname)) {
                std::string row = dbname + " " + trg.name + " " + trg.event + " " + trg.tableName + " " + trg.timing + " ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "trigger_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                    if (c.colName == "trigger_name" && c.op == "=" && trg.name != c.value) { match = false; break; }
                    if (c.colName == "event_object_table" && c.op == "=" && trg.tableName != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
        }
    } else if (tablename == "key_column_usage" || tablename == "KEY_COLUMN_USAGE") {
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& tname : getTableNames(dbname)) {
                TableSchema tbl = getTableSchema(dbname, tname);
                // Primary key columns
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (!tbl.cols[i].isPrimaryKey) continue;
                    std::string row = dbname + " " + tname + " " + tbl.cols[i].dataName + " PRIMARY ";
                    bool match = true;
                    for (const auto& c : conds) {
                        if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                        if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                        if (c.colName == "column_name" && c.op == "=" && tbl.cols[i].dataName != c.value) { match = false; break; }
                    }
                    if (match) result.push_back(row);
                }
                // Foreign key columns
                for (size_t i = 0; i < tbl.fkLen; ++i) {
                    for (const auto& colName : tbl.fks[i].colNames) {
                        std::string row = dbname + " " + tname + " " + colName + " FOREIGN ";
                        bool match = true;
                        for (const auto& c : conds) {
                            if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                            if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                            if (c.colName == "column_name" && c.op == "=" && colName != c.value) { match = false; break; }
                        }
                        if (match) result.push_back(row);
                    }
                }
                // Unique columns
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (!tbl.cols[i].isUnique || tbl.cols[i].isPrimaryKey) continue;
                    std::string row = dbname + " " + tname + " " + tbl.cols[i].dataName + " UNIQUE ";
                    bool match = true;
                    for (const auto& c : conds) {
                        if (c.colName == "table_schema" && c.op == "=" && dbname != c.value) { match = false; break; }
                        if (c.colName == "table_name" && c.op == "=" && tname != c.value) { match = false; break; }
                        if (c.colName == "column_name" && c.op == "=" && tbl.cols[i].dataName != c.value) { match = false; break; }
                    }
                    if (match) result.push_back(row);
                }
            }
        }
    }
    return result;
}

std::vector<std::string> StorageEngine::queryPgCatalog(
    const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::set<std::string>& selectCols,
    const std::vector<OrderBySpec>& orderBy) const {

    std::vector<std::string> result;
    auto conds = parseConditions(conditions);

    if (tablename == "pg_class" || tablename == "PG_CLASS") {
        // PostgreSQL pg_class: relname relkind relnamespace relpages reltuples
        for (const auto& dbname : getDatabaseNames()) {
            // Tables
            for (const auto& tname : getTableNames(dbname)) {
                std::string row = tname + " r " + dbname + " 0 0 ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "relname" && c.op == "=" && tname != c.value) { match = false; break; }
                    if (c.colName == "relnamespace" && c.op == "=" && dbname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
            // Views
            for (const auto& vname : getViewNames(dbname)) {
                std::string row = vname + " v " + dbname + " 0 0 ";
                bool match = true;
                for (const auto& c : conds) {
                    if (c.colName == "relname" && c.op == "=" && vname != c.value) { match = false; break; }
                    if (c.colName == "relnamespace" && c.op == "=" && dbname != c.value) { match = false; break; }
                }
                if (match) result.push_back(row);
            }
        }
    } else if (tablename == "pg_attribute" || tablename == "PG_ATTRIBUTE") {
        // PostgreSQL pg_attribute: attrelid attname atttypid attnum attnotnull attlen
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& tname : getTableNames(dbname)) {
                TableSchema tbl = getTableSchema(dbname, tname);
                for (size_t i = 0; i < tbl.len; ++i) {
                    const Column& col = tbl.cols[i];
                    std::string notnull = col.isNull ? "f" : "t";
                    int attlen = col.isVariableLength ? -1 : col.dsize;
                    std::string row = tname + " " + col.dataName + " " + col.dataType + " " +
                                      std::to_string(i + 1) + " " + notnull + " " + std::to_string(attlen) + " ";
                    bool match = true;
                    for (const auto& c : conds) {
                        if (c.colName == "attrelid" && c.op == "=" && tname != c.value) { match = false; break; }
                        if (c.colName == "attname" && c.op == "=" && col.dataName != c.value) { match = false; break; }
                    }
                    if (match) result.push_back(row);
                }
            }
        }
    } else if (tablename == "pg_type" || tablename == "PG_TYPE") {
        // PostgreSQL pg_type: typname typlen typtype
        static const std::vector<std::pair<std::string, int>> pgTypes = {
            {"int", 4}, {"bigint", 8}, {"smallint", 2}, {"varchar", -1}, {"char", 1},
            {"text", -1}, {"boolean", 1}, {"real", 4}, {"double precision", 8},
            {"date", 4}, {"timestamp", 8}, {"timestamptz", 8}, {"json", -1}, {"jsonb", -1},
            {"uuid", 16}, {"decimal", -1}, {"blob", -1}, {"binary", -1},
            {"serial", 4}, {"time", 8}, {"datetime", 8}, {"float", 4},
        };
        for (const auto& pt : pgTypes) {
            std::string row = pt.first + " " + std::to_string(pt.second) + " b ";
            bool match = true;
            for (const auto& c : conds) {
                if (c.colName == "typname" && c.op == "=" && pt.first != c.value) { match = false; break; }
            }
            if (match) result.push_back(row);
        }
    } else if (tablename == "pg_index" || tablename == "PG_INDEX") {
        // PostgreSQL pg_index: indrelid indkey indisunique indisprimary
        for (const auto& dbname : getDatabaseNames()) {
            for (const auto& tname : getTableNames(dbname)) {
                TableSchema tbl = getTableSchema(dbname, tname);
                // Single-column indexes
                auto idxCols = getIndexedColumns(dbname, tname);
                for (const auto& cname : idxCols) {
                    std::string isUnique = "f";
                    std::string isPrimary = "f";
                    for (size_t i = 0; i < tbl.len; ++i) {
                        if (tbl.cols[i].dataName == cname) {
                            if (tbl.cols[i].isUnique) isUnique = "t";
                            if (tbl.cols[i].isPrimaryKey) isPrimary = "t";
                        }
                    }
                    std::string row = tname + " " + cname + " " + isUnique + " " + isPrimary + " ";
                    bool match = true;
                    for (const auto& c : conds) {
                        if (c.colName == "indrelid" && c.op == "=" && tname != c.value) { match = false; break; }
                    }
                    if (match) result.push_back(row);
                }
            }
        }
    } else if (tablename == "pg_namespace" || tablename == "PG_NAMESPACE") {
        // PostgreSQL pg_namespace: nspname
        for (const auto& dbname : getDatabaseNames()) {
            std::string row = dbname + " ";
            bool match = true;
            for (const auto& c : conds) {
                if (c.colName == "nspname" && c.op == "=" && dbname != c.value) { match = false; break; }
            }
            if (match) result.push_back(row);
        }
        // Also add information_schema and pg_catalog as namespaces
        result.push_back("information_schema ");
        result.push_back("pg_catalog ");
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
                                               int timezoneOffsetMinutes,
                                               const std::vector<std::string>& distinctOnCols) {
    std::vector<std::string> result;

    // information_schema virtual tables
    if (dbname == "information_schema") {
        return queryInformationSchema(tablename, conditions, selectCols, orderBy);
    }

    // pg_catalog virtual tables
    if (dbname == "pg_catalog") {
        return queryPgCatalog(tablename, conditions, selectCols, orderBy);
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

    // Apply Row-Level Security policies
    std::vector<std::string> allConditions = conditions;
    auto rlsConds = buildRLSConditions(this, dbname, tablename, "SELECT");
    allConditions.insert(allConditions.end(), rlsConds.begin(), rlsConds.end());

    auto conds = parseConditions(allConditions);
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

    // If any ORDER BY spec is an expression, apply expression-based sorting
    // directly on the original row buffers to preserve MVCC/formatting state.
    bool hasExprOrder = false;
    for (const auto& spec : orderBy) {
        if (spec.isExpression) { hasExprOrder = true; break; }
    }
    if (hasExprOrder) {
        struct ExprKey {
            size_t idx;
            std::vector<std::string> exprVals;
        };
        std::vector<ExprKey> ekeys;
        ekeys.reserve(matchRows.size());
        for (size_t ri = 0; ri < matchRows.size(); ++ri) {
            ExprKey ek{ri, {}};
            // Build column value map for expression evaluation
            std::map<std::string, std::string> rowData;
            for (size_t ci = 0; ci < tbl.len; ++ci) {
                rowData[tbl.cols[ci].dataName] = extractColumnValue(matchRows[ri].second, tbl, ci);
            }
            for (const auto& spec : orderBy) {
                if (!spec.isExpression) continue;
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
                } else if (spec.exprFunc == "add") {
                    try { ev = std::to_string(std::stoll(argVal) + std::stoll(spec.exprArg2)); } catch (...) { ev = argVal; }
                } else if (spec.exprFunc == "sub") {
                    try { ev = std::to_string(std::stoll(argVal) - std::stoll(spec.exprArg2)); } catch (...) { ev = argVal; }
                } else {
                    ev = argVal;
                }
                ek.exprVals.push_back(ev);
            }
            ekeys.push_back(std::move(ek));
        }
        std::stable_sort(ekeys.begin(), ekeys.end(), [&](const ExprKey& a, const ExprKey& b) {
            size_t evi = 0;
            for (const auto& spec : orderBy) {
                if (!spec.isExpression) continue;
                const std::string& av = a.exprVals[evi];
                const std::string& bv = b.exprVals[evi];
                bool aNull = av.empty();
                bool bNull = bv.empty();
                if (aNull && bNull) { ++evi; continue; }
                if (aNull) return spec.nullsFirst;
                if (bNull) return !spec.nullsFirst;
                bool less = false, greater = false;
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
                ++evi;
            }
            return false;
        });
        std::vector<std::pair<int64_t, std::string>> sorted;
        sorted.reserve(matchRows.size());
        for (const auto& ek : ekeys) {
            sorted.push_back(std::move(matchRows[ek.idx]));
        }
        matchRows = std::move(sorted);
    }

    // DISTINCT ON: keep first row for each distinct combination of specified columns
    if (!distinctOnCols.empty()) {
        std::vector<size_t> distinctIdxs;
        for (const auto& colName : distinctOnCols) {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == colName) { distinctIdxs.push_back(i); break; }
            }
        }
        if (distinctIdxs.size() == distinctOnCols.size()) {
            std::set<std::string> seen;
            std::vector<std::pair<int64_t, std::string>> deduped;
            for (auto& mr : matchRows) {
                std::string key;
                for (size_t idx : distinctIdxs) {
                    if (!key.empty()) key += "\x01";
                    key += extractColumnValue(mr.second, tbl, idx);
                }
                if (seen.insert(key).second) deduped.push_back(std::move(mr));
            }
            matchRows = std::move(deduped);
        }
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

    if ((expr.funcName == "current_user" || expr.funcName == "session_user") && !expr.sessionUser.empty()) {
        return expr.sessionUser;
    }
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
        bool hasMultiByte = false;
        for (unsigned char c : val) if (c >= 0x80) { hasMultiByte = true; break; }
        if (hasMultiByte) return toUpperUtf8(val);
        for (char& c : val) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        return val;
    }
    if (expr.funcName == "lower" && !expr.funcArgs.empty()) {
        std::string val = getVal(expr.funcArgs[0]);
        bool hasMultiByte = false;
        for (unsigned char c : val) if (c >= 0x80) { hasMultiByte = true; break; }
        if (hasMultiByte) return toLowerUtf8(val);
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
    // ---- JSONB scalar functions ----
    if (expr.funcName == "jsonb_extract" && expr.funcArgs.size() >= 2) {
        // jsonb_extract is alias for json_extract
        StorageEngine::SelectExpr subExpr;
        subExpr.isScalar = true;
        subExpr.funcName = "json_extract";
        subExpr.funcArgs = expr.funcArgs;
        return applyScalarFunc(subExpr, rowBuffer, tbl, engine, dbname);
    }
    if (expr.funcName == "jsonb_extract_text" && expr.funcArgs.size() >= 2) {
        // jsonb_extract_text is alias for json_value
        StorageEngine::SelectExpr subExpr;
        subExpr.isScalar = true;
        subExpr.funcName = "json_value";
        subExpr.funcArgs = expr.funcArgs;
        return applyScalarFunc(subExpr, rowBuffer, tbl, engine, dbname);
    }
    if (expr.funcName == "jsonb_contains" && expr.funcArgs.size() >= 2) {
        // JSONB @> operator: check if target contains pattern (shallow check)
        std::string target = getVal(expr.funcArgs[0]);
        std::string pattern = getVal(expr.funcArgs[1]);
        if (target.empty() || pattern.empty()) return "false";
        // Simple shallow contains: extract all "key":value from pattern
        // and verify each exists in target
        size_t pi = 0;
        while (pi < pattern.size()) {
            size_t q = pattern.find('"', pi);
            if (q == std::string::npos) break;
            size_t q2 = pattern.find('"', q + 1);
            if (q2 == std::string::npos) break;
            std::string key = pattern.substr(q + 1, q2 - q - 1);
            size_t colon = pattern.find(':', q2 + 1);
            if (colon == std::string::npos) break;
            size_t vStart = colon + 1;
            while (vStart < pattern.size() && std::isspace(static_cast<unsigned char>(pattern[vStart]))) ++vStart;
            if (vStart >= pattern.size()) break;
            std::string val;
            if (pattern[vStart] == '{') {
                int d = 1; size_t ve = vStart + 1;
                while (ve < pattern.size() && d > 0) {
                    if (pattern[ve] == '{') ++d;
                    else if (pattern[ve] == '}') --d;
                    ++ve;
                }
                val = pattern.substr(vStart, ve - vStart);
                pi = ve;
            } else if (pattern[vStart] == '[') {
                int d = 1; size_t ve = vStart + 1;
                while (ve < pattern.size() && d > 0) {
                    if (pattern[ve] == '[') ++d;
                    else if (pattern[ve] == ']') --d;
                    ++ve;
                }
                val = pattern.substr(vStart, ve - vStart);
                pi = ve;
            } else if (pattern[vStart] == '"') {
                size_t ve = vStart + 1;
                while (ve < pattern.size() && pattern[ve] != '"') {
                    if (pattern[ve] == '\\' && ve + 1 < pattern.size()) ve += 2;
                    else ++ve;
                }
                val = pattern.substr(vStart, ve - vStart + 1);
                pi = ve + 1;
            } else {
                size_t ve = vStart;
                while (ve < pattern.size() && pattern[ve] != ',' && pattern[ve] != '}') ++ve;
                val = trim(pattern.substr(vStart, ve - vStart));
                pi = ve;
            }
            // Search for key in target
            std::string searchKey = "\"" + key + "\"";
            size_t tk = target.find(searchKey);
            if (tk == std::string::npos) return "false";
            size_t tc = target.find(':', tk + searchKey.size());
            if (tc == std::string::npos) return "false";
            size_t tvStart = tc + 1;
            while (tvStart < target.size() && std::isspace(static_cast<unsigned char>(target[tvStart]))) ++tvStart;
            std::string tval;
            if (tvStart < target.size() && target[tvStart] == '{') {
                int d = 1; size_t ve = tvStart + 1;
                while (ve < target.size() && d > 0) {
                    if (target[ve] == '{') ++d;
                    else if (target[ve] == '}') --d;
                    ++ve;
                }
                tval = target.substr(tvStart, ve - tvStart);
            } else if (tvStart < target.size() && target[tvStart] == '[') {
                int d = 1; size_t ve = tvStart + 1;
                while (ve < target.size() && d > 0) {
                    if (target[ve] == '[') ++d;
                    else if (target[ve] == ']') --d;
                    ++ve;
                }
                tval = target.substr(tvStart, ve - tvStart);
            } else if (tvStart < target.size() && target[tvStart] == '"') {
                size_t ve = tvStart + 1;
                while (ve < target.size() && target[ve] != '"') {
                    if (target[ve] == '\\' && ve + 1 < target.size()) ve += 2;
                    else ++ve;
                }
                tval = target.substr(tvStart, ve - tvStart + 1);
            } else {
                size_t ve = tvStart;
                while (ve < target.size() && target[ve] != ',' && target[ve] != '}') ++ve;
                tval = trim(target.substr(tvStart, ve - tvStart));
            }
            if (tval != val) return "false";
        }
        return "true";
    }
    if (expr.funcName == "jsonb_exists" && expr.funcArgs.size() >= 2) {
        // JSONB ? operator: check if top-level key exists
        std::string target = getVal(expr.funcArgs[0]);
        std::string key = getVal(expr.funcArgs[1]);
        if (target.empty() || key.empty()) return "false";
        if (key.size() >= 2 && key.front() == '\'' && key.back() == '\'') key = key.substr(1, key.size() - 2);
        std::string searchKey = "\"" + key + "\"";
        return (target.find(searchKey) != std::string::npos) ? "true" : "false";
    }
    if (expr.funcName == "jsonb_pretty" && !expr.funcArgs.empty()) {
        // Pretty-print JSON with indentation
        std::string json = getVal(expr.funcArgs[0]);
        if (json.empty()) return "";
        std::string out;
        int indent = 0;
        bool inStr = false;
        for (size_t i = 0; i < json.size(); ++i) {
            char c = json[i];
            if (c == '"' && (i == 0 || json[i - 1] != '\\')) {
                inStr = !inStr;
                out += c;
                continue;
            }
            if (inStr) { out += c; continue; }
            if (c == '{' || c == '[') {
                out += c;
                out += '\n';
                indent += 2;
                out.append(indent, ' ');
            } else if (c == '}' || c == ']') {
                out += '\n';
                indent -= 2;
                out.append(indent, ' ');
                out += c;
            } else if (c == ',') {
                out += c;
                out += '\n';
                out.append(indent, ' ');
            } else if (c == ':') {
                out += c;
                out += ' ';
            } else if (!std::isspace(static_cast<unsigned char>(c))) {
                out += c;
            }
        }
        return out;
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
    // Array functions
    if (expr.funcName == "array_get" && expr.funcArgs.size() >= 2) {
        std::string arr = getVal(expr.funcArgs[0]);
        try {
            int idx = std::stoi(getVal(expr.funcArgs[1]));
            if (idx <= 0) return "";
            std::vector<std::string> elems;
            if (arr.size() >= 2 && arr.front() == '[' && arr.back() == ']') {
                arr = arr.substr(1, arr.size() - 2);
            }
            size_t i = 0;
            while (i < arr.size()) {
                while (i < arr.size() && isspace(static_cast<unsigned char>(arr[i]))) ++i;
                if (i >= arr.size()) break;
                std::string elem;
                if (arr[i] == '\'' || arr[i] == '"') {
                    char q = arr[i++];
                    while (i < arr.size() && arr[i] != q) elem += arr[i++];
                    if (i < arr.size()) ++i;
                } else {
                    while (i < arr.size() && arr[i] != ',') elem += arr[i++];
                }
                elems.push_back(trim(elem));
                if (i < arr.size() && arr[i] == ',') ++i;
            }
            if (static_cast<size_t>(idx) > elems.size()) return "";
            return elems[idx - 1];
        } catch (...) { return ""; }
    }
    if (expr.funcName == "array_length" && !expr.funcArgs.empty()) {
        std::string arr = getVal(expr.funcArgs[0]);
        if (arr.size() >= 2 && arr.front() == '[' && arr.back() == ']') {
            arr = arr.substr(1, arr.size() - 2);
        }
        if (arr.empty()) return "0";
        int count = 1;
        for (char c : arr) if (c == ',') ++count;
        return std::to_string(count);
    }
    if (expr.funcName == "array_contains" && expr.funcArgs.size() >= 2) {
        std::string arr = getVal(expr.funcArgs[0]);
        std::string search = getVal(expr.funcArgs[1]);
        if (arr.size() >= 2 && arr.front() == '[' && arr.back() == ']') {
            arr = arr.substr(1, arr.size() - 2);
        }
        size_t i = 0;
        while (i < arr.size()) {
            while (i < arr.size() && isspace(static_cast<unsigned char>(arr[i]))) ++i;
            if (i >= arr.size()) break;
            std::string elem;
            if (arr[i] == '\'' || arr[i] == '"') {
                char q = arr[i++];
                while (i < arr.size() && arr[i] != q) elem += arr[i++];
                if (i < arr.size()) ++i;
            } else {
                while (i < arr.size() && arr[i] != ',') elem += arr[i++];
            }
            if (trim(elem) == search) return "1";
            if (i < arr.size() && arr[i] == ',') ++i;
        }
        return "0";
    }
    if (expr.funcName == "unnest" && !expr.funcArgs.empty()) {
        // Return raw array value; expansion happens in queryExpr
        return getVal(expr.funcArgs[0]);
    }
    // User-defined function fallback
    if (engine && !dbname.empty()) {
        auto udf = engine->getUDF(dbname, expr.funcName);
        if (!udf.expression.empty()) {
            std::string result = udf.expression;
            for (size_t i = 0; i < udf.paramNames.size() && i < expr.funcArgs.size(); ++i) {
                std::string val = getVal(expr.funcArgs[i]);
                size_t pos = 0;
                while ((pos = result.find(udf.paramNames[i], pos)) != std::string::npos) {
                    result.replace(pos, udf.paramNames[i].size(), val);
                    pos += val.size();
                }
            }
            return result;
        }
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

    // Detect unnest in SELECT expressions
    bool hasUnnest = false;
    size_t unnestIdx = 0;
    for (size_t i = 0; i < exprs.size(); ++i) {
        if (exprs[i].isScalar && exprs[i].funcName == "unnest") {
            hasUnnest = true;
            unnestIdx = i;
            break;
        }
    }

    for (auto& mr : matchRows) {
        if (hasUnnest) {
            // Compute all column values first
            std::vector<std::string> vals;
            vals.reserve(exprs.size());
            for (const auto& expr : exprs) {
                if (expr.isScalar) {
                    vals.push_back(applyScalarFunc(expr, mr.second, tbl, this, dbname));
                } else {
                    std::string v;
                    for (size_t ci = 0; ci < tbl.len; ++ci) {
                        if (tbl.cols[ci].dataName == expr.colName) {
                            v = extractColumnValue(mr.second, tbl, ci);
                            break;
                        }
                    }
                    vals.push_back(v);
                }
            }
            // Parse array elements from unnest column
            std::string arr = vals[unnestIdx];
            std::vector<std::string> elems;
            if (arr.size() >= 2 && arr.front() == '[' && arr.back() == ']') {
                arr = arr.substr(1, arr.size() - 2);
            }
            size_t ai = 0;
            while (ai < arr.size()) {
                while (ai < arr.size() && std::isspace(static_cast<unsigned char>(arr[ai]))) ++ai;
                if (ai >= arr.size()) break;
                std::string elem;
                if (arr[ai] == '\'' || arr[ai] == '"') {
                    char q = arr[ai++];
                    while (ai < arr.size() && arr[ai] != q) elem += arr[ai++];
                    if (ai < arr.size()) ++ai;
                } else {
                    while (ai < arr.size() && arr[ai] != ',') elem += arr[ai++];
                }
                elems.push_back(trim(elem));
                if (ai < arr.size() && arr[ai] == ',') ++ai;
            }
            if (elems.empty()) elems.push_back("");
            for (const auto& elem : elems) {
                std::string rowStr;
                for (size_t vi = 0; vi < vals.size(); ++vi) {
                    rowStr += (vi == unnestIdx ? elem : vals[vi]);
                    rowStr += ' ';
                }
                result.push_back(rowStr);
            }
        } else {
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
    }
    lockManager_.unlock(tablename);
    return result;
}

std::vector<std::string> StorageEngine::aggregate(
    const std::string& dbname, const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::vector<AggItem>& items) {
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
        const std::string& func = item.func;
        const std::string& colName = item.arg;
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
        std::vector<std::string> arrayAggVals;

        bool isDistinctCount = (func == "count" && colName.size() > 9 && colName.substr(0, 9) == "distinct ");
        std::string actualColName = isDistinctCount ? colName.substr(9) : colName;
        bool isJsonAgg = (func == "json_agg" || func == "jsonb_agg");
        bool isArrayAgg = (func == "array_agg");
        bool isVar = (func == "var_pop" || func == "var_samp" || func == "variance");
        bool isStddev = (func == "stddev_pop" || func == "stddev_samp" || func == "stddev");
        bool isStat = isVar || isStddev;
        // Welford's online algorithm for variance
        double wMean = 0.0, wM2 = 0.0;
        size_t wCount = 0;

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

        // Parse FILTER (WHERE ...) conditions if present
        auto filterConds = parseConditions(item.filterConds);

        if (isDistinctCount) {
            std::set<std::string> distinctVals;
            for (int64_t rid : matchIds) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                // Check FILTER condition
                if (!filterConds.empty()) {
                    bool pass = true;
                    for (const auto& fc : filterConds) {
                        if (!evalConditionOnRow(fc, row, tbl)) { pass = false; break; }
                    }
                    if (!pass) continue;
                }
                if (colIdx >= tbl.len) continue;
                std::string val = extractColumnValue(row, tbl, colIdx);
                if (!val.empty()) distinctVals.insert(val);
            }
            count = static_cast<int64_t>(distinctVals.size());
        } else {
            for (int64_t rid : matchIds) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                // Check FILTER condition
                if (!filterConds.empty()) {
                    bool pass = true;
                    for (const auto& fc : filterConds) {
                        if (!evalConditionOnRow(fc, row, tbl)) { pass = false; break; }
                    }
                    if (!pass) continue;
                }
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
                } else if (isArrayAgg) {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    arrayAggVals.push_back(val);
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
                    if (isStat) {
                        wCount++;
                        double delta = static_cast<double>(num) - wMean;
                        wMean += delta / wCount;
                        wM2 += delta * (static_cast<double>(num) - wMean);
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
        else if (isArrayAgg) {
            std::string arr = "{";
            bool first = true;
            for (const auto& v : arrayAggVals) {
                if (!first) arr += ",";
                arr += v;
                first = false;
            }
            arr += "}";
            rowResult += arr + ' ';
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
        else if (isVar || isStddev) {
            if (wCount == 0) {
                rowResult += "NULL ";
            } else if ((func == "var_samp" || func == "stddev_samp" || func == "variance" || func == "stddev") && wCount < 2) {
                rowResult += "NULL ";
            } else {
                double variance = 0.0;
                if (func == "var_pop" || func == "stddev_pop") {
                    variance = wM2 / wCount;
                } else {
                    variance = wM2 / (wCount - 1);
                }
                if (isStddev) variance = std::sqrt(variance);
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(4) << variance;
                rowResult += oss.str() + ' ';
            }
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
    const std::vector<AggItem>& items,
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
                           const std::string& func, const std::string& colName,
                           const std::vector<std::string>& filterConds = {}) -> std::string {
        bool isDistinctCount = (func == "count" && colName.size() > 9 && colName.substr(0, 9) == "distinct ");
        std::string actualColName = isDistinctCount ? colName.substr(9) : colName;
        bool isJsonAgg = (func == "json_agg" || func == "jsonb_agg");
        bool isArrayAgg = (func == "array_agg");
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
        std::vector<std::string> arrayAggVals;
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
            // Parse FILTER conditions
            auto parsedFilters = parseConditions(filterConds);
            for (int64_t rid : gids) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                // Check FILTER condition
                if (!parsedFilters.empty()) {
                    bool pass = true;
                    for (const auto& fc : parsedFilters) {
                        if (!evalConditionOnRow(fc, row, tbl)) { pass = false; break; }
                    }
                    if (!pass) continue;
                }

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
                } else if (isArrayAgg) {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    arrayAggVals.push_back(val);
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
        if (isArrayAgg) {
            std::string arr = "{";
            bool first = true;
            for (const auto& v : arrayAggVals) {
                if (!first) arr += ",";
                arr += v;
                first = false;
            }
            arr += "}";
            return arr;
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
        std::string aggVal = computeAgg(gids, h.func, h.colName, {});
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
            row += computeAgg(gids, item.func, item.arg, item.filterConds) + ' ';
        }
        result.push_back(row);
    }
    lockManager_.unlock(tablename);
    return result;
}

// ========================================================================
// Grouping Sets / ROLLUP / CUBE aggregate
// ========================================================================

std::vector<std::string> StorageEngine::groupAggregateSets(
    const std::string& dbname, const std::string& tablename,
    const std::vector<std::string>& conditions,
    const std::vector<AggItem>& items,
    const std::vector<std::string>& allGroupByCols,
    const std::vector<std::vector<std::string>>& groupingSets,
    const std::vector<std::string>& havingConds) {
    std::vector<std::string> result;
    if (!tableExists(dbname, tablename)) return result;
    lockManager_.lockShared(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);

    // Map all GROUP BY columns to indices
    std::vector<size_t> allGroupIdxs;
    for (const auto& gcol : allGroupByCols) {
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == gcol) { allGroupIdxs.push_back(i); break; }
        }
    }
    if (allGroupIdxs.size() != allGroupByCols.size()) {
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

    // Reuse computeAgg lambda from groupAggregate
    auto computeAgg = [&](const std::vector<int64_t>& gids,
                           const std::string& func, const std::string& colName,
                           const std::vector<std::string>& filterConds = {}) -> std::string {
        bool isDistinctCount = (func == "count" && colName.size() > 9 && colName.substr(0, 9) == "distinct ");
        std::string actualColName = isDistinctCount ? colName.substr(9) : colName;
        bool isJsonAgg = (func == "json_agg" || func == "jsonb_agg");
        bool isArrayAgg = (func == "array_agg");
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
        std::vector<std::string> arrayAggVals;

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
            auto parsedFilters = parseConditions(filterConds);
            for (int64_t rid : gids) {
                std::string row;
                if (!readRowByRid(pa, rid, row, tbl)) continue;
                if (!parsedFilters.empty()) {
                    bool pass = true;
                    for (const auto& fc : parsedFilters) {
                        if (!evalConditionOnRow(fc, row, tbl)) { pass = false; break; }
                    }
                    if (!pass) continue;
                }
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
                } else if (isArrayAgg) {
                    if (colIdx >= tbl.len) continue;
                    std::string val = extractColumnValue(row, tbl, colIdx);
                    arrayAggVals.push_back(val);
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
                    for (char c : v) { if (c == '"' || c == '\\') esc += '\\'; esc += c; }
                    json += '"' + esc + '"';
                }
                first = false;
            }
            json += "]";
            return json;
        }
        if (isArrayAgg) {
            std::string arr = "{";
            bool first = true;
            for (const auto& v : arrayAggVals) { if (!first) arr += ","; arr += v; first = false; }
            arr += "}";
            return arr;
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

    struct HavingCond {
        std::string func, colName, op, value;
    };
    std::vector<HavingCond> havings;
    for (const auto& hc : havingConds) {
        if (hc.empty()) continue;
        std::string s = hc;
        size_t lp = s.find('(');
        size_t rp = s.find(')');
        if (lp != std::string::npos && rp != std::string::npos && rp > lp + 1) {
            HavingCond h;
            h.func = s.substr(0, lp);
            h.colName = s.substr(lp + 1, rp - lp - 1);
            std::string rest = trim(s.substr(rp + 1));
            size_t opEnd = 0;
            while (opEnd < rest.size() && (rest[opEnd] == '<' || rest[opEnd] == '>' || rest[opEnd] == '=' || rest[opEnd] == '!')) ++opEnd;
            if (opEnd > 0) {
                h.op = rest.substr(0, opEnd);
                h.value = trim(rest.substr(opEnd));
                havings.push_back(h);
            }
        }
    }
    auto evalHaving = [&](const HavingCond& h, const std::vector<int64_t>& gids) -> bool {
        std::string aggVal = computeAgg(gids, h.func, h.colName, {});
        if (h.op == "=") return aggVal == h.value;
        if (h.op == "!=") return aggVal != h.value;
        double a = 0, v = 0;
        try { a = std::stod(aggVal); } catch (...) {}
        try { v = std::stod(h.value); } catch (...) {}
        if (h.op == ">") return a > v;
        if (h.op == "<") return a < v;
        if (h.op == ">=") return a >= v;
        if (h.op == "<=") return a <= v;
        return true;
    };

    // Process each grouping set
    for (const auto& gset : groupingSets) {
        std::vector<size_t> setIdxs;
        for (const auto& col : gset) {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == col) { setIdxs.push_back(i); break; }
            }
        }

        auto readSetKey = [&](int64_t rid) -> std::string {
            std::string row;
            if (!readRowByRid(pa, rid, row, tbl)) return "";
            std::string key;
            for (size_t idx : setIdxs) {
                if (!key.empty()) key += "\x01";
                key += extractColumnValue(row, tbl, idx);
            }
            return key;
        };

        std::map<std::string, std::vector<int64_t>> groups;
        for (int64_t rowIdx : matchIds) {
            groups[readSetKey(rowIdx)].push_back(rowIdx);
        }

        for (const auto& kv : groups) {
            const auto& gids = kv.second;
            bool pass = true;
            for (const auto& h : havings) {
                if (!evalHaving(h, gids)) { pass = false; break; }
            }
            if (!pass) continue;

            // Build a map from column name to its value in this group key
            std::map<std::string, std::string> colValues;
            const std::string& gkey = kv.first;
            size_t p = 0;
            size_t partIdx = 0;
            while (p < gkey.size()) {
                size_t sep = gkey.find('\x01', p);
                std::string part = (sep == std::string::npos) ? gkey.substr(p) : gkey.substr(p, sep - p);
                if (partIdx < gset.size()) colValues[gset[partIdx]] = part;
                if (sep == std::string::npos) break;
                p = sep + 1;
                ++partIdx;
            }

            std::string row;
            for (const auto& col : allGroupByCols) {
                if (!row.empty()) row += ' ';
                auto it = colValues.find(col);
                if (it != colValues.end()) row += it->second;
            }
            row += ' ';
            for (const auto& item : items) {
                row += computeAgg(gids, item.func, item.arg, item.filterConds) + ' ';
            }
            result.push_back(row);
        }
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
            } else if (spec.exprFunc == "add") {
                try {
                    int64_t a = std::stoll(argVal);
                    int64_t b = std::stoll(spec.exprArg2);
                    ev = std::to_string(a + b);
                } catch (...) { ev = argVal; }
            } else if (spec.exprFunc == "sub") {
                try {
                    int64_t a = std::stoll(argVal);
                    int64_t b = std::stoll(spec.exprArg2);
                    ev = std::to_string(a - b);
                } catch (...) { ev = argVal; }
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
            if (l.size() >= 6 && l.substr(0, 6) == "COMMIT") hasCommit = true;
            if (l.size() >= 8 && l.substr(0, 8) == "ROLLBACK") hasRollback = true;
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
        // After any recovery action, truncate UNLOGGED tables (PG semantics)
        try {
            auto tnames = getTableNames(dbname);
            for (const auto& tn : tnames) {
                TableSchema ts = getTableSchema(dbname, tn);
                if (ts.isUnlogged) {
                    std::filesystem::path dtPath = dbPath(dbname) / (tn + ".dt");
                    std::filesystem::path idxPath = dbPath(dbname) / (tn + ".idx");
                    if (std::filesystem::exists(dtPath)) std::filesystem::remove(dtPath);
                    if (std::filesystem::exists(idxPath)) std::filesystem::remove(idxPath);
                    // Recreate empty data file
                    std::ofstream(dtPath, std::ios::binary).close();
                }
            }
        } catch (...) {}
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
                             const std::string& tablename,
                             bool concurrent) {
    if (!databaseExists(dbname) || !tableExists(dbname, tablename)) return 0;
    if (concurrent) {
        lockManager_.lockShared(tablename);
    } else {
        lockManager_.lockExclusive(tablename);
    }

    TableSchema tbl = getTableSchema(dbname, tablename);
    PageAllocator* pa = getPageAllocator(dbname, tablename);
    if (!pa) { lockManager_.unlock(tablename); return 0; }

    uint32_t np = pa->numPages();
    size_t freedPages = 0;

    for (uint32_t pid = 1; pid < np; ++pid) {
        char* buf = pa->fetchPage(pid);
        if (!buf) continue;
        PageWrapper page(buf, pa->pageSize(), tbl.formatVersion);

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

        // Update FSM with reclaimed free space
        size_t freePct = page.freeSpace() * 100 / pa->pageSize();
        getFSM(dbname, tablename)->setFreePercent(pid, static_cast<uint8_t>(freePct));

        // Update VM: if page is fully live after compact, mark AllVisible
        VisibilityMap* vm = getVM(dbname, tablename);
        if (page.liveCount() > 0 && page.liveCount() == page.slotCount()) {
            vm->setAllVisible(pid, true);
        } else {
            vm->setAllVisible(pid, false);
        }

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

    // Clean up orphaned TOAST entries
    vacuumToast(dbname, tablename);

    return freedPages;
}

// ========================================================================
// VACUUM TOAST: remove orphaned toast files not referenced by any row
// ========================================================================
size_t StorageEngine::vacuumToast(const std::string& dbname,
                                   const std::string& tablename) {
    if (!databaseExists(dbname) || !tableExists(dbname, tablename)) return 0;

    TableSchema tbl = getTableSchema(dbname, tablename);
    std::set<uint64_t> activeToastIds;

    // Collect all toast IDs referenced by live rows
    forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
        std::string row(data, len);
        for (size_t i = 0; i < tbl.len; ++i) {
            if (!tbl.cols[i].isVariableLength) continue;
            std::string val = extractColumnValue(row, tbl, i);
            uint64_t toastId = 0;
            if (parseToastMarker(val, toastId)) {
                activeToastIds.insert(toastId);
            }
        }
    });

    // Scan toast directory and remove orphaned files
    auto tdir = toastDir(dbname, tablename);
    if (!std::filesystem::exists(tdir)) return 0;

    size_t removed = 0;
    for (const auto& entry : std::filesystem::directory_iterator(tdir)) {
        std::string fname = entry.path().filename().string();
        if (fname.size() < 5 || fname.substr(fname.size() - 4) != ".dat") continue;
        try {
            uint64_t toastId = static_cast<uint64_t>(std::stoull(fname.substr(0, fname.size() - 4)));
            if (activeToastIds.find(toastId) == activeToastIds.end()) {
                std::filesystem::remove(entry.path());
                ++removed;
            }
        } catch (...) {
            // skip non-numeric filenames
        }
    }
    return removed;
}

// ========================================================================
// VACUUM FULL: completely rewrite table, reclaiming all dead space
// ========================================================================
size_t StorageEngine::vacuumFull(const std::string& dbname,
                                 const std::string& tablename) {
    if (!databaseExists(dbname) || !tableExists(dbname, tablename)) return 0;
    lockManager_.lockExclusive(tablename);

    TableSchema tbl = getTableSchema(dbname, tablename);
    std::vector<std::map<std::string, std::string>> rows;

    // Collect all live rows
    forEachRow(dbname, tablename, [&](uint32_t, uint16_t, const char* data, size_t len) {
        std::string row(data, len);
        std::map<std::string, std::string> values;
        for (size_t i = 0; i < tbl.len; ++i) {
            values[tbl.cols[i].dataName] = extractColumnValue(row, tbl, i);
        }
        rows.push_back(std::move(values));
    });

    std::string key = dbname + "/" + tablename;
    // Evict caches so new files will be created
    pageAllocators_.erase(key);
    pkIndexCache_.erase(key);
    secondaryIndexCache_.erase(key);
    hashIndexCache_.erase(key);

    // Remove old data file
    std::filesystem::remove(dataPath(dbname, tablename));
    // Remove all index files for this table
    std::filesystem::remove(indexPath(dbname, tablename)); // PK index .idx
    auto indexedCols = getIndexedColumns(dbname, tablename);
    for (const auto& colname : indexedCols) {
        std::filesystem::remove(secondaryIndexPath(dbname, tablename, colname));
        std::filesystem::remove(hashIndexPath(dbname, tablename, colname));
    }
    // Remove any named secondary index meta files
    std::filesystem::path dbDir = dbPath(dbname);
    for (const auto& entry : std::filesystem::directory_iterator(dbDir)) {
        std::string fname = entry.path().filename().string();
        if (fname.size() > tablename.size() + 1 &&
            fname.substr(0, tablename.size() + 1) == tablename + "_") {
            auto endsWith = [](const std::string& s, const std::string& suffix) {
                return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
            };
            if (endsWith(fname, ".idx") || endsWith(fname, ".hidx") ||
                endsWith(fname, ".fti") || endsWith(fname, ".secidx") ||
                endsWith(fname, ".hashidx")) {
                std::filesystem::remove(entry.path());
            }
        }
    }

    // Re-create empty data file via getPageAllocator (lazy creation)
    {
        auto pa = std::make_unique<PageAllocator>(dataPath(dbname, tablename).string(), tbl.rowSize(), pageSizeForFormatVersion(tbl.formatVersion), tbl.formatVersion);
        pa->open();
        pageAllocators_[key] = std::move(pa);
    }

    // Release exclusive lock before re-inserting (insert acquires its own lock)
    lockManager_.unlock(tablename);

    // Re-insert all rows (this rebuilds PK and secondary indexes)
    for (const auto& values : rows) {
        insert(dbname, tablename, values);
    }

    // Reset dead tuple count
    auto dtKey = std::make_pair(dbname, tablename);
    std::lock_guard<std::mutex> lock(deadTupleMutex_);
    deadTupleCounts_[dtKey] = 0;

    // Clean up orphaned TOAST entries
    vacuumToast(dbname, tablename);

    return rows.size();
}

// ========================================================================
// Transaction logging helpers
// ========================================================================

void StorageEngine::logTxnInsert(const std::string& tableName, int64_t rowIdx) {
    txnLog_.push_back({TxnLogEntry::Op::Insert, tableName, rowIdx, ""});
    if (txnIsolationLevel_ == IsolationLevel::Serializable) {
        txnWrittenRids_.insert(rowIdx);
        std::lock_guard<std::mutex> lock(ssiMutex_);
        ssiWriteSets_[currentTxnId_].insert(rowIdx);
    }
}

void StorageEngine::logTxnUpdate(const std::string& tableName, int64_t rowIdx,
                                  const std::string& oldRowData) {
    txnLog_.push_back({TxnLogEntry::Op::Update, tableName, rowIdx, oldRowData});
    if (txnIsolationLevel_ == IsolationLevel::Serializable) {
        txnWrittenRids_.insert(rowIdx);
        std::lock_guard<std::mutex> lock(ssiMutex_);
        ssiWriteSets_[currentTxnId_].insert(rowIdx);
    }
}

void StorageEngine::logTxnDelete(const std::string& tableName, int64_t rowIdx,
                                  const std::string& oldRowData) {
    txnLog_.push_back({TxnLogEntry::Op::Delete, tableName, rowIdx, oldRowData});
    if (txnIsolationLevel_ == IsolationLevel::Serializable) {
        txnWrittenRids_.insert(rowIdx);
        std::lock_guard<std::mutex> lock(ssiMutex_);
        ssiWriteSets_[currentTxnId_].insert(rowIdx);
    }
}

// ========================================================================
// Transaction support (Undo Log based rollback, no full-db snapshot)
// ========================================================================

OpResult StorageEngine::beginTransaction(const std::string& dbname) {
    if (inTransaction_) return OpResult::Success;  // already in txn
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;

    // Flush all dirty pages so the on-disk files are up-to-date before backup
    auto tblNames = getTableNames(dbname);
    for (const auto& tn : tblNames) {
        PageAllocator* pa = getPageAllocator(dbname, tn);
        if (pa) pa->flush();
    }

    // Keep a backup for crash recovery (recoverAllDatabases)
    // Skip data/index files of UNLOGGED tables (they are truncated on crash)
    std::set<std::string> unloggedFiles;
    for (const auto& tn : tblNames) {
        TableSchema ts = getTableSchema(dbname, tn);
        if (ts.isUnlogged) {
            unloggedFiles.insert(tn + ".dt");
            unloggedFiles.insert(tn + ".idx");
        }
    }
    std::filesystem::path backup = dbPath(dbname);
    backup += ".txn_backup";
    if (std::filesystem::exists(backup)) {
        std::filesystem::remove_all(backup);
    }
    std::filesystem::create_directories(backup);
    for (const auto& entry : std::filesystem::directory_iterator(dbPath(dbname))) {
        if (unloggedFiles.count(entry.path().filename().string())) continue;
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
    // Initialize SSI tracking
    {
        std::lock_guard<std::mutex> lock(ssiMutex_);
        ssiReadSets_[currentTxnId_].clear();
        ssiWriteSets_[currentTxnId_].clear();
    }
    txnReadRids_.clear();
    txnWrittenRids_.clear();
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

    // SSI conflict detection for Serializable isolation
    if (txnIsolationLevel_ == IsolationLevel::Serializable) {
        std::lock_guard<std::mutex> lock(ssiMutex_);
        bool hasOutgoing = false, hasIncoming = false;

        for (const auto& [otherTxId, otherWriteSet] : ssiWriteSets_) {
            if (otherTxId == currentTxnId_) continue;
            for (int64_t rid : txnReadRids_) {
                if (otherWriteSet.count(rid)) {
                    hasOutgoing = true;
                    ssiOutEdges_[currentTxnId_].insert(otherTxId);
                    ssiInEdges_[otherTxId].insert(currentTxnId_);
                    break;
                }
            }
            if (hasOutgoing) break;
        }

        for (const auto& [otherTxId, otherReadSet] : ssiReadSets_) {
            if (otherTxId == currentTxnId_) continue;
            for (int64_t rid : txnWrittenRids_) {
                if (otherReadSet.count(rid)) {
                    hasIncoming = true;
                    ssiInEdges_[currentTxnId_].insert(otherTxId);
                    ssiOutEdges_[otherTxId].insert(currentTxnId_);
                    break;
                }
            }
            if (hasIncoming) break;
        }

        if (hasOutgoing && hasIncoming) {
            // Dangerous structure detected - must abort
            uint64_t abortedId = currentTxnId_;
            ssiReadSets_.erase(abortedId);
            ssiWriteSets_.erase(abortedId);
            ssiOutEdges_.erase(abortedId);
            for (auto& [k, v] : ssiInEdges_) v.erase(abortedId);
            ssiInEdges_.erase(abortedId);
            for (auto& [k, v] : ssiOutEdges_) v.erase(abortedId);
            txnReadRids_.clear();
            txnWrittenRids_.clear();
            rollbackTransaction();
            return OpResult::SerializationFailure;
        }

        // No conflict - clean up SSI data for committed transaction
        ssiReadSets_.erase(currentTxnId_);
        ssiWriteSets_.erase(currentTxnId_);
        ssiOutEdges_.erase(currentTxnId_);
        for (auto& [k, v] : ssiInEdges_) v.erase(currentTxnId_);
        ssiInEdges_.erase(currentTxnId_);
        for (auto& [k, v] : ssiOutEdges_) v.erase(currentTxnId_);
        txnReadRids_.clear();
        txnWrittenRids_.clear();
    }

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
                PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
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
            std::string currentRow;
            bool foundCurrent = false;
            if (pageBuf) {
                PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
                // Read current row data before restoring (needed to remove stale index entries)
                const char* currentData = nullptr;
                size_t currentLen = 0;
                bool got = page.read(slotId, currentData, currentLen);
                if (got && currentLen >= MVCC_HEADER_SIZE) {
                    currentRow.assign(currentData + MVCC_HEADER_SIZE, currentLen - MVCC_HEADER_SIZE);
                    foundCurrent = true;
                } else if (!got) {
                    std::string targetPk = extractPKValue(it->rowData, tbl);
                    page.forEachLive([&](uint16_t sid, const char* data, size_t len) {
                        if (foundCurrent || len <= MVCC_HEADER_SIZE) return;
                        std::string rowData(data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                        std::string pk = extractPKValue(rowData, tbl);
                        if (pk == targetPk) {
                            currentRow = rowData;
                            foundCurrent = true;
                        }
                    });
                }
                // Restore old row data
                if (got && currentLen >= MVCC_HEADER_SIZE) {
                    std::string fullRow;
                    fullRow.append(currentData, MVCC_HEADER_SIZE);  // keep MVCC header
                    fullRow.append(it->rowData);                    // restore old data
                    page.update(slotId, fullRow.data(), fullRow.size());
                } else if (!got) {
                    std::string targetPk = extractPKValue(it->rowData, tbl);
                    bool restored = false;
                    page.forEachLive([&](uint16_t sid, const char* data, size_t len) {
                        if (restored || len <= MVCC_HEADER_SIZE) return;
                        std::string rowData(data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                        std::string pk = extractPKValue(rowData, tbl);
                        if (pk == targetPk) {
                            std::string fullRow;
                            fullRow.append(data, MVCC_HEADER_SIZE);
                            fullRow.append(it->rowData);
                            page.update(sid, fullRow.data(), fullRow.size());
                            restored = true;
                        }
                    });
                }
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            // Rebuild indexes: remove current (stale) values then insert restored values
            BPTree* pkIdx = getPKIndex(txnDB_, it->tableName);
            if (pkIdx) {
                if (foundCurrent) {
                    std::string currentPk = extractPKValue(currentRow, tbl);
                    if (!currentPk.empty()) pkIdx->remove(currentPk);
                }
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
                if (foundCurrent) {
                    std::string currentVal = extractColumnValue(currentRow, tbl, colIdx);
                    if (!currentVal.empty()) secIdx->remove(currentVal);
                }
                std::string val = extractColumnValue(it->rowData, tbl, colIdx);
                if (!val.empty()) secIdx->insertMulti(val, it->rowIdx);
            }
        } else if (it->op == TxnLogEntry::Op::Delete) {
            // Undo DELETE: restore the row by clearing tombstone and writing back old data
            uint32_t pageId; uint16_t slotId;
            decodeRid(it->rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
                page.restore(slotId);
                // The slot may have been reused by a later INSERT in the same txn.
                // Overwrite with the logged old data (preserving any MVCC header).
                const char* currentData = nullptr;
                size_t currentLen = 0;
                if (page.read(slotId, currentData, currentLen) && currentLen >= MVCC_HEADER_SIZE) {
                    std::string fullRow;
                    fullRow.append(currentData, MVCC_HEADER_SIZE);
                    fullRow.append(it->rowData);
                    page.update(slotId, fullRow.data(), fullRow.size());
                } else {
                    page.update(slotId, it->rowData.data(), it->rowData.size());
                }
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

    // Clean up SSI data for aborted transaction
    {
        std::lock_guard<std::mutex> lock(ssiMutex_);
        ssiReadSets_.erase(currentTxnId_);
        ssiWriteSets_.erase(currentTxnId_);
        ssiOutEdges_.erase(currentTxnId_);
        for (auto& [k, v] : ssiInEdges_) v.erase(currentTxnId_);
        ssiInEdges_.erase(currentTxnId_);
        for (auto& [k, v] : ssiOutEdges_) v.erase(currentTxnId_);
    }
    txnReadRids_.clear();
    txnWrittenRids_.clear();

    currentTxnId_ = 0;
    inTransaction_ = false;
    readOnly_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

// ========================================================================
// Two-phase commit support
// ========================================================================

static std::filesystem::path preparedDir() {
    return std::filesystem::path("info") / ".prepared";
}

static std::filesystem::path preparedPath(const std::string& xid) {
    return preparedDir() / xid;
}

OpResult StorageEngine::prepareTransaction(const std::string& xid) {
    if (!inTransaction_) return OpResult::InvalidValue;
    if (xid.empty()) return OpResult::InvalidValue;

    std::filesystem::path pdir = preparedDir();
    if (!std::filesystem::exists(pdir)) {
        std::filesystem::create_directories(pdir);
    }
    std::filesystem::path pfile = preparedPath(xid);
    if (std::filesystem::exists(pfile)) return OpResult::DuplicateKey;

    std::ofstream ofs(pfile, std::ios::binary);
    if (!ofs) return OpResult::InvalidValue;

    // Transaction metadata
    ofs << "TXN_ID " << currentTxnId_ << "\n";
    ofs << "DBNAME " << txnDB_ << "\n";
    ofs << "ISOLATION " << static_cast<int>(txnIsolationLevel_) << "\n";
    ofs << "READONLY " << (readOnly_ ? 1 : 0) << "\n";

    // Held locks (resource mode)
    auto locks = lockManager_.getLockHolds();
    for (const auto& lk : locks) {
        ofs << "LOCK " << lk.resource << " " << lk.mode << "\n";
    }

    // Transaction log
    for (const auto& entry : txnLog_) {
        ofs << "LOG ";
        if (entry.op == TxnLogEntry::Op::Insert) ofs << "INSERT";
        else if (entry.op == TxnLogEntry::Op::Update) ofs << "UPDATE";
        else if (entry.op == TxnLogEntry::Op::Delete) ofs << "DELETE";
        ofs << " " << entry.tableName << " " << entry.rowIdx << " ";
        for (unsigned char c : entry.rowData) {
            char buf[3];
            snprintf(buf, sizeof(buf), "%02x", c);
            ofs << buf;
        }
        ofs << "\n";
    }
    ofs.close();

    // WAL PREPARE marker
    walAppend(walPath(txnDB_), "PREPARE " + std::to_string(currentTxnId_) + " " + xid);

    // Remove from active set (prepared txns are not active for ReadView)
    {
        std::lock_guard<std::mutex> lock(globalTxnMutex_);
        activeTransactions_.erase(currentTxnId_);
    }

    // Clear transaction state but KEEP locks (2PC semantics)
    txnLog_.clear();
    savepoints_.clear();
    walClear(walPath(txnDB_));

    {
        std::lock_guard<std::mutex> lock(ssiMutex_);
        ssiReadSets_.erase(currentTxnId_);
        ssiWriteSets_.erase(currentTxnId_);
        ssiOutEdges_.erase(currentTxnId_);
        for (auto& [k, v] : ssiInEdges_) v.erase(currentTxnId_);
        ssiInEdges_.erase(currentTxnId_);
        for (auto& [k, v] : ssiOutEdges_) v.erase(currentTxnId_);
    }
    txnReadRids_.clear();
    txnWrittenRids_.clear();

    currentTxnId_ = 0;
    inTransaction_ = false;
    readOnly_ = false;
    txnDB_.clear();
    return OpResult::Success;
}

OpResult StorageEngine::commitPrepared(const std::string& xid) {
    std::filesystem::path pfile = preparedPath(xid);
    if (!std::filesystem::exists(pfile)) return OpResult::TableNotExist;

    uint64_t savedTxnId = 0;
    std::string savedDB;
    {
        std::ifstream ifs(pfile, std::ios::binary);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.substr(0, 6) == "TXN_ID") savedTxnId = std::stoull(line.substr(7));
            else if (line.substr(0, 6) == "DBNAME") savedDB = line.substr(7);
        }
    }
    if (savedTxnId == 0 || savedDB.empty()) return OpResult::InvalidValue;

    // WAL COMMIT PREPARED marker
    walAppend(walPath(savedDB), "COMMIT PREPARED " + std::to_string(savedTxnId) + " " + xid);

    // Normal commit logic
    TxnIdGenerator::instance().notifyCommit(savedTxnId);
    {
        std::lock_guard<std::mutex> lock(globalTxnMutex_);
        activeTransactions_.erase(savedTxnId);
    }

    txnLog_.clear();
    savepoints_.clear();
    walClear(walPath(savedDB));
    lockManager_.unlockAll();
    lockManager_.unlockAllGaps();
    currentTxnId_ = 0;
    inTransaction_ = false;
    readOnly_ = false;
    txnDB_.clear();

    std::filesystem::remove(pfile);
    return OpResult::Success;
}

OpResult StorageEngine::rollbackPrepared(const std::string& xid) {
    std::filesystem::path pfile = preparedPath(xid);
    if (!std::filesystem::exists(pfile)) return OpResult::TableNotExist;

    uint64_t savedTxnId = 0;
    std::string savedDB;
    IsolationLevel savedIso = IsolationLevel::RepeatableRead;
    std::vector<TxnLogEntry> savedLog;

    {
        std::ifstream ifs(pfile, std::ios::binary);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.substr(0, 6) == "TXN_ID") {
                savedTxnId = std::stoull(line.substr(7));
            } else if (line.substr(0, 6) == "DBNAME") {
                savedDB = line.substr(7);
            } else if (line.substr(0, 9) == "ISOLATION") {
                savedIso = static_cast<IsolationLevel>(std::stoi(line.substr(10)));
            } else if (line.substr(0, 3) == "LOG") {
                size_t pos = 4;
                size_t sp = line.find(' ', pos);
                std::string opStr = line.substr(pos, sp - pos);
                pos = sp + 1;
                sp = line.find(' ', pos);
                std::string tbl = line.substr(pos, sp - pos);
                pos = sp + 1;
                sp = line.find(' ', pos);
                int64_t rid = std::stoll(line.substr(pos, sp - pos));
                pos = sp + 1;
                std::string hexData = line.substr(pos);
                std::string rowData;
                for (size_t i = 0; i + 1 < hexData.size(); i += 2) {
                    rowData.push_back(static_cast<char>(std::stoi(hexData.substr(i, 2), nullptr, 16)));
                }
                TxnLogEntry entry;
                if (opStr == "INSERT") entry.op = TxnLogEntry::Op::Insert;
                else if (opStr == "UPDATE") entry.op = TxnLogEntry::Op::Update;
                else if (opStr == "DELETE") entry.op = TxnLogEntry::Op::Delete;
                entry.tableName = tbl;
                entry.rowIdx = rid;
                entry.rowData = rowData;
                savedLog.push_back(entry);
            }
        }
    }

    if (savedTxnId == 0 || savedDB.empty()) return OpResult::InvalidValue;

    // Temporarily restore state and use normal rollback
    currentTxnId_ = savedTxnId;
    txnDB_ = savedDB;
    txnIsolationLevel_ = savedIso;
    inTransaction_ = true;
    txnLog_ = std::move(savedLog);

    OpResult res = rollbackTransaction();

    // WAL ROLLBACK PREPARED marker
    walAppend(walPath(savedDB), "ROLLBACK PREPARED " + std::to_string(savedTxnId) + " " + xid);

    std::filesystem::remove(pfile);
    return res;
}

std::vector<std::string> StorageEngine::listPreparedTransactions() const {
    std::vector<std::string> result;
    std::filesystem::path pdir = preparedDir();
    if (!std::filesystem::exists(pdir)) return result;
    for (const auto& entry : std::filesystem::directory_iterator(pdir)) {
        if (entry.is_regular_file()) {
            result.push_back(entry.path().filename().string());
        }
    }
    return result;
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
                PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
                page.remove(slotId);
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
        } else if (entry.op == TxnLogEntry::Op::Update) {
            uint32_t pageId; uint16_t slotId;
            decodeRid(entry.rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            std::string currentRow;
            bool foundCurrent = false;
            if (pageBuf) {
                PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
                // Read current row data before restoring (needed to remove stale index entries)
                const char* currentData = nullptr;
                size_t currentLen = 0;
                if (page.read(slotId, currentData, currentLen) && currentLen >= MVCC_HEADER_SIZE) {
                    currentRow.assign(currentData + MVCC_HEADER_SIZE, currentLen - MVCC_HEADER_SIZE);
                    foundCurrent = true;
                } else {
                    std::string targetPk = extractPKValue(entry.rowData, tbl);
                    page.forEachLive([&](uint16_t sid, const char* data, size_t len) {
                        if (foundCurrent || len <= MVCC_HEADER_SIZE) return;
                        std::string rowData(data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                        std::string pk = extractPKValue(rowData, tbl);
                        if (pk == targetPk) {
                            currentRow = rowData;
                            foundCurrent = true;
                        }
                    });
                }
                // Restore old row data
                if (page.read(slotId, currentData, currentLen) && currentLen >= MVCC_HEADER_SIZE) {
                    std::string fullRow;
                    fullRow.append(currentData, MVCC_HEADER_SIZE);
                    fullRow.append(entry.rowData);
                    page.update(slotId, fullRow.data(), fullRow.size());
                } else {
                    std::string targetPk = extractPKValue(entry.rowData, tbl);
                    bool restored = false;
                    page.forEachLive([&](uint16_t sid, const char* data, size_t len) {
                        if (restored || len <= MVCC_HEADER_SIZE) return;
                        std::string rowData(data + MVCC_HEADER_SIZE, len - MVCC_HEADER_SIZE);
                        std::string pk = extractPKValue(rowData, tbl);
                        if (pk == targetPk) {
                            std::string fullRow;
                            fullRow.append(data, MVCC_HEADER_SIZE);
                            fullRow.append(entry.rowData);
                            page.update(sid, fullRow.data(), fullRow.size());
                            restored = true;
                        }
                    });
                }
                pa->markDirty(pageId);
                pa->unpinPage(pageId);
            }
            // Rebuild indexes: remove current (stale) values then insert restored values
            BPTree* pkIdx = getPKIndex(txnDB_, entry.tableName);
            if (pkIdx) {
                if (foundCurrent) {
                    std::string currentPk = extractPKValue(currentRow, tbl);
                    if (!currentPk.empty()) pkIdx->remove(currentPk);
                }
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
                if (foundCurrent) {
                    std::string currentVal = extractColumnValue(currentRow, tbl, colIdx);
                    if (!currentVal.empty()) secIdx->remove(currentVal);
                }
                std::string val = extractColumnValue(entry.rowData, tbl, colIdx);
                if (!val.empty()) secIdx->insertMulti(val, entry.rowIdx);
            }
        } else if (entry.op == TxnLogEntry::Op::Delete) {
            uint32_t pageId; uint16_t slotId;
            decodeRid(entry.rowIdx, pageId, slotId);
            char* pageBuf = pa->fetchPage(pageId);
            if (pageBuf) {
                PageWrapper page(pageBuf, pa->pageSize(), tbl.formatVersion);
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
// Row-Level Security (RLS)
// ========================================================================

std::filesystem::path StorageEngine::rlsPath(const std::string& dbname, const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".rls");
}

OpResult StorageEngine::createPolicy(const std::string& dbname, const std::string& tablename,
                                      const RowPolicy& policy) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    auto rpath = rlsPath(dbname, tablename);
    auto policies = getPolicies(dbname, tablename);
    for (const auto& p : policies) {
        if (p.name == policy.name) return OpResult::TableAlreadyExist;
    }
    policies.push_back(policy);
    std::ofstream ofs(rpath);
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& p : policies) {
        ofs << "POLICY " << escapeString(p.name) << " " << p.cmd;
        ofs << " USING:" << escapeString(p.usingExpr);
        ofs << " WITHCHECK:" << escapeString(p.withCheckExpr);
        ofs << " ROLES:";
        for (size_t i = 0; i < p.roles.size(); ++i) {
            if (i > 0) ofs << ",";
            ofs << escapeString(p.roles[i]);
        }
        ofs << "\n";
    }
    return OpResult::Success;
}

OpResult StorageEngine::alterPolicy(const std::string& dbname, const std::string& tablename,
                                     const std::string& policyName, const RowPolicy& policy) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    auto rpath = rlsPath(dbname, tablename);
    auto policies = getPolicies(dbname, tablename);
    bool found = false;
    for (const auto& p : policies) {
        if (p.name == policy.name && policy.name != policyName) return OpResult::TableAlreadyExist;
    }
    for (auto& p : policies) {
        if (p.name == policyName) {
            p = policy;
            found = true;
            break;
        }
    }
    if (!found) return OpResult::TableNotExist;
    std::ofstream ofs(rpath);
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& p : policies) {
        ofs << "POLICY " << escapeString(p.name) << " " << p.cmd;
        ofs << " USING:" << escapeString(p.usingExpr);
        ofs << " WITHCHECK:" << escapeString(p.withCheckExpr);
        ofs << " ROLES:";
        for (size_t i = 0; i < p.roles.size(); ++i) {
            if (i > 0) ofs << ",";
            ofs << escapeString(p.roles[i]);
        }
        ofs << "\n";
    }
    return OpResult::Success;
}

OpResult StorageEngine::dropPolicy(const std::string& dbname, const std::string& tablename,
                                    const std::string& policyName) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    auto rpath = rlsPath(dbname, tablename);
    auto policies = getPolicies(dbname, tablename);
    bool found = false;
    for (auto it = policies.begin(); it != policies.end(); ) {
        if (it->name == policyName) {
            it = policies.erase(it);
            found = true;
        } else {
            ++it;
        }
    }
    if (!found) return OpResult::TableNotExist;
    std::ofstream ofs(rpath);
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& p : policies) {
        ofs << "POLICY " << escapeString(p.name) << " " << p.cmd;
        ofs << " USING:" << escapeString(p.usingExpr);
        ofs << " WITHCHECK:" << escapeString(p.withCheckExpr);
        ofs << " ROLES:";
        for (size_t i = 0; i < p.roles.size(); ++i) {
            if (i > 0) ofs << ",";
            ofs << escapeString(p.roles[i]);
        }
        ofs << "\n";
    }
    return OpResult::Success;
}

std::vector<StorageEngine::RowPolicy> StorageEngine::getPolicies(const std::string& dbname,
                                                                   const std::string& tablename) const {
    std::vector<RowPolicy> result;
    auto rpath = rlsPath(dbname, tablename);
    if (!std::filesystem::exists(rpath)) return result;
    std::ifstream ifs(rpath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.substr(0, 7) != "POLICY ") continue;
        size_t pos = 7;
        size_t sp = line.find(' ', pos);
        if (sp == std::string::npos) continue;
        RowPolicy p;
        p.name = unescapeString(line.substr(pos, sp - pos));
        pos = sp + 1;
        sp = line.find(" USING:", pos);
        if (sp == std::string::npos) continue;
        p.cmd = line.substr(pos, sp - pos);
        pos = sp + 7;
        sp = line.find(" WITHCHECK:", pos);
        if (sp == std::string::npos) continue;
        p.usingExpr = unescapeString(line.substr(pos, sp - pos));
        pos = sp + 11;
        sp = line.find(" ROLES:", pos);
        if (sp == std::string::npos) continue;
        p.withCheckExpr = unescapeString(line.substr(pos, sp - pos));
        pos = sp + 7;
        std::string rolesStr = line.substr(pos);
        if (!rolesStr.empty()) {
            std::stringstream ss(rolesStr);
            std::string role;
            while (std::getline(ss, role, ',')) {
                if (!role.empty()) p.roles.push_back(unescapeString(role));
            }
        }
        result.push_back(p);
    }
    return result;
}

std::vector<StorageEngine::RowPolicy> StorageEngine::getApplicablePolicies(
    const std::string& dbname, const std::string& tablename,
    const std::string& cmd, const std::string& username) const {
    auto all = getPolicies(dbname, tablename);
    std::vector<RowPolicy> result;
    for (const auto& p : all) {
        if (p.cmd != "ALL" && p.cmd != cmd) continue;
        if (p.roles.empty()) {
            result.push_back(p);
        } else {
            for (const auto& r : p.roles) {
                if (r == username) {
                    result.push_back(p);
                    break;
                }
            }
        }
    }
    return result;
}

OpResult StorageEngine::enableRowLevelSecurity(const std::string& dbname, const std::string& tablename,
                                                  bool force) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    tbl.rowLevelSecurity = true;
    tbl.forceRowLevelSecurity = force;
    std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
    writeSchema(out, tbl);
    return OpResult::Success;
}

OpResult StorageEngine::disableRowLevelSecurity(const std::string& dbname, const std::string& tablename) {
    if (!tableExists(dbname, tablename)) return OpResult::TableNotExist;
    TableSchema tbl = getTableSchema(dbname, tablename);
    tbl.rowLevelSecurity = false;
    tbl.forceRowLevelSecurity = false;
    std::ofstream out(schemaPath(dbname, tablename), std::ios::binary);
    writeSchema(out, tbl);
    return OpResult::Success;
}

// ========================================================================
// Table-level permissions
// ========================================================================

std::filesystem::path StorageEngine::permPath(const std::string& dbname) const {
    return dbPath(dbname) / ".permissions";
}

static std::filesystem::path grantChainPath(const std::filesystem::path& dbPath) {
    return dbPath / ".grant_chain";
}

std::filesystem::path StorageEngine::seqPath(const std::string& dbname, const std::string& tablename) const {
    return dbPath(dbname) / (tablename + ".seq");
}

std::filesystem::path StorageEngine::seclabelPath(const std::string& dbname) const {
    return dbPath(dbname) / ".security_labels";
}

void StorageEngine::setSecurityLabel(const std::string& dbname, const std::string& objType,
                                     const std::string& objName, const std::string& label) {
    auto spath = seclabelPath(dbname);
    std::map<std::string, std::string> labels; // key=objType|objName -> label
    if (std::filesystem::exists(spath)) {
        std::ifstream ifs(spath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            size_t p1 = line.find(' ');
            size_t p2 = line.find(' ', p1 + 1);
            if (p1 == std::string::npos || p2 == std::string::npos) continue;
            std::string key = line.substr(0, p1) + "|" + line.substr(p1 + 1, p2 - p1 - 1);
            labels[key] = line.substr(p2 + 1);
        }
    }
    std::string key = objType + "|" + objName;
    if (label.empty()) {
        labels.erase(key);
    } else {
        labels[key] = label;
    }
    std::ofstream ofs(spath);
    for (const auto& kv : labels) {
        size_t dp = kv.first.find('|');
        std::string ot = kv.first.substr(0, dp);
        std::string on = kv.first.substr(dp + 1);
        ofs << ot << " " << on << " " << kv.second << "\n";
    }
}

std::string StorageEngine::getSecurityLabel(const std::string& dbname, const std::string& objType,
                                            const std::string& objName) const {
    auto spath = seclabelPath(dbname);
    if (!std::filesystem::exists(spath)) return "";
    std::ifstream ifs(spath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string ot, on, lab;
        ss >> ot >> on;
        std::getline(ss, lab);
        if (!lab.empty() && lab[0] == ' ') lab = lab.substr(1);
        if (ot == objType && on == objName) return lab;
    }
    return "";
}

std::vector<std::tuple<std::string, std::string, std::string>> StorageEngine::getAllSecurityLabels(
    const std::string& dbname) const {
    std::vector<std::tuple<std::string, std::string, std::string>> result;
    auto spath = seclabelPath(dbname);
    if (!std::filesystem::exists(spath)) return result;
    std::ifstream ifs(spath);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string ot, on, lab;
        ss >> ot >> on;
        std::getline(ss, lab);
        if (!lab.empty() && lab[0] == ' ') lab = lab.substr(1);
        result.emplace_back(ot, on, lab);
    }
    return result;
}

// ========================================================================
// Default Privileges
// ========================================================================
static std::filesystem::path defaultPrivPath(const std::filesystem::path& dbPath) {
    return dbPath / ".default_privs";
}

void StorageEngine::addDefaultPrivilege(const std::string& dbname, const std::string& owner,
                                        const std::string& schema, const std::string& objType,
                                        const std::string& privilege, const std::string& grantee) {
    auto dpp = defaultPrivPath(dbPath(dbname));
    std::ofstream ofs(dpp, std::ios::app);
    ofs << owner << " " << schema << " " << objType << " " << privilege << " " << grantee << "\n";
}

void StorageEngine::removeDefaultPrivilege(const std::string& dbname, const std::string& owner,
                                           const std::string& schema, const std::string& objType,
                                           const std::string& privilege, const std::string& grantee) {
    auto dpp = defaultPrivPath(dbPath(dbname));
    if (!std::filesystem::exists(dpp)) return;
    std::vector<std::string> remaining;
    {
        std::ifstream ifs(dpp);
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string o, s, t, p, g;
            ss >> o >> s >> t >> p >> g;
            if (o == owner && s == schema && t == objType && p == privilege && g == grantee) continue;
            remaining.push_back(line);
        }
    }
    std::ofstream ofs(dpp);
    for (const auto& r : remaining) ofs << r << "\n";
}

void StorageEngine::applyDefaultPrivileges(const std::string& dbname, const std::string& schema,
                                           const std::string& objType, const std::string& objName,
                                           const std::string& owner) const {
    auto dpp = defaultPrivPath(dbPath(dbname));
    if (!std::filesystem::exists(dpp)) return;
    std::ifstream ifs(dpp);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string o, s, t, p, g;
        ss >> o >> s >> t >> p >> g;
        if (o != owner || s != schema || t != objType) continue;
        TablePrivilege priv;
        if (p == "select") priv = TablePrivilege::Select;
        else if (p == "insert") priv = TablePrivilege::Insert;
        else if (p == "update") priv = TablePrivilege::Update;
        else if (p == "delete") priv = TablePrivilege::Delete;
        else if (p == "all") priv = TablePrivilege::All;
        else if (p == "usage") priv = TablePrivilege::Usage;
        else if (p == "execute") priv = TablePrivilege::Execute;
        else continue;
        const_cast<StorageEngine*>(this)->grant(dbname, objName, g, priv, {}, false, owner);
    }
}

std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>>
StorageEngine::getDefaultPrivileges(const std::string& dbname) const {
    std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>> result;
    auto dpp = defaultPrivPath(dbPath(dbname));
    if (!std::filesystem::exists(dpp)) return result;
    std::ifstream ifs(dpp);
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string o, s, t, p, g;
        ss >> o >> s >> t >> p >> g;
        result.emplace_back(o, s, t, p, g);
    }
    return result;
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

void StorageEngine::resetSequence(const std::string& dbname, const std::string& tablename,
                                  const std::string& colname, int64_t val) {
    writeNextSeq(dbname, tablename, colname, val);
}

// ========================================================================
// Domain support
// ========================================================================
static std::filesystem::path domainPath(const std::string& dbname) {
    return std::filesystem::path(dbname) / ".domains";
}

OpResult StorageEngine::createDomain(const std::string& dbname, const DomainInfo& info) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = domainPath(dbname);
    auto existing = getDomain(dbname, info.name);
    if (!existing.name.empty()) return OpResult::TableAlreadyExist;
    std::ofstream ofs(path, std::ios::app);
    if (!ofs) return OpResult::InvalidValue;
    ofs << info.name << "|" << info.baseType << "|" << info.defaultValue << "|"
        << info.checkExpr << "|" << info.constraintName << "\n";
    return OpResult::Success;
}

OpResult StorageEngine::alterDomain(const std::string& dbname, const std::string& name,
                                    const DomainInfo& info) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = domainPath(dbname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    if (info.name != name && !getDomain(dbname, info.name).name.empty()) {
        return OpResult::TableAlreadyExist;
    }
    std::ifstream ifs(path);
    std::vector<DomainInfo> domains;
    std::string line;
    bool found = false;
    while (std::getline(ifs, line)) {
        size_t sp1 = line.find('|');
        if (sp1 == std::string::npos) continue;
        size_t sp2 = line.find('|', sp1 + 1);
        size_t sp3 = (sp2 == std::string::npos) ? std::string::npos : line.find('|', sp2 + 1);
        size_t sp4 = (sp3 == std::string::npos) ? std::string::npos : line.find('|', sp3 + 1);
        DomainInfo current;
        current.name = line.substr(0, sp1);
        current.baseType = (sp2 == std::string::npos) ? "" : line.substr(sp1 + 1, sp2 - sp1 - 1);
        current.defaultValue = (sp2 == std::string::npos)
            ? ""
            : ((sp3 == std::string::npos) ? line.substr(sp2 + 1) : line.substr(sp2 + 1, sp3 - sp2 - 1));
        current.checkExpr = (sp3 == std::string::npos)
            ? ""
            : ((sp4 == std::string::npos) ? line.substr(sp3 + 1) : line.substr(sp3 + 1, sp4 - sp3 - 1));
        if (sp4 != std::string::npos) current.constraintName = line.substr(sp4 + 1);
        if (current.name == name) {
            domains.push_back(info);
            found = true;
        } else {
            domains.push_back(current);
        }
    }
    if (!found) return OpResult::TableNotExist;
    std::ofstream ofs(path, std::ios::trunc);
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& d : domains) {
        ofs << d.name << "|" << d.baseType << "|" << d.defaultValue << "|"
            << d.checkExpr << "|" << d.constraintName << "\n";
    }
    return OpResult::Success;
}

OpResult StorageEngine::dropDomain(const std::string& dbname, const std::string& name) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = domainPath(dbname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::ifstream ifs(path);
    std::vector<std::string> lines;
    std::string line;
    bool found = false;
    while (std::getline(ifs, line)) {
        size_t sp = line.find('|');
        if (sp != std::string::npos && line.substr(0, sp) == name) {
            found = true;
        } else {
            lines.push_back(line);
        }
    }
    if (!found) return OpResult::TableNotExist;
    std::ofstream ofs(path, std::ios::trunc);
    for (const auto& l : lines) ofs << l << '\n';
    return OpResult::Success;
}

StorageEngine::DomainInfo StorageEngine::getDomain(const std::string& dbname, const std::string& name) const {
    DomainInfo result;
    auto path = domainPath(dbname);
    if (!std::filesystem::exists(path)) return result;
    std::ifstream ifs(path);
    std::string line;
    while (std::getline(ifs, line)) {
        size_t sp1 = line.find('|');
        if (sp1 == std::string::npos) continue;
        if (line.substr(0, sp1) != name) continue;
        size_t sp2 = line.find('|', sp1 + 1);
        size_t sp3 = (sp2 == std::string::npos) ? std::string::npos : line.find('|', sp2 + 1);
        size_t sp4 = (sp3 == std::string::npos) ? std::string::npos : line.find('|', sp3 + 1);
        result.name = name;
        result.baseType = (sp2 == std::string::npos) ? "" : line.substr(sp1 + 1, sp2 - sp1 - 1);
        result.defaultValue = (sp2 == std::string::npos)
            ? ""
            : ((sp3 == std::string::npos) ? line.substr(sp2 + 1) : line.substr(sp2 + 1, sp3 - sp2 - 1));
        result.checkExpr = (sp3 == std::string::npos)
            ? ""
            : ((sp4 == std::string::npos) ? line.substr(sp3 + 1) : line.substr(sp3 + 1, sp4 - sp3 - 1));
        if (sp4 != std::string::npos) result.constraintName = line.substr(sp4 + 1);
        break;
    }
    return result;
}

std::vector<std::string> StorageEngine::getDomainNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto path = domainPath(dbname);
    if (!std::filesystem::exists(path)) return result;
    std::ifstream ifs(path);
    std::string line;
    while (std::getline(ifs, line)) {
        size_t sp = line.find('|');
        if (sp != std::string::npos) result.push_back(line.substr(0, sp));
    }
    return result;
}

// ========================================================================
// Composite types (ROW types)
// ========================================================================
static std::filesystem::path compositeTypePath(const std::string& dbname) {
    return std::filesystem::path(dbname) / ".types";
}

OpResult StorageEngine::createCompositeType(const std::string& dbname, const CompositeType& ct) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = compositeTypePath(dbname);
    if (isCompositeType(dbname, ct.name)) return OpResult::TableAlreadyExist;
    std::ofstream ofs(path, std::ios::app);
    if (!ofs) return OpResult::InvalidValue;
    ofs << ct.name;
    for (const auto& f : ct.fields) {
        ofs << "|" << f.first << ":" << f.second;
    }
    ofs << '\n';
    return OpResult::Success;
}

OpResult StorageEngine::alterCompositeType(const std::string& dbname, const std::string& name,
                                           const CompositeType& ct) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = compositeTypePath(dbname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    if (ct.name != name && isCompositeType(dbname, ct.name)) return OpResult::TableAlreadyExist;
    std::ifstream ifs(path);
    std::vector<CompositeType> types;
    std::string line;
    bool found = false;
    while (std::getline(ifs, line)) {
        size_t sp = line.find('|');
        if (sp == std::string::npos) continue;
        CompositeType current;
        current.name = line.substr(0, sp);
        size_t pos = sp + 1;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            std::string fieldDef = (next == std::string::npos) ? line.substr(pos) : line.substr(pos, next - pos);
            size_t colon = fieldDef.find(':');
            if (colon != std::string::npos) {
                current.fields.emplace_back(fieldDef.substr(0, colon), fieldDef.substr(colon + 1));
            }
            pos = (next == std::string::npos) ? line.size() : next + 1;
        }
        if (current.name == name) {
            types.push_back(ct);
            found = true;
        } else {
            types.push_back(current);
        }
    }
    if (!found) return OpResult::TableNotExist;
    std::ofstream ofs(path, std::ios::trunc);
    if (!ofs) return OpResult::InvalidValue;
    for (const auto& t : types) {
        ofs << t.name;
        for (const auto& f : t.fields) ofs << "|" << f.first << ":" << f.second;
        ofs << '\n';
    }
    return OpResult::Success;
}

OpResult StorageEngine::dropCompositeType(const std::string& dbname, const std::string& name) {
    if (!databaseExists(dbname)) return OpResult::DatabaseNotExist;
    auto path = compositeTypePath(dbname);
    if (!std::filesystem::exists(path)) return OpResult::TableNotExist;
    std::ifstream ifs(path);
    std::vector<std::string> lines;
    std::string line;
    bool found = false;
    while (std::getline(ifs, line)) {
        size_t sp = line.find('|');
        if (sp != std::string::npos && line.substr(0, sp) == name) {
            found = true;
        } else {
            lines.push_back(line);
        }
    }
    if (!found) return OpResult::TableNotExist;
    std::ofstream ofs(path, std::ios::trunc);
    for (const auto& l : lines) ofs << l << '\n';
    return OpResult::Success;
}

StorageEngine::CompositeType StorageEngine::getCompositeType(const std::string& dbname, const std::string& name) const {
    CompositeType result;
    auto path = compositeTypePath(dbname);
    if (!std::filesystem::exists(path)) return result;
    std::ifstream ifs(path);
    std::string line;
    while (std::getline(ifs, line)) {
        size_t sp = line.find('|');
        if (sp == std::string::npos || line.substr(0, sp) != name) continue;
        result.name = name;
        size_t pos = sp + 1;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            std::string fieldDef = (next == std::string::npos) ? line.substr(pos) : line.substr(pos, next - pos);
            size_t colon = fieldDef.find(':');
            if (colon != std::string::npos) {
                result.fields.emplace_back(fieldDef.substr(0, colon), fieldDef.substr(colon + 1));
            }
            pos = (next == std::string::npos) ? line.size() : next + 1;
        }
        break;
    }
    return result;
}

std::vector<std::string> StorageEngine::getCompositeTypeNames(const std::string& dbname) const {
    std::vector<std::string> result;
    auto path = compositeTypePath(dbname);
    if (!std::filesystem::exists(path)) return result;
    std::ifstream ifs(path);
    std::string line;
    while (std::getline(ifs, line)) {
        size_t sp = line.find('|');
        if (sp != std::string::npos) result.push_back(line.substr(0, sp));
    }
    return result;
}

bool StorageEngine::isCompositeType(const std::string& dbname, const std::string& name) const {
    return !getCompositeType(dbname, name).name.empty();
}

std::vector<std::string> StorageEngine::getInheritedChildren(const std::string& dbname,
                                                             const std::string& parentName) const {
    std::vector<std::string> children;
    auto inhPath = std::filesystem::path(dbPath(dbname)) / ".inherits";
    if (!std::filesystem::exists(inhPath)) return children;
    std::ifstream ifs(inhPath);
    if (!ifs) return children;
    std::string line;
    while (std::getline(ifs, line)) {
        size_t sep = line.find('|');
        if (sep == std::string::npos) continue;
        std::string parent = line.substr(0, sep);
        std::string child = line.substr(sep + 1);
        if (parent == parentName) children.push_back(child);
    }
    return children;
}

// ========================================================================
// Advisory locks (session-level)
// ========================================================================
bool StorageEngine::advisoryLock(int64_t key) {
    std::lock_guard<std::mutex> lock(advisoryMutex_);
    auto it = advisoryLocks_.find(key);
    if (it == advisoryLocks_.end()) {
        advisoryLocks_[key] = 1;
        return true;
    }
    if (it->second > 0) {
        ++it->second;
        return true;
    }
    return false; // held by shared lock
}

bool StorageEngine::advisoryUnlock(int64_t key) {
    std::lock_guard<std::mutex> lock(advisoryMutex_);
    auto it = advisoryLocks_.find(key);
    if (it == advisoryLocks_.end() || it->second <= 0) return false;
    if (--it->second == 0) advisoryLocks_.erase(it);
    return true;
}

bool StorageEngine::advisoryLockShared(int64_t key) {
    std::lock_guard<std::mutex> lock(advisoryMutex_);
    auto it = advisoryLocks_.find(key);
    if (it == advisoryLocks_.end()) {
        advisoryLocks_[key] = -1;
        return true;
    }
    if (it->second < 0) {
        --it->second;
        return true;
    }
    return false; // held by exclusive lock
}

bool StorageEngine::advisoryUnlockShared(int64_t key) {
    std::lock_guard<std::mutex> lock(advisoryMutex_);
    auto it = advisoryLocks_.find(key);
    if (it == advisoryLocks_.end() || it->second >= 0) return false;
    if (++it->second == 0) advisoryLocks_.erase(it);
    return true;
}

bool StorageEngine::advisoryLockExists(int64_t key) const {
    std::lock_guard<std::mutex> lock(advisoryMutex_);
    return advisoryLocks_.find(key) != advisoryLocks_.end();
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

void StorageEngine::recordModification(const std::string& dbname,
                                       const std::string& tablename,
                                       size_t delta) {
    auto key = std::make_pair(dbname, tablename);
    std::lock_guard<std::mutex> lock(modifyMutex_);
    modifyCounts_[key] += delta;
}

void StorageEngine::maybeAutoAnalyze(const std::string& dbname,
                                     const std::string& tablename) {
    if (!g_config.autoAnalyzeEnabled) return;
    auto key = std::make_pair(dbname, tablename);
    size_t count = 0;
    {
        std::lock_guard<std::mutex> lock(modifyMutex_);
        auto it = modifyCounts_.find(key);
        if (it == modifyCounts_.end()) return;
        count = it->second;
    }
    if (count >= static_cast<size_t>(g_config.autoAnalyzeThreshold)) {
        analyzeTable(dbname, tablename);
        std::lock_guard<std::mutex> lock(modifyMutex_);
        modifyCounts_[key] = 0;
    }
}

static std::string privToStr(StorageEngine::TablePrivilege p) {
    switch (p) {
        case StorageEngine::TablePrivilege::Select: return "select";
        case StorageEngine::TablePrivilege::Insert: return "insert";
        case StorageEngine::TablePrivilege::Update: return "update";
        case StorageEngine::TablePrivilege::Delete: return "delete";
        case StorageEngine::TablePrivilege::All: return "all";
        case StorageEngine::TablePrivilege::Usage: return "usage";
        case StorageEngine::TablePrivilege::Execute: return "execute";
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

static constexpr const char* GRANT_OPTION_MARK = "__grant__";

// Check if columns set represents table-level permission (ignoring grant option marker)
static bool isTableLevelIgnoreGrant(const std::set<std::string>& cols) {
    for (const auto& c : cols) {
        if (c != GRANT_OPTION_MARK) return false;
    }
    return true;
}

static bool colsEmptyIgnoreGrant(const std::string& colsStr) {
    if (colsStr.empty()) return true;
    // colsStr may start with space
    size_t start = 0;
    while (start < colsStr.size() && std::isspace(static_cast<unsigned char>(colsStr[start]))) ++start;
    if (start >= colsStr.size()) return true;
    // Check if remaining is only __grant__
    std::string rest = colsStr.substr(start);
    auto cols = parseColumns(rest);
    for (const auto& c : cols) {
        if (c != GRANT_OPTION_MARK) return false;
    }
    return true;
}

void StorageEngine::grant(const std::string& dbname, const std::string& tablename,
                          const std::string& username, TablePrivilege priv,
                          const std::vector<std::string>& columns,
                          bool withGrantOption,
                          const std::string& grantedBy) {
    auto ppath = permPath(dbname);
    std::map<std::string, std::set<std::string>> perms;
    if (std::filesystem::exists(ppath)) {
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
    if (columns.empty()) {
        perms[key] = {};
    } else {
        if (perms[key].empty()) {
            auto it = perms.find(key);
            if (it != perms.end() && it->second.empty()) {
                it->second.clear();
            }
        }
        for (const auto& c : columns) perms[key].insert(c);
    }
    if (withGrantOption) {
        perms[key].insert(GRANT_OPTION_MARK);
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
    // Record grant chain when withGrantOption is used and granter is known
    if (withGrantOption && !grantedBy.empty()) {
        auto gcp = grantChainPath(dbPath(dbname));
        std::ofstream gcfs(gcp, std::ios::app);
        gcfs << username << " " << tablename << " " << privToStr(priv) << " " << grantedBy << "\n";
    }
}

void StorageEngine::revoke(const std::string& dbname, const std::string& tablename,
                           const std::string& username, TablePrivilege priv,
                           const std::vector<std::string>& columns, bool cascade) {
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
    {
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

    // CASCADE: recursively revoke from users who received this privilege via the revoked user
    if (cascade) {
        auto gcp = grantChainPath(dbPath(dbname));
        if (std::filesystem::exists(gcp)) {
            std::vector<std::tuple<std::string, std::string, std::string>> chains; // grantee, obj, priv
            {
                std::ifstream ifs(gcp);
                std::string line;
                while (std::getline(ifs, line)) {
                    if (line.empty()) continue;
                    std::stringstream ss(line);
                    std::string gee, obj, pr, grter;
                    ss >> gee >> obj >> pr >> grter;
                    if (grter == username && obj == tablename && pr == privToStr(priv)) {
                        chains.emplace_back(gee, obj, pr);
                    }
                }
            }
            // Remove matching grant-chain entries
            {
                std::ifstream ifs(gcp);
                std::vector<std::string> remaining;
                std::string line;
                while (std::getline(ifs, line)) {
                    if (line.empty()) continue;
                    std::stringstream ss(line);
                    std::string gee, obj, pr, grter;
                    ss >> gee >> obj >> pr >> grter;
                    bool match = (grter == username && obj == tablename && pr == privToStr(priv));
                    if (!match) remaining.push_back(line);
                }
                std::ofstream ofs(gcp);
                for (const auto& r : remaining) ofs << r << "\n";
            }
            // Recursively revoke from downstream grantees
            for (const auto& ch : chains) {
                revoke(dbname, std::get<1>(ch), std::get<0>(ch), priv, {}, true);
            }
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
    bool hasDbLevel = false;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::stringstream ss(line);
        std::string u, t, p, cols;
        ss >> u >> t >> p;
        std::getline(ss, cols);
        if (u != username) continue;
        if (t == tablename && (p == "all" || p == target)) {
            if (colsEmptyIgnoreGrant(cols)) return true;
        }
        if (t == "*" && (p == "all" || p == target)) hasDbLevel = true;
    }
    return hasDbLevel;
}

bool StorageEngine::hasGrantOption(const std::string& dbname, const std::string& tablename,
                                   const std::string& username, TablePrivilege priv) const {
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return false;
    std::string target = privToStr(priv);
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
        if (t != tablename && t != "*") continue;
        if (p == "all" || p == target) {
            auto colList = parseColumns(cols);
            for (const auto& c : colList) {
                if (c == GRANT_OPTION_MARK) return true;
            }
        }
    }
    return false;
}

bool StorageEngine::hasColumnPermission(const std::string& dbname, const std::string& tablename,
                                        const std::string& username, TablePrivilege priv,
                                        const std::vector<std::string>& columns) const {
    if (columns.empty()) return hasPermission(dbname, tablename, username, priv);
    auto ppath = permPath(dbname);
    if (!std::filesystem::exists(ppath)) return false;
    std::string target = privToStr(priv);
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
                    for (const auto& c : parseColumns(cols)) {
                        if (c != GRANT_OPTION_MARK) allowedCols.insert(c);
                    }
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
