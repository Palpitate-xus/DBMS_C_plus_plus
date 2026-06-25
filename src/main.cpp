#include <algorithm>
#include <chrono>
#include <cctype>
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "TableManage.h"
#include "ExecutionPlan.h"
#include "NetworkServer.h"
#include "logs.h"
#include "permissions.h"
#include "Session.h"
#include "Config.h"
#include "parser/parser.h"
#include "commands/DdlExecutor.h"
#include "catalog/CatalogService.h"

using namespace std;
using dbms::Column;
using dbms::makeDateColumn;
using dbms::makeIntColumn;
using dbms::makeStringColumn;
using dbms::makeVarCharColumn;
using dbms::makeNCharColumn;
using dbms::makeNVarCharColumn;
using dbms::makeTimestampColumn;
using dbms::makeTimestamptzColumn;
using dbms::makeTextColumn;
using dbms::makeBlobColumn;
using dbms::makeBinaryColumn;
using dbms::makeVarBinaryColumn;
using dbms::makeJsonColumn;
using dbms::makeJsonbColumn;
using dbms::makeXmlColumn;
using dbms::makePgLsnColumn;
using dbms::makeInt4RangeColumn;
using dbms::makeInt8RangeColumn;
using dbms::makeNumRangeColumn;
using dbms::makeTsRangeColumn;
using dbms::makeTstzRangeColumn;
using dbms::makeDateRangeColumn;
using dbms::makeTsVectorColumn;
using dbms::makeTsQueryColumn;
using dbms::makeFloatColumn;
using dbms::makeDoubleColumn;
using dbms::makePointColumn;
using dbms::makeINetColumn;
using dbms::makeCidrColumn;
using dbms::makeDecimalColumn;
using dbms::makeBooleanColumn;
using dbms::makeUuidColumn;
using dbms::makeTimeColumn;
using dbms::makeDateTimeColumn;
using dbms::makeIntervalColumn;
using dbms::DBStatus;
using dbms::StorageEngine;
using dbms::TableSchema;

StorageEngine g_engine;

// ========================================================================
// Slow query log enhancements
// ========================================================================
double g_slowQueryThresholdMs = 100.0;
static constexpr size_t MAX_SLOW_LOG_ENTRIES = 100;
int g_checkpointInterval = 30;  // auto checkpoint every N SQLs

// ========================================================================
// Query plan cache
// ========================================================================
struct CachedPlanEntry {
    std::string planText;
    std::string dbname;
    std::chrono::steady_clock::time_point cachedAt;
};
static std::map<std::string, CachedPlanEntry> g_queryPlanCache;
static std::mutex g_planCacheMutex;
static size_t g_planCacheHits = 0;
static size_t g_planCacheMisses = 0;

struct SlowQueryEntry {
    std::string timestamp;
    std::string username;
    std::string dbname;
    double ms = 0.0;
    std::string sql;
};
static std::vector<SlowQueryEntry> g_slowQueryBuffer;
static std::mutex g_slowQueryMutex;

void logSlowQuery(const std::string& sql, double ms,
                  const std::string& username = "",
                  const std::string& dbname = "");

// ========================================================================
// SQL execution statistics (pg_stat_statements)
// ========================================================================
struct SqlStatEntry {
    std::string sql;
    uint64_t calls = 0;
    double totalTimeMs = 0.0;
    double minTimeMs = 0.0;
    double maxTimeMs = 0.0;
    double meanTimeMs = 0.0;
    std::string dbname;
};
static std::map<std::string, SqlStatEntry> g_sqlStats; // key = db + "|" + normalized_sql
static std::mutex g_sqlStatsMutex;

// ========================================================================
// NOTIFY / LISTEN async messaging
// ========================================================================
static std::mutex g_notifyMutex;
static std::map<std::string, std::set<std::string>> g_listeners; // channel -> usernames
static std::map<std::string, std::vector<std::pair<std::string, std::string>>> g_pendingNotifies; // username -> [(channel, payload)]

static void checkNotifications(Session& s) {
    std::lock_guard<std::mutex> lock(g_notifyMutex);
    auto it = g_pendingNotifies.find(s.username);
    if (it != g_pendingNotifies.end()) {
        for (const auto& np : it->second) {
            std::cout << "NOTIFY " << np.first;
            if (!np.second.empty()) std::cout << " " << np.second;
            std::cout << std::endl;
        }
        g_pendingNotifies.erase(it);
    }
}

std::string normalizeSqlForStats(const std::string& sql) {
    std::string s = sql;
    // Trim whitespace
    size_t a = 0;
    while (a < s.size() && isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    s = s.substr(a, b - a);
    // Replace consecutive whitespace with single space
    std::string r;
    bool lastWasSpace = false;
    for (char c : s) {
        if (isspace(static_cast<unsigned char>(c))) {
            if (!lastWasSpace) r += ' ';
            lastWasSpace = true;
        } else {
            r += static_cast<char>(tolower(static_cast<unsigned char>(c)));
            lastWasSpace = false;
        }
    }
    return r;
}

void recordSqlStat(const std::string& sql, double ms, const std::string& dbname) {
    std::lock_guard<std::mutex> lock(g_sqlStatsMutex);
    std::string key = dbname + "|" + normalizeSqlForStats(sql);
    auto& entry = g_sqlStats[key];
    if (entry.calls == 0) {
        entry.sql = sql;
        entry.dbname = dbname;
        entry.minTimeMs = ms;
        entry.maxTimeMs = ms;
    } else {
        if (ms < entry.minTimeMs) entry.minTimeMs = ms;
        if (ms > entry.maxTimeMs) entry.maxTimeMs = ms;
    }
    entry.calls++;
    entry.totalTimeMs += ms;
    entry.meanTimeMs = entry.totalTimeMs / static_cast<double>(entry.calls);
}

static std::vector<SqlStatEntry> getSqlStats(const std::string& dbFilter = "") {
    std::lock_guard<std::mutex> lock(g_sqlStatsMutex);
    std::vector<SqlStatEntry> result;
    for (const auto& kv : g_sqlStats) {
        if (dbFilter.empty() || kv.second.dbname == dbFilter) {
            result.push_back(kv.second);
        }
    }
    // Sort by total time descending
    std::sort(result.begin(), result.end(),
        [](const SqlStatEntry& a, const SqlStatEntry& b) {
            return a.totalTimeMs > b.totalTimeMs;
        });
    return result;
}

// Forward declaration for SHOW VARIABLES
extern dbms::Config g_config;

// ========================================================================
// Row-Level Security helper
// ========================================================================
// ========================================================================
// Utility
// ========================================================================
static string toLower(string s) {
    for (char& c : s) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
    return s;
}

static string trim(const string& s) {
    size_t a = 0;
    while (a < s.size() && isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

static string stripQuotes(const string& s) {
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') || (s.front() == '"' && s.back() == '"'))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

static pair<set<string>, bool> parseReturningClause(const string& sql, size_t searchStart) {
    size_t retPos = sql.find("returning", searchStart);
    if (retPos == string::npos) return {{}, false};
    string retStr = trim(sql.substr(retPos + 9));
    if (retStr == "*") return {{}, true};
    set<string> cols;
    stringstream rss(retStr);
    string item;
    while (getline(rss, item, ',')) cols.insert(trim(item));
    return {cols, false};
}

static vector<string> tokenize(const string& sql) {
    vector<string> tokens;
    size_t i = 0;
    while (i < sql.size()) {
        while (i < sql.size() && isspace(static_cast<unsigned char>(sql[i]))) ++i;
        if (i >= sql.size()) break;
        if (sql[i] == '(' || sql[i] == ')') {
            tokens.emplace_back(1, sql[i]);
            ++i;
            continue;
        }
        size_t j = i;
        while (j < sql.size() && !isspace(static_cast<unsigned char>(sql[j]))
               && sql[j] != '(' && sql[j] != ')')
            ++j;
        tokens.push_back(sql.substr(i, j - i));
        i = j;
    }
    return tokens;
}

// ========================================================================
// CASE WHEN preprocessing forward declaration
// ========================================================================
static string preprocessCaseWhen(string s);

// ========================================================================
// SQL preprocessing
// ========================================================================
static string foldConstants(const string& s);
static string sqlProcessor(string raw) {
    raw = toLower(raw);
    raw.erase(remove(raw.begin(), raw.end(), '\n'), raw.end());
    raw.erase(remove(raw.begin(), raw.end(), '\t'), raw.end());
    raw.erase(remove(raw.begin(), raw.end(), '\r'), raw.end());
    // Trim leading/trailing whitespace
    size_t start = raw.find_first_not_of(' ');
    if (start == string::npos) raw.clear();
    else raw = raw.substr(start, raw.find_last_not_of(' ') - start + 1);
    if (!raw.empty() && raw.back() == ';') raw.pop_back();
    raw = preprocessCaseWhen(raw);
    // Normalize boolean literals: true/false → 1/0
    {
        string out;
        size_t i = 0;
        while (i < raw.size()) {
            // Check for "true" as a standalone token
            if ((i == 0 || !isalnum(static_cast<unsigned char>(raw[i-1]))) &&
                raw.substr(i, 4) == "true" &&
                (i + 4 >= raw.size() || !isalnum(static_cast<unsigned char>(raw[i+4])))) {
                out += "1";
                i += 4;
            } else if ((i == 0 || !isalnum(static_cast<unsigned char>(raw[i-1]))) &&
                       raw.substr(i, 5) == "false" &&
                       (i + 5 >= raw.size() || !isalnum(static_cast<unsigned char>(raw[i+5])))) {
                out += "0";
                i += 5;
            } else {
                out += raw[i];
                ++i;
            }
        }
        raw = out;
    }
    // Convert array subscript syntax: col[n] -> array_get(col, n)
    {
        string out;
        size_t i = 0;
        while (i < raw.size()) {
            size_t bracketOpen = raw.find('[', i);
            if (bracketOpen == string::npos) {
                out += raw.substr(i);
                break;
            }
            // Find column name before bracket
            size_t colStart = bracketOpen;
            while (colStart > i && isspace(static_cast<unsigned char>(raw[colStart - 1]))) --colStart;
            while (colStart > i && (isalnum(static_cast<unsigned char>(raw[colStart - 1])) || raw[colStart - 1] == '_')) --colStart;
            if (colStart == bracketOpen) {
                out += raw[i];
                ++i;
                continue;
            }
            size_t bracketClose = raw.find(']', bracketOpen);
            if (bracketClose == string::npos) {
                out += raw.substr(i);
                break;
            }
            string colName = raw.substr(colStart, bracketOpen - colStart);
            string idxStr = trim(raw.substr(bracketOpen + 1, bracketClose - bracketOpen - 1));
            // Skip empty brackets (e.g., type declarations like int[])
            if (idxStr.empty()) {
                out += raw.substr(i, bracketClose - i + 1);
                i = bracketClose + 1;
                continue;
            }
            out += raw.substr(i, colStart - i);
            out += "array_get(" + colName + ", " + idxStr + ")";
            i = bracketClose + 1;
        }
        raw = out;
    }
    // Convert ANY syntax: expr = any(col) -> array_contains(col, expr)
    // Convert ALL syntax: expr = all(col) -> not array_contains(col, expr) with negation (simplified)
    {
        string out;
        size_t i = 0;
        while (i < raw.size()) {
            size_t anyPos = raw.find("any(", i);
            size_t allPos = raw.find("all(", i);
            size_t foundPos = string::npos;
            bool isAny = true;
            if (anyPos != string::npos && (allPos == string::npos || anyPos < allPos)) {
                foundPos = anyPos;
                isAny = true;
            } else if (allPos != string::npos) {
                foundPos = allPos;
                isAny = false;
            }
            if (foundPos == string::npos) {
                out += raw.substr(i);
                break;
            }
            // Find the operator before ANY/ALL
            size_t opEnd = foundPos;
            while (opEnd > i && isspace(static_cast<unsigned char>(raw[opEnd - 1]))) --opEnd;
            size_t opStart = opEnd;
            while (opStart > i && !isalnum(static_cast<unsigned char>(raw[opStart - 1])) && raw[opStart - 1] != '_' && raw[opStart - 1] != ')' && raw[opStart - 1] != '\'' && raw[opStart - 1] != '"') --opStart;
            if (opStart == opEnd) {
                out += raw[i];
                ++i;
                continue;
            }
            string op = trim(raw.substr(opStart, opEnd - opStart));
            // Find expression before operator
            size_t exprStart = opStart;
            while (exprStart > i && isspace(static_cast<unsigned char>(raw[exprStart - 1]))) --exprStart;
            // Handle parenthesized expressions or simple values
            int parenDepth = 0;
            size_t exprEnd = exprStart;
            while (exprEnd > i) {
                if (raw[exprEnd - 1] == ')') parenDepth++;
                else if (raw[exprEnd - 1] == '(') {
                    if (parenDepth == 0) break;
                    parenDepth--;
                }
                exprEnd--;
                if (parenDepth == 0 && (raw[exprEnd] == ' ' || raw[exprEnd] == ',')) break;
            }
            // Simpler approach: find where the condition starts (after WHERE, AND, OR)
            string beforeAny = raw.substr(i, foundPos - i);
            size_t whereOp = string::npos;
            for (size_t k = beforeAny.size(); k > 0; --k) {
                char c = beforeAny[k - 1];
                if (c == ' ' || c == '(' || c == ',') {
                    whereOp = k;
                    break;
                }
            }
            if (whereOp == string::npos) whereOp = 0;
            string leftExpr = trim(beforeAny.substr(0, whereOp));
            if (!leftExpr.empty()) leftExpr += " ";
            string condExpr = trim(beforeAny.substr(whereOp));
            // Parse array column from any(col) or all(col)
            size_t parenOpen = foundPos + (isAny ? 4 : 4);
            size_t parenClose = raw.find(')', parenOpen);
            if (parenClose == string::npos) {
                out += raw.substr(i);
                break;
            }
            string arrCol = trim(raw.substr(parenOpen, parenClose - parenOpen));
            out += leftExpr;
            if (isAny) {
                out += "array_contains(" + arrCol + ", " + condExpr + ") = 1";
            } else {
                // ALL: simplified - only support = all() as array_contains with negation
                // For other ops, fallback to a less accurate approach
                out += "array_contains(" + arrCol + ", " + condExpr + ") = 0";
            }
            i = parenClose + 1;
        }
        raw = out;
    }
    // Convert IS DISTINCT FROM / IS NOT DISTINCT FROM to equivalent expressions
    {
        auto extractExprBefore = [&](size_t pos) -> std::pair<size_t, std::string> {
            size_t end = pos;
            while (end > 0 && isspace(static_cast<unsigned char>(raw[end - 1]))) end--;
            size_t start = end;
            while (start > 0 && !isspace(static_cast<unsigned char>(raw[start - 1])) && raw[start - 1] != '(' && raw[start - 1] != ',') start--;
            return {start, raw.substr(start, end - start)};
        };
        auto extractExprAfter = [&](size_t pos, size_t len) -> std::pair<size_t, std::string> {
            size_t start = pos + len;
            while (start < raw.size() && isspace(static_cast<unsigned char>(raw[start]))) start++;
            size_t end = start;
            while (end < raw.size() && raw[end] != ',' && raw[end] != ')' && raw[end] != ' ' && raw[end] != ';') end++;
            return {end, raw.substr(start, end - start)};
        };
        size_t pos = 0;
        // IS NOT DISTINCT FROM first (longer match)
        while ((pos = raw.find("is not distinct from", pos)) != string::npos) {
            auto [leftStart, leftExpr] = extractExprBefore(pos);
            auto [rightEnd, rightExpr] = extractExprAfter(pos, 20);
            string replacement = "((" + leftExpr + " = " + rightExpr + ") or (" + leftExpr + " is null and " + rightExpr + " is null))";
            raw = raw.substr(0, leftStart) + replacement + raw.substr(rightEnd);
            pos = leftStart + replacement.size();
        }
        pos = 0;
        while ((pos = raw.find("is distinct from", pos)) != string::npos) {
            auto [leftStart, leftExpr] = extractExprBefore(pos);
            auto [rightEnd, rightExpr] = extractExprAfter(pos, 16);
            string replacement = "((" + leftExpr + " <> " + rightExpr + ") or (" + leftExpr + " is null and " + rightExpr + " is not null) or (" + leftExpr + " is not null and " + rightExpr + " is null))";
            raw = raw.substr(0, leftStart) + replacement + raw.substr(rightEnd);
            pos = leftStart + replacement.size();
        }
    }
    // Convert SQL:2008 FETCH FIRST ... ROWS ONLY to LIMIT syntax
    {
        size_t fetchPos = raw.find("fetch first");
        if (fetchPos != string::npos) {
            size_t rowsPos = raw.find(" rows ", fetchPos);
            if (rowsPos != string::npos) {
                string numStr = trim(raw.substr(fetchPos + 11, rowsPos - fetchPos - 11));
                size_t onlyPos = raw.find("only", rowsPos);
                size_t endPos = (onlyPos != string::npos) ? onlyPos + 4 : raw.size();
                string replacement = "limit " + numStr;
                raw = raw.substr(0, fetchPos) + replacement + raw.substr(endPos);
            }
        }
    }
    return raw;
}

// ========================================================================
// CASE WHEN preprocessing: "case when a then b when c then d else e end"
//                         → "case_when(a,b,c,d,e)"
// ========================================================================
static string normalizeCaseCondition(string s) {
    s = trim(s);
    static const char* ops[] = {"<<", ">>", "<^", ">^", "<@", "&&", ">=", "<=", "!=", "<>", ">", "<", "="};
    for (const char* op : ops) {
        size_t len = strlen(op);
        size_t pos = s.find(op);
        if (pos != string::npos) {
            string before = trim(s.substr(0, pos));
            string after = trim(s.substr(pos + len));
            return string(op) + before + " " + after;
        }
    }
    size_t isnotPos = s.find("is not null");
    if (isnotPos != string::npos) {
        return "isnotnull " + trim(s.substr(0, isnotPos));
    }
    size_t isPos = s.find("is null");
    if (isPos != string::npos) {
        return "isnull " + trim(s.substr(0, isPos));
    }
    size_t likePos = s.find("like");
    if (likePos != string::npos) {
        string before = trim(s.substr(0, likePos));
        string after = trim(s.substr(likePos + 4));
        return "like" + before + " " + after;
    }
    return s;
}

static string preprocessCaseWhen(string s) {
    size_t pos = 0;
    while (true) {
        size_t casePos = s.find("case when", pos);
        if (casePos == string::npos) break;
        size_t endPos = s.find("end", casePos);
        if (endPos == string::npos) break;
        string content = trim(s.substr(casePos + 9, endPos - casePos - 9));

        vector<pair<string, string>> whenThen;
        string elseVal;
        size_t curr = 0;
        while (curr < content.size()) {
            size_t thenPos = content.find("then", curr);
            if (thenPos == string::npos) break;
            string cond = normalizeCaseCondition(content.substr(curr, thenPos - curr));

            size_t nextWhen = content.find("when", thenPos + 4);
            size_t elsePos = content.find("else", thenPos + 4);

            if (elsePos != string::npos && (nextWhen == string::npos || elsePos < nextWhen)) {
                string thenVal = trim(content.substr(thenPos + 4, elsePos - thenPos - 4));
                whenThen.emplace_back(cond, thenVal);
                elseVal = trim(content.substr(elsePos + 4));
                break;
            } else if (nextWhen != string::npos) {
                string thenVal = trim(content.substr(thenPos + 4, nextWhen - thenPos - 4));
                whenThen.emplace_back(cond, thenVal);
                curr = nextWhen + 4;
            } else {
                string thenVal = trim(content.substr(thenPos + 4));
                whenThen.emplace_back(cond, thenVal);
                break;
            }
        }

        string replacement = "case_when(";
        for (size_t i = 0; i < whenThen.size(); ++i) {
            if (i > 0) replacement += ",";
            replacement += whenThen[i].first + "," + whenThen[i].second;
        }
        if (!elseVal.empty()) {
            if (!whenThen.empty()) replacement += ",";
            replacement += elseVal;
        }
        replacement += ")";

        s = s.substr(0, casePos) + replacement + s.substr(endPos + 3);
        pos = casePos + replacement.size();
    }
    return s;
}

// ========================================================================
// Scalar function helpers
// ========================================================================
static bool isScalarFunc(const string& name) {
    static const set<string> scalars = {"length", "char_length", "character_length", "upper", "lower", "trim", "substring", "concat",
                                         "abs", "round", "ceil", "floor",
                                         "now", "current_timestamp", "extract",
                                         "year", "month", "day",
                                         "hour", "minute", "second",
                                         "case_when", "cast", "convert",
                                         "to_number", "to_char", "to_date",
                                         "coalesce", "nullif",
                                         "replace", "position", "instr",
                                         "power", "sqrt", "mod",
                                         "ln", "log", "exp", "random", "rand",
                                         "lpad", "rpad", "reverse",
                                         "greatest", "least", "if", "iif",
                                         "date_add", "date_sub",
                                         "datediff", "date_trunc", "date_format",
                                         "age",
                                         "json_extract", "json_value",
                                         "jsonb_extract", "jsonb_extract_text",
                                         "jsonb_contains", "jsonb_exists", "jsonb_pretty",
                                         "sin", "cos", "tan",
                                         "split_part",
                                         "uuid_generate",
                                         "array_get", "array_length", "array_contains",
                                         "unnest",
                                         "subquery",
                                         "current_user", "session_user"};
    return scalars.find(name) != scalars.end();
}

static vector<string> splitSelectColumns(const string& s) {
    vector<string> cols;
    size_t i = 0;
    int parenDepth = 0;
    string current;
    while (i < s.size()) {
        if (s[i] == '(') parenDepth++;
        else if (s[i] == ')') parenDepth--;
        else if (s[i] == ',' && parenDepth == 0) {
            cols.push_back(trim(current));
            current.clear();
            ++i;
            continue;
        }
        current += s[i];
        ++i;
    }
    if (!current.empty()) cols.push_back(trim(current));
    return cols;
}

// Find a keyword at top-level (parenthesis depth 0), skipping content inside parens and single-quoted strings.
// Returns string::npos if not found. The match boundary is checked at word level.
static size_t findTopLevelKeyword(const string& sql, const string& kw, size_t startPos = 0) {
    int depth = 0;
    bool inStr = false;
    size_t klen = kw.size();
    for (size_t i = startPos; i < sql.size(); ++i) {
        char c = sql[i];
        if (inStr) {
            if (c == '\'') inStr = false;
            continue;
        }
        if (c == '\'') { inStr = true; continue; }
        if (c == '(') { depth++; continue; }
        if (c == ')') { depth--; continue; }
        if (depth == 0 && i + klen <= sql.size() && sql.compare(i, klen, kw) == 0) {
            bool leftOk = (i == 0) || !isalnum(static_cast<unsigned char>(sql[i-1]));
            bool rightOk = (i + klen == sql.size()) || !isalnum(static_cast<unsigned char>(sql[i+klen]));
            if (leftOk && rightOk) return i;
        }
    }
    return string::npos;
}

static vector<string> splitFuncArgs(const string& s) {
    vector<string> args;
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && isspace(static_cast<unsigned char>(s[i]))) ++i;
        if (i >= s.size()) break;
        string arg;
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

static vector<string> splitTopLevelComma(const string& s);

static bool parseValuesRows(const string& valuesPart,
                            vector<vector<string>>& rows,
                            string& error) {
    size_t i = 0;
    size_t expectedCols = 0;
    while (i < valuesPart.size()) {
        while (i < valuesPart.size() && isspace(static_cast<unsigned char>(valuesPart[i]))) ++i;
        if (i >= valuesPart.size()) break;
        if (valuesPart[i] != '(') {
            error = "SQL syntax error: VALUES requires parenthesized rows";
            return false;
        }

        ++i;
        string inner;
        int parenDepth = 0;
        bool inQuote = false;
        bool closed = false;
        for (; i < valuesPart.size(); ++i) {
            char c = valuesPart[i];
            if (c == '\'') {
                inner += c;
                if (inQuote && i + 1 < valuesPart.size() && valuesPart[i + 1] == '\'') {
                    inner += valuesPart[++i];
                    continue;
                }
                inQuote = !inQuote;
                continue;
            }
            if (!inQuote) {
                if (c == '(') {
                    ++parenDepth;
                } else if (c == ')') {
                    if (parenDepth == 0) {
                        closed = true;
                        ++i;
                        break;
                    }
                    --parenDepth;
                }
            }
            inner += c;
        }
        if (!closed) {
            error = "SQL syntax error: unterminated VALUES row";
            return false;
        }

        vector<string> cells = splitTopLevelComma(inner);
        if (cells.empty()) {
            error = "SQL syntax error: VALUES row cannot be empty";
            return false;
        }
        if (rows.empty()) {
            expectedCols = cells.size();
        } else if (cells.size() != expectedCols) {
            error = "VALUES lists must all be the same length";
            return false;
        }
        rows.push_back(std::move(cells));

        while (i < valuesPart.size() && isspace(static_cast<unsigned char>(valuesPart[i]))) ++i;
        if (i >= valuesPart.size()) break;
        if (valuesPart[i] != ',') {
            error = "SQL syntax error: expected comma between VALUES rows";
            return false;
        }
        ++i;
    }
    if (rows.empty()) {
        error = "SQL syntax error: VALUES requires at least one row";
        return false;
    }
    return true;
}

static string formatValuesCell(string cell) {
    cell = trim(cell);
    if (cell == "null") return "NULL";
    if (cell.size() >= 2 && cell.front() == '\'' && cell.back() == '\'') {
        string out = cell.substr(1, cell.size() - 2);
        size_t pos = 0;
        while ((pos = out.find("''", pos)) != string::npos) {
            out.replace(pos, 2, "'");
            ++pos;
        }
        return out;
    }
    return cell;
}

static bool executeValuesStatement(const string& sql) {
    string rest = trim(sql.substr(6));
    vector<vector<string>> rows;
    string error;
    if (!parseValuesRows(rest, rows, error)) {
        cout << error << endl;
        return true;
    }

    for (size_t col = 0; col < rows.front().size(); ++col) {
        if (col > 0) cout << ' ';
        cout << "column" << (col + 1);
    }
    cout << endl;
    for (const auto& row : rows) {
        for (size_t col = 0; col < row.size(); ++col) {
            if (col > 0) cout << ' ';
            cout << formatValuesCell(row[col]);
        }
        cout << endl;
    }
    return false;
}

// ========================================================================
// Forward declarations for command handlers (defined later in the file)
// ========================================================================
bool checkAdmin(const Session& s);
bool checkDB(const Session& s);
bool execute(const std::string& rawSql, Session& s);
string resolveTableName(Session& s, const string& name);
static vector<string> splitConds(const string& s);
static string normalizeConditionStr(string s);
static string modifyLogic(const string& logic);
static bool setSessionAuthorization(Session& s, const string& targetRaw);
static vector<string> splitValues(const string& s);
static vector<string> parseCSVLine(const string& line);

// ========================================================================
// Cursor command handlers (extracted for Parser switch/case dispatch)
// ========================================================================
static bool handleDeclareCursor(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(7));
    size_t cursorPos = rest.find("cursor");
    if (cursorPos == string::npos) {
        cout << "SQL syntax error: DECLARE cursor_name CURSOR FOR select_sql" << endl;
        return true;
    }
    string cursorName = trim(rest.substr(0, cursorPos));
    string afterCursor = trim(rest.substr(cursorPos + 6));
    if (afterCursor.substr(0, 3) != "for") {
        cout << "SQL syntax error: DECLARE cursor_name CURSOR FOR select_sql" << endl;
        return true;
    }
    string selectSql = trim(afterCursor.substr(3));
    // Execute SELECT and capture output
    std::stringstream ss;
    auto oldBuf = std::cout.rdbuf(ss.rdbuf());
    bool err = execute(selectSql, s);
    std::cout.rdbuf(oldBuf);
    if (err) {
        cout << "DECLARE CURSOR failed: " << ss.str() << endl;
        return true;
    }
    Session::Cursor cur;
    string line;
    bool first = true;
    while (getline(ss, line)) {
        if (first) {
            stringstream hs(line);
            string col;
            while (hs >> col) cur.colNames.push_back(col);
            first = false;
        } else if (!line.empty()) {
            cur.rows.push_back(line);
        }
    }
    cur.pos = -1;
    s.cursors[cursorName] = std::move(cur);
    cout << "Cursor " << cursorName << " declared" << endl;
    return false;
}

static bool handleFetchCursor(const string& sql, Session& s) {
    string rest = trim(sql.substr(5));
    string direction = "next";
    string cursorName;
    size_t fromPos = rest.find("from");
    if (fromPos == string::npos) fromPos = rest.find("in");
    if (fromPos != string::npos) {
        string beforeFrom = trim(rest.substr(0, fromPos));
        if (!beforeFrom.empty()) direction = beforeFrom;
        cursorName = trim(rest.substr(fromPos + 4));
    } else {
        cursorName = rest;
    }
    auto it = s.cursors.find(cursorName);
    if (it == s.cursors.end()) {
        cout << "Cursor " << cursorName << " not found" << endl;
        return true;
    }
    auto& cur = it->second;
    if (cur.rows.empty()) {
        cout << "No data" << endl;
        return false;
    }
    auto moveTo = [&](int p) {
        if (p < 0 || p >= (int)cur.rows.size()) {
            cout << "No more rows" << endl;
            return false;
        }
        // Output header on first fetch
        if (cur.pos == -1 && !cur.colNames.empty()) {
            for (size_t i = 0; i < cur.colNames.size(); ++i) {
                if (i > 0) cout << ' ';
                cout << cur.colNames[i];
            }
            cout << endl;
        }
        cout << cur.rows[p] << endl;
        cur.pos = p;
        return true;
    };
    if (direction == "next") {
        moveTo(cur.pos + 1);
    } else if (direction == "prior" || direction == "backward") {
        moveTo(cur.pos - 1);
    } else if (direction == "first") {
        moveTo(0);
    } else if (direction == "last") {
        moveTo((int)cur.rows.size() - 1);
    } else if (direction == "all") {
        for (size_t i = 0; i < cur.rows.size(); ++i) cout << cur.rows[i] << endl;
        cur.pos = (int)cur.rows.size() - 1;
    } else {
        // ABSOLUTE n, RELATIVE n, FORWARD n, BACKWARD n
        int n = 1;
        try {
            size_t dp = direction.find_first_of("0123456789");
            if (dp != string::npos) n = stoi(direction.substr(dp));
        } catch (...) {}
        if (direction.find("absolute") != string::npos) {
            moveTo(n);
        } else if (direction.find("relative") != string::npos) {
            moveTo(cur.pos + n);
        } else if (direction.find("forward") != string::npos) {
            moveTo(cur.pos + n);
        } else if (direction.find("backward") != string::npos) {
            moveTo(cur.pos - n);
        } else {
            moveTo(cur.pos + 1);
        }
    }
    return false;
}

static bool handleMoveCursor(const string& sql, Session& s) {
    string rest = trim(sql.substr(4));
    string direction = "next";
    string cursorName;
    size_t sepPos = string::npos;
    size_t sepLen = 0;
    if (rest.substr(0, 5) == "from ") {
        sepPos = 0;
        sepLen = 5;
    } else if (rest.substr(0, 3) == "in ") {
        sepPos = 0;
        sepLen = 3;
    } else {
        size_t fromPos = rest.find(" from ");
        size_t inPos = rest.find(" in ");
        if (fromPos != string::npos && (inPos == string::npos || fromPos < inPos)) {
            sepPos = fromPos;
            sepLen = 6;
        } else if (inPos != string::npos) {
            sepPos = inPos;
            sepLen = 4;
        }
    }
    if (sepPos != string::npos) {
        string before = trim(rest.substr(0, sepPos));
        if (!before.empty()) direction = before;
        cursorName = trim(rest.substr(sepPos + sepLen));
    } else {
        cursorName = rest;
    }
    if (cursorName.empty()) {
        cout << "SQL syntax error: MOVE [direction] FROM cursor_name" << endl;
        return true;
    }
    auto it = s.cursors.find(cursorName);
    if (it == s.cursors.end()) {
        cout << "Cursor " << cursorName << " not found" << endl;
        return true;
    }
    auto& cur = it->second;
    int rowCount = static_cast<int>(cur.rows.size());
    int moved = 0;
    auto parseCount = [](const string& dir, int defaultValue) {
        if (dir.find("all") != string::npos) return std::numeric_limits<int>::max();
        try {
            size_t p = dir.find_first_of("-0123456789");
            if (p != string::npos) return std::stoi(dir.substr(p));
        } catch (...) {}
        return defaultValue;
    };
    if (rowCount == 0) {
        cout << "MOVE 0" << endl;
        return false;
    }
    if (direction == "next" || direction.substr(0, 7) == "forward") {
        int n = (direction == "next") ? 1 : parseCount(direction, 1);
        if (n < 0) n = 0;
        int available = rowCount - (cur.pos + 1);
        moved = std::min(n, std::max(0, available));
        cur.pos += moved;
    } else if (direction == "prior" || direction.substr(0, 8) == "backward") {
        int n = (direction == "prior") ? 1 : parseCount(direction, 1);
        if (n < 0) n = 0;
        int available = cur.pos + 1;
        moved = std::min(n, std::max(0, available));
        cur.pos -= moved;
    } else if (direction == "first") {
        cur.pos = 0;
        moved = 1;
    } else if (direction == "last") {
        cur.pos = rowCount - 1;
        moved = 1;
    } else if (direction.substr(0, 8) == "absolute") {
        int n = parseCount(direction, 0);
        int target = (n > 0) ? n - 1 : (n < 0 ? rowCount + n : -1);
        if (target >= 0 && target < rowCount) {
            cur.pos = target;
            moved = 1;
        } else {
            cur.pos = (target < 0) ? -1 : rowCount - 1;
            moved = 0;
        }
    } else if (direction.substr(0, 8) == "relative") {
        int n = parseCount(direction, 0);
        int target = cur.pos + n;
        if (target < -1) target = -1;
        if (target >= rowCount) target = rowCount - 1;
        moved = std::abs(target - cur.pos);
        cur.pos = target;
    } else if (direction == "all") {
        moved = std::max(0, rowCount - (cur.pos + 1));
        cur.pos = rowCount - 1;
    } else {
        cout << "SQL syntax error: unsupported MOVE direction" << endl;
        return true;
    }
    cout << "MOVE " << moved << endl;
    return false;
}

static bool handleCloseCursor(const string& sql, Session& s) {
    string cursorName = trim(sql.substr(5));
    if (cursorName.empty()) {
        cout << "SQL syntax error: CLOSE cursor_name" << endl;
        return true;
    }
    auto it = s.cursors.find(cursorName);
    if (it == s.cursors.end()) {
        cout << "Cursor " << cursorName << " not found" << endl;
        return true;
    }
    s.cursors.erase(it);
    cout << "Cursor " << cursorName << " closed" << endl;
    return false;
}

// ========================================================================
// Transaction command handlers
// ========================================================================
static bool handleBeginTransaction(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    // Parse optional isolation level: "begin read committed"
    // Parse optional read only: "begin read only"
    string rest = trim(sql.substr(5));
    bool readOnly = false;
    if (!rest.empty()) {
        if (rest.find("read uncommitted") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::READ_UNCOMMITTED);
            s.isolationLevel = 0;
        } else if (rest.find("read committed") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::READ_COMMITTED);
            s.isolationLevel = 1;
        } else if (rest.find("repeatable read") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::REPEATABLE_READ);
            s.isolationLevel = 2;
        } else if (rest.find("serializable") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::SERIALIZABLE);
            s.isolationLevel = 3;
        }
        if (rest.find("read only") != string::npos) {
            readOnly = true;
        }
    }
    auto res = g_engine.beginTransaction(s.currentDB);
    if (res != DBStatus::OK) {
        cout << "Begin transaction failed" << endl;
        return true;
    }
    g_engine.setReadOnly(readOnly);
    if (readOnly) {
        cout << "Read-only transaction started" << endl;
        log(s.username, "begin read-only transaction", getTime());
    } else {
        cout << "Transaction started" << endl;
        log(s.username, "begin transaction", getTime());
    }
    return false;
}

static bool handleCommitTransaction(const string& sql, Session& s) {
    (void)sql;
    auto res = g_engine.commitTransaction();
    if (res != DBStatus::OK) {
        cout << "Commit failed" << endl;
        return true;
    }
    cout << "Transaction committed" << endl;
    log(s.username, "commit transaction", getTime());
    return false;
}

static bool handleCommitPrepared(const string& sql, Session& s) {
    string xid = stripQuotes(trim(sql.substr(15)));
    if (xid.empty()) {
        cout << "SQL syntax error: COMMIT PREPARED requires a transaction ID" << endl;
        return true;
    }
    auto res = g_engine.commitPrepared(xid);
    if (res == DBStatus::OK) {
        cout << "COMMIT PREPARED " << xid << endl;
        log(s.username, "commit prepared " + xid, getTime());
    } else if (res == DBStatus::TABLE_NOT_FOUND) {
        cout << "Prepared transaction not found" << endl;
    } else {
        cout << "COMMIT PREPARED failed" << endl;
    }
    return false;
}

static bool handleRollbackTransaction(const string& sql, Session& s) {
    (void)sql;
    auto res = g_engine.rollbackTransaction();
    if (res != DBStatus::OK) {
        cout << "Rollback failed" << endl;
        return true;
    }
    cout << "Transaction rolled back" << endl;
    log(s.username, "rollback transaction", getTime());
    return false;
}

static bool handleRollbackPrepared(const string& sql, Session& s) {
    string xid = stripQuotes(trim(sql.substr(17)));
    if (xid.empty()) {
        cout << "SQL syntax error: ROLLBACK PREPARED requires a transaction ID" << endl;
        return true;
    }
    auto res = g_engine.rollbackPrepared(xid);
    if (res == DBStatus::OK) {
        cout << "ROLLBACK PREPARED " << xid << endl;
        log(s.username, "rollback prepared " + xid, getTime());
    } else if (res == DBStatus::TABLE_NOT_FOUND) {
        cout << "Prepared transaction not found" << endl;
    } else {
        cout << "ROLLBACK PREPARED failed" << endl;
    }
    return false;
}

static bool handleSavepoint(const string& sql, Session& s) {
    if (!g_engine.inTransaction()) {
        cout << "Not in transaction" << endl;
        return true;
    }
    string name = sql.substr(10);
    while (!name.empty() && isspace((unsigned char)name.back())) name.pop_back();
    if (name.empty()) {
        cout << "Savepoint name required" << endl;
        return true;
    }
    auto res = g_engine.savepoint(name);
    if (res != DBStatus::OK) {
        cout << "Savepoint failed" << endl;
        return true;
    }
    cout << "Savepoint " << name << " created" << endl;
    log(s.username, "savepoint " + name, getTime());
    return false;
}

static bool handleReleaseSavepoint(const string& sql, Session& s) {
    if (!g_engine.inTransaction()) {
        cout << "Not in transaction" << endl;
        return true;
    }
    string name = sql.substr(18);
    while (!name.empty() && isspace((unsigned char)name.back())) name.pop_back();
    if (name.empty()) {
        cout << "Savepoint name required" << endl;
        return true;
    }
    auto res = g_engine.releaseSavepoint(name);
    if (res != DBStatus::OK) {
        cout << "Savepoint not found" << endl;
        return true;
    }
    cout << "Savepoint " << name << " released" << endl;
    log(s.username, "release savepoint " + name, getTime());
    return false;
}

static bool handleRollbackToSavepoint(const string& sql, Session& s) {
    if (!g_engine.inTransaction()) {
        cout << "Not in transaction" << endl;
        return true;
    }
    string name = sql.substr(21);
    while (!name.empty() && isspace((unsigned char)name.front())) name.erase(name.begin());
    while (!name.empty() && isspace((unsigned char)name.back())) name.pop_back();
    if (name.empty()) {
        cout << "Savepoint name required" << endl;
        return true;
    }
    auto res = g_engine.rollbackToSavepoint(name);
    if (res != DBStatus::OK) {
        cout << "Savepoint not found" << endl;
        return true;
    }
    cout << "Rolled back to savepoint " << name << endl;
    log(s.username, "rollback to savepoint " + name, getTime());
    return false;
}

// ========================================================================
// SET / RESET / ALTER SYSTEM command handlers
// ========================================================================
static bool handleResetCommand(const string& sql, Session& s) {
    // RESET ROLE
    if (sql == "reset role") {
        s.currentRole = s.originalRole;
        cout << "Role reset to " << s.currentRole << endl;
        return false;
    }
    if (sql == "reset session authorization") {
        string authUser = s.authenticatedUser.empty() ? s.username : s.authenticatedUser;
        int authPerm = s.authenticatedUser.empty() ? s.permission : s.authenticatedPermission;
        s.username = authUser;
        s.permission = authPerm;
        s.originalRole = authUser;
        s.currentRole.clear();
        cout << "RESET SESSION AUTHORIZATION" << endl;
        return false;
    }
    // RESET parameter | RESET ALL
    auto resetIsolation = [&]() {
        s.isolationLevel = 2;
        g_engine.setIsolationLevel(dbms::IsolationLevel::REPEATABLE_READ);
    };
    string rest = (sql == "reset all") ? "all" : trim(sql.substr(6));
    if (rest == "all") {
        string authUser = s.authenticatedUser.empty() ? s.username : s.authenticatedUser;
        int authPerm = s.authenticatedUser.empty() ? s.permission : s.authenticatedPermission;
        s.username = authUser;
        s.permission = authPerm;
        s.currentRole = s.originalRole;
        s.originalRole = authUser;
        s.currentRole.clear();
        s.timezoneOffsetMinutes = 0;
        s.statementTimeoutMs = s.defaultStatementTimeoutMs;
        s.constraintsDeferred = false;
        resetIsolation();
        cout << "RESET ALL" << endl;
        return false;
    }
    if (rest == "timezone" || rest == "time zone") {
        s.timezoneOffsetMinutes = 0;
        cout << "RESET " << rest << endl;
        return false;
    }
    if (rest == "statement_timeout" || rest == "statement_timeout_ms") {
        s.statementTimeoutMs = s.defaultStatementTimeoutMs;
        cout << "RESET " << rest << endl;
        return false;
    }
    if (rest == "transaction_isolation" || rest == "transaction isolation") {
        resetIsolation();
        cout << "RESET " << rest << endl;
        return false;
    }
    cout << "RESET currently supports ROLE, ALL, TIME ZONE, statement_timeout, and transaction_isolation" << endl;
    return true;
}

// ========================================================================
// SET / RESET / ALTER SYSTEM command helpers
// ========================================================================
static bool applyConfigParam(const string& param, const string& val, bool isGlobal, Session& s) {
    bool ok = false;
    if (param == "max_connections") {
        try { g_config.maxConnections = std::stoi(val); ok = true; } catch (...) {}
    } else if (param == "slow_query_threshold_ms" || param == "slow_query_threshold") {
        try { g_config.slowQueryThresholdMs = std::stod(val); ok = true; } catch (...) {}
    } else if (param == "checkpoint_interval") {
        try { g_config.checkpointInterval = std::stoi(val); ok = true; } catch (...) {}
    } else if (param == "statement_timeout_ms" || param == "statement_timeout") {
        try { g_config.statementTimeoutMs = std::stoi(val); s.statementTimeoutMs = g_config.statementTimeoutMs; ok = true; } catch (...) {}
    } else if (param == "buffer_pool_frames") {
        try { g_config.bufferPoolFrames = static_cast<size_t>(std::stoull(val)); ok = true; } catch (...) {}
    } else if (param == "enable_query_plan_cache") {
        g_config.enableQueryPlanCache = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "query_plan_cache_size") {
        try { g_config.queryPlanCacheSize = static_cast<size_t>(std::stoull(val)); ok = true; } catch (...) {}
    } else if (param == "password_policy_level") {
        try { g_config.passwordPolicyLevel = std::stoi(val); ok = true; } catch (...) {}
    } else if (param == "password_hash_algorithm") {
        g_config.passwordHashAlgorithm = val; ok = true;
    } else if (param == "audit_level") {
        try { g_config.auditLevel = std::stoi(val); ok = true; } catch (...) {}
    } else if (param == "auto_vacuum") {
        g_config.autoVacuumEnabled = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "auto_vacuum_threshold") {
        try { g_config.autoVacuumThreshold = std::stoi(val); ok = true; } catch (...) {}
    } else if (param == "auto_analyze") {
        g_config.autoAnalyzeEnabled = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "auto_analyze_threshold") {
        try { g_config.autoAnalyzeThreshold = std::stoi(val); ok = true; } catch (...) {}
    } else if (param == "work_mem_kb" || param == "work_mem") {
        try { g_config.workMemKb = static_cast<size_t>(std::stoull(val)); ok = true; } catch (...) {}
    } else if (param == "enable_seq_scan") {
        g_config.enableSeqScan = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "enable_hash_join") {
        g_config.enableHashJoin = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "enable_merge_join") {
        g_config.enableMergeJoin = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "auto_explain") {
        g_config.autoExplainEnabled = (val == "1" || val == "true" || val == "on");
        ok = true;
    } else if (param == "auto_explain_threshold_ms" || param == "auto_explain_threshold") {
        try { g_config.autoExplainThresholdMs = std::stod(val); ok = true; } catch (...) {}
    } else if (param == "lock_timeout_ms" || param == "lock_timeout") {
        try { g_config.lockTimeoutMs = std::stoi(val); g_engine.getLockManager().setLockTimeout(g_config.lockTimeoutMs); ok = true; } catch (...) {}
    } else if (param == "deadlock_timeout_ms" || param == "deadlock_timeout") {
        try { g_config.deadlockTimeoutMs = std::stoi(val); g_engine.getLockManager().setDeadlockTimeout(g_config.deadlockTimeoutMs); ok = true; } catch (...) {}
    } else {
        cout << "Unknown parameter: " << param << endl;
        return true;
    }
    if (!ok) {
        cout << "Invalid value for parameter " << param << endl;
        return true;
    }
    if (isGlobal) {
        if (!g_config.save("dbms.conf")) {
            cout << "Failed to persist configuration" << endl;
            return true;
        }
        cout << "Set global " << param << " = " << val << endl;
    } else {
        cout << "Set " << param << " = " << val << " (session)" << endl;
    }
    return false;
}

static bool handleSetCommand(const string& sql, Session& s) {
    // SET ROLE rolename
    if (sql.substr(0, 9) == "set role ") {
        string roleName = trim(sql.substr(9));
        if (roleName.empty()) {
            cout << "SQL syntax error: SET ROLE role_name" << endl;
            return true;
        }
        s.currentRole = roleName;
        cout << "Role set to " << roleName << endl;
        return false;
    }

    // SET SESSION AUTHORIZATION user_name | DEFAULT
    if (sql.substr(0, 25) == "set session authorization") {
        return setSessionAuthorization(s, sql.substr(25));
    }

    // SET CONSTRAINTS { ALL | constraint_name [, ...] } { DEFERRED | IMMEDIATE }
    if (sql.substr(0, 15) == "set constraints") {
        string rest = trim(sql.substr(15));
        if (rest.empty()) {
            cout << "SQL syntax error: SET CONSTRAINTS name [, ...] IMMEDIATE|DEFERRED" << endl;
            return true;
        }
        bool deferred = false;
        bool immediate = false;
        if (rest.size() >= 8 && rest.substr(rest.size() - 8) == "deferred") {
            deferred = true;
        } else if (rest.size() >= 9 && rest.substr(rest.size() - 9) == "immediate") {
            immediate = true;
        }
        if (!deferred && !immediate) {
            cout << "SQL syntax error: SET CONSTRAINTS name [, ...] IMMEDIATE|DEFERRED" << endl;
            return true;
        }
        s.constraintsDeferred = deferred;
        cout << "SET CONSTRAINTS" << endl;
        return false;
    }

    // SET TRANSACTION ISOLATION LEVEL (must come before generic SET)
    if (sql.substr(0, 25) == "set transaction isolation" ||
        sql.substr(0, 31) == "set transaction isolation level") {
        string rawRest;
        if (sql.substr(0, 31) == "set transaction isolation level") {
            rawRest = sql.substr(31);
        } else {
            rawRest = sql.substr(25);
        }
        string rest = trim(rawRest);
        if (rest.find("read uncommitted") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::READ_UNCOMMITTED);
            s.isolationLevel = 0;
            cout << "Isolation level set to READ UNCOMMITTED" << endl;
        } else if (rest.find("read committed") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::READ_COMMITTED);
            s.isolationLevel = 1;
            cout << "Isolation level set to READ COMMITTED" << endl;
        } else if (rest.find("repeatable read") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::REPEATABLE_READ);
            s.isolationLevel = 2;
            cout << "Isolation level set to REPEATABLE READ" << endl;
        } else if (rest.find("serializable") != string::npos) {
            g_engine.setIsolationLevel(dbms::IsolationLevel::SERIALIZABLE);
            s.isolationLevel = 3;
            cout << "Isolation level set to SERIALIZABLE" << endl;
        } else {
            cout << "Unknown isolation level" << endl;
            return true;
        }
        return false;
    }

    // SET TIMEZONE = '+08:00' | SET TIME ZONE '+08:00' | SET TIME ZONE 'UTC'
    if (sql.substr(0, 13) == "set timezone " || sql.substr(0, 14) == "set time zone ") {
        size_t off = (sql.substr(0, 13) == "set timezone ") ? 13 : 14;
        string tzVal = trim(sql.substr(off));
        if (!tzVal.empty() && tzVal[0] == '=') tzVal = trim(tzVal.substr(1));
        s.timezoneOffsetMinutes = parseTimezoneOffset(tzVal);
        int absOff = std::abs(s.timezoneOffsetMinutes);
        int tzh = absOff / 60;
        int tzm = absOff % 60;
        std::string sign = (s.timezoneOffsetMinutes >= 0) ? "+" : "-";
        char buf[32];
        snprintf(buf, sizeof(buf), "%s%02d:%02d", sign.c_str(), tzh, tzm);
        cout << "Timezone set to UTC" << buf << " (" << tzVal << ")" << endl;
        return false;
    }

    // SET parameter = value  (session-level, non-persistent)
    // SET GLOBAL parameter = value (persistent to dbms.conf)
    if (sql.substr(0, 3) == "set" && sql.size() > 3 && isspace(static_cast<unsigned char>(sql[3]))) {
        string rest = trim(sql.substr(3));
        bool isGlobal = false;
        if (rest.size() > 7 && rest.substr(0, 7) == "global ") {
            isGlobal = true;
            rest = trim(rest.substr(7));
        }
        size_t eqPos = rest.find('=');
        if (eqPos == string::npos) {
            cout << "SQL syntax error: SET [GLOBAL] parameter = value" << endl;
            return true;
        }
        string param = trim(rest.substr(0, eqPos));
        string val = trim(rest.substr(eqPos + 1));
        // User-defined variable: SET @var = value
        if (!param.empty() && param[0] == '@') {
            s.userVariables[param.substr(1)] = val;
            cout << "Set variable " << param << " = " << val << endl;
            return false;
        }
        return applyConfigParam(param, val, isGlobal, s);
    }

    // set auto_vacuum = on / off / threshold N (legacy compat)
    if (sql.substr(0, 15) == "set auto_vacuum") {
        string rest = trim(sql.substr(15));
        if (rest == "= on" || rest == "on" || rest == "= 1" || rest == "1") {
            g_config.autoVacuumEnabled = true;
            cout << "auto_vacuum set to ON" << endl;
        } else if (rest == "= off" || rest == "off" || rest == "= 0" || rest == "0") {
            g_config.autoVacuumEnabled = false;
            cout << "auto_vacuum set to OFF" << endl;
        } else if (rest.substr(0, 10) == "threshold " || rest.substr(0, 12) == "= threshold ") {
            size_t off = (rest.substr(0, 10) == "threshold ") ? 10 : 12;
            try {
                g_config.autoVacuumThreshold = std::stoi(trim(rest.substr(off)));
                cout << "auto_vacuum_threshold set to " << g_config.autoVacuumThreshold << endl;
            } catch (...) {
                cout << "Invalid threshold value" << endl;
                return true;
            }
        } else {
            cout << "Usage: SET auto_vacuum = ON|OFF|THRESHOLD N" << endl;
            return true;
        }
        return false;
    }

    cout << "SQL syntax error: unsupported SET command" << endl;
    return true;
}

static bool handleAlterSystem(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(12));
    if (rest.size() >= 3 && rest.substr(0, 3) == "set") {
        rest = trim(rest.substr(3));
        size_t eqPos = rest.find('=');
        if (eqPos == string::npos) {
            cout << "SQL syntax error: ALTER SYSTEM SET parameter = value" << endl;
            return true;
        }
        string param = trim(rest.substr(0, eqPos));
        string val = trim(rest.substr(eqPos + 1));
        return applyConfigParam(param, val, true, s);
    }
    cout << "SQL syntax error: ALTER SYSTEM SET parameter = value" << endl;
    return true;
}

// ========================================================================
// Utility command handlers
// ========================================================================
static bool handleCommentOn(const string& sql, Session& s, const string& rawSql) {
    if (!checkDB(s)) return true;
    size_t onPos = sql.find(" on ");
    if (onPos == string::npos) {
        cout << "SQL syntax error" << endl;
        return true;
    }
    string rest = trim(sql.substr(onPos + 4));
    auto extractComment = [&](const string& raw) -> string {
        size_t rawIsPos = raw.find(" is ");
        if (rawIsPos == string::npos) return "";
        string afterIs = trim(raw.substr(rawIsPos + 4));
        if (afterIs.size() >= 2 && ((afterIs.front() == '\'' && afterIs.back() == '\'') ||
                                           (afterIs.front() == '"' && afterIs.back() == '"'))) {
            return afterIs.substr(1, afterIs.size() - 2);
        }
        return afterIs;
    };
    if (rest.substr(0, 5) == "table") {
        string afterTable = trim(rest.substr(5));
        size_t isPos = afterTable.find(" is ");
        if (isPos == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = resolveTableName(s, trim(afterTable.substr(0, isPos)));
        string comment = extractComment(rawSql);
        auto res = g_engine.commentOnTable(s.currentDB, tname, comment);
        if (res == DBStatus::TABLE_NOT_FOUND) {
            cout << "Table not found" << endl;
            return true;
        }
        cout << "Comment added" << endl;
        return false;
    }
    if (rest.substr(0, 6) == "column") {
        string afterCol = trim(rest.substr(6));
        size_t isPos = afterCol.find(" is ");
        if (isPos == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string qual = trim(afterCol.substr(0, isPos));
        size_t dotPos = qual.find('.');
        if (dotPos == string::npos) {
            cout << "SQL syntax error: use table.column" << endl;
            return true;
        }
        string tname = resolveTableName(s, trim(qual.substr(0, dotPos)));
        string cname = trim(qual.substr(dotPos + 1));
        string comment = extractComment(rawSql);
        auto res = g_engine.commentOnColumn(s.currentDB, tname, cname, comment);
        if (res == DBStatus::TABLE_NOT_FOUND) {
            cout << "Table not found" << endl;
            return true;
        }
        cout << "Comment added" << endl;
        return false;
    }
    cout << "SQL syntax error" << endl;
    return true;
}

static bool handleLockTable(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(4));
    if (rest.substr(0, 5) == "table") rest = trim(rest.substr(5));
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 4) {
        cout << "SQL syntax error: LOCK TABLE tname IN SHARE|EXCLUSIVE MODE" << endl;
        return true;
    }
    string tname = resolveTableName(s, tokens[0]);
    if (!g_engine.tableExists(s.currentDB, tname)) {
        cout << "Table " << tname << " not exist" << endl;
        return true;
    }
    string mode = tokens[2];
    for (auto& c : mode) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
    bool ok = false;
    if (mode == "share") {
        ok = g_engine.getLockManager().lockShared(tname);
    } else if (mode == "exclusive") {
        ok = g_engine.getLockManager().lockExclusive(tname);
    }
    if (!ok) {
        cout << "Lock acquisition failed (deadlock detected)" << endl;
        return true;
    }
    cout << "Table " << tname << " locked in " << mode << " mode" << endl;
    return false;
}

static bool handleAnalyze(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(7));
    if (rest.empty() || rest == "all tables" || rest == "database") {
        auto tables = g_engine.getTableNames(s.currentDB);
        if (tables.empty()) {
            cout << "No tables to analyze in database " << s.currentDB << endl;
            return false;
        }
        for (const auto& tname : tables) {
            g_engine.analyzeTable(s.currentDB, tname);
            cout << "Table " << tname << " analyzed" << endl;
        }
        return false;
    }
    if (rest.substr(0, 5) != "table") {
        cout << "SQL syntax error" << endl;
        return true;
    }
    string afterTable = trim(rest.substr(5));
    size_t colsPos = afterTable.find(" columns ");
    if (colsPos != string::npos) {
        string tname = trim(afterTable.substr(0, colsPos));
        string colsPart = trim(afterTable.substr(colsPos + 9));
        if (!colsPart.empty() && colsPart.front() == '(') colsPart = colsPart.substr(1);
        if (!colsPart.empty() && colsPart.back() == ')') colsPart.pop_back();
        vector<string> colnames;
        stringstream css(colsPart);
        string c;
        while (getline(css, c, ',')) {
            string tc = trim(c);
            if (!tc.empty()) colnames.push_back(tc);
        }
        if (colnames.size() < 2) {
            cout << "Multi-column analyze requires at least 2 columns" << endl;
            return true;
        }
        g_engine.analyzeMultiColumn(s.currentDB, tname, colnames);
        string key;
        for (size_t i = 0; i < colnames.size(); ++i) {
            if (i > 0) key += ",";
            key += colnames[i];
        }
        cout << "Multi-column stats (" << key << ") for table " << tname << " analyzed" << endl;
        return false;
    }
    string tname = afterTable;
    g_engine.analyzeTable(s.currentDB, tname);
    cout << "Table " << tname << " analyzed" << endl;
    return false;
}

static bool handleRefreshMaterializedView(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(6));
    if (rest.substr(0, 20) == "materialized view ") {
        rest = trim(rest.substr(20));
        if (rest.substr(0, 12) == "concurrently") {
            rest = trim(rest.substr(12));
        }
        string viewname = rest;
        if (!g_engine.isMaterializedView(s.currentDB, viewname)) {
            cout << "Materialized view " << viewname << " not found" << endl;
            return true;
        }
        string viewSql = g_engine.getMaterializedViewSQL(s.currentDB, viewname);
        if (viewSql.empty()) {
            cout << "Failed to read materialized view SQL" << endl;
            return true;
        }
        string lsql = toLower(viewSql);
        size_t fromPos = lsql.find(" from ");
        if (fromPos == string::npos) {
            cout << "Invalid materialized view SQL" << endl;
            return true;
        }
        string colsPart = trim(viewSql.substr(6, fromPos - 6));
        vector<string> colNames;
        if (colsPart == "*") {
            cout << "Materialized view with SELECT * not supported, use explicit columns" << endl;
            return true;
        }
        size_t cp = 0;
        while (cp < colsPart.size()) {
            size_t comma = colsPart.find(',', cp);
            string c = trim(colsPart.substr(cp, comma - cp));
            size_t aliasPos = toLower(c).find(" as ");
            if (aliasPos != string::npos) {
                c = trim(c.substr(aliasPos + 4));
            } else {
                size_t sp = c.find(' ');
                if (sp != string::npos) {
                    string before = trim(c.substr(0, sp));
                    string after = trim(c.substr(sp + 1));
                    if (after != "" && after.find_first_of("+-*/=<>") == string::npos) {
                        c = after;
                    } else {
                        c = before;
                    }
                }
            }
            size_t dotPos = c.find('.');
            if (dotPos != string::npos) c = c.substr(dotPos + 1);
            colNames.push_back(c);
            if (comma == string::npos) break;
            cp = comma + 1;
        }
        string inner = trim(viewSql.substr(fromPos + 5));
        size_t wPos = toLower(inner).find(" where ");
        size_t oPos = toLower(inner).find(" order by ");
        size_t lPos = toLower(inner).find(" limit ");
        string tname = trim(inner.substr(0,
            min(wPos != string::npos ? wPos : inner.size(),
                min(oPos != string::npos ? oPos : inner.size(),
                    lPos != string::npos ? lPos : inner.size()))));
        tname = resolveTableName(s, tname);
        vector<string> conds;
        if (wPos != string::npos) {
            size_t condEnd = min(oPos != string::npos ? oPos : inner.size(),
                                lPos != string::npos ? lPos : inner.size());
            string condStr = normalizeConditionStr(trim(inner.substr(wPos + 6, condEnd - wPos - 6)));
            if (!condStr.empty()) {
                vector<string> rawConds = splitConds(condStr);
                for (auto& c : rawConds) {
                    string mc = modifyLogic(c);
                    if (!mc.empty()) conds.push_back(mc);
                }
            }
        }
        auto results = g_engine.query(s.currentDB, tname, conds, {}, {}, false, false);
        string backingTable = dbms::StorageEngine::materializedViewPrefix(viewname);
        if (g_engine.tableExists(s.currentDB, backingTable)) {
            g_engine.truncateTable(s.currentDB, backingTable);
        }
        int inserted = 0;
        for (const auto& row : results) {
            stringstream ss(row);
            map<string, string> values;
            for (const auto& cname : colNames) {
                string val;
                ss >> val;
                values[cname] = val;
            }
            auto res = g_engine.insert(s.currentDB, backingTable, values);
            if (res == DBStatus::OK) ++inserted;
        }
        cout << "Materialized view " << viewname << " refreshed (" << inserted << " rows)" << endl;
        return false;
    }
    cout << "SQL syntax error: REFRESH MATERIALIZED VIEW [CONCURRENTLY] viewname" << endl;
    return true;
}

static bool handleCheckpoint(const string& sql, Session& s) {
    (void)sql;
    if (!checkDB(s)) return true;
    g_engine.checkpoint(s.currentDB);
    cout << "Checkpoint completed" << endl;
    log(s.username, "checkpoint", getTime());
    return false;
}

static bool handleVacuum(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    if (sql.size() >= 11 && sql.substr(0, 11) == "vacuum full") {
        string tname = trim(sql.substr(11));
        if (tname.empty()) {
            cout << "VACUUM FULL requires a table name" << endl;
            return true;
        }
        string resolvedName = resolveTableName(s, tname);
        size_t n = g_engine.vacuumFull(s.currentDB, resolvedName);
        cout << "VACUUM FULL completed, " << n << " rows rewritten" << endl;
        return false;
    }
    string rest = trim(sql.substr(6));
    bool concurrent = false;
    if (rest.substr(0, 12) == "concurrently") {
        concurrent = true;
        rest = trim(rest.substr(12));
    }
    if (rest.empty()) {
        auto tables = g_engine.getTableNames(s.currentDB);
        size_t totalFreed = 0;
        for (const auto& tbl : tables) {
            totalFreed += g_engine.vacuum(s.currentDB, tbl, concurrent);
        }
        string mode = concurrent ? " CONCURRENTLY" : "";
        cout << "VACUUM" << mode << " completed, " << totalFreed << " pages freed" << endl;
    } else {
        string resolvedName = resolveTableName(s, rest);
        size_t freed = g_engine.vacuum(s.currentDB, resolvedName, concurrent);
        string mode = concurrent ? " CONCURRENTLY" : "";
        cout << "VACUUM" << mode << " completed, " << freed << " pages freed" << endl;
    }
    return false;
}

static bool handleSecurityLabel(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(15));
    if (rest.substr(0, 3) != "on ") {
        cout << "SQL syntax error: SECURITY LABEL ON object_type object_name IS 'label'" << endl;
        return true;
    }
    rest = trim(rest.substr(3));
    size_t isPos = rest.find(" is ");
    if (isPos == string::npos) {
        cout << "SQL syntax error: SECURITY LABEL ON object IS 'label'" << endl;
        return true;
    }
    string objPart = trim(rest.substr(0, isPos));
    string labelPart = trim(rest.substr(isPos + 4));
    if (labelPart.size() >= 2 && labelPart.front() == '\'' && labelPart.back() == '\'') {
        labelPart = labelPart.substr(1, labelPart.size() - 2);
    }
    if (labelPart == "NULL" || labelPart == "null") {
        labelPart.clear();
    }
    string objType = "table";
    string objName = objPart;
    size_t sp = objPart.find(' ');
    if (sp != string::npos) {
        objType = objPart.substr(0, sp);
        objName = trim(objPart.substr(sp + 1));
    }
    string resolvedName = resolveTableName(s, objName);
    if (objType == "table" && !g_engine.tableExists(s.currentDB, resolvedName)) {
        cout << "Table " << resolvedName << " not exist" << endl;
        return true;
    }
    g_engine.setSecurityLabel(s.currentDB, objType, resolvedName, labelPart);
    if (labelPart.empty()) {
        cout << "Security label removed from " << objType << " " << resolvedName << endl;
    } else {
        cout << "Security label set on " << objType << " " << resolvedName << endl;
    }
    return false;
}

static bool handleTruncate(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    if (g_engine.inTransaction()) {
        g_engine.commitTransaction();
        cout << "Note: DDL caused implicit commit of open transaction" << endl;
    }
    string rest = trim(sql.substr(8));
    if (rest.substr(0, 5) == "table") rest = trim(rest.substr(5));
    bool restartIdentity = (rest.find("restart identity") != string::npos);
    bool cascade = (rest.find("cascade") != string::npos);
    size_t optPos = rest.find(' ');
    string tname = rest;
    if (optPos != string::npos) {
        tname = trim(rest.substr(0, optPos));
    }
    tname = resolveTableName(s, tname);
    auto res = g_engine.truncateTable(s.currentDB, tname);
    if (res == DBStatus::TABLE_NOT_FOUND) {
        cout << "Table not exist" << endl;
        return true;
    }
    if (restartIdentity) {
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isAutoIncrement) {
                g_engine.resetSequence(s.currentDB, tname, tbl.cols[i].dataName, 1);
            }
        }
    }
    if (cascade) {
        vector<string> allTables = g_engine.getTableNames(s.currentDB);
        for (const string& otherTname : allTables) {
            if (otherTname == tname) continue;
            TableSchema otherTbl = g_engine.getTableSchema(s.currentDB, otherTname);
            for (size_t fi = 0; fi < otherTbl.fkLen; ++fi) {
                if (otherTbl.fks[fi].refTable == tname) {
                    g_engine.truncateTable(s.currentDB, otherTname);
                    break;
                }
            }
        }
    }
    cout << "TRUNCATE TABLE " << tname << " completed" << endl;
    log(s.username, "truncate table " + tname, getTime());
    return false;
}

static bool handleReindex(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(7));
    if (rest.substr(0, 5) == "table") {
        string tname = trim(rest.substr(5));
        tname = resolveTableName(s, tname);
        if (!g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        auto res = g_engine.reindex(s.currentDB, tname);
        if (res != DBStatus::OK) {
            cout << "Reindex failed" << endl;
            return true;
        }
        cout << "Reindex succeeded" << endl;
        log(s.username, "reindex table " + tname, getTime());
        return false;
    }
    cout << "SQL syntax error: REINDEX TABLE tablename" << endl;
    return true;
}

static bool handleCallProcedure(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(4));
    if (rest.empty()) {
        cout << "SQL syntax error: CALL procedure_name" << endl;
        return true;
    }
    string procname;
    vector<string> args;
    size_t lp = rest.find('(');
    if (lp != string::npos) {
        procname = trim(rest.substr(0, lp));
        size_t rp = string::npos;
        int depth = 0;
        bool inQuote = false;
        for (size_t i = lp; i < rest.size(); ++i) {
            if (rest[i] == '\'') inQuote = !inQuote;
            if (!inQuote) {
                if (rest[i] == '(') ++depth;
                else if (rest[i] == ')') {
                    --depth;
                    if (depth == 0) { rp = i; break; }
                }
            }
        }
        if (rp == string::npos) {
            cout << "SQL syntax error: unclosed parenthesis in CALL" << endl;
            return true;
        }
        string alist = trim(rest.substr(lp + 1, rp - lp - 1));
        if (!alist.empty()) {
            args = splitValues(alist);
            for (auto& a : args) a = trim(a);
        }
    } else {
        procname = rest;
    }
    if (!g_engine.procedureExists(s.currentDB, procname)) {
        cout << "Procedure " << procname << " not exist" << endl;
        return true;
    }
    auto params = g_engine.getProcedureParams(s.currentDB, procname);
    if (args.size() != params.size()) {
        cout << "SQL syntax error: procedure " << procname << " expects "
             << params.size() << " arguments, got " << args.size() << endl;
        return true;
    }
    unordered_map<string, string> argMap;
    for (size_t i = 0; i < params.size(); ++i) {
        const auto& pp = params[i];
        const string& arg = args[i];
        if (pp.mode == "OUT") {
            if (arg.empty() || arg[0] != '@') {
                cout << "SQL syntax error: OUT parameter must be a variable reference (@var)" << endl;
                return true;
            }
            argMap[pp.name] = arg;
        } else if (pp.mode == "INOUT") {
            if (arg.empty() || arg[0] != '@') {
                cout << "SQL syntax error: INOUT parameter must be a variable reference (@var)" << endl;
                return true;
            }
            argMap[pp.name] = arg;
        } else {
            if (!arg.empty() && arg[0] == '@') {
                auto it = s.userVariables.find(arg.substr(1));
                argMap[pp.name] = (it != s.userVariables.end()) ? it->second : "null";
            } else {
                argMap[pp.name] = arg;
            }
        }
    }
    auto stmts = g_engine.getProcedureStatements(s.currentDB, procname);
    for (const auto& stmt : stmts) {
        string replaced = stmt;
        vector<pair<string, string>> sortedMap(argMap.begin(), argMap.end());
        sort(sortedMap.begin(), sortedMap.end(),
             [](const auto& a, const auto& b) { return a.first.size() > b.first.size(); });
        for (const auto& kv : sortedMap) {
            string placeholder = "?" + kv.first;
            size_t pos = 0;
            while ((pos = replaced.find(placeholder, pos)) != string::npos) {
                replaced.replace(pos, placeholder.size(), kv.second);
                pos += kv.second.size();
            }
        }
        execute(replaced, s);
    }
    return false;
}

static bool handleCopy(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(4));
    size_t fromPos = rest.find(" from ");
    size_t toPos = rest.find(" to ");
    if (fromPos != string::npos) {
        string tname = trim(rest.substr(0, fromPos));
        tname = resolveTableName(s, tname);
        string fileRest = trim(rest.substr(fromPos + 6));
        size_t q1 = fileRest.find('\'');
        size_t q2 = fileRest.find('\'', q1 + 1);
        if (q1 == string::npos || q2 == string::npos) {
            cout << "SQL syntax error: COPY FROM requires 'filename'" << endl;
            return true;
        }
        string filename = fileRest.substr(q1 + 1, q2 - q1 - 1);
        if (!g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        ifstream csvIn(filename);
        if (!csvIn) {
            cout << "Cannot open file: " << filename << endl;
            return true;
        }
        size_t imported = 0, skipped = 0;
        string line;
        bool firstLine = true;
        while (getline(csvIn, line)) {
            if (trim(line).empty()) continue;
            auto fields = parseCSVLine(line);
            if (fields.size() != tbl.len) {
                if (firstLine) { firstLine = false; continue; }
                skipped++;
                continue;
            }
            firstLine = false;
            map<string, string> values;
            for (size_t i = 0; i < tbl.len; ++i) {
                values[tbl.cols[i].dataName] = trim(fields[i]);
            }
            auto res = g_engine.insert(s.currentDB, tname, values);
            if (res == DBStatus::OK) imported++;
            else skipped++;
        }
        cout << "COPY " << imported << " rows imported, " << skipped << " skipped" << endl;
        return false;
    }
    if (toPos != string::npos) {
        string tname = trim(rest.substr(0, toPos));
        tname = resolveTableName(s, tname);
        string fileRest = trim(rest.substr(toPos + 4));
        size_t q1 = fileRest.find('\'');
        size_t q2 = fileRest.find('\'', q1 + 1);
        if (q1 == string::npos || q2 == string::npos) {
            cout << "SQL syntax error: COPY TO requires 'filename'" << endl;
            return true;
        }
        string filename = fileRest.substr(q1 + 1, q2 - q1 - 1);
        if (!g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        ofstream csvOut(filename);
        if (!csvOut) {
            cout << "Cannot write file: " << filename << endl;
            return true;
        }
        for (size_t i = 0; i < tbl.len; ++i) {
            if (i > 0) csvOut << ",";
            csvOut << tbl.cols[i].dataName;
        }
        csvOut << "\n";
        size_t exported = 0;
        g_engine.forEachRow(s.currentDB, tname, [&](uint32_t, uint16_t, const char* data, size_t len) {
            std::string row(data, len);
            for (size_t i = 0; i < tbl.len; ++i) {
                if (i > 0) csvOut << ",";
                string val = dbms::StorageEngine::extractColumnValueStatic(row, tbl, i);
                bool needQuote = (val.find(',') != string::npos || val.find('"') != string::npos || val.find('\n') != string::npos);
                if (needQuote) {
                    csvOut << '"';
                    for (char c : val) {
                        if (c == '"') csvOut << "\"\"";
                        else csvOut << c;
                    }
                    csvOut << '"';
                } else {
                    csvOut << val;
                }
            }
            csvOut << "\n";
            ++exported;
        });
        cout << "COPY " << exported << " rows exported to " << filename << endl;
        return false;
    }
    cout << "SQL syntax error: COPY table_name FROM 'file' | TO 'file'" << endl;
    return true;
}

static bool handlePrepare(const string& sql, Session& s) {
    string rest = trim(sql.substr(8));
    if (rest.substr(0, 12) == "transaction ") {
        string xid = stripQuotes(trim(rest.substr(12)));
        if (xid.empty()) {
            cout << "SQL syntax error: PREPARE TRANSACTION requires a transaction ID" << endl;
            return true;
        }
        auto res = g_engine.prepareTransaction(xid);
        if (res == DBStatus::OK) {
            cout << "PREPARE TRANSACTION " << xid << endl;
            log(s.username, "prepare transaction " + xid, getTime());
        } else if (res == DBStatus::INVALID_VALUE) {
            cout << "No active transaction" << endl;
        } else if (res == DBStatus::DUPLICATE_KEY) {
            cout << "Transaction ID already exists" << endl;
        } else {
            cout << "PREPARE TRANSACTION failed" << endl;
        }
        return false;
    }
    size_t fromPos = rest.find(" from ");
    if (fromPos == string::npos) {
        cout << "SQL syntax error: expected FROM" << endl;
        return true;
    }
    string stmtName = trim(rest.substr(0, fromPos));
    string templateSql = trim(rest.substr(fromPos + 6));
    templateSql = stripQuotes(templateSql);
    if (templateSql.empty()) {
        cout << "SQL syntax error: empty statement" << endl;
        return true;
    }
    s.preparedStmts[stmtName] = templateSql;
    cout << "Statement " << stmtName << " prepared" << endl;
    return false;
}

static bool handleExecutePrepared(const string& sql, Session& s) {
    string rest = trim(sql.substr(8));
    size_t usingPos = rest.find(" using ");
    string stmtName, usingClause;
    if (usingPos == string::npos) {
        stmtName = rest;
    } else {
        stmtName = trim(rest.substr(0, usingPos));
        usingClause = trim(rest.substr(usingPos + 7));
    }
    auto it = s.preparedStmts.find(stmtName);
    if (it == s.preparedStmts.end()) {
        cout << "Prepared statement " << stmtName << " not found" << endl;
        return true;
    }
    string expanded = it->second;
    if (!usingClause.empty()) {
        if (usingClause.size() >= 2 && usingClause.front() == '(' && usingClause.back() == ')') {
            usingClause = trim(usingClause.substr(1, usingClause.size() - 2));
        }
        stringstream vss(usingClause);
        string val;
        vector<string> values;
        while (getline(vss, val, ',')) values.push_back(trim(val));
        size_t vidx = 0;
        size_t pos = 0;
        while ((pos = expanded.find('?', pos)) != string::npos) {
            if (vidx >= values.size()) {
                cout << "Not enough parameters for prepared statement" << endl;
                return true;
            }
            expanded = expanded.substr(0, pos) + values[vidx] + expanded.substr(pos + 1);
            pos += values[vidx].size();
            ++vidx;
        }
        if (vidx < values.size()) {
            cout << "Too many parameters for prepared statement" << endl;
            return true;
        }
    }
    return execute(expanded, s);
}

static bool handleDeallocate(const string& sql, Session& s) {
    string stmtName = trim(sql.substr(19));
    if (s.preparedStmts.erase(stmtName)) {
        cout << "Statement " << stmtName << " deallocated" << endl;
    } else {
        cout << "Prepared statement " << stmtName << " not found" << endl;
    }
    return false;
}

static bool handleExplain(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    dbms::QueryPlanner::ExplainOptions opts;
    bool isJson = false;
    string rest = trim(sql.substr(7));
    if (!rest.empty() && rest.front() == '(') {
        size_t closeParen = rest.find(')');
        if (closeParen != string::npos) {
            string optStr = trim(rest.substr(1, closeParen - 1));
            rest = trim(rest.substr(closeParen + 1));
            stringstream oss(optStr);
            string opt;
            while (getline(oss, opt, ',')) {
                string o = trim(opt);
                size_t sp = o.find(' ');
                string name = (sp == string::npos) ? o : trim(o.substr(0, sp));
                string val = (sp == string::npos) ? "true" : trim(o.substr(sp + 1));
                bool on = (val != "false" && val != "0" && val != "off");
                if (name == "analyze") opts.analyze = on;
                else if (name == "buffers") opts.buffers = on;
                else if (name == "timing") opts.timing = on;
                else if (name == "costs") opts.costs = on;
                else if (name == "settings") opts.settings = on;
                else if (name == "verbose") opts.verbose = on;
                else if (name == "format" && val == "json") isJson = true;
            }
        }
    } else {
        if (rest.size() >= 8 && rest.substr(0, 8) == "analyze ") {
            opts.analyze = true;
            rest = trim(rest.substr(8));
        }
        if (rest.size() >= 8 && rest.substr(0, 8) == "buffers ") {
            opts.buffers = true;
            rest = trim(rest.substr(8));
        }
        if (rest.size() >= 8 && rest.substr(0, 8) == "verbose ") {
            opts.verbose = true;
            rest = trim(rest.substr(8));
        }
        if (rest.size() >= 11 && rest.substr(0, 11) == "format json") {
            isJson = true;
            rest = trim(rest.substr(11));
        }
    }
    string inner = rest;
    if (inner.size() < 6 || inner.substr(0, 6) != "select") {
        cout << "EXPLAIN only supports SELECT" << endl;
        return true;
    }
    size_t fromPos = inner.find("from");
    if (fromPos == string::npos) {
        cout << "SQL syntax error" << endl;
        return true;
    }
    string columns = trim(inner.substr(6, fromPos - 6));
    bool isDistinct = false;
    if (columns.size() >= 9 && columns.substr(0, 9) == "distinct ") {
        isDistinct = true;
        columns = trim(columns.substr(9));
    }
    size_t wherePos = inner.find("where", fromPos);
    size_t orderPos = inner.find("order by", fromPos);
    size_t limitPos = inner.find("limit", fromPos);
    string tname = trim(inner.substr(fromPos + 4,
        (wherePos != string::npos) ? (wherePos - fromPos - 4)
        : (orderPos != string::npos) ? (orderPos - fromPos - 4)
        : (limitPos != string::npos) ? (limitPos - fromPos - 4)
        : (inner.size() - fromPos - 4)));
    vector<string> conds;
    if (wherePos != string::npos) {
        size_t condEnd = (orderPos != string::npos) ? orderPos
                       : (limitPos != string::npos) ? limitPos
                       : inner.size();
        string condStr = normalizeConditionStr(trim(inner.substr(wherePos + 5, condEnd - wherePos - 5)));
        if (!condStr.empty()) {
            vector<string> rawConds = splitConds(condStr);
            for (auto& c : rawConds) {
                string mc = modifyLogic(c);
                if (!mc.empty()) conds.push_back(mc);
            }
        }
    }
    string orderByCol = "";
    bool orderByAsc = true;
    if (orderPos != string::npos) {
        size_t sortEnd = (limitPos != string::npos) ? limitPos : inner.size();
        string sortStr = trim(inner.substr(orderPos + 8, sortEnd - orderPos - 8));
        if (!sortStr.empty()) {
            if (sortStr.size() >= 5 && sortStr.substr(sortStr.size() - 4) == "desc") {
                orderByCol = trim(sortStr.substr(0, sortStr.size() - 4));
                orderByAsc = false;
            } else {
                orderByCol = trim(sortStr);
            }
        }
    }
    size_t limitVal = 0;
    if (limitPos != string::npos) {
        string lstr = trim(inner.substr(limitPos + 5));
        try { limitVal = static_cast<size_t>(std::stoull(lstr)); } catch (...) {}
    }
    set<string> selectCols;
    bool selectAll = (columns == "*");
    if (!selectAll) {
        for (const auto& item : splitSelectColumns(columns)) {
            selectCols.insert(trim(item));
        }
    }
    dbms::PlanContext ctx;
    ctx.dbname = s.currentDB;
    ctx.tablename = tname;
    ctx.conds = dbms::StorageEngine::parseConditions(conds);
    ctx.selectCols = selectCols;
    ctx.orderByCol = orderByCol;
    ctx.orderByAsc = orderByAsc;
    ctx.limit = limitVal;
    ctx.distinct = isDistinct;

    string cacheKey = s.currentDB + "::" + inner;
    if (opts.buffers) cacheKey += ":B";
    if (opts.verbose) cacheKey += ":V";
    if (isJson) cacheKey += ":J";
    if (opts.analyze) cacheKey += ":A";
    if (opts.timing) cacheKey += ":T";
    if (opts.costs) cacheKey += ":C";
    if (opts.settings) cacheKey += ":S";
    string planOutput;
    bool cacheHit = false;
    {
        std::lock_guard<std::mutex> lock(g_planCacheMutex);
        auto it = g_queryPlanCache.find(cacheKey);
        if (it != g_queryPlanCache.end() && it->second.dbname == s.currentDB) {
            planOutput = it->second.planText;
            it->second.cachedAt = std::chrono::steady_clock::now();
            cacheHit = true;
            ++g_planCacheHits;
        } else {
            ++g_planCacheMisses;
        }
    }

    if (!cacheHit) {
        auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
        if (isJson) {
            planOutput = dbms::QueryPlanner::explainJson(plan, &g_engine, s.currentDB, opts);
        } else {
            planOutput = dbms::QueryPlanner::explain(plan, &g_engine, s.currentDB, opts);
        }
        {
            std::lock_guard<std::mutex> lock(g_planCacheMutex);
            if (g_queryPlanCache.size() >= 100) {
                auto oldest = g_queryPlanCache.begin();
                for (auto it = g_queryPlanCache.begin(); it != g_queryPlanCache.end(); ++it) {
                    if (it->second.cachedAt < oldest->second.cachedAt) oldest = it;
                }
                g_queryPlanCache.erase(oldest);
            }
            g_queryPlanCache[cacheKey] = {planOutput, s.currentDB, std::chrono::steady_clock::now()};
        }
    }

    cout << planOutput;
    if (cacheHit) cout << "\n[plan cache hit]";

    if (opts.analyze) {
        auto execStart = std::chrono::steady_clock::now();
        size_t actualRows = 0;
        auto answers = g_engine.query(s.currentDB, tname, conds, selectCols,
            {dbms::StorageEngine::OrderBySpec{orderByCol, orderByAsc}}, false);
        actualRows = answers.size();
        auto execEnd = std::chrono::steady_clock::now();
        double execMs = std::chrono::duration<double, std::milli>(execEnd - execStart).count();
        cout << "\n--- ANALYZE ---\n";
        if (opts.timing) {
            cout << "Actual time: " << std::fixed << std::setprecision(3) << execMs << " ms\n";
        }
        cout << "Actual rows: " << actualRows << "\n";
    }
    return false;
}

// ========================================================================
// Window function support
// ========================================================================
struct WindowFunc {
    string name;
    string arg;
    string orderByCol;
    bool orderByAsc;
    vector<string> partitionByCols;
    bool isAggregate = false;
    // Frame type: ROWS (default), RANGE, GROUPS
    enum class FrameType { ROWS, RANGE, GROUPS };
    FrameType frameType = FrameType::ROWS;
    // For ROWS: frameStartOffset / frameEndOffset are relative to current row index
    // For RANGE: they are value offsets from current row's ORDER BY value
    // For GROUPS: they are peer-group offsets
    // -1 means unbounded (partition start/end), 0 means current row / current value / current group
    int frameStartOffset = -1; // -1 = unbounded preceding, N = N preceding
    int frameEndOffset = 0;    // 0 = current row, N = N following, -1 = unbounded following
    bool hasFrame = false;
};

static bool parseWindowFunc(const string& item, WindowFunc& wf) {
    string low = toLower(item);
    size_t overPos = low.find("over");
    if (overPos == string::npos) return false;

    string funcPart = trim(item.substr(0, overPos));
    string overPart = trim(item.substr(overPos + 4));

    size_t lp = funcPart.find('(');
    size_t rp = funcPart.rfind(')');
    if (lp == string::npos || rp == string::npos) return false;
    wf.name = toLower(trim(funcPart.substr(0, lp)));
    wf.arg = trim(funcPart.substr(lp + 1, rp - lp - 1));

    // Detect aggregate window functions: sum, count, avg, max, min
    static const set<string> aggNames = {"sum", "count", "avg", "max", "min"};
    wf.isAggregate = aggNames.count(wf.name) != 0;

    size_t overLp = overPart.find('(');
    size_t overRp = overPart.rfind(')');
    if (overLp == string::npos || overRp == string::npos) return false;
    string overContent = trim(overPart.substr(overLp + 1, overRp - overLp - 1));

    string lowContent = toLower(overContent);
    size_t partPos = lowContent.find("partition by");
    size_t orderPos = lowContent.find("order by");

    // Parse PARTITION BY (optional)
    if (partPos != string::npos) {
        size_t partEnd = (orderPos != string::npos) ? orderPos : overContent.size();
        string partRest = trim(overContent.substr(partPos + 12, partEnd - partPos - 12));
        stringstream pss(partRest);
        string col;
        while (getline(pss, col, ',')) {
            string c = trim(col);
            if (!c.empty()) wf.partitionByCols.push_back(c);
        }
    }

    // PERCENT_RANK, CUME_DIST, NTH_VALUE require ORDER BY (set flag for validation later)
    if (wf.name == "percent_rank" || wf.name == "cume_dist" || wf.name == "nth_value") {
        wf.isAggregate = false; // treated as ranking functions
    }

    // ORDER BY is optional for window functions like row_number with partition only
    if (orderPos != string::npos) {
        string orderRest = trim(overContent.substr(orderPos + 8));
        // Detect frame clause: ROWS / RANGE / GROUPS BETWEEN
        string lowOrderRest = toLower(orderRest);
        size_t framePos = string::npos;
        string frameKeyword;
        for (const char* kw : {"rows", "range", "groups"}) {
            size_t fp = lowOrderRest.find(kw);
            if (fp != string::npos) {
                framePos = fp;
                frameKeyword = kw;
                break;
            }
        }
        if (framePos != string::npos) {
            string frameStr = trim(orderRest.substr(framePos));
            orderRest = trim(orderRest.substr(0, framePos));
            string lfs = toLower(frameStr);
            if (lfs.find("between") != string::npos) {
                wf.hasFrame = true;
                if (frameKeyword == "range") wf.frameType = WindowFunc::FrameType::RANGE;
                else if (frameKeyword == "groups") wf.frameType = WindowFunc::FrameType::GROUPS;
                else wf.frameType = WindowFunc::FrameType::ROWS;
                // start bound
                if (lfs.find("unbounded preceding") != string::npos) {
                    wf.frameStartOffset = -1;
                } else if (lfs.find("current row") != string::npos &&
                           lfs.find("current row") < lfs.find(" and ")) {
                    wf.frameStartOffset = 0;
                } else {
                    size_t precPos = lfs.find("preceding");
                    if (precPos != string::npos) {
                        string numStr;
                        for (size_t i = lfs.find("between") + 7; i < precPos; ++i) {
                            if (isdigit(static_cast<unsigned char>(lfs[i]))) numStr += lfs[i];
                        }
                        if (!numStr.empty()) wf.frameStartOffset = std::stoi(numStr);
                    }
                }
                // end bound
                if (lfs.find("unbounded following") != string::npos) {
                    wf.frameEndOffset = -1;
                } else if (lfs.find("current row") != string::npos) {
                    wf.frameEndOffset = 0;
                } else {
                    size_t follPos = lfs.find("following");
                    if (follPos != string::npos) {
                        string numStr;
                        for (size_t i = lfs.rfind(" and ", follPos) + 5; i < follPos; ++i) {
                            if (isdigit(static_cast<unsigned char>(lfs[i]))) numStr += lfs[i];
                        }
                        if (!numStr.empty()) wf.frameEndOffset = std::stoi(numStr);
                    }
                }
            }
        }
        vector<string> ot = tokenize(orderRest);
        if (!ot.empty()) {
            wf.orderByCol = ot[0];
            wf.orderByAsc = true;
            if (ot.size() > 1 && toLower(ot[1]) == "desc") wf.orderByAsc = false;
        }
    } else if (wf.partitionByCols.empty() && !wf.isAggregate) {
        // Neither PARTITION BY nor ORDER BY - invalid for non-aggregate window functions
        return false;
    }

    return true;
}

// ========================================================================
// Temporary table helpers
// ========================================================================
string tempTablePrefix(const string& name) { return "__tmp_" + name; }

string resolveTableName(Session& s, const string& name) {
    if (s.tempTables.count(name)) return tempTablePrefix(name);
    // Materialized views redirect to backing table
    if (g_engine.isMaterializedView(s.currentDB, name)) {
        return dbms::StorageEngine::materializedViewPrefix(name);
    }
    // Schema-qualified table: schema.table -> schema__table
    size_t dotPos = name.find('.');
    if (dotPos != string::npos && dotPos > 0 && dotPos + 1 < name.size()) {
        string schema = name.substr(0, dotPos);
        string table = name.substr(dotPos + 1);
        if (g_engine.schemaExists(s.currentDB, schema)) {
            return schema + "__" + table;
        }
    }
    return name;
}

static bool isTempTable(Session& s, const string& name) {
    return s.tempTables.count(name) != 0;
}

// ========================================================================
// INSTEAD OF trigger helper: execute trigger action on view
// Returns true if an INSTEAD OF trigger was executed, false otherwise
// ========================================================================
static bool executeInsteadOfTrigger(Session& s, const string& viewname,
                                     const string& event,
                                     const map<string, string>& newValues,
                                     const map<string, string>& oldValues) {
    if (!g_engine.viewExists(s.currentDB, viewname)) return false;
    auto triggers = g_engine.getTriggers(s.currentDB, viewname, "instead of", event);
    if (triggers.empty()) return false;
    for (const auto& trg : triggers) {
        string action = trg.action;
        // Substitute NEW.column (case-insensitive search)
        for (const auto& [col, val] : newValues) {
            string placeholderLower = "new." + col;
            string placeholderUpper = "NEW." + col;
            size_t pos = 0;
            while ((pos = action.find(placeholderLower, pos)) != string::npos) {
                action.replace(pos, placeholderLower.size(), val);
                pos += val.size();
            }
            pos = 0;
            while ((pos = action.find(placeholderUpper, pos)) != string::npos) {
                action.replace(pos, placeholderUpper.size(), val);
                pos += val.size();
            }
        }
        // Substitute OLD.column (case-insensitive search)
        for (const auto& [col, val] : oldValues) {
            string placeholderLower = "old." + col;
            string placeholderUpper = "OLD." + col;
            size_t pos = 0;
            while ((pos = action.find(placeholderLower, pos)) != string::npos) {
                action.replace(pos, placeholderLower.size(), val);
                pos += val.size();
            }
            pos = 0;
            while ((pos = action.find(placeholderUpper, pos)) != string::npos) {
                action.replace(pos, placeholderUpper.size(), val);
                pos += val.size();
            }
        }
        // Execute the action SQL via trigger executor
        g_engine.executeTriggerAction(action);
    }
    return true;
}

// ========================================================================
// Normalize condition string: remove spaces around operators so tokenize
// keeps each condition as a single token (e.g. "score > 80" → "score>80")
// ========================================================================
// ========================================================================
// Split condition string by " and " into individual conditions
// ========================================================================
static vector<string> splitConds(const string& s) {
    vector<string> result;
    size_t start = 0;
    while (start < s.size()) {
        size_t andPos = s.find(" and ", start);
        if (andPos == string::npos) {
            result.push_back(trim(s.substr(start)));
            break;
        }
        result.push_back(trim(s.substr(start, andPos - start)));
        start = andPos + 5;
    }
    return result;
}

// ========================================================================
// CSV parsing helpers
// ========================================================================
static vector<string> parseCSVLine(const string& line) {
    vector<string> fields;
    size_t i = 0;
    while (i < line.size()) {
        string field;
        if (line[i] == '"') {
            ++i; // skip opening quote
            while (i < line.size()) {
                if (line[i] == '"') {
                    if (i + 1 < line.size() && line[i + 1] == '"') {
                        field += '"';
                        i += 2;
                    } else {
                        ++i; // skip closing quote
                        break;
                    }
                } else {
                    field += line[i++];
                }
            }
        } else {
            while (i < line.size() && line[i] != ',') {
                field += line[i++];
            }
        }
        fields.push_back(field);
        if (i < line.size() && line[i] == ',') ++i;
    }
    return fields;
}

static string escapeCSVField(const string& val) {
    bool needsQuote = val.find(',') != string::npos ||
                      val.find('"') != string::npos ||
                      val.find('\n') != string::npos;
    if (!needsQuote) return val;
    string result = "\"";
    for (char c : val) {
        if (c == '"') result += "\"\"";
        else result += c;
    }
    result += '"';
    return result;
}

// Quote-aware comma split for INSERT VALUES
static vector<string> splitValues(const string& s) {
    vector<string> parts;
    size_t start = 0;
    bool inQuote = false;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i < s.size() && s[i] == '\'') inQuote = !inQuote;
        if (i == s.size() || (!inQuote && s[i] == ',')) {
            parts.push_back(trim(s.substr(start, i - start)));
            start = i + 1;
        }
    }
    return parts;
}

static vector<string> splitTopLevelComma(const string& s) {
    vector<string> parts;
    string current;
    int parenDepth = 0;
    bool inQuote = false;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'') {
            current += c;
            if (inQuote && i + 1 < s.size() && s[i + 1] == '\'') {
                current += s[++i];
                continue;
            }
            inQuote = !inQuote;
            continue;
        }
        if (!inQuote) {
            if (c == '(') {
                ++parenDepth;
            } else if (c == ')') {
                if (parenDepth > 0) --parenDepth;
            } else if (c == ',' && parenDepth == 0) {
                parts.push_back(trim(current));
                current.clear();
                continue;
            }
        }
        current += c;
    }
    if (!trim(current).empty() || !parts.empty()) parts.push_back(trim(current));
    return parts;
}

static vector<string> splitTopLevelSemicolon(const string& s) {
    vector<string> parts;
    string current;
    int parenDepth = 0;
    bool inQuote = false;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'') {
            current += c;
            if (inQuote && i + 1 < s.size() && s[i + 1] == '\'') {
                current += s[++i];
                continue;
            }
            inQuote = !inQuote;
            continue;
        }
        if (!inQuote) {
            if (c == '(') ++parenDepth;
            else if (c == ')' && parenDepth > 0) --parenDepth;
            else if (c == ';' && parenDepth == 0) {
                if (!trim(current).empty()) parts.push_back(trim(current));
                current.clear();
                continue;
            }
        }
        current += c;
    }
    if (!trim(current).empty()) parts.push_back(trim(current));
    return parts;
}

static vector<string> splitByDelimiter(const string& s, char delim) {
    vector<string> parts;
    size_t start = 0;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == delim) {
            parts.push_back(s.substr(start, i - start));
            start = i + 1;
        }
    }
    return parts;
}

static string joinStrings(const vector<string>& parts, const string& delim) {
    string out;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) out += delim;
        out += parts[i];
    }
    return out;
}

static bool startsWithKeyword(const string& s, const string& prefix) {
    if (s.size() < prefix.size() || s.compare(0, prefix.size(), prefix) != 0) return false;
    return s.size() == prefix.size() || isspace(static_cast<unsigned char>(s[prefix.size()]));
}

static bool isSqlIdentChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static size_t findTopLevelSqlKeyword(const string& sql, const string& kw, size_t startPos = 0) {
    int depth = 0;
    bool inStr = false;
    size_t klen = kw.size();
    for (size_t i = startPos; i < sql.size(); ++i) {
        char c = sql[i];
        if (inStr) {
            if (c == '\'') inStr = false;
            continue;
        }
        if (c == '\'') { inStr = true; continue; }
        if (c == '(') { depth++; continue; }
        if (c == ')') { if (depth > 0) depth--; continue; }
        if (depth == 0 && i + klen <= sql.size() && sql.compare(i, klen, kw) == 0) {
            bool leftOk = (i == 0) || !isSqlIdentChar(sql[i - 1]);
            bool rightOk = (i + klen == sql.size()) || !isSqlIdentChar(sql[i + klen]);
            if (leftOk && rightOk) return i;
        }
    }
    return string::npos;
}

static string stripTrailingDropBehavior(string s) {
    s = trim(s);
    for (const string kw : {" cascade", " restrict"}) {
        if (s.size() >= kw.size() && s.substr(s.size() - kw.size()) == kw) {
            return trim(s.substr(0, s.size() - kw.size()));
        }
    }
    return s;
}

static string catalogEscape(const string& s) {
    string out;
    for (char c : s) {
        if (c == '\\') out += "\\\\";
        else if (c == '\n') out += "\\n";
        else if (c == '|') out += "\\p";
        else out += c;
    }
    return out;
}

static string catalogUnescape(const string& s) {
    string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            char n = s[++i];
            if (n == 'n') out += '\n';
            else if (n == 'p') out += '|';
            else out += n;
        } else {
            out += s[i];
        }
    }
    return out;
}

struct TablespaceInfo {
    string name;
    string owner;
    string location;
    string options;
};

static std::filesystem::path tablespaceCatalogPath() {
    return ".tablespaces";
}

static map<string, TablespaceInfo> loadTablespaces() {
    map<string, TablespaceInfo> result;
    ifstream in(tablespaceCatalogPath());
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 4) continue;
        TablespaceInfo ts{parts[0], parts[1], parts[2], parts[3]};
        result[ts.name] = ts;
    }
    return result;
}

static bool saveTablespaces(const map<string, TablespaceInfo>& spaces) {
    ofstream out(tablespaceCatalogPath(), ios::trunc);
    if (!out) return false;
    for (const auto& kv : spaces) {
        const auto& ts = kv.second;
        out << ts.name << "|" << ts.owner << "|" << ts.location << "|" << ts.options << "\n";
    }
    return true;
}

struct ExtendedStatisticInfo {
    string name;
    string tableName;
    vector<string> columns;
    vector<string> kinds;
    int target = -1;
};

static std::filesystem::path extendedStatsCatalogPath(const string& dbname) {
    return g_engine.dbPath(dbname) / ".extended_stats";
}

static map<string, ExtendedStatisticInfo> loadExtendedStats(const string& dbname) {
    map<string, ExtendedStatisticInfo> result;
    ifstream in(extendedStatsCatalogPath(dbname));
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 5) continue;
        ExtendedStatisticInfo st;
        st.name = parts[0];
        st.tableName = parts[1];
        for (auto& c : splitByDelimiter(parts[2], ',')) {
            c = trim(c);
            if (!c.empty()) st.columns.push_back(c);
        }
        for (auto& k : splitByDelimiter(parts[3], ',')) {
            k = trim(k);
            if (!k.empty()) st.kinds.push_back(k);
        }
        try { st.target = stoi(parts[4]); } catch (...) { st.target = -1; }
        if (!st.name.empty()) result[st.name] = st;
    }
    return result;
}

static bool saveExtendedStats(const string& dbname, const map<string, ExtendedStatisticInfo>& stats) {
    ofstream out(extendedStatsCatalogPath(dbname), ios::trunc);
    if (!out) return false;
    for (const auto& kv : stats) {
        const auto& st = kv.second;
        out << st.name << "|" << st.tableName << "|"
            << joinStrings(st.columns, ",") << "|"
            << joinStrings(st.kinds, ",") << "|"
            << st.target << "\n";
    }
    return true;
}

static void removeMultiColumnStatsEntry(const string& dbname,
                                        const string& tableName,
                                        const vector<string>& columns) {
    auto spath = g_engine.statsPath(dbname);
    if (!std::filesystem::exists(spath)) return;
    string key = joinStrings(columns, ",");
    vector<string> kept;
    ifstream in(spath);
    string line;
    while (getline(in, line)) {
        stringstream ss(line);
        string tname, tag, colKey;
        ss >> tname >> tag >> colKey;
        if (tname == tableName && tag == "__multi__" && colKey == key) continue;
        kept.push_back(line);
    }
    ofstream out(spath, ios::trunc);
    for (const auto& keptLine : kept) out << keptLine << "\n";
}

struct CollationInfo {
    string name;
    string provider;
    string locale;
    string owner;
    string options;
    string behavior;
};

static std::filesystem::path collationCatalogPath(const string& dbname) {
    return g_engine.dbPath(dbname) / ".collations";
}

static string inferCollationBehavior(const string& name, const string& options) {
    string combined = toLower(name + " " + options);
    if (combined.find("nocase") != string::npos ||
        combined.find("case_insensitive") != string::npos ||
        combined.find("ci") != string::npos) {
        return "nocase";
    }
    if (combined.find("reverse") != string::npos) return "reverse";
    return "binary";
}

static map<string, CollationInfo> loadCollations(const string& dbname) {
    map<string, CollationInfo> result;
    ifstream in(collationCatalogPath(dbname));
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 6) continue;
        result[parts[0]] = {parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]};
    }
    return result;
}

static bool saveCollations(const string& dbname, const map<string, CollationInfo>& collations) {
    ofstream out(collationCatalogPath(dbname), ios::trunc);
    if (!out) return false;
    for (const auto& kv : collations) {
        const auto& c = kv.second;
        out << c.name << "|" << c.provider << "|" << c.locale << "|"
            << c.owner << "|" << c.options << "|" << c.behavior << "\n";
    }
    return true;
}

static string resolveCollationForSort(const string& dbname, const string& name) {
    string lowered = toLower(name);
    if (lowered == "nocase" || lowered == "reverse" || lowered == "binary") return lowered;
    auto collations = loadCollations(dbname);
    auto it = collations.find(name);
    if (it != collations.end()) return it->second.behavior;
    it = collations.find(lowered);
    if (it != collations.end()) return it->second.behavior;
    return name;
}

struct CastInfo {
    string sourceType;
    string targetType;
    string method;
    string functionName;
    string context;
};

static std::filesystem::path castCatalogPath(const string& dbname) {
    return g_engine.dbPath(dbname) / ".casts";
}

static string castKey(const string& sourceType, const string& targetType) {
    return sourceType + "->" + targetType;
}

static map<string, CastInfo> loadCasts(const string& dbname) {
    map<string, CastInfo> result;
    ifstream in(castCatalogPath(dbname));
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 5) continue;
        CastInfo c{parts[0], parts[1], parts[2], parts[3], parts[4]};
        result[castKey(c.sourceType, c.targetType)] = c;
    }
    return result;
}

static bool saveCasts(const string& dbname, const map<string, CastInfo>& casts) {
    ofstream out(castCatalogPath(dbname), ios::trunc);
    if (!out) return false;
    for (const auto& kv : casts) {
        const auto& c = kv.second;
        out << c.sourceType << "|" << c.targetType << "|" << c.method << "|"
            << c.functionName << "|" << c.context << "\n";
    }
    return true;
}

struct ConversionInfo {
    string name;
    string sourceEncoding;
    string destEncoding;
    string functionName;
    bool isDefault = false;
};

static std::filesystem::path conversionCatalogPath(const string& dbname) {
    return g_engine.dbPath(dbname) / ".conversions";
}

static map<string, ConversionInfo> loadConversions(const string& dbname) {
    map<string, ConversionInfo> result;
    ifstream in(conversionCatalogPath(dbname));
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 5) continue;
        result[parts[0]] = {parts[0], parts[1], parts[2], parts[3], parts[4] == "1"};
    }
    return result;
}

static bool saveConversions(const string& dbname, const map<string, ConversionInfo>& conversions) {
    ofstream out(conversionCatalogPath(dbname), ios::trunc);
    if (!out) return false;
    for (const auto& kv : conversions) {
        const auto& c = kv.second;
        out << c.name << "|" << c.sourceEncoding << "|" << c.destEncoding << "|"
            << c.functionName << "|" << (c.isDefault ? "1" : "0") << "\n";
    }
    return true;
}

struct CatalogObjectInfo {
    string kind;
    string name;
    string owner;
    string definition;
    string options;
};

bool checkAdmin(const Session& s);
bool checkDB(const Session& s);
bool execute(const std::string& rawSql, Session& s);

static std::filesystem::path compatObjectCatalogPath(const string& dbname) {
    return g_engine.dbPath(dbname) / ".pg_compat_objects";
}

static string compatObjectKey(const string& kind, const string& name) {
    return kind + "|" + name;
}

static map<string, CatalogObjectInfo> loadCompatObjects(const string& dbname) {
    map<string, CatalogObjectInfo> result;
    ifstream in(compatObjectCatalogPath(dbname));
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 5) continue;
        CatalogObjectInfo obj;
        obj.kind = catalogUnescape(parts[0]);
        obj.name = catalogUnescape(parts[1]);
        obj.owner = catalogUnescape(parts[2]);
        obj.definition = catalogUnescape(parts[3]);
        obj.options = catalogUnescape(parts[4]);
        if (!obj.kind.empty() && !obj.name.empty()) {
            result[compatObjectKey(obj.kind, obj.name)] = obj;
        }
    }
    return result;
}

static bool saveCompatObjects(const string& dbname, const map<string, CatalogObjectInfo>& objects) {
    ofstream out(compatObjectCatalogPath(dbname), ios::trunc);
    if (!out) return false;
    for (const auto& kv : objects) {
        const auto& obj = kv.second;
        out << catalogEscape(obj.kind) << "|"
            << catalogEscape(obj.name) << "|"
            << catalogEscape(obj.owner) << "|"
            << catalogEscape(obj.definition) << "|"
            << catalogEscape(obj.options) << "\n";
    }
    return true;
}

struct CompatObjectPrefix {
    string phrase;
    string kind;
};

static const vector<CompatObjectPrefix>& compatCreatePrefixes() {
    static const vector<CompatObjectPrefix> prefixes = {
        {"text search configuration", "text_search_configuration"},
        {"text search dictionary", "text_search_dictionary"},
        {"text search template", "text_search_template"},
        {"text search parser", "text_search_parser"},
        {"foreign data wrapper", "foreign_data_wrapper"},
        {"operator family", "operator_family"},
        {"operator class", "operator_class"},
        {"event trigger", "event_trigger"},
        {"access method", "access_method"},
        {"foreign table", "foreign_table"},
        {"user mapping", "user_mapping"},
        {"publication", "publication"},
        {"subscription", "subscription"},
        {"extension", "extension"},
        {"assertion", "assertion"},
        {"aggregate", "aggregate"},
        {"transform", "transform"},
        {"operator", "operator"},
        {"language", "language"},
        {"server", "server"},
        {"rule", "rule"}
    };
    return prefixes;
}

static const vector<CompatObjectPrefix>& compatAlterDropPrefixes() {
    static const vector<CompatObjectPrefix> prefixes = {
        {"text search configuration", "text_search_configuration"},
        {"text search dictionary", "text_search_dictionary"},
        {"text search template", "text_search_template"},
        {"text search parser", "text_search_parser"},
        {"foreign data wrapper", "foreign_data_wrapper"},
        {"materialized view", "materialized_view"},
        {"operator family", "operator_family"},
        {"operator class", "operator_class"},
        {"event trigger", "event_trigger"},
        {"access method", "access_method"},
        {"foreign table", "foreign_table"},
        {"large object", "large_object"},
        {"user mapping", "user_mapping"},
        {"publication", "publication"},
        {"subscription", "subscription"},
        {"conversion", "conversion"},
        {"collation", "collation"},
        {"extension", "extension"},
        {"assertion", "assertion"},
        {"aggregate", "aggregate"},
        {"transform", "transform"},
        {"operator", "operator"},
        {"language", "language"},
        {"function", "function"},
        {"procedure", "procedure"},
        {"routine", "routine"},
        {"database", "database"},
        {"sequence", "sequence"},
        {"trigger", "trigger"},
        {"domain", "domain"},
        {"policy", "policy"},
        {"server", "server"},
        {"index", "index"},
        {"group", "group"},
        {"type", "type"},
        {"rule", "rule"}
    };
    return prefixes;
}

static const vector<CompatObjectPrefix>& compatDropPrefixes() {
    static const vector<CompatObjectPrefix> prefixes = {
        {"text search configuration", "text_search_configuration"},
        {"text search dictionary", "text_search_dictionary"},
        {"text search template", "text_search_template"},
        {"text search parser", "text_search_parser"},
        {"foreign data wrapper", "foreign_data_wrapper"},
        {"operator family", "operator_family"},
        {"operator class", "operator_class"},
        {"event trigger", "event_trigger"},
        {"access method", "access_method"},
        {"foreign table", "foreign_table"},
        {"large object", "large_object"},
        {"user mapping", "user_mapping"},
        {"publication", "publication"},
        {"subscription", "subscription"},
        {"extension", "extension"},
        {"assertion", "assertion"},
        {"aggregate", "aggregate"},
        {"transform", "transform"},
        {"operator", "operator"},
        {"language", "language"},
        {"routine", "routine"},
        {"server", "server"},
        {"group", "group"},
        {"rule", "rule"}
    };
    return prefixes;
}

static bool consumeCompatPrefix(string& rest,
                                const vector<CompatObjectPrefix>& prefixes,
                                string& kind,
                                string& phrase) {
    rest = trim(rest);
    for (const auto& p : prefixes) {
        if (startsWithKeyword(rest, p.phrase)) {
            kind = p.kind;
            phrase = p.phrase;
            rest = trim(rest.substr(p.phrase.size()));
            return true;
        }
    }
    return false;
}

static string firstCompatNameToken(const string& rest) {
    string s = trim(rest);
    if (s.empty()) return "";
    size_t end = s.size();
    for (const char* kw : {" using ", " for ", " from ", " with ", " options ",
                             " owner ", " on ", " as ", " returns ", " handler "}) {
        size_t p = findTopLevelKeyword(s, trim(kw));
        if (p != string::npos) end = min(end, p);
    }
    size_t lp = s.find('(');
    if (lp != string::npos) end = min(end, lp);
    size_t sp = s.find(' ');
    if (sp != string::npos && sp < end) end = sp;
    return stripQuotes(trim(s.substr(0, end)));
}

static string parseUserMappingName(const string& rest) {
    string s = trim(rest);
    if (startsWithKeyword(s, "if not exists")) s = trim(s.substr(13));
    if (startsWithKeyword(s, "if exists")) s = trim(s.substr(9));
    if (!startsWithKeyword(s, "for")) return "";
    s = trim(s.substr(3));
    size_t serverPos = findTopLevelKeyword(s, "server");
    if (serverPos == string::npos) return "";
    string userName = trim(s.substr(0, serverPos));
    string afterServer = trim(s.substr(serverPos + 6));
    string serverName = firstCompatNameToken(afterServer);
    if (userName.empty() || serverName.empty()) return "";
    return userName + "@" + serverName;
}

static string parseTransformName(const string& rest) {
    string s = trim(rest);
    if (!startsWithKeyword(s, "for")) return "";
    s = trim(s.substr(3));
    size_t langPos = findTopLevelKeyword(s, "language");
    if (langPos == string::npos) return "";
    string typeName = trim(s.substr(0, langPos));
    string langName = firstCompatNameToken(trim(s.substr(langPos + 8)));
    if (typeName.empty() || langName.empty()) return "";
    return typeName + "/" + langName;
}

static string parseCompatObjectName(const string& kind, const string& rest) {
    if (kind == "user_mapping") return parseUserMappingName(rest);
    if (kind == "transform") return parseTransformName(rest);
    if (kind == "operator" || kind == "aggregate") {
        string s = stripTrailingDropBehavior(rest);
        if (startsWithKeyword(s, "if exists")) s = trim(s.substr(9));
        size_t lp = s.find('(');
        if (lp != string::npos) return stripQuotes(trim(s.substr(0, lp)));
        return firstCompatNameToken(s);
    }
    if (kind == "rule") {
        string name = firstCompatNameToken(rest);
        return name;
    }
    string s = stripTrailingDropBehavior(rest);
    if (startsWithKeyword(s, "if not exists")) s = trim(s.substr(13));
    if (startsWithKeyword(s, "if exists")) s = trim(s.substr(9));
    return firstCompatNameToken(s);
}

static bool isCompatObjectCreate(const string& sql) {
    if (!startsWithKeyword(sql, "create")) return false;
    string rest = trim(sql.substr(6));
    if (startsWithKeyword(rest, "or replace")) rest = trim(rest.substr(10));
    while (startsWithKeyword(rest, "trusted") || startsWithKeyword(rest, "procedural")) {
        size_t sp = rest.find(' ');
        if (sp == string::npos) return false;
        rest = trim(rest.substr(sp + 1));
    }
    string kind, phrase;
    return consumeCompatPrefix(rest, compatCreatePrefixes(), kind, phrase);
}

static bool handleCreateCompatObject(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(6));
    bool orReplace = false;
    string leadingOptions;
    if (startsWithKeyword(rest, "or replace")) {
        orReplace = true;
        rest = trim(rest.substr(10));
    }
    while (startsWithKeyword(rest, "trusted") || startsWithKeyword(rest, "procedural")) {
        size_t sp = rest.find(' ');
        string opt = (sp == string::npos) ? rest : rest.substr(0, sp);
        leadingOptions += (leadingOptions.empty() ? "" : " ") + opt;
        if (sp == string::npos) return false;
        rest = trim(rest.substr(sp + 1));
    }
    string kind, phrase;
    if (!consumeCompatPrefix(rest, compatCreatePrefixes(), kind, phrase)) return false;
    bool ifNotExists = false;
    if (startsWithKeyword(rest, "if not exists")) {
        ifNotExists = true;
        rest = trim(rest.substr(13));
    }
    string name = parseCompatObjectName(kind, rest);
    if (name.empty()) {
        cout << "SQL syntax error: CREATE " << phrase << " requires an object name" << endl;
        return true;
    }
    auto objects = loadCompatObjects(s.currentDB);
    string key = compatObjectKey(kind, name);
    if (objects.count(key) && !orReplace) {
        if (ifNotExists) {
            cout << phrase << " " << name << " already exists, skipping" << endl;
            return false;
        }
        cout << phrase << " " << name << " already exists" << endl;
        return true;
    }
    bool existed = objects.count(key) > 0;
    objects[key] = {kind, name, s.username, sql, leadingOptions};
    if (!saveCompatObjects(s.currentDB, objects)) {
        cout << "Create " << phrase << " failed" << endl;
        return true;
    }
    cout << phrase << " " << name << (orReplace && existed ? " replaced" : " created") << endl;
    return false;
}

static bool handleAlterCompatObject(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(5));
    string kind, phrase;
    if (!consumeCompatPrefix(rest, compatAlterDropPrefixes(), kind, phrase)) return false;
    string name = parseCompatObjectName(kind, rest);
    if (name.empty()) {
        cout << "SQL syntax error: ALTER " << phrase << " requires an object name" << endl;
        return true;
    }
    auto objects = loadCompatObjects(s.currentDB);
    string key = compatObjectKey(kind, name);
    auto it = objects.find(key);
    if (it == objects.end()) {
        objects[key] = {kind, name, s.username, "", ""};
        it = objects.find(key);
    }
    size_t ownerPos = findTopLevelKeyword(rest, "owner to");
    if (ownerPos != string::npos) {
        string owner = firstCompatNameToken(trim(rest.substr(ownerPos + 8)));
        if (!owner.empty()) it->second.owner = owner;
    }
    size_t renamePos = findTopLevelKeyword(rest, "rename to");
    if (renamePos != string::npos) {
        string newName = firstCompatNameToken(trim(rest.substr(renamePos + 9)));
        if (newName.empty()) {
            cout << "SQL syntax error: ALTER " << phrase << " RENAME TO requires a new name" << endl;
            return true;
        }
        string newKey = compatObjectKey(kind, newName);
        if (objects.count(newKey)) {
            cout << phrase << " " << newName << " already exists" << endl;
            return true;
        }
        auto info = it->second;
        objects.erase(it);
        info.name = newName;
        info.definition = sql;
        objects[newKey] = info;
        saveCompatObjects(s.currentDB, objects);
        cout << phrase << " " << name << " renamed to " << newName << endl;
        return false;
    }
    it->second.definition = sql;
    saveCompatObjects(s.currentDB, objects);
    cout << phrase << " " << name << " altered" << endl;
    return false;
}

static bool handleDropCompatObject(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(4));
    string kind, phrase;
    if (!consumeCompatPrefix(rest, compatDropPrefixes(), kind, phrase)) return false;
    bool ifExists = false;
    if (startsWithKeyword(rest, "if exists")) {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    auto objects = loadCompatObjects(s.currentDB);
    bool allOk = true;
    vector<string> names;
    if (kind == "user_mapping" || kind == "transform" || kind == "operator" || kind == "aggregate") {
        names.push_back(parseCompatObjectName(kind, rest));
    } else {
        for (auto item : splitTopLevelComma(stripTrailingDropBehavior(rest))) {
            item = stripTrailingDropBehavior(item);
            string name = parseCompatObjectName(kind, item);
            if (!name.empty()) names.push_back(name);
        }
    }
    if (names.empty()) {
        cout << "SQL syntax error: DROP " << phrase << " requires an object name" << endl;
        return true;
    }
    for (const auto& name : names) {
        string key = compatObjectKey(kind, name);
        auto it = objects.find(key);
        if (it == objects.end()) {
            if (!ifExists) {
                cout << phrase << " " << name << " not exist" << endl;
                allOk = false;
            }
            continue;
        }
        objects.erase(it);
        cout << phrase << " " << name << " dropped" << endl;
    }
    saveCompatObjects(s.currentDB, objects);
    return !allOk;
}

static bool handleImportForeignSchema(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(21)); // after "import foreign schema"
    string remoteSchema = firstCompatNameToken(rest);
    size_t serverPos = findTopLevelKeyword(rest, "server");
    string serverName;
    if (serverPos != string::npos) {
        serverName = firstCompatNameToken(trim(rest.substr(serverPos + 6)));
    }
    if (remoteSchema.empty() || serverName.empty()) {
        cout << "SQL syntax error: IMPORT FOREIGN SCHEMA schema FROM SERVER server INTO schema" << endl;
        return true;
    }
    string name = remoteSchema + "@" + serverName;
    auto objects = loadCompatObjects(s.currentDB);
    objects[compatObjectKey("imported_foreign_schema", name)] =
        {"imported_foreign_schema", name, s.username, sql, ""};
    if (!saveCompatObjects(s.currentDB, objects)) {
        cout << "IMPORT FOREIGN SCHEMA failed" << endl;
        return true;
    }
    cout << "foreign schema " << remoteSchema << " imported from server " << serverName << endl;
    return false;
}

static bool showCompatObjects(Session& s, const string& rest) {
    if (!checkDB(s)) return true;
    string filter = trim(rest);
    if (startsWithKeyword(filter, "compat objects")) filter = trim(filter.substr(14));
    else if (startsWithKeyword(filter, "compatibility objects")) filter = trim(filter.substr(21));
    else if (startsWithKeyword(filter, "pg compat objects")) filter = trim(filter.substr(17));
    else return false;
    auto objects = loadCompatObjects(s.currentDB);
    if (objects.empty()) {
        cout << "No compatibility objects found" << endl;
        return true;
    }
    cout << "kind name owner definition" << endl;
    for (const auto& kv : objects) {
        const auto& obj = kv.second;
        if (!filter.empty() && obj.kind != filter) continue;
        string def = obj.definition;
        if (def.size() > 80) def = def.substr(0, 77) + "...";
        cout << obj.kind << " " << obj.name << " " << obj.owner << " " << def << endl;
    }
    return true;
}

struct ConstraintCompatFlags {
    string type = "constraint";
    bool deferrable = false;
    bool initiallyDeferred = false;
    bool notValid = false;
    bool validated = true;
};

static string constraintCompatName(const string& tableName, const string& constraintName) {
    return stripQuotes(trim(tableName)) + "." + stripQuotes(trim(constraintName));
}

static string constraintOptionString(const string& options, const string& key, const string& fallback = "") {
    string needle = key + "=";
    size_t pos = options.find(needle);
    if (pos == string::npos) return fallback;
    pos += needle.size();
    size_t end = options.find(';', pos);
    if (end == string::npos) end = options.size();
    string value = trim(options.substr(pos, end - pos));
    return value.empty() ? fallback : value;
}

static bool constraintOptionBool(const string& options, const string& key, bool fallback = false) {
    string value = constraintOptionString(options, key, fallback ? "1" : "0");
    return value == "1" || value == "true" || value == "yes";
}

static string constraintFlagOptions(const ConstraintCompatFlags& flags) {
    ostringstream out;
    out << "type=" << flags.type
        << ";deferrable=" << (flags.deferrable ? 1 : 0)
        << ";initially_deferred=" << (flags.initiallyDeferred ? 1 : 0)
        << ";not_valid=" << (flags.notValid ? 1 : 0)
        << ";validated=" << (flags.validated ? 1 : 0);
    return out.str();
}

static ConstraintCompatFlags constraintFlagsFromOptions(const string& options,
                                                        const string& fallbackType = "constraint") {
    ConstraintCompatFlags flags;
    flags.type = constraintOptionString(options, "type", fallbackType);
    flags.deferrable = constraintOptionBool(options, "deferrable", false);
    flags.initiallyDeferred = constraintOptionBool(options, "initially_deferred", false);
    flags.notValid = constraintOptionBool(options, "not_valid", false);
    flags.validated = constraintOptionBool(options, "validated", !flags.notValid);
    return flags;
}

static ConstraintCompatFlags parseConstraintFlags(const string& sql,
                                                  const string& type = "constraint") {
    ConstraintCompatFlags flags;
    flags.type = type;
    flags.notValid = sql.find("not valid") != string::npos;
    flags.validated = !flags.notValid;
    flags.initiallyDeferred = sql.find("initially deferred") != string::npos;
    flags.deferrable = flags.initiallyDeferred ||
                       (sql.find("deferrable") != string::npos &&
                        sql.find("not deferrable") == string::npos);
    return flags;
}

static ConstraintCompatFlags mergeConstraintFlagsFromSql(const string& sql,
                                                         ConstraintCompatFlags flags) {
    if (sql.find("not deferrable") != string::npos) {
        flags.deferrable = false;
        flags.initiallyDeferred = false;
    } else if (sql.find("deferrable") != string::npos) {
        flags.deferrable = true;
    }
    if (sql.find("initially deferred") != string::npos) {
        flags.initiallyDeferred = true;
        flags.deferrable = true;
    } else if (sql.find("initially immediate") != string::npos) {
        flags.initiallyDeferred = false;
    }
    if (sql.find("not valid") != string::npos) {
        flags.notValid = true;
        flags.validated = false;
    }
    if (sql.find("validate constraint") != string::npos) {
        flags.notValid = false;
        flags.validated = true;
    }
    return flags;
}

static bool tableConstraintExists(const string& dbname,
                                  const string& tableName,
                                  const string& constraintName) {
    if (!g_engine.tableExists(dbname, tableName)) return false;
    TableSchema tbl = g_engine.getTableSchema(dbname, tableName);
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].checkConstraintName == constraintName) return true;
    }
    for (const auto& name : tbl.uniqueConstraintNames) {
        if (name == constraintName) return true;
    }
    for (size_t i = 0; i < tbl.fkLen; ++i) {
        if (tbl.fks[i].name == constraintName) return true;
    }
    return false;
}

static bool constraintCompatExists(const string& dbname,
                                   const string& tableName,
                                   const string& constraintName) {
    auto objects = loadCompatObjects(dbname);
    string name = constraintCompatName(tableName, constraintName);
    return objects.count(compatObjectKey("constraint_option", name)) > 0 ||
           objects.count(compatObjectKey("exclusion_constraint", name)) > 0;
}

static bool knownConstraintExists(const string& dbname,
                                  const string& tableName,
                                  const string& constraintName) {
    return tableConstraintExists(dbname, tableName, constraintName) ||
           constraintCompatExists(dbname, tableName, constraintName);
}

static bool recordConstraintCompat(const string& dbname,
                                   const string& tableName,
                                   const string& constraintName,
                                   const string& type,
                                   const string& definition,
                                   const string& owner,
                                   ConstraintCompatFlags flags) {
    auto objects = loadCompatObjects(dbname);
    string name = constraintCompatName(tableName, constraintName);
    flags.type = type;
    objects[compatObjectKey("constraint_option", name)] =
        {"constraint_option", name, owner, definition, constraintFlagOptions(flags)};
    return saveCompatObjects(dbname, objects);
}

static bool recordExclusionConstraintCompat(const string& dbname,
                                            const string& tableName,
                                            const string& constraintName,
                                            const string& definition,
                                            const string& owner,
                                            ConstraintCompatFlags flags) {
    auto objects = loadCompatObjects(dbname);
    string name = constraintCompatName(tableName, constraintName);
    flags.type = "exclusion";
    string options = constraintFlagOptions(flags);
    objects[compatObjectKey("exclusion_constraint", name)] =
        {"exclusion_constraint", name, owner, definition, options};
    objects[compatObjectKey("constraint_option", name)] =
        {"constraint_option", name, owner, definition, options};
    return saveCompatObjects(dbname, objects);
}

static bool removeConstraintCompat(const string& dbname,
                                   const string& tableName,
                                   const string& constraintName) {
    auto objects = loadCompatObjects(dbname);
    string name = constraintCompatName(tableName, constraintName);
    bool removed = false;
    removed = objects.erase(compatObjectKey("constraint_option", name)) > 0 || removed;
    removed = objects.erase(compatObjectKey("exclusion_constraint", name)) > 0 || removed;
    if (!removed) return false;
    return saveCompatObjects(dbname, objects);
}

static bool alterConstraintOptionsCompat(const string& dbname,
                                         const string& tableName,
                                         const string& constraintName,
                                         const string& definition,
                                         const string& owner) {
    auto objects = loadCompatObjects(dbname);
    string name = constraintCompatName(tableName, constraintName);
    string optionKey = compatObjectKey("constraint_option", name);
    auto optionIt = objects.find(optionKey);
    ConstraintCompatFlags flags = optionIt == objects.end()
        ? ConstraintCompatFlags{}
        : constraintFlagsFromOptions(optionIt->second.options, "constraint");
    flags = mergeConstraintFlagsFromSql(definition, flags);
    objects[optionKey] = {"constraint_option", name, owner, definition, constraintFlagOptions(flags)};

    string exclusionKey = compatObjectKey("exclusion_constraint", name);
    auto exclusionIt = objects.find(exclusionKey);
    if (exclusionIt != objects.end()) {
        ConstraintCompatFlags exclusionFlags = constraintFlagsFromOptions(exclusionIt->second.options, "exclusion");
        exclusionFlags = mergeConstraintFlagsFromSql(definition, exclusionFlags);
        exclusionFlags.type = "exclusion";
        exclusionIt->second.owner = owner;
        exclusionIt->second.definition = definition;
        exclusionIt->second.options = constraintFlagOptions(exclusionFlags);
    }
    return saveCompatObjects(dbname, objects);
}

static bool validateConstraintCompat(const string& dbname,
                                     const string& tableName,
                                     const string& constraintName,
                                     const string& definition,
                                     const string& owner) {
    auto objects = loadCompatObjects(dbname);
    string name = constraintCompatName(tableName, constraintName);
    string optionKey = compatObjectKey("constraint_option", name);
    auto optionIt = objects.find(optionKey);
    ConstraintCompatFlags flags = optionIt == objects.end()
        ? ConstraintCompatFlags{}
        : constraintFlagsFromOptions(optionIt->second.options, "constraint");
    flags.notValid = false;
    flags.validated = true;
    objects[optionKey] = {"constraint_option", name, owner, definition, constraintFlagOptions(flags)};

    string exclusionKey = compatObjectKey("exclusion_constraint", name);
    auto exclusionIt = objects.find(exclusionKey);
    if (exclusionIt != objects.end()) {
        ConstraintCompatFlags exclusionFlags = constraintFlagsFromOptions(exclusionIt->second.options, "exclusion");
        exclusionFlags.type = "exclusion";
        exclusionFlags.notValid = false;
        exclusionFlags.validated = true;
        exclusionIt->second.owner = owner;
        exclusionIt->second.options = constraintFlagOptions(exclusionFlags);
    }
    return saveCompatObjects(dbname, objects);
}

static bool isPgStringBackedType(const string& typeName) {
    static const set<string> exact = {
        "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange",
        "int4multirange", "int8multirange", "nummultirange", "tsmultirange",
        "tstzmultirange", "datemultirange", "tsvector", "tsquery", "macaddr",
        "macaddr8", "oid", "regclass", "regcollation", "regconfig", "regdictionary",
        "regnamespace", "regoper", "regoperator", "regproc", "regprocedure",
        "regrole", "regtype", "xid", "cid", "tid", "record", "anyelement",
        "anyarray", "anynonarray", "anyenum", "anyrange", "anymultirange",
        "cstring", "trigger", "event_trigger"
    };
    if (exact.count(typeName)) return true;
    return (typeName.size() >= 5 &&
            typeName.substr(typeName.size() - 5) == "range") ||
           (typeName.size() >= 10 &&
            typeName.substr(typeName.size() - 10) == "multirange");
}

static Column makePgStringBackedColumn(const string& cname,
                                       const string& typeName,
                                       bool isNull,
                                       bool isPK) {
    Column col = makeVarCharColumn(cname, isNull, 65535, isPK);
    col.dataType = typeName;
    col.isVariableLength = true;
    col.dsize = 65535;
    return col;
}

static bool handleLoadSharedLibrary(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string lib = stripQuotes(trim(sql.substr(4)));
    if (lib.empty()) {
        cout << "SQL syntax error: LOAD 'library'" << endl;
        return true;
    }
    auto objects = loadCompatObjects(s.currentDB);
    objects[compatObjectKey("loaded_library", lib)] =
        {"loaded_library", lib, s.username, sql, ""};
    if (!saveCompatObjects(s.currentDB, objects)) {
        cout << "LOAD failed" << endl;
        return true;
    }
    cout << "Library " << lib << " loaded" << endl;
    return false;
}

static bool extractQuotedSqlBody(const string& s, string& body) {
    string rest = trim(s);
    size_t d1 = rest.find("$$");
    if (d1 != string::npos) {
        size_t d2 = rest.find("$$", d1 + 2);
        if (d2 == string::npos) return false;
        body = rest.substr(d1 + 2, d2 - d1 - 2);
        return true;
    }
    size_t q1 = rest.find('\'');
    if (q1 != string::npos) {
        size_t q2 = q1 + 1;
        while (q2 < rest.size()) {
            if (rest[q2] == '\'' && q2 + 1 < rest.size() && rest[q2 + 1] == '\'') {
                q2 += 2;
                continue;
            }
            if (rest[q2] == '\'') break;
            ++q2;
        }
        if (q2 >= rest.size()) return false;
        body = rest.substr(q1 + 1, q2 - q1 - 1);
        size_t p = 0;
        while ((p = body.find("''", p)) != string::npos) {
            body.replace(p, 2, "'");
            ++p;
        }
        return true;
    }
    return false;
}

static bool handleDoBlock(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    string body;
    if (!extractQuotedSqlBody(sql.substr(2), body)) {
        cout << "SQL syntax error: DO requires a quoted block" << endl;
        return true;
    }
    body = trim(body);
    if (startsWithKeyword(body, "begin")) {
        body = trim(body.substr(5));
        if (body.size() >= 3 && body.substr(body.size() - 3) == "end") {
            body = trim(body.substr(0, body.size() - 3));
        } else if (body.size() >= 4 && body.substr(body.size() - 4) == "end;") {
            body = trim(body.substr(0, body.size() - 4));
        }
    }
    int executed = 0;
    for (auto stmt : splitTopLevelSemicolon(body)) {
        stmt = trim(stmt);
        if (stmt.empty() || stmt == "begin" || stmt == "end") continue;
        if (stmt.size() >= 3 && stmt.substr(stmt.size() - 3) == "end") {
            stmt = trim(stmt.substr(0, stmt.size() - 3));
            if (stmt.empty()) continue;
        }
        bool failed = execute(stmt, s);
        if (failed) return true;
        ++executed;
    }
    cout << "DO " << executed << " statement(s)" << endl;
    return false;
}

static bool handleSelectIntoTable(const string& sql, Session& s, bool& handled) {
    handled = false;
    if (!startsWithKeyword(sql, "select")) return false;
    size_t intoPos = findTopLevelKeyword(sql, "into", 6);
    if (intoPos == string::npos) return false;
    size_t fromPos = findTopLevelKeyword(sql, "from", intoPos + 4);
    if (fromPos == string::npos || intoPos > fromPos) return false;
    string target = trim(sql.substr(intoPos + 4, fromPos - intoPos - 4));
    if (target.empty() || startsWithKeyword(target, "outfile")) return false;
    bool temporary = false;
    bool unlogged = false;
    if (startsWithKeyword(target, "temporary")) {
        temporary = true;
        target = trim(target.substr(9));
    } else if (startsWithKeyword(target, "temp")) {
        temporary = true;
        target = trim(target.substr(4));
    } else if (startsWithKeyword(target, "unlogged")) {
        unlogged = true;
        target = trim(target.substr(8));
    }
    if (target.empty() || target.find(' ') != string::npos) return false;
    string selectList = trim(sql.substr(6, intoPos - 6));
    string fromRest = trim(sql.substr(fromPos));
    string createTarget = temporary ? tempTablePrefix(target) : target;
    string createSql = string("create ") +
        (unlogged ? "unlogged " : "") +
        "table " + createTarget + " as select " + selectList + " " + fromRest;
    handled = true;
    bool failed = execute(createSql, s);
    if (!failed && temporary) s.tempTables.insert(target);
    return failed;
}

static bool tableHasColumns(const string& dbname, const string& tablename, const vector<string>& columns) {
    TableSchema tbl = g_engine.getTableSchema(dbname, tablename);
    if (tbl.len == 0) return false;
    set<string> existing;
    for (size_t i = 0; i < tbl.len; ++i) existing.insert(tbl.cols[i].dataName);
    for (string col : columns) {
        size_t dotPos = col.find('.');
        if (dotPos != string::npos) col = trim(col.substr(dotPos + 1));
        if (!existing.count(col)) return false;
    }
    return true;
}

static string normalizeConditionStr(string s) {
    static const char* ops[] = {"<<", ">>", "<^", ">^", "<@", "&&", ">=", "<=", "!=", "<>", ">", "<", "="};
    for (const char* op : ops) {
        size_t len = strlen(op);
        size_t pos = 0;
        while ((pos = s.find(op, pos)) != string::npos) {
            size_t before = pos;
            while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
            size_t after = pos + len;
            while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
            if (before != pos || after != pos + len) {
                s = s.substr(0, before) + op + s.substr(after);
                pos = before + len;
            } else {
                pos += len;
            }
        }
    }
    // Normalize LIKE keyword: "name like 'a%'" → "namelike'a%'"
    size_t pos = 0;
    while ((pos = s.find("like", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 4;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 4) {
            s = s.substr(0, before) + "like" + s.substr(after);
            pos = before + 4;
        } else {
            pos += 4;
        }
    }
    // Normalize REGEXP keyword: "name regexp '^a'" → "nameregexp'^a'"
    pos = 0;
    while ((pos = s.find("regexp", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 6;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 6) {
            s = s.substr(0, before) + "regexp" + s.substr(after);
            pos = before + 6;
        } else {
            pos += 6;
        }
    }
    // Normalize SIMILAR TO keyword: "name similar to '^a%'" → "nameregexp'^a%'"
    pos = 0;
    while ((pos = s.find("similar to", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 10;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        s = s.substr(0, before) + "regexp" + s.substr(after);
        pos = before + 6;
    }
    // Normalize OVERLAPS keyword: "(d1,d2) overlaps (d3,d4)" → "(d1,d2)overlaps(d3,d4)"
    pos = 0;
    while ((pos = s.find("overlaps", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 8;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 8) {
            s = s.substr(0, before) + "overlaps" + s.substr(after);
            pos = before + 8;
        } else {
            pos += 8;
        }
    }
    // Convert OVERLAPS expression to parenthesis-free form:
    // "(s1,e1)overlaps(s2,e2)" → "overlaps:s1,e1,s2,e2"
    // This avoids tokenize() splitting on '(' / ')' and breaking the condition.
    pos = 0;
    while ((pos = s.find("overlaps", pos)) != string::npos) {
        size_t leftStart = pos;
        while (leftStart > 0 && s[leftStart - 1] != '(') leftStart--;
        if (leftStart == 0 || s[leftStart - 1] != '(') { pos += 8; continue; }
        leftStart--;
        size_t rightEnd = pos + 8;
        while (rightEnd < s.size() && s[rightEnd] != ')') rightEnd++;
        if (rightEnd >= s.size()) { pos += 8; continue; }
        rightEnd++;
        string expr = s.substr(leftStart, rightEnd - leftStart);
        // expr = "(s1,e1)overlaps(s2,e2)"
        string inner = expr.substr(1, expr.size() - 2); // "s1,e1)overlaps(s2,e2"
        size_t opPos = inner.find(")overlaps(");
        if (opPos != string::npos) {
            string replacement = "overlaps:" + inner.substr(0, opPos) + "," + inner.substr(opPos + 10);
            s = s.substr(0, leftStart) + replacement + s.substr(rightEnd);
            pos = leftStart + replacement.size();
        } else {
            pos += 8;
        }
    }
    // Normalize CONTAINS keyword: "name contains 'word'" → "namecontains'word'"
    pos = 0;
    while ((pos = s.find("contains", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 8;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 8) {
            s = s.substr(0, before) + "contains" + s.substr(after);
            pos = before + 8;
        } else {
            pos += 8;
        }
    }
    // Normalize IS NOT NULL (before IS NULL to avoid partial match)
    pos = 0;
    while ((pos = s.find("is not null", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 11;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 11) {
            s = s.substr(0, before) + "isnotnull" + s.substr(after);
            pos = before + 9;
        } else {
            pos += 11;
        }
    }
    // Normalize IS NULL
    pos = 0;
    while ((pos = s.find("is null", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 7;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 7) {
            s = s.substr(0, before) + "isnull" + s.substr(after);
            pos = before + 6;
        } else {
            pos += 7;
        }
    }
    // Constant folding: "1 + 2" → "3", "5 * 3" → "15", "10 / 2" → "5"
    s = foldConstants(s);
    return s;
}

// ========================================================================
// Constant folding: evaluate arithmetic expressions of literal numbers
// ========================================================================
static string foldConstants(const string& s) {
    // Helper: count unescaped single quotes before a position
    auto insideQuotes = [&](size_t pos) -> bool {
        int count = 0;
        for (size_t k = 0; k < pos; ++k) {
            if (s[k] == '\'' && (k == 0 || s[k-1] != '\\')) count++;
        }
        return (count % 2) == 1;
    };
    string result;
    size_t i = 0;
    while (i < s.size()) {
        // Look for: number [spaces] op [spaces] number
        // where both numbers are standalone literals (not part of identifiers)
        size_t num1Start = i;
        while (num1Start < s.size() && isspace(static_cast<unsigned char>(s[num1Start]))) num1Start++;
        if (num1Start >= s.size()) { result += s[i++]; continue; }

        size_t num1End = num1Start;
        bool num1Float = false;
        while (num1End < s.size() && (isdigit(static_cast<unsigned char>(s[num1End])) || s[num1End] == '.')) {
            if (s[num1End] == '.') num1Float = true;
            num1End++;
        }
        if (num1End == num1Start) { result += s[i++]; continue; }

        size_t opPos = num1End;
        while (opPos < s.size() && isspace(static_cast<unsigned char>(s[opPos]))) opPos++;
        if (opPos >= s.size()) { result += s[i++]; continue; }
        char op = s[opPos];
        if (op != '+' && op != '-' && op != '*' && op != '/') { result += s[i++]; continue; }

        size_t num2Start = opPos + 1;
        while (num2Start < s.size() && isspace(static_cast<unsigned char>(s[num2Start]))) num2Start++;
        size_t num2End = num2Start;
        bool num2Float = false;
        while (num2End < s.size() && (isdigit(static_cast<unsigned char>(s[num2End])) || s[num2End] == '.')) {
            if (s[num2End] == '.') num2Float = true;
            num2End++;
        }
        if (num2End == num2Start) { result += s[i++]; continue; }

        // Don't fold inside SQL string literals (e.g., '192.168.1.0/24')
        if (insideQuotes(num1Start) || insideQuotes(num2Start)) {
            result += s[i++]; continue;
        }

        // Ensure standalone: not part of an identifier or larger expression
        if (num1Start > 0 && (isalnum(static_cast<unsigned char>(s[num1Start - 1])) || s[num1Start - 1] == '_')) {
            result += s[i++]; continue;
        }
        if (num2End < s.size() && (isalnum(static_cast<unsigned char>(s[num2End])) || s[num2End] == '_')) {
            result += s[i++]; continue;
        }

        string n1 = s.substr(num1Start, num1End - num1Start);
        string n2 = s.substr(num2Start, num2End - num2Start);
        string folded;
        if (num1Float || num2Float) {
            double v1 = 0, v2 = 0, r = 0;
            try { v1 = std::stod(n1); v2 = std::stod(n2); } catch (...) { result += s[i++]; continue; }
            switch (op) {
                case '+': r = v1 + v2; break;
                case '-': r = v1 - v2; break;
                case '*': r = v1 * v2; break;
                case '/': if (v2 != 0) r = v1 / v2; else { result += s[i++]; continue; }
            }
            folded = std::to_string(r);
            // Trim trailing zeros
            while (folded.size() > 1 && folded.back() == '0') folded.pop_back();
            if (!folded.empty() && folded.back() == '.') folded += '0';
        } else {
            long long v1 = 0, v2 = 0, r = 0;
            try { v1 = std::stoll(n1); v2 = std::stoll(n2); } catch (...) { result += s[i++]; continue; }
            switch (op) {
                case '+': r = v1 + v2; break;
                case '-': r = v1 - v2; break;
                case '*': r = v1 * v2; break;
                case '/': if (v2 != 0) r = v1 / v2; else { result += s[i++]; continue; }
            }
            folded = std::to_string(r);
        }
        result += s.substr(i, num1Start - i);
        result += folded;
        i = num2End;
    }
    return result;
}

// ========================================================================
// Condition conversion: "col=value" → "=col value"
// ========================================================================
static string modifyLogic(const string& logic) {
    if (logic == "(" || logic == ")" || logic == "and" || logic == "or") return logic;
    // Handle LIKE
    size_t likePos = logic.find("like");
    if (likePos != string::npos) {
        string before = logic.substr(0, likePos);
        string after = logic.substr(likePos + 4);
        return "like" + before + " " + after;
    }
    // Handle REGEXP
    size_t regexpPos = logic.find("regexp");
    if (regexpPos != string::npos) {
        string before = logic.substr(0, regexpPos);
        string after = logic.substr(regexpPos + 6);
        return "regexp" + before + " " + after;
    }
    // Handle CONTAINS
    size_t containsPos = logic.find("contains");
    if (containsPos != string::npos) {
        string before = logic.substr(0, containsPos);
        string after = logic.substr(containsPos + 8);
        return "contains" + before + " " + after;
    }
    // Handle OVERLAPS
    size_t overlapsPos = logic.find("overlaps");
    if (overlapsPos != string::npos) {
        string before = logic.substr(0, overlapsPos);
        string after = logic.substr(overlapsPos + 8);
        return "overlaps" + before + " " + after;
    }
    // Handle IS NOT NULL
    size_t isnotPos = logic.find("isnotnull");
    if (isnotPos != string::npos && isnotPos + 9 == logic.size()) {
        return "isnotnull " + logic.substr(0, isnotPos);
    }
    // Handle IS NULL
    size_t isPos = logic.find("isnull");
    if (isPos != string::npos && isPos + 6 == logic.size()) {
        return "isnull " + logic.substr(0, isPos);
    }
    size_t opStart = string::npos;
    size_t opLen = 0;
    for (size_t i = 0; i < logic.size(); ++i) {
        if (logic[i] == '>' || logic[i] == '<' || logic[i] == '=' || logic[i] == '!' || logic[i] == '&') {
            opStart = i;
            opLen = 1;
            if (i + 1 < logic.size() && (logic[i + 1] == '=' || logic[i + 1] == '>')) {
                if ((logic[i] == '<' && logic[i + 1] == '=') ||
                    (logic[i] == '>' && logic[i + 1] == '=') ||
                    (logic[i] == '!' && logic[i + 1] == '=') ||
                    (logic[i] == '<' && logic[i + 1] == '>')) {
                    opLen = 2;
                }
            }
            // Spatial operators: <<, >>, <^, >^, <@ and network overlap: &&
            if (i + 1 < logic.size() && opLen == 1) {
                if ((logic[i] == '<' && (logic[i + 1] == '<' || logic[i + 1] == '^' || logic[i + 1] == '@')) ||
                    (logic[i] == '>' && (logic[i + 1] == '>' || logic[i + 1] == '^')) ||
                    (logic[i] == '&' && logic[i + 1] == '&')) {
                    opLen = 2;
                }
            }
            break;
        }
    }
    if (opStart == string::npos) return "";
    string op = logic.substr(opStart, opLen);
    string before = logic.substr(0, opStart);
    string after = logic.substr(opStart + opLen);
    return op + before + " " + after;
}

// ========================================================================
// Breakdown logic expressions into DNF (OR of ANDs)
// ========================================================================
static vector<vector<string>> breakDownConditions(const vector<string>& tokens) {
    if (tokens.size() == 3) {  // single condition wrapped in parens: ( cond )
        return {{tokens[1]}};
    }

    struct Frame {
        vector<vector<string>> groups;
    };

    vector<Frame> stack;
    stack.push_back(Frame());
    vector<string> operandStack;
    vector<string> opStack;

    auto applyAnd = [&](const string& right) {
        Frame& cur = stack.back();
        if (!operandStack.empty()) {
            string left = operandStack.back(); operandStack.pop_back();
            if (cur.groups.empty()) {
                cur.groups.push_back({left, right});
            } else {
                for (auto& g : cur.groups) g.push_back(right);
            }
        } else {
            if (cur.groups.empty()) {
                cur.groups.push_back({right});
            } else {
                for (auto& g : cur.groups) g.push_back(right);
            }
        }
    };

    auto applyOr = [&](const string& right) {
        Frame& cur = stack.back();
        if (!operandStack.empty()) {
            string left = operandStack.back(); operandStack.pop_back();
            if (cur.groups.empty()) {
                cur.groups.push_back({left});
                cur.groups.push_back({right});
            } else {
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
            // Left side already in groups; just add right as a new group
            cur.groups.push_back({right});
        }
    };

    auto flushOp = [&](const string& op, const string& right) {
        if (op == "and") applyAnd(right);
        else if (op == "or") applyOr(right);
    };

    for (const string& tok : tokens) {
        if (tok == "and" || tok == "or") {
            opStack.push_back(tok);
        } else if (tok == "(") {
            stack.push_back(Frame());
            opStack.clear();
            operandStack.clear();
        } else if (tok == ")") {
            Frame cur = std::move(stack.back());
            stack.pop_back();
            if (cur.groups.empty() && !operandStack.empty()) {
                cur.groups.push_back({operandStack.back()});
            }
            // Merge into parent
            if (stack.empty()) {
                stack.push_back(std::move(cur));
                break;
            }
            if (cur.groups.empty()) continue;
            if (stack.back().groups.empty()) {
                stack.back().groups = std::move(cur.groups);
            } else {
                auto old = stack.back().groups;
                stack.back().groups.clear();
                for (auto& g1 : old) {
                    for (auto& g2 : cur.groups) {
                        vector<string> merged = g1;
                        merged.insert(merged.end(), g2.begin(), g2.end());
                        stack.back().groups.push_back(std::move(merged));
                    }
                }
            }
            operandStack.clear();
            opStack.clear();
        } else {
            if (!opStack.empty()) {
                string op = opStack.back(); opStack.pop_back();
                flushOp(op, tok);
            } else {
                operandStack.push_back(tok);
            }
        }
    }

    if (!operandStack.empty() && stack.back().groups.empty()) {
        stack.back().groups.push_back({operandStack.back()});
    }
    return stack.empty() ? vector<vector<string>>{} : stack.back().groups;
}

// ========================================================================
// Parse CREATE TABLE columns
// ========================================================================
static TableSchema parseTableColumns(const string& sql, size_t nameEnd, const string& dbname = "") {
    TableSchema tbl;
    std::vector<std::string> pkColNames;  // raw names from PRIMARY KEY (a,b)
    std::vector<std::vector<std::string>> uniqueColNames; // raw names from UNIQUE (a,b)
    size_t pos = nameEnd;
    while (pos < sql.size()) {
        if (sql[pos] != '{' && sql[pos] != '(') { ++pos; continue; }
        bool isBrace = (sql[pos] == '{');
        char closeChar = isBrace ? '}' : ')';
        ++pos;
        while (pos < sql.size()) {
            // Find next comma or closeChar, respecting parenthesis depth
            size_t endPos = string::npos;
            int parenDepth = 0;
            for (size_t i = pos; i < sql.size(); ++i) {
                if (sql[i] == '(') { ++parenDepth; continue; }
                if (sql[i] == ')') {
                    if (parenDepth > 0) { --parenDepth; continue; }
                    // Top-level close paren - end of table definition
                    if (closeChar == ')') { endPos = i; break; }
                }
                if (sql[i] == ',' && parenDepth == 0) { endPos = i; break; }
                if (sql[i] == closeChar && parenDepth == 0) { endPos = i; break; }
            }
            if (endPos == string::npos) break;

            string segment = trim(sql.substr(pos, endPos - pos));
            if (segment.empty()) { pos = endPos + 1; continue; }

            // Detect table-level constraints before treating as column def
            vector<string> parts = tokenize(segment);
            if (!parts.empty() && parts[0] == "primary" && parts.size() > 2 && parts[1] == "key") {
                // PRIMARY KEY (col1, col2, ...)
                for (size_t i = 2; i < parts.size(); ++i) {
                    if (parts[i] == "(" || parts[i] == ")" || parts[i] == ",") continue;
                    std::string c = parts[i];
                    if (!c.empty() && c.back() == ',') c.pop_back();
                    pkColNames.push_back(c);
                }
                pos = endPos + 1;
                continue;
            }
            if (!parts.empty() && parts[0] == "unique") {
                // UNIQUE (col1, col2, ...)
                std::vector<std::string> cols;
                for (size_t i = 1; i < parts.size(); ++i) {
                    if (parts[i] == "(" || parts[i] == ")" || parts[i] == ",") continue;
                    std::string c = parts[i];
                    if (!c.empty() && c.back() == ',') c.pop_back();
                    cols.push_back(c);
                }
                if (!cols.empty()) uniqueColNames.push_back(std::move(cols));
                pos = endPos + 1;
                continue;
            }
            if (!parts.empty() && parts[0] == "foreign" && parts.size() > 1 && parts[1] == "key") {
                // FOREIGN KEY (col1, col2, ...) REFERENCES refTable(refCol1, refCol2, ...)
                dbms::ForeignKey fk;
                size_t i = 2;
                // Collect local columns
                while (i < parts.size() && (parts[i] == "(" || parts[i] == ",")) ++i;
                while (i < parts.size() && parts[i] != ")" && parts[i] != "references") {
                    if (parts[i] != ",") {
                        std::string c = parts[i];
                        if (!c.empty() && c.back() == ',') c.pop_back();
                        if (!c.empty()) fk.colNames.push_back(c);
                    }
                    ++i;
                }
                while (i < parts.size() && parts[i] != "references") ++i;
                if (i < parts.size() && parts[i] == "references") {
                    ++i; // skip "references"
                    if (i < parts.size()) {
                        std::string rt = parts[i];
                        if (!rt.empty() && rt.back() == ',') rt.pop_back();
                        fk.refTable = rt;
                        ++i;
                        // Collect referenced columns from (refCol1, refCol2, ...)
                        while (i < parts.size() && (parts[i] == "(" || parts[i] == ",")) ++i;
                        while (i < parts.size() && parts[i] != ")") {
                            if (parts[i] != ",") {
                                std::string c = parts[i];
                                if (!c.empty() && c.back() == ',') c.pop_back();
                                if (!c.empty()) fk.refCols.push_back(c);
                            }
                            ++i;
                        }
                    }
                }
                fk.onDelete = "restrict";
                fk.onUpdate = "restrict";
                // Parse ON DELETE / ON UPDATE from remaining tokens
                auto parseActionToken = [&](size_t& idx) -> string {
                    string a = trim(parts[idx]);
                    if (a.size() >= 7 && a.substr(0, 7) == "cascade") { idx++; return "cascade"; }
                    if (a == "set" && idx + 1 < parts.size() && parts[idx + 1] == "null") { idx += 2; return "setnull"; }
                    if (a.size() >= 7 && a.substr(0, 7) == "setnull") { idx++; return "setnull"; }
                    idx++; return "restrict";
                };
                while (i < parts.size()) {
                    if (parts[i] == "on" && i + 2 < parts.size()) {
                        if (parts[i + 1] == "delete") {
                            i += 2;
                            fk.onDelete = parseActionToken(i);
                        } else if (parts[i + 1] == "update") {
                            i += 2;
                            fk.onUpdate = parseActionToken(i);
                        } else {
                            ++i;
                        }
                    } else {
                        ++i;
                    }
                }
                if (!fk.colNames.empty() && !fk.refTable.empty() && !fk.refCols.empty()) {
                    tbl.appendFK(fk);
                }
                pos = endPos + 1;
                continue;
            }

            string cname, ctype;
            bool isNull = true;
            bool isPK = false;
            bool isUnique = false;
            bool isUnsigned = false;
            bool isAutoIncrement = false;
            std::string defaultVal;
            std::string checkExpr;
            std::string generatedExpr;
            dbms::ForeignKey fk;

            if (isBrace) {
                // {col:type flags} format
                size_t colon = segment.find(':');
                if (colon == string::npos) break;
                cname = trim(segment.substr(0, colon));
                ctype = trim(segment.substr(colon + 1));
                std::transform(ctype.begin(), ctype.end(), ctype.begin(), ::tolower);
            } else {
                // (col type flags) format, or (col:type:flags) format with colons
                size_t firstColon = segment.find(':');
                if (firstColon != string::npos) {
                    // (col:type:flags) colon-separated format
                    cname = trim(segment.substr(0, firstColon));
                    string rest = trim(segment.substr(firstColon + 1));
                    size_t secondColon = rest.find(':');
                    if (secondColon == string::npos) {
                        // (col:type flags) mixed format - parse type then remaining tokens as flags
                        parts = tokenize(rest);
                        if (!parts.empty()) {
                            ctype = parts[0];
                            for (size_t i = 1; i < parts.size(); ++i) {
                                if (parts[i] == "primary") {
                                    if (i + 1 < parts.size() && parts[i + 1] == "key") {
                                        isPK = true; ++i;
                                    }
                                } else if (parts[i] == "key") {
                                } else if (parts[i] == "not" && i + 1 < parts.size() && parts[i + 1] == "null") {
                                    isNull = false; ++i;
                                } else if (parts[i] == "0") {
                                    isNull = false;
                                } else if (parts[i] == "unique") {
                                    isUnique = true;
                                } else if (parts[i] == "unsigned") {
                                    isUnsigned = true;
                                } else if (parts[i] == "default" && i + 1 < parts.size()) {
                                    defaultVal = parts[i + 1];
                                    if (defaultVal.size() >= 2 && defaultVal.front() == '\'' && defaultVal.back() == '\'') {
                                        defaultVal = defaultVal.substr(1, defaultVal.size() - 2);
                                    }
                                    ++i;
                                } else if (parts[i] == "check" && i + 1 < parts.size()) {
                                    ++i;
                                    int parenDepth = 0;
                                    std::string expr;
                                    for (; i < parts.size(); ++i) {
                                        if (parts[i] == "(") {
                                            if (parenDepth > 0) expr += "(";
                                            ++parenDepth;
                                        } else if (parts[i] == ")") {
                                            --parenDepth;
                                            if (parenDepth > 0) expr += ")";
                                        } else {
                                            expr += parts[i];
                                        }
                                        if (parenDepth == 0 && !expr.empty()) break;
                                    }
                                    checkExpr = normalizeConditionStr(expr);
                                } else if (parts[i] == "generated" && i + 3 < parts.size() && parts[i + 1] == "always" && parts[i + 2] == "as" && parts[i + 3] == "identity") {
                                    isAutoIncrement = true;
                                    i += 3;
                                } else if (parts[i] == "generated" && i + 4 < parts.size() && parts[i + 1] == "by" && parts[i + 2] == "default" && parts[i + 3] == "as" && parts[i + 4] == "identity") {
                                    isAutoIncrement = true;
                                    i += 4;
                                } else if (parts[i] == "generated" && i + 2 < parts.size() && parts[i + 1] == "always" && parts[i + 2] == "as") {
                                    i += 3;
                                    int parenDepth = 0;
                                    std::string expr;
                                    for (; i < parts.size(); ++i) {
                                        if (parts[i] == "(") {
                                            if (parenDepth > 0) expr += "(";
                                            ++parenDepth;
                                        } else if (parts[i] == ")") {
                                            --parenDepth;
                                            if (parenDepth > 0) expr += ")";
                                        } else {
                                            expr += parts[i];
                                        }
                                        if (parenDepth == 0 && !expr.empty()) break;
                                    }
                                    generatedExpr = normalizeConditionStr(expr);
                                }
                            }
                        }
                    } else {
                        ctype = trim(rest.substr(0, secondColon));
                        string flagsStr = trim(rest.substr(secondColon + 1));
                        stringstream fs(flagsStr);
                        string f;
                        while (getline(fs, f, ':')) {
                            string tf = trim(f);
                            if (tf == "0") isNull = false;
                            if (tf == "1") isPK = true;
                            if (tf == "pk" || tf == "PK") isPK = true;
                            if (tf == "unsigned") isUnsigned = true;
                            if (tf == "unique") isUnique = true;
                        }
                    }
                    std::transform(ctype.begin(), ctype.end(), ctype.begin(), ::tolower);
                } else {
                    // (col type flags) space-separated format (original)
                    parts = tokenize(segment);
                    if (parts.empty()) break;
                    cname = parts[0];
                    if (parts.size() >= 2) {
                        ctype = parts[1];
                        std::transform(ctype.begin(), ctype.end(), ctype.begin(), ::tolower);
                        for (size_t i = 2; i < parts.size(); ++i) {
                            if (parts[i] == "primary") {
                                if (i + 1 < parts.size() && parts[i + 1] == "key") {
                                    isPK = true; ++i;
                                }
                            } else if (parts[i] == "key") {
                                // handled above
                            } else if (parts[i] == "not" && i + 1 < parts.size() && parts[i + 1] == "null") {
                                isNull = false; ++i;
                            } else if (parts[i] == "0") {
                                isNull = false;
                            } else if (parts[i] == "unique") {
                                isUnique = true;
                            } else if (parts[i] == "unsigned") {
                                isUnsigned = true;
                            } else if (parts[i] == "default" && i + 1 < parts.size()) {
                                defaultVal = parts[i + 1];
                                if (defaultVal.size() >= 2 && defaultVal.front() == '\'' && defaultVal.back() == '\'') {
                                    defaultVal = defaultVal.substr(1, defaultVal.size() - 2);
                                }
                                ++i;
                            } else if (parts[i] == "check" && i + 1 < parts.size()) {
                                // Collect CHECK expression
                                ++i;
                                int parenDepth = 0;
                                std::string expr;
                                for (; i < parts.size(); ++i) {
                                    if (parts[i] == "(") {
                                        if (parenDepth > 0) expr += "(";
                                        ++parenDepth;
                                    } else if (parts[i] == ")") {
                                        --parenDepth;
                                        if (parenDepth > 0) expr += ")";
                                    } else {
                                        expr += parts[i];
                                    }
                                    if (parenDepth == 0 && !expr.empty()) break;
                                }
                                // Store normalized expression
                                checkExpr = normalizeConditionStr(expr);
                            } else if (parts[i] == "generated" && i + 3 < parts.size() && parts[i + 1] == "always" && parts[i + 2] == "as" && parts[i + 3] == "identity") {
                                // GENERATED ALWAYS AS IDENTITY
                                isAutoIncrement = true;
                                i += 3;
                            } else if (parts[i] == "generated" && i + 4 < parts.size() && parts[i + 1] == "by" && parts[i + 2] == "default" && parts[i + 3] == "as" && parts[i + 4] == "identity") {
                                // GENERATED BY DEFAULT AS IDENTITY
                                isAutoIncrement = true;
                                i += 4;
                            } else if (parts[i] == "generated" && i + 2 < parts.size() && parts[i + 1] == "always" && parts[i + 2] == "as") {
                                // GENERATED ALWAYS AS (expr)
                                i += 3;
                                int parenDepth = 0;
                                std::string expr;
                                for (; i < parts.size(); ++i) {
                                    if (parts[i] == "(") {
                                        if (parenDepth > 0) expr += "(";
                                        ++parenDepth;
                                    } else if (parts[i] == ")") {
                                        --parenDepth;
                                        if (parenDepth > 0) expr += ")";
                                    } else {
                                        expr += parts[i];
                                    }
                                    if (parenDepth == 0 && !expr.empty()) break;
                                }
                                // Store generated expression (normalize as condition)
                                generatedExpr = normalizeConditionStr(expr);
                            }
                        }
                    }
                }
            }

            if (cname.empty() || ctype.empty()) break;

            // Resolve domain types
            if (!dbname.empty()) {
                auto domain = g_engine.getDomain(dbname, ctype);
                if (!domain.name.empty()) {
                    ctype = domain.baseType;
                    if (defaultVal.empty() && !domain.defaultValue.empty()) defaultVal = domain.defaultValue;
                    if (checkExpr.empty() && !domain.checkExpr.empty()) {
                        checkExpr = domain.checkExpr;
                        // Replace 'value' placeholder with actual column name
                        size_t vp = 0;
                        while ((vp = checkExpr.find("value", vp)) != string::npos) {
                            bool leftOk = (vp == 0) || !isalnum(static_cast<unsigned char>(checkExpr[vp - 1]));
                            bool rightOk = (vp + 5 >= checkExpr.size()) || !isalnum(static_cast<unsigned char>(checkExpr[vp + 5]));
                            if (leftOk && rightOk) {
                                checkExpr = checkExpr.substr(0, vp) + cname + checkExpr.substr(vp + 5);
                            }
                            vp += cname.size();
                        }
                    }
                }
            }

            // Resolve composite types: expand into multiple columns
            if (!dbname.empty() && !cname.empty()) {
                string typeNameOnly = ctype;
                size_t sp = ctype.find(' ');
                if (sp != string::npos) typeNameOnly = ctype.substr(0, sp);
                size_t lp = typeNameOnly.find('(');
                if (lp != string::npos) typeNameOnly = typeNameOnly.substr(0, lp);
                auto ct = g_engine.getCompositeType(dbname, typeNameOnly);
                if (!ct.name.empty()) {
                    for (const auto& f : ct.fields) {
                        Column fcol;
                        fcol.dataName = cname + "_" + f.first;
                        // Parse field type (e.g., "varchar(100)" -> "varchar", dsize=100)
                        string ftype = f.second;
                        std::transform(ftype.begin(), ftype.end(), ftype.begin(), ::tolower);
                        size_t flp = ftype.find('(');
                        if (flp != string::npos) {
                            size_t frp = ftype.find(')', flp);
                            if (frp != string::npos) {
                                try { fcol.dsize = std::stoi(trim(ftype.substr(flp + 1, frp - flp - 1))); } catch (...) { fcol.dsize = 255; }
                            }
                            ftype = ftype.substr(0, flp);
                        }
                        fcol.dataType = ftype;
                        if (ftype == "varchar" || ftype == "text") {
                            fcol.isVariableLength = true;
                            if (fcol.dsize == 0) fcol.dsize = 255;
                        } else if (ftype == "int" || ftype == "integer") {
                            fcol.dsize = 4;
                        } else if (ftype == "bigint") {
                            fcol.dsize = 8;
                        } else if (ftype == "float") {
                            fcol.dsize = 4;
                        } else if (ftype == "double") {
                            fcol.dsize = 8;
                        } else if (ftype == "date") {
                            fcol.dsize = 12;
                        } else if (ftype == "boolean" || ftype == "bool") {
                            fcol.dsize = 1;
                        } else {
                            if (fcol.dsize == 0) fcol.dsize = 255;
                        }
                        fcol.isNull = isNull;
                        tbl.append(fcol);
                    }
                    pos = endPos + 1;
                    continue;
                }
            }

            // Parse type and flags for brace format
            if (isBrace) {
                size_t fkPos = ctype.find("->");
                string typeAndFlags = (fkPos == string::npos) ? ctype : trim(ctype.substr(0, fkPos));
                string fkStr = (fkPos == string::npos) ? "" : trim(ctype.substr(fkPos + 2));

                size_t sp = typeAndFlags.find(' ');
                string typeName = (sp == string::npos) ? typeAndFlags : trim(typeAndFlags.substr(0, sp));
                string flagsStr = (sp == string::npos) ? "" : trim(typeAndFlags.substr(sp + 1));
                {
                    stringstream fs(flagsStr);
                    string f;
                    while (fs >> f) {
                        if (f == "0") isNull = false;
                        if (f == "pk" || f == "PK") isPK = true;
                        if (f == "unsigned") isUnsigned = true;
                    }
                }

                if (!fkStr.empty()) {
                    size_t lp = fkStr.find('(');
                    size_t rp = fkStr.find(')');
                    if (lp != string::npos && rp != string::npos && rp > lp) {
                        fk.colNames.push_back(cname);
                        fk.refTable = trim(fkStr.substr(0, lp));
                        fk.refCols.push_back(trim(fkStr.substr(lp + 1, rp - lp - 1)));
                        fk.onDelete = "restrict";
                        fk.onUpdate = "restrict";
                        // Parse ON DELETE / ON UPDATE action
                        string afterRp = trim(fkStr.substr(rp + 1));
                        auto parseAction = [](const string& s) -> string {
                            string a = trim(s);
                            if (a.size() >= 7 && a.substr(0, 7) == "cascade") return "cascade";
                            if ((a.size() >= 7 && a.substr(0, 7) == "setnull") ||
                                (a.size() >= 8 && a.substr(0, 8) == "set null")) return "setnull";
                            return "restrict";
                        };
                        size_t odPos = afterRp.find("on delete");
                        if (odPos != string::npos) {
                            fk.onDelete = parseAction(afterRp.substr(odPos + 9));
                        }
                        size_t ouPos = afterRp.find("on update");
                        if (ouPos != string::npos) {
                            fk.onUpdate = parseAction(afterRp.substr(ouPos + 9));
                        }
                    }
                }
                ctype = typeName;
                std::transform(ctype.begin(), ctype.end(), ctype.begin(), ::tolower);
            }

            // Check for array type (e.g., int[], varchar[])
            bool isArray = false;
            if (ctype.size() >= 2 && ctype.substr(ctype.size() - 2) == "[]") {
                isArray = true;
                ctype = ctype.substr(0, ctype.size() - 2);
            }

            Column col;
            bool colCreated = false;

            if (ctype.substr(0, 6) == "serial") {
                col = makeIntColumn(cname, false, 2, isPK);
                isAutoIncrement = true;
                col.isAutoIncrement = true;
                colCreated = true;
            } else if (ctype.substr(0, 8) == "interval") {
                col = makeIntervalColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (isPgStringBackedType(ctype)) {
                col = makePgStringBackedColumn(cname, ctype, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 3) == "int") {
                col = makeIntColumn(cname, isNull, 2, isPK, isUnsigned);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "tiny") {
                col = makeIntColumn(cname, isNull, 1, isPK, isUnsigned);
                colCreated = true;
            } else if (ctype.substr(0, 8) == "smallint") {
                col = makeIntColumn(cname, isNull, 0, isPK, isUnsigned);
                colCreated = true;
            } else if (ctype.substr(0, 6) == "bigint") {
                col = makeIntColumn(cname, isNull, 3, isPK, isUnsigned);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "long") {
                col = makeIntColumn(cname, isNull, 3, isPK, isUnsigned);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "bool") {
                col = makeBooleanColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 3) == "bit") {
                string bitType = (ctype == "bit" && parts.size() >= 3 && parts[2] == "varying")
                    ? "bit varying" : "bit";
                col = makePgStringBackedColumn(cname, bitType, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "uuid") {
                col = makeUuidColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "date") {
                col = makeDateColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 12) == "timestamptz") {
                col = makeTimestamptzColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 9) == "timestamp") {
                col = makeTimestampColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "time") {
                col = makeTimeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 8) == "datetime") {
                col = makeDateTimeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "char") {
                size_t len = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    if (lenStr == "(" && parts.size() >= 4) lenStr = parts[3];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) len = len * 10 + (c - '0');
                }
                if (len == 0) {
                    size_t start = 4;
                    if (start < ctype.size() && ctype[start] == '(') ++start;
                    for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                        len = len * 10 + (ctype[i] - '0');
                }
                if (len == 0) len = 1;
                col = makeStringColumn(cname, isNull, len, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 7) == "varchar") {
                size_t len = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    if (lenStr == "(" && parts.size() >= 4) lenStr = parts[3];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) len = len * 10 + (c - '0');
                }
                if (len == 0) {
                    size_t start = 7;
                    if (start < ctype.size() && ctype[start] == '(') ++start;
                    for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                        len = len * 10 + (ctype[i] - '0');
                }
                if (len == 0) len = 1;
                col = makeVarCharColumn(cname, isNull, len, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 7) == "nvarchar") {
                size_t len = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    if (lenStr == "(" && parts.size() >= 4) lenStr = parts[3];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) len = len * 10 + (c - '0');
                }
                if (len == 0) {
                    size_t start = 7;
                    if (start < ctype.size() && ctype[start] == '(') ++start;
                    for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                        len = len * 10 + (ctype[i] - '0');
                }
                if (len == 0) len = 1;
                col = makeNVarCharColumn(cname, isNull, len, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 5) == "nchar") {
                size_t len = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    if (lenStr == "(" && parts.size() >= 4) lenStr = parts[3];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) len = len * 10 + (c - '0');
                }
                if (len == 0) {
                    size_t start = 5;
                    if (start < ctype.size() && ctype[start] == '(') ++start;
                    for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                        len = len * 10 + (ctype[i] - '0');
                }
                if (len == 0) len = 1;
                col = makeNCharColumn(cname, isNull, len, isPK);
                colCreated = true;
            } else if (ctype == "binary") {
                size_t blen = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    if (lenStr == "(" && parts.size() >= 4) lenStr = parts[3];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) blen = blen * 10 + (c - '0');
                }
                if (blen == 0) blen = 1;
                col = makeBinaryColumn(cname, isNull, blen, isPK);
                colCreated = true;
            } else if (ctype == "varbinary") {
                size_t vlen = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    if (lenStr == "(" && parts.size() >= 4) lenStr = parts[3];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) vlen = vlen * 10 + (c - '0');
                }
                if (vlen == 0) vlen = 1;
                col = makeVarBinaryColumn(cname, isNull, vlen, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "blob") {
                col = makeBlobColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "text") {
                col = makeTextColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 5) == "jsonb") {
                col = makeJsonbColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "json") {
                col = makeJsonColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 6) == "pg_lsn") {
                col = makePgLsnColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 3) == "xml") {
                col = makeXmlColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 9) == "int4range") {
                col = makeInt4RangeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 9) == "int8range") {
                col = makeInt8RangeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 8) == "numrange") {
                col = makeNumRangeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 7) == "tsrange") {
                col = makeTsRangeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 8) == "tstzrang") {
                col = makeTstzRangeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 9) == "daterange") {
                col = makeDateRangeColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 8) == "tsvector") {
                col = makeTsVectorColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 7) == "tsquery") {
                col = makeTsQueryColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 5) == "float") {
                col = makeFloatColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 6) == "double" || ctype.substr(0, 5) == "money") {
                col = makeDoubleColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 5) == "point") {
                col = makePointColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype == "inet") {
                col = makeINetColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype == "cidr") {
                col = makeCidrColumn(cname, isNull, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 7) == "decimal") {
                int prec = 10, sc = 0;
                size_t lp = ctype.find('(');
                size_t rp = ctype.find(')');
                if (lp != string::npos && rp != string::npos && rp > lp) {
                    string inside = ctype.substr(lp + 1, rp - lp - 1);
                    size_t comma = inside.find(',');
                    if (comma != string::npos) {
                        try { prec = stoi(trim(inside.substr(0, comma))); } catch (...) {}
                        try { sc = stoi(trim(inside.substr(comma + 1))); } catch (...) {}
                    } else {
                        try { prec = stoi(trim(inside)); } catch (...) {}
                    }
                }
                col = makeDecimalColumn(cname, isNull, prec, sc, isPK);
                colCreated = true;
            } else if (ctype.substr(0, 4) == "enum") {
                // Parse ENUM('a', 'b', 'c') from segment
                vector<string> enumVals;
                size_t lp = segment.find('(');
                size_t rp = segment.rfind(')');
                if (lp != string::npos && rp != string::npos && rp > lp) {
                    string inside = segment.substr(lp + 1, rp - lp - 1);
                    // Split by comma, respecting quotes
                    size_t i = 0;
                    while (i < inside.size()) {
                        while (i < inside.size() && isspace(static_cast<unsigned char>(inside[i]))) ++i;
                        if (i >= inside.size()) break;
                        if (inside[i] == '\'' || inside[i] == '"') {
                            char quote = inside[i++];
                            size_t start = i;
                            while (i < inside.size() && inside[i] != quote) ++i;
                            enumVals.push_back(inside.substr(start, i - start));
                            if (i < inside.size()) ++i; // skip quote
                        } else {
                            size_t start = i;
                            while (i < inside.size() && inside[i] != ',') ++i;
                            enumVals.push_back(trim(inside.substr(start, i - start)));
                        }
                        while (i < inside.size() && (inside[i] == ',' || isspace(static_cast<unsigned char>(inside[i])))) ++i;
                    }
                }
                col = makeStringColumn(cname, isNull, 64, isPK);
                col.enumValues = enumVals;
                colCreated = true;
            }
            // Apply array flag and append column
            if (colCreated) {
                if (isArray) {
                    col.isArray = true;
                    col.isVariableLength = true;
                    col.dataType += "[]";
                    col.dsize = 256;  // arrays need larger storage
                }
                col.isAutoIncrement = isAutoIncrement;
                col.isUnique = isUnique;
                col.defaultValue = defaultVal;
                col.checkExpr = checkExpr;
                col.generatedExpr = generatedExpr;
                tbl.append(col);
            }
            if (!fk.colNames.empty()) tbl.appendFK(fk);
            pos = endPos + 1;
        }
        break;
    }
    // Resolve composite PRIMARY KEY column names to indices
    for (const auto& cname : pkColNames) {
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == cname) {
                tbl.pkColIndices.push_back(i);
                break;
            }
        }
    }
    // Resolve composite UNIQUE constraint column names to indices
    for (const auto& ucols : uniqueColNames) {
        std::vector<size_t> indices;
        for (const auto& cname : ucols) {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].dataName == cname) {
                    indices.push_back(i);
                    break;
                }
            }
        }
        if (!indices.empty()) tbl.uniqueConstraints.push_back(std::move(indices));
    }
    return tbl;
}

// ========================================================================
// Parse UPDATE SET clause: "col1=val1, col2='val 2'"
// ========================================================================
static std::map<std::string, std::string> parseSetClause(const std::string& sql, size_t startPos, size_t endPos) {
    std::map<std::string, std::string> updates;
    std::string clause = trim(sql.substr(startPos, endPos - startPos));
    size_t pos = 0;
    while (pos < clause.size()) {
        size_t eqPos = clause.find('=', pos);
        size_t commaPos = clause.find(',', pos);
        if (eqPos == std::string::npos) break;
        std::string col = trim(clause.substr(pos, eqPos - pos));
        size_t valEnd = (commaPos == std::string::npos) ? clause.size() : commaPos;
        std::string val = trim(clause.substr(eqPos + 1, valEnd - eqPos - 1));
        updates[col] = stripQuotes(val);
        pos = (commaPos == std::string::npos) ? clause.size() : commaPos + 1;
    }
    return updates;
}

// ========================================================================
// Subquery helpers
// ========================================================================
static std::vector<std::string> runSubQuery(const std::string& rawSql, Session& s) {
    std::string sql = sqlProcessor(rawSql);
    size_t fromPos = sql.find("from");
    if (fromPos == std::string::npos) return {};
    std::string columns = trim(sql.substr(6, fromPos - 6));

    size_t wherePos = sql.find("where", fromPos);
    size_t tnameEnd = (wherePos != std::string::npos) ? wherePos : sql.size();
    std::string tname = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));

    if (!g_engine.tableExists(s.currentDB, tname)) return {};

    std::set<std::string> selectCols;
    if (columns != "*") {
        std::stringstream css(columns);
        std::string item;
        while (std::getline(css, item, ',')) selectCols.insert(trim(item));
    }

    std::vector<std::string> answers;
    if (wherePos != std::string::npos) {
        std::string condStr = normalizeConditionStr(trim(sql.substr(wherePos + 5)));
        std::vector<std::string> tokens = tokenize(condStr);
        tokens.insert(tokens.begin(), "(");
        tokens.push_back(")");
        for (auto& t : tokens) t = modifyLogic(t);
        auto groups = breakDownConditions(tokens);
        std::set<std::string> seen;
        for (const auto& g : groups) {
            auto part = g_engine.query(s.currentDB, tname, g, selectCols, {});
            for (const auto& row : part) {
                if (seen.insert(row).second) answers.push_back(row);
            }
        }
    } else {
        answers = g_engine.query(s.currentDB, tname, {}, selectCols, {});
    }

    for (auto& s : answers) {
        s = trim(s);
    }
    return answers;
}

// Helper: find matching closing paren from start position
static size_t findMatchingParen(const std::string& s, size_t start) {
    int depth = 1;
    bool inQuote = false;
    for (size_t i = start + 1; i < s.size(); ++i) {
        if (s[i] == '\'') {
            if (inQuote && i + 1 < s.size() && s[i + 1] == '\'') {
                ++i;
                continue;
            }
            inQuote = !inQuote;
            continue;
        }
        if (inQuote) continue;
        if (s[i] == '(') ++depth;
        else if (s[i] == ')') { --depth; if (depth == 0) return i; }
    }
    return std::string::npos;
}

// Execute a SELECT subquery for derived table use; returns rows and fills outColNames.
// Supports basic SELECT with WHERE, ORDER BY, LIMIT, OFFSET.
static std::vector<std::string> runDerivedSubQuery(const std::string& rawSql, Session& s,
                                                    std::vector<std::string>& outColNames) {
    std::string sql = sqlProcessor(rawSql);
    size_t fromPos = sql.find("from");
    if (fromPos == std::string::npos) return {};

    std::string columns = trim(sql.substr(6, fromPos - 6));

    size_t wherePos = sql.find("where", fromPos);
    size_t orderPos = sql.find("order by", fromPos);
    size_t limitPos = sql.find("limit", fromPos);
    size_t offsetPos = sql.find("offset", fromPos);

    size_t tnameEnd = (wherePos != std::string::npos) ? wherePos
                     : (orderPos != std::string::npos) ? orderPos
                     : (limitPos != std::string::npos) ? limitPos
                     : (offsetPos != std::string::npos) ? offsetPos
                     : sql.size();
    std::string tname = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));

    if (!g_engine.tableExists(s.currentDB, tname)) return {};

    // Parse column names
    outColNames.clear();
    if (columns == "*") {
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        for (size_t i = 0; i < tbl.len; ++i) outColNames.push_back(tbl.cols[i].dataName);
    } else {
        for (const auto& itemRaw : splitSelectColumns(columns)) {
            std::string item = trim(itemRaw);
            size_t asPos = item.find(" as ");
            if (asPos != std::string::npos) {
                outColNames.push_back(trim(item.substr(asPos + 4)));
            } else {
                outColNames.push_back(item);
            }
        }
    }

    std::set<std::string> selectCols;
    for (const auto& c : outColNames) selectCols.insert(c);

    std::vector<std::string> conds;
    if (wherePos != std::string::npos) {
        size_t condEnd = (orderPos != std::string::npos) ? orderPos
                        : (limitPos != std::string::npos) ? limitPos
                        : (offsetPos != std::string::npos) ? offsetPos
                        : sql.size();
        std::string condStr = normalizeConditionStr(trim(sql.substr(wherePos + 5, condEnd - wherePos - 5)));
        std::vector<std::string> tokens = tokenize(condStr);
        tokens.insert(tokens.begin(), "(");
        tokens.push_back(")");
        for (auto& t : tokens) t = modifyLogic(t);
        auto groups = breakDownConditions(tokens);
        if (!groups.empty()) conds = groups[0];
    }

    std::vector<StorageEngine::OrderBySpec> orderBy;
    if (orderPos != std::string::npos) {
        size_t orderEnd = (limitPos != std::string::npos) ? limitPos
                         : (offsetPos != std::string::npos) ? offsetPos
                         : sql.size();
        std::string orderRest = trim(sql.substr(orderPos + 8, orderEnd - orderPos - 8));
        std::stringstream oss(orderRest);
        std::string part;
        while (std::getline(oss, part, ',')) {
            part = trim(part);
            std::vector<std::string> ot = tokenize(part);
            if (ot.empty()) continue;
            StorageEngine::OrderBySpec spec;
            // Detect function expression: funcName ( arg )
            if (ot.size() >= 4 && ot[1] == "(" && ot[3] == ")") {
                spec.isExpression = true;
                spec.exprFunc = toLower(ot[0]);
                spec.exprArg = ot[2];
                spec.colName = ot[0] + "(" + ot[2] + ")";
                // Scan for ASC/DESC and NULLS FIRST/LAST
                for (size_t ti = 4; ti < ot.size(); ++ti) {
                    std::string lt = toLower(ot[ti]);
                    if (lt == "desc") spec.ascending = false;
                    else if (lt == "asc") spec.ascending = true;
                    else if (lt == "nulls" && ti + 1 < ot.size()) {
                        std::string nl = toLower(ot[ti + 1]);
                        if (nl == "first") spec.nullsFirst = true;
                        else if (nl == "last") spec.nullsFirst = false;
                        ++ti;
                    }
                }
            }
            // Detect simple arithmetic: col + num or col - num
            else if (ot.size() >= 3 && (ot[1] == "+" || ot[1] == "-")) {
                spec.isExpression = true;
                spec.exprFunc = (ot[1] == "+") ? "add" : "sub";
                spec.exprArg = ot[0];
                spec.exprArg2 = ot[2];
                spec.colName = ot[0] + " " + ot[1] + " " + ot[2];
                for (size_t ti = 3; ti < ot.size(); ++ti) {
                    std::string lt = toLower(ot[ti]);
                    if (lt == "desc") spec.ascending = false;
                    else if (lt == "asc") spec.ascending = true;
                    else if (lt == "nulls" && ti + 1 < ot.size()) {
                        std::string nl = toLower(ot[ti + 1]);
                        if (nl == "first") spec.nullsFirst = true;
                        else if (nl == "last") spec.nullsFirst = false;
                        ++ti;
                    }
                }
            }
            // Plain column name
            else {
                spec.colName = ot[0];
                for (size_t ti = 1; ti < ot.size(); ++ti) {
                    std::string lt = toLower(ot[ti]);
                    if (lt == "desc") spec.ascending = false;
                    else if (lt == "asc") spec.ascending = true;
                    else if (lt == "nulls" && ti + 1 < ot.size()) {
                        std::string nl = toLower(ot[ti + 1]);
                        if (nl == "first") spec.nullsFirst = true;
                        else if (nl == "last") spec.nullsFirst = false;
                        ++ti;
                    }
                }
            }
            orderBy.push_back(spec);
        }
    }

    std::vector<std::string> answers;
    if (conds.empty()) {
        answers = g_engine.query(s.currentDB, tname, {}, selectCols, orderBy);
    } else {
        answers = g_engine.query(s.currentDB, tname, conds, selectCols, orderBy);
    }

    size_t limitVal = 0, offsetVal = 0;
    if (limitPos != std::string::npos) {
        size_t limEnd = (offsetPos != std::string::npos) ? offsetPos : sql.size();
        std::string lstr = trim(sql.substr(limitPos + 5, limEnd - limitPos - 5));
        try { limitVal = static_cast<size_t>(std::stoull(lstr)); } catch (...) {}
    }
    if (offsetPos != std::string::npos) {
        std::string ostr = trim(sql.substr(offsetPos + 6));
        try { offsetVal = static_cast<size_t>(std::stoull(ostr)); } catch (...) {}
    }
    if (offsetVal < answers.size()) {
        if (limitVal > 0 && offsetVal + limitVal < answers.size())
            answers.erase(answers.begin() + offsetVal + limitVal, answers.end());
        if (offsetVal > 0)
            answers.erase(answers.begin(), answers.begin() + offsetVal);
    }

    for (auto& row : answers) row = trim(row);
    return answers;
}

// Helper: create a temp table from query rows and column names.
// Returns the user-visible temp name (without __tmp_ prefix).
static std::string createTempTableFromRows(Session& s,
                                           const std::vector<std::string>& rows,
                                           const std::vector<std::string>& colNames,
                                           int& counter) {
    std::string tmpName = "__cte_" + std::to_string(counter++);
    std::string actualName = tempTablePrefix(tmpName);
    TableSchema tmpTbl;
    tmpTbl.tablename = actualName;
    for (const auto& cname : colNames) {
        Column col;
        col.dataName = cname;
        col.dataType = "varchar";
        col.isVariableLength = true;
        col.dsize = 255;
        col.isNull = true;
        tmpTbl.append(col);
    }
    auto res = g_engine.createTable(s.currentDB, tmpTbl);
    if (res != DBStatus::OK) return "";
    s.tempTables.insert(tmpName);

    for (const auto& row : rows) {
        std::map<std::string, std::string> values;
        std::stringstream rss(row);
        std::string val;
        size_t colIdx = 0;
        while (colIdx < colNames.size() && rss >> val) {
            values[colNames[colIdx++]] = val;
        }
        if (!values.empty()) g_engine.insert(s.currentDB, actualName, values);
    }
    return tmpName;
}

// Forward declaration for CTE DML support
bool execute(const std::string& rawSql, Session& s);

// Process CTEs (WITH clause): WITH cte AS (SELECT ...) [, ...] SELECT ...
// Returns modified SQL with CTE references replaced by temp table names.
static std::string processCTEs(const std::string& sql, Session& s) {
    std::string result = sql;
    size_t withPos = result.find("with ");
    if (withPos == std::string::npos) return result;

    size_t pos = withPos + 5; // skip "with "
    int cteCount = 0;

    // Check for RECURSIVE keyword
    bool recursiveMode = false;
    {
        size_t recPos = pos;
        while (recPos < result.size() && isspace((unsigned char)result[recPos])) ++recPos;
        if (recPos + 9 <= result.size() && result.compare(recPos, 9, "recursive") == 0
            && (recPos + 9 == result.size() || isspace((unsigned char)result[recPos + 9]))) {
            recursiveMode = true;
            pos = recPos + 9; // skip "recursive"
        }
    }

    while (pos < result.size()) {
        // Skip leading whitespace
        while (pos < result.size() && isspace((unsigned char)result[pos])) ++pos;
        if (pos >= result.size()) break;

        // Find " as (" to split name and subquery
        size_t asPos = result.find(" as ", pos);
        if (asPos == std::string::npos) break;
        std::string cteName = trim(result.substr(pos, asPos - pos));
        if (cteName.empty()) break;

        // Find opening paren of subquery
        size_t parenStart = result.find('(', asPos + 4);
        if (parenStart == std::string::npos) break;
        size_t parenEnd = findMatchingParen(result, parenStart);
        // debug removed
        if (parenEnd == std::string::npos) break;

        std::string innerSelect = trim(result.substr(parenStart + 1, parenEnd - parenStart - 1));

        std::string tmpName;
        std::vector<std::string> colNames;

        // Detect DML in CTE: INSERT/UPDATE/DELETE with optional RETURNING
        bool isDmlCte = false;
        std::string dmlLower = toLower(innerSelect);
        if (dmlLower.substr(0, 6) == "insert" ||
            dmlLower.substr(0, 6) == "update" ||
            dmlLower.substr(0, 6) == "delete") {
            isDmlCte = true;
            // Extract RETURNING clause
            size_t retPos = dmlLower.find(" returning ");
            if (retPos != string::npos) {
                string retColsStr = trim(innerSelect.substr(retPos + 11));
                if (retColsStr == "*") {
                    // Need to infer columns from the target table
                    string tableName;
                    if (dmlLower.substr(0, 6) == "insert") {
                        size_t intoPos = dmlLower.find(" into ");
                        if (intoPos != string::npos) {
                            string afterInto = trim(innerSelect.substr(intoPos + 6));
                            size_t sp = afterInto.find(' ');
                            tableName = (sp == string::npos) ? afterInto : afterInto.substr(0, sp);
                        }
                    } else if (dmlLower.substr(0, 6) == "update") {
                        size_t sp = innerSelect.find(' ');
                        if (sp != string::npos) tableName = trim(innerSelect.substr(sp + 1));
                        sp = tableName.find(' ');
                        if (sp != string::npos) tableName = tableName.substr(0, sp);
                    } else { // delete
                        size_t fromPos = dmlLower.find(" from ");
                        if (fromPos != string::npos) {
                            string afterFrom = trim(innerSelect.substr(fromPos + 6));
                            size_t sp = afterFrom.find(' ');
                            tableName = (sp == string::npos) ? afterFrom : afterFrom.substr(0, sp);
                        }
                    }
                    if (!tableName.empty() && g_engine.tableExists(s.currentDB, tableName)) {
                        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tableName);
                        for (size_t i = 0; i < tbl.len; ++i) colNames.push_back(tbl.cols[i].dataName);
                    }
                } else {
                    // Parse comma-separated column names
                    stringstream css(retColsStr);
                    string c;
                    while (getline(css, c, ',')) {
                        string tc = trim(c);
                        if (!tc.empty()) colNames.push_back(tc);
                    }
                }
                // Don't strip RETURNING; we'll handle it manually
            }

            vector<string> returnRows;
            string tableName;
            if (dmlLower.substr(0, 6) == "insert") {
                size_t intoPos = dmlLower.find(" into ");
                if (intoPos != string::npos) {
                    string afterInto = trim(innerSelect.substr(intoPos + 6));
                    size_t sp = afterInto.find(' ');
                    tableName = (sp == string::npos) ? afterInto : afterInto.substr(0, sp);
                }
            } else if (dmlLower.substr(0, 6) == "update") {
                size_t sp = innerSelect.find(' ');
                if (sp != string::npos) {
                    string afterUpd = trim(innerSelect.substr(sp + 1));
                    sp = afterUpd.find(' ');
                    tableName = (sp == string::npos) ? afterUpd : afterUpd.substr(0, sp);
                }
            } else { // delete
                size_t fromPos = dmlLower.find(" from ");
                if (fromPos != string::npos) {
                    string afterFrom = trim(innerSelect.substr(fromPos + 6));
                    size_t sp = afterFrom.find(' ');
                    tableName = (sp == string::npos) ? afterFrom : afterFrom.substr(0, sp);
                }
            }

            if (!colNames.empty() && !tableName.empty() &&
                g_engine.tableExists(s.currentDB, tableName)) {
                // Build a SELECT query to get RETURNING data before DML
                string selectSql = "select ";
                for (size_t i = 0; i < colNames.size(); ++i) {
                    if (i > 0) selectSql += ", ";
                    selectSql += colNames[i];
                }
                selectSql += " from " + tableName;
                // Append WHERE conditions from the DML (strip RETURNING first)
                size_t wherePos = dmlLower.find(" where ");
                if (wherePos != string::npos) {
                    string whereClause = innerSelect.substr(wherePos);
                    size_t retPos2 = toLower(whereClause).find(" returning ");
                    if (retPos2 != string::npos) whereClause = trim(whereClause.substr(0, retPos2));
                    selectSql += whereClause;
                }
                // For INSERT, we can't pre-select; use the VALUES directly
                if (dmlLower.substr(0, 6) == "insert") {
                    // Execute INSERT first without RETURNING
                    string dmlNoRet = innerSelect;
                    size_t rPos = toLower(dmlNoRet).find(" returning ");
                    if (rPos != string::npos) dmlNoRet = trim(dmlNoRet.substr(0, rPos));
                    stringstream nullOut;
                    streambuf* oldBuf = cout.rdbuf(nullOut.rdbuf());
                    execute(dmlNoRet, s);
                    cout.rdbuf(oldBuf);
                    // Query the inserted row using primary key if possible
                    size_t valPos = dmlLower.find(" values ");
                    if (valPos != string::npos) {
                        string vals = trim(innerSelect.substr(valPos + 8));
                        // Remove parens
                        if (vals.size() >= 2 && vals.front() == '(' && vals.back() == ')') {
                            vals = vals.substr(1, vals.size() - 2);
                        }
                        // Build a simple row from values
                        stringstream vss(vals);
                        string v;
                        vector<string> valueList;
                        while (getline(vss, v, ',')) valueList.push_back(trim(v));
                        // For single-row insert with matching columns, construct row
                        if (valueList.size() == colNames.size()) {
                            string row;
                            for (size_t i = 0; i < valueList.size(); ++i) {
                                if (i > 0) row += " ";
                                row += valueList[i];
                            }
                            returnRows.push_back(row);
                        } else {
                            // Try to query the table for the last inserted row
                            auto allRows = g_engine.query(s.currentDB, tableName, {}, {}, {});
                            if (!allRows.empty()) returnRows.push_back(allRows.back());
                        }
                    }
                } else {
                    // DELETE/UPDATE: pre-select matching rows
                    auto preRows = runDerivedSubQuery(selectSql, s, colNames);
                    returnRows = preRows;
                    // Execute DML without RETURNING
                    string dmlNoRet = innerSelect;
                    size_t rPos = toLower(dmlNoRet).find(" returning ");
                    if (rPos != string::npos) dmlNoRet = trim(dmlNoRet.substr(0, rPos));
                    stringstream nullOut;
                    streambuf* oldBuf = cout.rdbuf(nullOut.rdbuf());
                    execute(dmlNoRet, s);
                    cout.rdbuf(oldBuf);
                }
            } else {
                // No RETURNING or no table: just execute DML
                string dmlNoRet = innerSelect;
                size_t rPos = toLower(dmlNoRet).find(" returning ");
                if (rPos != string::npos) dmlNoRet = trim(dmlNoRet.substr(0, rPos));
                stringstream nullOut2;
                streambuf* oldBuf = cout.rdbuf(nullOut2.rdbuf());
                execute(dmlNoRet, s);
                cout.rdbuf(oldBuf);
            }

            if (!colNames.empty()) {
                tmpName = createTempTableFromRows(s, returnRows, colNames, cteCount);
            } else {
                colNames.push_back("count");
                vector<string> emptyRow;
                tmpName = createTempTableFromRows(s, emptyRow, colNames, cteCount);
            }
            if (tmpName.empty()) break;
        }

        if (!isDmlCte) {
            // Detect UNION ALL split for recursive CTE
        size_t unionAllPos = std::string::npos;
        if (recursiveMode) {
            // Look for top-level UNION ALL
            int depth = 0;
            for (size_t k = 0; k + 9 <= innerSelect.size(); ++k) {
                if (innerSelect[k] == '(') depth++;
                else if (innerSelect[k] == ')') depth--;
                if (depth == 0 && innerSelect.compare(k, 9, "union all") == 0) {
                    unionAllPos = k;
                    break;
                }
            }
        }

        if (recursiveMode && unionAllPos != std::string::npos) {
            // Recursive CTE: anchor UNION ALL recursive_part
            std::string anchorSql = trim(innerSelect.substr(0, unionAllPos));
            std::string recursiveSql = trim(innerSelect.substr(unionAllPos + 9));

            // Execute anchor to get initial rows
            std::vector<std::string> anchorRows = runDerivedSubQuery(anchorSql, s, colNames);
            if (colNames.empty()) break;

            // Create temp table from anchor rows
            tmpName = createTempTableFromRows(s, anchorRows, colNames, cteCount);
            if (tmpName.empty()) break;
            std::string tmpActualName = tempTablePrefix(tmpName);

            // Iteratively execute recursive part
            std::vector<std::string> allRows = anchorRows;
            std::set<std::string> seenRows(anchorRows.begin(), anchorRows.end());
            const int MAX_ITERATIONS = 1000;
            for (int iter = 0; iter < MAX_ITERATIONS; ++iter) {
                // Replace CTE name references in recursiveSql with the temp table name
                std::string recSql = recursiveSql;
                size_t rp = 0;
                while ((rp = recSql.find(cteName, rp)) != std::string::npos) {
                    // Word boundary check
                    bool leftOk = (rp == 0) || !isalnum(static_cast<unsigned char>(recSql[rp - 1]));
                    bool rightOk = (rp + cteName.size() == recSql.size()) ||
                                    !isalnum(static_cast<unsigned char>(recSql[rp + cteName.size()]));
                    if (leftOk && rightOk) {
                        recSql = recSql.substr(0, rp) + tmpActualName + recSql.substr(rp + cteName.size());
                        rp += tmpActualName.size();
                    } else {
                        rp += cteName.size();
                    }
                }
                std::vector<std::string> recCols;
                std::vector<std::string> newRows = runDerivedSubQuery(recSql, s, recCols);
                bool added = false;
                for (const auto& row : newRows) {
                    if (seenRows.insert(row).second) {
                        // Insert into temp table
                        std::map<std::string, std::string> values;
                        std::stringstream rss(row);
                        std::string val;
                        size_t colIdx = 0;
                        while (colIdx < colNames.size() && rss >> val) {
                            values[colNames[colIdx++]] = val;
                        }
                        if (!values.empty()) {
                            g_engine.insert(s.currentDB, tmpActualName, values);
                            allRows.push_back(row);
                            added = true;
                        }
                    }
                }
                if (!added) break;
            }
        } else {
            // Non-recursive CTE: execute and store
            auto rows = runDerivedSubQuery(innerSelect, s, colNames);
            if (colNames.empty()) break;
            tmpName = createTempTableFromRows(s, rows, colNames, cteCount);
            if (tmpName.empty()) break;
        }
        } // end if (!isDmlCte)

        // Replace CTE name references in the rest of the SQL
        size_t replacePos = parenEnd + 1;
        while ((replacePos = result.find(cteName + ".", replacePos)) != std::string::npos) {
            result = result.substr(0, replacePos) + result.substr(replacePos + cteName.size() + 1);
        }
        replacePos = parenEnd + 1;
        while ((replacePos = result.find(cteName, replacePos)) != std::string::npos) {
            result = result.substr(0, replacePos) + tmpName + result.substr(replacePos + cteName.size());
            replacePos += tmpName.size();
        }

        // Move past this CTE definition
        pos = parenEnd + 1;
        while (pos < result.size() && isspace((unsigned char)result[pos])) ++pos;
        if (pos < result.size() && result[pos] == ',') {
            ++pos;
            continue;
        }
        break;
    }

    // Remove the WITH clause from the SQL
    size_t mainQueryStart = pos;
    while (mainQueryStart < result.size() && isspace((unsigned char)result[mainQueryStart])) ++mainQueryStart;
    if (mainQueryStart < result.size()) {
        result = result.substr(0, withPos) + result.substr(mainQueryStart);
    }
    // debug removed
    return result;
}

// Detect and process derived tables (subqueries in FROM clause).
// Replaces (SELECT ...) AS alias with a temporary table name.
// Returns modified SQL.
static std::string processDerivedTables(const std::string& sql, Session& s) {
    std::string result = sql;
    int derivedCount = 0;

    while (true) {
        size_t parenStart = result.find("(select");
        if (parenStart == std::string::npos) break;
        // Skip LATERAL subqueries — they are handled dynamically per left-row
        std::string beforeParen = trim(result.substr(0, parenStart));
        if (beforeParen.size() >= 7 && beforeParen.substr(beforeParen.size() - 7) == "lateral") {
            // Move past this occurrence to avoid infinite loop
            parenStart = result.find("(select", parenStart + 1);
            if (parenStart == std::string::npos) break;
            beforeParen = trim(result.substr(0, parenStart));
            if (beforeParen.size() >= 7 && beforeParen.substr(beforeParen.size() - 7) == "lateral") {
                continue;
            }
        }
        size_t parenEnd = findMatchingParen(result, parenStart);
        if (parenEnd == std::string::npos) break;

        std::string afterParen = trim(result.substr(parenEnd + 1));
        if (afterParen.size() < 3 || afterParen.substr(0, 3) != "as ") break;
        std::string alias = trim(afterParen.substr(3));
        size_t sp = alias.find(' ');
        if (sp != std::string::npos) alias = alias.substr(0, sp);
        if (alias.empty()) break;

        std::string innerSelect = trim(result.substr(parenStart + 1, parenEnd - parenStart - 1));
        std::vector<std::string> colNames;
        auto rows = runDerivedSubQuery(innerSelect, s, colNames);
        if (colNames.empty()) break;

        int counter = derivedCount;
        std::string tmpName = createTempTableFromRows(s, rows, colNames, counter);
        if (tmpName.empty()) break;
        derivedCount = counter;

        // Replace alias references with bare column names
        std::string aliasDot = alias + ".";
        size_t pos = 0;
        while ((pos = result.find(aliasDot, pos)) != std::string::npos) {
            result = result.substr(0, pos) + result.substr(pos + aliasDot.size());
            // pos stays same since we removed characters
        }

        // Replace the derived table definition with temp table name
        size_t aliasEnd = parenEnd + 1;
        while (aliasEnd < result.size() && isspace((unsigned char)result[aliasEnd])) ++aliasEnd;
        if (aliasEnd + 2 < result.size() && result.substr(aliasEnd, 3) == "as ") {
            aliasEnd += 3;
            while (aliasEnd < result.size() && isspace((unsigned char)result[aliasEnd])) ++aliasEnd;
            aliasEnd += alias.size();
        }
        result = result.substr(0, parenStart) + tmpName + result.substr(aliasEnd);
    }

    return result;
}

// Detect and process LATERAL JOINs by materializing them into temporary tables.
// Supports: FROM t CROSS JOIN LATERAL (SELECT ...) AS alias
//           FROM t, LATERAL (SELECT ...) AS alias
// The lateral subquery can reference columns from the left table.
static std::string processLateralJoins(const std::string& sql, Session& s) {
    std::string result = sql;
    int lateralCount = 0;

    while (true) {
        size_t latPos = result.find("lateral");
        if (latPos == std::string::npos) break;
        size_t parenStart = result.find("(select", latPos);
        if (parenStart == std::string::npos) break;
        size_t parenEnd = findMatchingParen(result, parenStart);
        if (parenEnd == std::string::npos) break;

        std::string afterParen = trim(result.substr(parenEnd + 1));
        if (afterParen.size() < 3 || afterParen.substr(0, 3) != "as ") break;
        std::string alias = trim(afterParen.substr(3));
        size_t sp = alias.find(' ');
        if (sp != std::string::npos) alias = alias.substr(0, sp);
        if (alias.empty()) break;

        // Extract the inner SELECT
        std::string innerSelect = trim(result.substr(parenStart + 1, parenEnd - parenStart - 1));

        // Determine left table: text between "from" and the lateral construct
        std::string leftTableName, leftAlias;
        size_t fromPos = result.rfind("from", latPos);
        if (fromPos != std::string::npos) {
            std::string afterFrom = trim(result.substr(fromPos + 4, latPos - fromPos - 4));
            // afterFrom could be "t cross join " or "t, " or "t inner join u cross join "
            // Find the last table reference before lateral
            size_t lastJoin = afterFrom.rfind("join");
            size_t lastComma = afterFrom.rfind(",");
            std::string tableSegment;
            if (lastJoin != std::string::npos) {
                // Take text before "join" (e.g., "t cross" -> "t")
                tableSegment = trim(afterFrom.substr(0, lastJoin));
            } else if (lastComma != std::string::npos) {
                // If comma is at end (e.g., "t,"), take text before comma
                std::string afterComma = trim(afterFrom.substr(lastComma + 1));
                if (afterComma.empty()) {
                    tableSegment = trim(afterFrom.substr(0, lastComma));
                } else {
                    tableSegment = afterComma;
                }
            } else {
                tableSegment = trim(afterFrom);
            }
            size_t spc = tableSegment.find(' ');
            if (spc != std::string::npos) {
                leftTableName = trim(tableSegment.substr(0, spc));
                leftAlias = trim(tableSegment.substr(spc + 1));
            } else {
                leftTableName = tableSegment;
            }
        }
        if (leftTableName.empty()) break;

        std::string resolvedLeft = resolveTableName(s, leftTableName);
        if (!g_engine.tableExists(s.currentDB, resolvedLeft)) break;
        TableSchema leftTbl = g_engine.getTableSchema(s.currentDB, resolvedLeft);
        std::string leftPrefix = leftAlias.empty() ? leftTableName : leftAlias;

        // Read all left rows
        std::vector<std::string> leftRows;
        std::vector<std::string> leftColNames;
        for (size_t i = 0; i < leftTbl.len; ++i) leftColNames.push_back(leftTbl.cols[i].dataName);
        g_engine.forEachRow(s.currentDB, resolvedLeft, [&](uint32_t, uint16_t, const char* data, size_t len) {
            leftRows.emplace_back(data, len);
        });

        // Execute lateral subquery for each left row and collect results
        std::vector<std::string> allRows;
        std::vector<std::string> colNames;
        for (const auto& lrow : leftRows) {
            std::string replacedSql = innerSelect;
            // Replace left table column references with literal values
            for (size_t ci = 0; ci < leftTbl.len; ++ci) {
                std::string val = g_engine.extractColumnValue(lrow, leftTbl, ci);
                // Escape single quotes in value
                std::string escVal;
                for (char c : val) {
                    if (c == '\'') escVal += "''";
                    else escVal += c;
                }
                bool isNum = leftTbl.cols[ci].dataType != "char" && !leftTbl.cols[ci].isVariableLength;
                std::string lit = isNum ? escVal : "'" + escVal + "'";

                for (const std::string& pref : {leftPrefix + ".", resolvedLeft + "."}) {
                    std::string place = pref + leftTbl.cols[ci].dataName;
                    size_t pos = 0;
                    while ((pos = replacedSql.find(place, pos)) != std::string::npos) {
                        replacedSql.replace(pos, place.size(), lit);
                        pos += lit.size();
                    }
                }
            }
            auto rows = runDerivedSubQuery(replacedSql, s, colNames);
            for (const auto& r : rows) {
                allRows.push_back(r);
            }
        }

        if (colNames.empty()) break;

        int counter = lateralCount;
        std::string tmpName = createTempTableFromRows(s, allRows, colNames, counter);
        if (tmpName.empty()) break;
        lateralCount = counter;

        // Replace the lateral join construct with temp table name first
        size_t aliasEnd = parenEnd + 1;
        while (aliasEnd < result.size() && isspace((unsigned char)result[aliasEnd])) ++aliasEnd;
        if (aliasEnd + 2 < result.size() && result.substr(aliasEnd, 3) == "as ") {
            aliasEnd += 3;
            while (aliasEnd < result.size() && isspace((unsigned char)result[aliasEnd])) ++aliasEnd;
            aliasEnd += alias.size();
        }
        // If preceded by comma, replace comma with "cross join" to make a valid JOIN
        size_t commaBefore = result.rfind(",", latPos);
        if (commaBefore != std::string::npos && commaBefore > fromPos) {
            result = result.substr(0, commaBefore) + " cross join " + tmpName + result.substr(aliasEnd);
        } else {
            result = result.substr(0, latPos) + tmpName + result.substr(aliasEnd);
        }

        // Then replace alias references with bare column names
        std::string aliasDot = alias + ".";
        size_t pos = 0;
        while ((pos = result.find(aliasDot, pos)) != std::string::npos) {
            result = result.substr(0, pos) + result.substr(pos + aliasDot.size());
        }
    }

    return result;
}

// Expand IN (...) / EXISTS (...) / ANY / ALL subqueries into plain conditions
static std::string expandSubqueries(std::string sql, Session& s) {
    // ---------- EXISTS ----------
    while (true) {
        size_t pos = sql.find("exists");
        if (pos == std::string::npos) break;
        size_t parenStart = sql.find('(', pos);
        if (parenStart == std::string::npos) break;
        size_t parenEnd = findMatchingParen(sql, parenStart);
        if (parenEnd == std::string::npos) break;

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        bool hasResult = false;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            auto rows = runSubQuery(inner, s);
            hasResult = !rows.empty();
        }
        // Replace with special markers recognized by evalConditionOnRow
        std::string replacement = hasResult ? "__true__=1" : "__false__=1";
        sql = sql.substr(0, pos) + replacement + sql.substr(parenEnd + 1);
    }

    // ---------- ANY / ALL ----------
    while (true) {
        auto findAnyAll = [&](const std::string& key) -> size_t {
            size_t p = sql.find(" " + key + " ");
            if (p != std::string::npos) return p + 1; // point to key itself
            p = sql.find(" " + key + "(");
            if (p != std::string::npos) return p + 1;
            return std::string::npos;
        };
        size_t anyPos = findAnyAll("any");
        size_t allPos = findAnyAll("all");
        size_t pos = std::string::npos;
        bool isAny = false;
        if (anyPos != std::string::npos && allPos != std::string::npos) {
            pos = std::min(anyPos, allPos);
            isAny = (pos == anyPos);
        } else if (anyPos != std::string::npos) {
            pos = anyPos; isAny = true;
        } else if (allPos != std::string::npos) {
            pos = allPos; isAny = false;
        } else {
            break;
        }

        // Find operator before colName (e.g. "> any", "= any")
        size_t opStart = pos;
        while (opStart > 0 && std::isspace(static_cast<unsigned char>(sql[opStart - 1]))) --opStart;
        size_t opEnd = opStart;
        while (opEnd > 0 && (sql[opEnd - 1] == '<' || sql[opEnd - 1] == '>' || sql[opEnd - 1] == '=' || sql[opEnd - 1] == '!')) --opEnd;
        if (opEnd == opStart) break; // no operator found
        std::string op = sql.substr(opEnd, opStart - opEnd);

        size_t colNameEnd = opEnd;
        while (colNameEnd > 0 && std::isspace(static_cast<unsigned char>(sql[colNameEnd - 1]))) --colNameEnd;
        size_t colNameStart = colNameEnd;
        while (colNameStart > 0 && !std::isspace(static_cast<unsigned char>(sql[colNameStart - 1]))) --colNameStart;
        std::string colName = trim(sql.substr(colNameStart, colNameEnd - colNameStart));

        size_t parenStart = sql.find('(', pos);
        if (parenStart == std::string::npos) break;
        size_t parenEnd = findMatchingParen(sql, parenStart);
        if (parenEnd == std::string::npos) break;

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        std::vector<std::string> values;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            values = runSubQuery(inner, s);
        }

        std::string replacement;
        if (values.empty()) {
            // SQL standard: ANY(empty) = false, ALL(empty) = true
            replacement = isAny ? "1=0" : "1=1";
        } else {
            for (size_t i = 0; i < values.size(); ++i) {
                if (i > 0) replacement += isAny ? " or " : " and ";
                replacement += colName + op + values[i];
            }
        }
        sql = sql.substr(0, colNameStart) + replacement + sql.substr(parenEnd + 1);
    }

    // ---------- IN ----------
    while (true) {
        size_t pos = sql.find(" in ");
        if (pos == std::string::npos) break;

        size_t parenStart = sql.find('(', pos);
        if (parenStart == std::string::npos) break;

        bool onlySpace = true;
        for (size_t i = pos + 4; i < parenStart; ++i) {
            if (!std::isspace(static_cast<unsigned char>(sql[i]))) { onlySpace = false; break; }
        }
        if (!onlySpace) {
            sql.erase(pos, 4);
            sql.insert(pos, " in ");
            pos += 4;
            continue;
        }

        size_t parenEnd = findMatchingParen(sql, parenStart);
        if (parenEnd == std::string::npos) break;

        size_t colStart = pos;
        while (colStart > 0 && std::isspace(static_cast<unsigned char>(sql[colStart - 1]))) --colStart;
        size_t colNameStart = colStart;
        while (colNameStart > 0 && !std::isspace(static_cast<unsigned char>(sql[colNameStart - 1]))) --colNameStart;
        std::string colName = trim(sql.substr(colNameStart, colStart - colNameStart));

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        std::vector<std::string> values;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            values = runSubQuery(inner, s);
        } else {
            size_t vpos = 0;
            while (vpos < inner.size()) {
                size_t comma = inner.find(',', vpos);
                std::string val = trim((comma == std::string::npos) ? inner.substr(vpos) : inner.substr(vpos, comma - vpos));
                values.push_back(val);
                if (comma == std::string::npos) break;
                vpos = comma + 1;
            }
        }

        std::string replacement;
        if (values.empty()) {
            replacement = colName + "=__empty__ __empty__";
        } else {
            for (size_t i = 0; i < values.size(); ++i) {
                if (i > 0) replacement += " or ";
                replacement += colName + "=" + values[i];
            }
        }
        sql = sql.substr(0, colNameStart) + replacement + sql.substr(parenEnd + 1);
    }
    return sql;
}

// ========================================================================
// SQL execution
// ========================================================================
bool checkAdmin(const Session& s) {
    if (s.permission == 0 && !userIsAdminViaRole(s.username)) {
        cout << "permission denied" << endl;
        log(s.username, "permission denied", getTime());
        return false;
    }
    return true;
}

static bool authenticatedUserIsAdmin(const Session& s) {
    const string& authUser = s.authenticatedUser.empty() ? s.username : s.authenticatedUser;
    int authPerm = s.authenticatedUser.empty() ? s.permission : s.authenticatedPermission;
    return authPerm == 1 || userIsAdminViaRole(authUser);
}

bool checkDB(const Session& s);

static bool setSessionAuthorization(Session& s, const string& targetRaw) {
    string target = stripQuotes(trim(targetRaw));
    if (target.empty()) {
        cout << "SQL syntax error: SET SESSION AUTHORIZATION user_name | DEFAULT" << endl;
        return true;
    }
    string authUser = s.authenticatedUser.empty() ? s.username : s.authenticatedUser;
    int authPerm = s.authenticatedUser.empty() ? s.permission : s.authenticatedPermission;
    if (target == "default") target = authUser;
    if (!authenticatedUserIsAdmin(s) && target != authUser) {
        cout << "permission denied" << endl;
        return true;
    }
    int perm = permissionQuery(target);
    if (perm < 0) {
        cout << "User " << target << " not exist" << endl;
        return true;
    }
    s.username = target;
    s.permission = perm;
    s.originalRole = target;
    s.currentRole.clear();
    if (s.authenticatedUser.empty()) {
        s.authenticatedUser = authUser;
        s.authenticatedPermission = authPerm;
    }
    cout << "SET SESSION AUTHORIZATION " << target << endl;
    return false;
}

static std::filesystem::path roleAttributesPath() {
    return std::filesystem::path(".pg_role_attrs");
}

static map<string, string> loadRoleAttributes() {
    map<string, string> attrs;
    ifstream in(roleAttributesPath());
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 2) continue;
        attrs[catalogUnescape(parts[0])] = catalogUnescape(parts[1]);
    }
    return attrs;
}

static bool saveRoleAttributes(const map<string, string>& attrs) {
    ofstream out(roleAttributesPath(), ios::trunc);
    if (!out) return false;
    for (const auto& kv : attrs) {
        out << catalogEscape(kv.first) << "|" << catalogEscape(kv.second) << "\n";
    }
    return true;
}

static bool renameExplicitRole(const string& oldName, const string& newName) {
    ifstream in("role.dat");
    if (!in) return false;
    vector<pair<string, string>> rows;
    string roleName, member;
    bool found = false;
    bool conflict = false;
    while (in >> roleName >> member) {
        if (roleName == newName) conflict = true;
        if (roleName == oldName) {
            roleName = newName;
            found = true;
        }
        rows.emplace_back(roleName, member);
    }
    if (!found || conflict) return false;
    ofstream out("role.dat", ios::trunc);
    for (const auto& row : rows) out << row.first << " " << row.second << "\n";
    auto attrs = loadRoleAttributes();
    auto it = attrs.find(oldName);
    if (it != attrs.end()) {
        attrs[newName] = it->second;
        attrs.erase(it);
        saveRoleAttributes(attrs);
    }
    return true;
}

static bool handleCreateGroup(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(12)); // after "create group"
    if (rest.empty()) {
        cout << "SQL syntax error: CREATE GROUP group_name [WITH USER user,...]" << endl;
        return true;
    }
    vector<string> tokens = tokenize(rest);
    if (tokens.empty()) {
        cout << "SQL syntax error: CREATE GROUP group_name [WITH USER user,...]" << endl;
        return true;
    }
    string groupName = tokens[0];
    int res = createRole(groupName);
    if (res == -1) {
        cout << "error: group already exists" << endl;
        return true;
    }
    size_t userPos = findTopLevelKeyword(rest, "user");
    if (userPos != string::npos) {
        string userList = trim(rest.substr(userPos + 4));
        for (auto userName : splitTopLevelComma(userList)) {
            userName = stripQuotes(trim(userName));
            if (!userName.empty()) grantRoleToUser(groupName, userName);
        }
    }
    cout << "CREATE GROUP succeeded" << endl;
    return false;
}

static bool handleAlterGroup(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(11)); // after "alter group"
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 3) {
        cout << "SQL syntax error: ALTER GROUP group_name ADD|DROP USER user [, ...] | RENAME TO newname" << endl;
        return true;
    }
    string groupName = tokens[0];
    if (!roleExists(groupName)) {
        cout << "Group " << groupName << " not exist" << endl;
        return true;
    }
    if (tokens[1] == "rename" && tokens.size() >= 4 && tokens[2] == "to") {
        string newName = tokens[3];
        if (roleExists(newName) || permissionQuery(newName) != -1) {
            cout << "Group " << newName << " already exists" << endl;
            return true;
        }
        if (!renameExplicitRole(groupName, newName)) {
            cout << "Rename group failed" << endl;
            return true;
        }
        cout << "Group " << groupName << " renamed to " << newName << endl;
        return false;
    }
    bool addUser = tokens[1] == "add" && tokens.size() >= 4 && tokens[2] == "user";
    bool dropUser = tokens[1] == "drop" && tokens.size() >= 4 && tokens[2] == "user";
    if (!addUser && !dropUser) {
        cout << "SQL syntax error: ALTER GROUP group_name ADD|DROP USER user [, ...] | RENAME TO newname" << endl;
        return true;
    }
    size_t userPos = findTopLevelKeyword(rest, "user");
    string userList = userPos == string::npos ? "" : trim(rest.substr(userPos + 4));
    int changed = 0;
    for (auto userName : splitTopLevelComma(userList)) {
        userName = stripQuotes(trim(userName));
        if (userName.empty()) continue;
        if (addUser) {
            int grantRes = grantRoleToUser(groupName, userName);
            if (grantRes == 0) ++changed;
        } else {
            if (revokeRoleFromUser(groupName, userName)) ++changed;
        }
    }
    cout << "Group " << groupName << (addUser ? " users added: " : " users dropped: ") << changed << endl;
    return false;
}

static bool handleAlterRole(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(10)); // after "alter role"
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 2) {
        cout << "SQL syntax error: ALTER ROLE role_name [WITH] option [...] | RENAME TO newname" << endl;
        return true;
    }
    string roleName = tokens[0];
    if (!roleExists(roleName) && permissionQuery(roleName) == -1) {
        cout << "Role " << roleName << " not exist" << endl;
        return true;
    }
    if (tokens.size() >= 4 && tokens[1] == "rename" && tokens[2] == "to") {
        string newName = tokens[3];
        bool isUser = (permissionQuery(roleName) != -1);
        if (roleExists(newName) || permissionQuery(newName) != -1) {
            cout << "Role " << newName << " already exists" << endl;
            return true;
        }
        bool ok = isUser ? renameUser(roleName, newName) : renameExplicitRole(roleName, newName);
        if (!ok) {
            cout << "Rename role failed" << endl;
            return true;
        }
        cout << "Role " << roleName << " renamed to " << newName << endl;
        return false;
    }
    auto attrs = loadRoleAttributes();
    attrs[roleName] = trim(rest.substr(roleName.size()));
    if (!saveRoleAttributes(attrs)) {
        cout << "Alter role failed" << endl;
        return true;
    }
    cout << "Role " << roleName << " altered" << endl;
    return false;
}

static bool handleDropGroup(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(10)); // after "drop group"
    bool ifExists = false;
    if (startsWithKeyword(rest, "if exists")) {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    string groupName = firstCompatNameToken(stripTrailingDropBehavior(rest));
    if (groupName.empty()) {
        cout << "SQL syntax error: DROP GROUP group_name" << endl;
        return true;
    }
    if (!dropRole(groupName)) {
        if (ifExists) {
            cout << "Group " << groupName << " does not exist, skipping" << endl;
            return false;
        }
        cout << "Group " << groupName << " not exist" << endl;
        return true;
    }
    auto attrs = loadRoleAttributes();
    attrs.erase(groupName);
    saveRoleAttributes(attrs);
    cout << "Group dropped" << endl;
    return false;
}

static bool handleDropRoleGlobal(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(9)); // after "drop role"
    bool ifExists = false;
    if (startsWithKeyword(rest, "if exists")) {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    string roleName = firstCompatNameToken(stripTrailingDropBehavior(rest));
    if (roleName.empty()) {
        cout << "SQL syntax error: DROP ROLE role_name" << endl;
        return true;
    }
    if (!dropRole(roleName)) {
        if (ifExists) {
            cout << "Role " << roleName << " does not exist, skipping" << endl;
            return false;
        }
        cout << "Role " << roleName << " not exist" << endl;
        return true;
    }
    auto attrs = loadRoleAttributes();
    attrs.erase(roleName);
    saveRoleAttributes(attrs);
    cout << "Role dropped" << endl;
    return false;
}

struct DatabaseOptionInfo {
    string name;
    string owner;
    string options;
    string definition;
};

static std::filesystem::path databaseOptionsPath() {
    return ".pg_database_options";
}

static map<string, DatabaseOptionInfo> loadDatabaseOptions() {
    map<string, DatabaseOptionInfo> result;
    ifstream in(databaseOptionsPath());
    string line;
    while (getline(in, line)) {
        if (trim(line).empty()) continue;
        auto parts = splitByDelimiter(line, '|');
        if (parts.size() < 4) continue;
        DatabaseOptionInfo info;
        info.name = catalogUnescape(parts[0]);
        info.owner = catalogUnescape(parts[1]);
        info.options = catalogUnescape(parts[2]);
        info.definition = catalogUnescape(parts[3]);
        if (!info.name.empty()) result[info.name] = info;
    }
    return result;
}

static bool saveDatabaseOptions(const map<string, DatabaseOptionInfo>& options) {
    ofstream out(databaseOptionsPath(), ios::trunc);
    if (!out) return false;
    for (const auto& kv : options) {
        const auto& info = kv.second;
        out << catalogEscape(info.name) << "|"
            << catalogEscape(info.owner) << "|"
            << catalogEscape(info.options) << "|"
            << catalogEscape(info.definition) << "\n";
    }
    return true;
}

static bool handleAlterDatabase(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(14)); // after "alter database"
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 2) {
        cout << "SQL syntax error: ALTER DATABASE name RENAME TO newname | OWNER TO user | SET/RESET option" << endl;
        return true;
    }
    string dbname = tokens[0];
    if (!g_engine.databaseExists(dbname)) {
        cout << "Database " << dbname << " not exist" << endl;
        return true;
    }

    auto dbOptions = loadDatabaseOptions();
    auto ensureInfo = [&]() -> DatabaseOptionInfo& {
        auto& info = dbOptions[dbname];
        if (info.name.empty()) info.name = dbname;
        if (info.owner.empty()) info.owner = s.username;
        return info;
    };

    if (tokens.size() >= 4 && tokens[1] == "rename" && tokens[2] == "to") {
        string newName = tokens[3];
        if (g_engine.databaseExists(newName)) {
            cout << "Database " << newName << " already exists" << endl;
            return true;
        }
        try {
            auto oldArchive = std::filesystem::path(g_engine.dbPath(dbname).string() + ".archive");
            auto newArchive = std::filesystem::path(g_engine.dbPath(newName).string() + ".archive");
            std::filesystem::rename(g_engine.dbPath(dbname), g_engine.dbPath(newName));
            if (std::filesystem::exists(oldArchive)) std::filesystem::rename(oldArchive, newArchive);
        } catch (...) {
            cout << "Rename database failed" << endl;
            return true;
        }
        auto it = dbOptions.find(dbname);
        if (it != dbOptions.end()) {
            auto info = it->second;
            dbOptions.erase(it);
            info.name = newName;
            info.definition = sql;
            dbOptions[newName] = info;
            saveDatabaseOptions(dbOptions);
        }
        if (s.currentDB == dbname) s.currentDB = newName;
        cout << "Database " << dbname << " renamed to " << newName << endl;
        return false;
    }

    auto& info = ensureInfo();
    info.definition = sql;
    if (tokens.size() >= 4 && tokens[1] == "owner" && tokens[2] == "to") {
        info.owner = tokens[3];
        if (!saveDatabaseOptions(dbOptions)) {
            cout << "Alter database failed" << endl;
            return true;
        }
        cout << "Database " << dbname << " owner changed" << endl;
        return false;
    }
    if (tokens[1] == "set" || tokens[1] == "reset") {
        size_t actionPos = findTopLevelKeyword(rest, tokens[1]);
        info.options = (actionPos == string::npos) ? tokens[1] : trim(rest.substr(actionPos));
        if (!saveDatabaseOptions(dbOptions)) {
            cout << "Alter database failed" << endl;
            return true;
        }
        cout << "Database " << dbname << " options updated" << endl;
        return false;
    }
    cout << "SQL syntax error: ALTER DATABASE name RENAME TO newname | OWNER TO user | SET/RESET option" << endl;
    return true;
}

static string extractCheckExpression(const string& text, size_t checkPos) {
    size_t lp = text.find('(', checkPos);
    if (lp == string::npos) return "";
    size_t rp = findMatchingParen(text, lp);
    if (rp == string::npos) return "";
    return trim(text.substr(lp + 1, rp - lp - 1));
}

static bool domainConstraintMatches(const dbms::StorageEngine::DomainInfo& info,
                                    const string& constraintName) {
    if (constraintName.empty()) return false;
    if (!info.constraintName.empty()) return info.constraintName == constraintName;
    return constraintName == info.name + "_check";
}

static bool handleAlterDomain(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(12)); // after "alter domain"
    string domainName = firstCompatNameToken(rest);
    if (domainName.empty()) {
        cout << "SQL syntax error: ALTER DOMAIN requires a domain name" << endl;
        return true;
    }
    string action = trim(rest.substr(domainName.size()));
    auto info = g_engine.getDomain(s.currentDB, domainName);
    if (info.name.empty()) {
        cout << "Domain " << domainName << " not exist" << endl;
        return true;
    }

    string message;
    if (startsWithKeyword(action, "rename to")) {
        string newName = firstCompatNameToken(trim(action.substr(9)));
        if (newName.empty()) {
            cout << "SQL syntax error: ALTER DOMAIN name RENAME TO newname" << endl;
            return true;
        }
        info.name = newName;
        message = "Domain " + domainName + " renamed to " + newName;
    } else if (startsWithKeyword(action, "set default")) {
        string defaultValue = trim(action.substr(11));
        if (defaultValue.empty()) {
            cout << "SQL syntax error: ALTER DOMAIN name SET DEFAULT value" << endl;
            return true;
        }
        info.defaultValue = stripQuotes(defaultValue);
        message = "Domain " + domainName + " default set";
    } else if (startsWithKeyword(action, "drop default")) {
        info.defaultValue.clear();
        message = "Domain " + domainName + " default dropped";
    } else if (startsWithKeyword(action, "add constraint") || startsWithKeyword(action, "add check")) {
        size_t checkPos = findTopLevelSqlKeyword(action, "check");
        string checkExpr = (checkPos == string::npos) ? "" : extractCheckExpression(action, checkPos);
        if (checkExpr.empty()) {
            cout << "SQL syntax error: ALTER DOMAIN name ADD [CONSTRAINT cname] CHECK (...)" << endl;
            return true;
        }
        string constraintName;
        if (startsWithKeyword(action, "add constraint")) {
            string beforeCheck = (checkPos == string::npos) ? "" : trim(action.substr(14, checkPos - 14));
            constraintName = firstCompatNameToken(beforeCheck);
        }
        if (constraintName.empty()) constraintName = domainName + "_check";
        info.checkExpr = checkExpr;
        info.constraintName = constraintName;
        message = "Domain " + domainName + " constraint " + constraintName + " added";
    } else if (startsWithKeyword(action, "drop constraint")) {
        string clause = trim(action.substr(15));
        bool ifExists = false;
        if (startsWithKeyword(clause, "if exists")) {
            ifExists = true;
            clause = trim(clause.substr(9));
        }
        clause = stripTrailingDropBehavior(clause);
        string constraintName = firstCompatNameToken(clause);
        if (constraintName.empty()) {
            cout << "SQL syntax error: ALTER DOMAIN name DROP CONSTRAINT constraint_name" << endl;
            return true;
        }
        if (info.checkExpr.empty() || !domainConstraintMatches(info, constraintName)) {
            if (ifExists) {
                cout << "Domain constraint " << constraintName << " not found, skipping" << endl;
                return false;
            }
            cout << "Domain constraint " << constraintName << " not found" << endl;
            return true;
        }
        info.checkExpr.clear();
        info.constraintName.clear();
        message = "Domain " + domainName + " constraint " + constraintName + " dropped";
    } else if (startsWithKeyword(action, "validate constraint")) {
        string constraintName = firstCompatNameToken(trim(action.substr(19)));
        if (constraintName.empty()) {
            cout << "SQL syntax error: ALTER DOMAIN name VALIDATE CONSTRAINT constraint_name" << endl;
            return true;
        }
        if (info.checkExpr.empty() || !domainConstraintMatches(info, constraintName)) {
            cout << "Domain constraint " << constraintName << " not found" << endl;
            return true;
        }
        cout << "Domain constraint " << constraintName << " validated" << endl;
        return false;
    } else {
        return handleAlterCompatObject(sql, s);
    }

    auto res = g_engine.alterDomain(s.currentDB, domainName, info);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        cout << "Domain " << info.name << " already exists" << endl;
        return true;
    }
    if (res == DBStatus::TABLE_NOT_FOUND) {
        cout << "Domain " << domainName << " not exist" << endl;
        return true;
    }
    if (res != DBStatus::OK) {
        cout << "Alter domain failed" << endl;
        return true;
    }
    cout << message << endl;
    return false;
}

static bool handleAlterEnumValue(dbms::StorageEngine::EnumType et, const string& action,
                                 const string& typeName, Session& s) {
    auto unquote = [](string v) -> string {
        v = trim(v);
        if (v.size() >= 2 && v.front() == '\'' && v.back() == '\'') return v.substr(1, v.size() - 2);
        return v;
    };
    // Extract a leading label which may be quoted ('x') or bare; advances `clause`.
    auto takeLabel = [&](string& clause) -> string {
        clause = trim(clause);
        if (!clause.empty() && clause.front() == '\'') {
            size_t end = clause.find('\'', 1);
            if (end == string::npos) { string all = clause.substr(1); clause.clear(); return all; }
            string lbl = clause.substr(1, end - 1);
            clause = trim(clause.substr(end + 1));
            return lbl;
        }
        size_t sp = clause.find(' ');
        string lbl = (sp == string::npos) ? clause : clause.substr(0, sp);
        clause = (sp == string::npos) ? "" : trim(clause.substr(sp));
        return lbl;
    };

    if (startsWithKeyword(action, "add value")) {
        string clause = trim(action.substr(9));
        bool ifNotExists = false;
        if (startsWithKeyword(clause, "if not exists")) {
            ifNotExists = true;
            clause = trim(clause.substr(13));
        }
        string newLabel = takeLabel(clause);
        if (newLabel.empty()) {
            cout << "SQL syntax error: ALTER TYPE name ADD VALUE 'label'" << endl;
            return true;
        }
        int mode = 0; // 0 append, 1 before, 2 after
        string anchor;
        if (startsWithKeyword(clause, "before")) { mode = 1; anchor = unquote(trim(clause.substr(6))); }
        else if (startsWithKeyword(clause, "after")) { mode = 2; anchor = unquote(trim(clause.substr(5))); }

        for (const auto& l : et.labels) {
            if (l == newLabel) {
                if (ifNotExists) {
                    cout << "Enum value " << newLabel << " already exists, skipping" << endl;
                    return false;
                }
                cout << "Enum value " << newLabel << " already exists" << endl;
                return true;
            }
        }
        if (mode == 0 || anchor.empty()) {
            et.labels.push_back(newLabel);
        } else {
            auto it = std::find(et.labels.begin(), et.labels.end(), anchor);
            if (it == et.labels.end()) {
                cout << "Enum anchor value " << anchor << " not found" << endl;
                return true;
            }
            if (mode == 2) ++it;
            et.labels.insert(it, newLabel);
        }
        if (g_engine.updateEnumType(s.currentDB, et) != DBStatus::OK) {
            cout << "Alter type add value failed" << endl;
            return true;
        }
        cout << "Type " << typeName << " value " << newLabel << " added" << endl;
        return false;
    }

    if (startsWithKeyword(action, "rename value")) {
        string clause = trim(action.substr(12));
        string oldLabel = takeLabel(clause);
        if (startsWithKeyword(clause, "to")) clause = trim(clause.substr(2));
        string newLabel = takeLabel(clause);
        if (oldLabel.empty() || newLabel.empty()) {
            cout << "SQL syntax error: ALTER TYPE name RENAME VALUE 'old' TO 'new'" << endl;
            return true;
        }
        auto it = std::find(et.labels.begin(), et.labels.end(), oldLabel);
        if (it == et.labels.end()) {
            cout << "Enum value " << oldLabel << " not found" << endl;
            return true;
        }
        if (std::find(et.labels.begin(), et.labels.end(), newLabel) != et.labels.end()) {
            cout << "Enum value " << newLabel << " already exists" << endl;
            return true;
        }
        *it = newLabel;
        if (g_engine.updateEnumType(s.currentDB, et) != DBStatus::OK) {
            cout << "Alter type rename value failed" << endl;
            return true;
        }
        cout << "Type " << typeName << " value " << oldLabel << " renamed to " << newLabel << endl;
        return false;
    }

    cout << "SQL syntax error: unsupported ALTER TYPE action on enum " << typeName << endl;
    return true;
}

static bool handleAlterType(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(10)); // after "alter type"
    string typeName = firstCompatNameToken(rest);
    if (typeName.empty()) {
        cout << "SQL syntax error: ALTER TYPE requires a type name" << endl;
        return true;
    }
    string action = trim(rest.substr(typeName.size()));
    auto ct = g_engine.getCompositeType(s.currentDB, typeName);
    if (ct.name.empty()) {
        // Not a composite type — it may be an enum (ALTER TYPE ... ADD/RENAME VALUE).
        auto et = g_engine.getEnumType(s.currentDB, typeName);
        if (!et.name.empty()) {
            return handleAlterEnumValue(et, action, typeName, s);
        }
        return handleAlterCompatObject(sql, s);
    }

    string message;
    if (startsWithKeyword(action, "rename to")) {
        string newName = firstCompatNameToken(trim(action.substr(9)));
        if (newName.empty()) {
            cout << "SQL syntax error: ALTER TYPE name RENAME TO newname" << endl;
            return true;
        }
        ct.name = newName;
        message = "Type " + typeName + " renamed to " + newName;
    } else if (startsWithKeyword(action, "add attribute")) {
        string fieldDef = trim(action.substr(13));
        vector<string> parts = tokenize(fieldDef);
        if (parts.size() < 2) {
            cout << "SQL syntax error: ALTER TYPE name ADD ATTRIBUTE attr type" << endl;
            return true;
        }
        string attrName = parts[0];
        for (const auto& f : ct.fields) {
            if (f.first == attrName) {
                cout << "Attribute " << attrName << " already exists" << endl;
                return true;
            }
        }
        size_t attrPos = fieldDef.find(attrName);
        string attrType = trim(fieldDef.substr(attrPos + attrName.size()));
        attrType = stripTrailingDropBehavior(attrType);
        ct.fields.emplace_back(attrName, attrType);
        message = "Type " + typeName + " attribute " + attrName + " added";
    } else if (startsWithKeyword(action, "drop attribute")) {
        string clause = trim(action.substr(14));
        bool ifExists = false;
        if (startsWithKeyword(clause, "if exists")) {
            ifExists = true;
            clause = trim(clause.substr(9));
        }
        clause = stripTrailingDropBehavior(clause);
        string attrName = firstCompatNameToken(clause);
        if (attrName.empty()) {
            cout << "SQL syntax error: ALTER TYPE name DROP ATTRIBUTE attr" << endl;
            return true;
        }
        auto it = std::find_if(ct.fields.begin(), ct.fields.end(),
                               [&](const auto& f) { return f.first == attrName; });
        if (it == ct.fields.end()) {
            if (ifExists) {
                cout << "Attribute " << attrName << " not found, skipping" << endl;
                return false;
            }
            cout << "Attribute " << attrName << " not found" << endl;
            return true;
        }
        ct.fields.erase(it);
        if (ct.fields.empty()) {
            cout << "Composite type must keep at least one attribute" << endl;
            return true;
        }
        message = "Type " + typeName + " attribute " + attrName + " dropped";
    } else if (startsWithKeyword(action, "rename attribute")) {
        string clause = trim(action.substr(16));
        size_t toPos = findTopLevelSqlKeyword(clause, "to");
        if (toPos == string::npos) {
            cout << "SQL syntax error: ALTER TYPE name RENAME ATTRIBUTE attr TO newattr" << endl;
            return true;
        }
        string attrName = firstCompatNameToken(trim(clause.substr(0, toPos)));
        string newAttrName = firstCompatNameToken(trim(clause.substr(toPos + 2)));
        if (attrName.empty() || newAttrName.empty()) {
            cout << "SQL syntax error: ALTER TYPE name RENAME ATTRIBUTE attr TO newattr" << endl;
            return true;
        }
        for (const auto& f : ct.fields) {
            if (f.first == newAttrName) {
                cout << "Attribute " << newAttrName << " already exists" << endl;
                return true;
            }
        }
        auto it = std::find_if(ct.fields.begin(), ct.fields.end(),
                               [&](const auto& f) { return f.first == attrName; });
        if (it == ct.fields.end()) {
            cout << "Attribute " << attrName << " not found" << endl;
            return true;
        }
        it->first = newAttrName;
        message = "Type " + typeName + " attribute " + attrName + " renamed to " + newAttrName;
    } else if (startsWithKeyword(action, "alter attribute")) {
        string clause = trim(action.substr(15));
        size_t typePos = findTopLevelSqlKeyword(clause, "type");
        if (typePos == string::npos) {
            return handleAlterCompatObject(sql, s);
        }
        string attrName = firstCompatNameToken(trim(clause.substr(0, typePos)));
        string newType = trim(clause.substr(typePos + 4));
        if (attrName.empty() || newType.empty()) {
            cout << "SQL syntax error: ALTER TYPE name ALTER ATTRIBUTE attr TYPE type" << endl;
            return true;
        }
        auto it = std::find_if(ct.fields.begin(), ct.fields.end(),
                               [&](const auto& f) { return f.first == attrName; });
        if (it == ct.fields.end()) {
            cout << "Attribute " << attrName << " not found" << endl;
            return true;
        }
        it->second = newType;
        message = "Type " + typeName + " attribute " + attrName + " type changed";
    } else {
        return handleAlterCompatObject(sql, s);
    }

    auto res = g_engine.alterCompositeType(s.currentDB, typeName, ct);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        cout << "Type " << ct.name << " already exists" << endl;
        return true;
    }
    if (res == DBStatus::TABLE_NOT_FOUND) {
        cout << "Type " << typeName << " not exist" << endl;
        return true;
    }
    if (res != DBStatus::OK) {
        cout << "Alter type failed" << endl;
        return true;
    }
    cout << message << endl;
    return false;
}

static vector<string> parsePolicyRoles(const string& roleStr) {
    vector<string> roles;
    for (auto role : splitTopLevelComma(roleStr)) {
        role = trim(role);
        if (!role.empty()) roles.push_back(role);
    }
    return roles;
}

static bool handleAlterPolicy(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(12)); // after "alter policy"
    size_t onPos = findTopLevelKeyword(rest, "on");
    if (onPos == string::npos) {
        cout << "SQL syntax error: ALTER POLICY name ON table ..." << endl;
        return true;
    }
    string policyName = trim(rest.substr(0, onPos));
    string afterOn = trim(rest.substr(onPos + 2));
    vector<string> afterTokens = tokenize(afterOn);
    if (policyName.empty() || afterTokens.empty()) {
        cout << "SQL syntax error: ALTER POLICY name ON table ..." << endl;
        return true;
    }
    size_t renamePos = findTopLevelKeyword(afterOn, "rename to");
    size_t toPos = findTopLevelKeyword(afterOn, "to");
    size_t usingPos = findTopLevelKeyword(afterOn, "using");
    size_t withCheckPos = findTopLevelKeyword(afterOn, "with check");
    size_t tableEnd = afterOn.size();
    for (size_t pos : {renamePos, toPos, usingPos, withCheckPos}) {
        if (pos != string::npos) tableEnd = min(tableEnd, pos);
    }
    string tableName = resolveTableName(s, trim(afterOn.substr(0, tableEnd)));
    string options = trim(afterOn.substr(tableEnd));
    if (!g_engine.tableExists(s.currentDB, tableName)) {
        cout << "Table not found" << endl;
        return true;
    }
    auto policies = g_engine.getPolicies(s.currentDB, tableName);
    auto it = std::find_if(policies.begin(), policies.end(),
                           [&](const auto& p) { return p.name == policyName; });
    if (it == policies.end()) {
        cout << "Policy " << policyName << " not found" << endl;
        return true;
    }
    dbms::StorageEngine::RowPolicy updated = *it;
    if (startsWithKeyword(options, "rename to")) {
        string newName = firstCompatNameToken(trim(options.substr(9)));
        if (newName.empty()) {
            cout << "SQL syntax error: ALTER POLICY name ON table RENAME TO newname" << endl;
            return true;
        }
        updated.name = newName;
    } else {
        toPos = findTopLevelKeyword(options, "to");
        usingPos = findTopLevelKeyword(options, "using");
        withCheckPos = findTopLevelKeyword(options, "with check");
        if (toPos == string::npos && usingPos == string::npos && withCheckPos == string::npos) {
            cout << "SQL syntax error: ALTER POLICY name ON table [TO roles] [USING (...)] [WITH CHECK (...)]" << endl;
            return true;
        }
        if (toPos != string::npos) {
            size_t roleEnd = options.size();
            if (usingPos != string::npos && usingPos > toPos) roleEnd = min(roleEnd, usingPos);
            if (withCheckPos != string::npos && withCheckPos > toPos) roleEnd = min(roleEnd, withCheckPos);
            updated.roles = parsePolicyRoles(options.substr(toPos + 2, roleEnd - toPos - 2));
        }
        if (usingPos != string::npos) {
            size_t exprEnd = (withCheckPos != string::npos && withCheckPos > usingPos) ? withCheckPos : options.size();
            updated.usingExpr = stripQuotes(trim(options.substr(usingPos + 5, exprEnd - usingPos - 5)));
        }
        if (withCheckPos != string::npos) {
            updated.withCheckExpr = stripQuotes(trim(options.substr(withCheckPos + 10)));
        }
    }
    auto res = g_engine.alterPolicy(s.currentDB, tableName, policyName, updated);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        cout << "Policy " << updated.name << " already exists" << endl;
        return true;
    }
    if (res != DBStatus::OK) {
        cout << "Alter policy failed" << endl;
        return true;
    }
    if (updated.name != policyName) {
        cout << "Policy " << policyName << " renamed to " << updated.name << " on " << tableName << endl;
    } else {
        cout << "Policy " << policyName << " altered on " << tableName << endl;
    }
    return false;
}

static bool handleDropPolicy(const string& sql, Session& s) {
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(12));
    size_t onPos = rest.find(" on ");
    if (onPos == string::npos) {
        cout << "SQL syntax error: DROP POLICY name ON table" << endl;
        return true;
    }
    string pname = trim(rest.substr(0, onPos));
    string tname = resolveTableName(s, trim(rest.substr(onPos + 4)));
    auto res = g_engine.dropPolicy(s.currentDB, tname, pname);
    if (res == DBStatus::OK) {
        cout << "Policy " << pname << " dropped from " << tname << endl;
    } else if (res == DBStatus::TABLE_NOT_FOUND) {
        cout << "Table or policy not found" << endl;
    } else {
        cout << "Drop policy failed" << endl;
    }
    return false;
}

static bool handleDropDatabaseGlobal(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(13)); // after "drop database"
    bool ifExists = false;
    if (startsWithKeyword(rest, "if exists")) {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    string dbname = firstCompatNameToken(stripTrailingDropBehavior(rest));
    if (dbname.empty()) {
        cout << "SQL syntax error: DROP DATABASE name" << endl;
        return true;
    }
    auto res = g_engine.dropDatabase(dbname);
    if (res == DBStatus::DATABASE_NOT_FOUND) {
        if (ifExists) {
            cout << "Database " << dbname << " does not exist, skipping" << endl;
            return false;
        }
        cout << "Database " << dbname << " not exist" << endl;
        return true;
    }
    auto dbOptions = loadDatabaseOptions();
    if (dbOptions.erase(dbname) > 0) saveDatabaseOptions(dbOptions);
    if (s.currentDB == dbname) s.currentDB.clear();
    cout << "Database dropped" << endl;
    log(s.username, "database dropped", getTime());
    return false;
}

static bool handleCreateTablespace(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(17));
    bool ifNotExists = false;
    if (rest.substr(0, 13) == "if not exists") {
        ifNotExists = true;
        rest = trim(rest.substr(13));
    }
    size_t locPos = rest.find(" location ");
    if (locPos == string::npos) {
        cout << "SQL syntax error: CREATE TABLESPACE name [OWNER user] LOCATION 'path'" << endl;
        return true;
    }
    string beforeLoc = trim(rest.substr(0, locPos));
    string afterLoc = trim(rest.substr(locPos + 10));
    vector<string> tokens = tokenize(beforeLoc);
    if (tokens.empty()) {
        cout << "SQL syntax error: CREATE TABLESPACE requires a name" << endl;
        return true;
    }
    string name = tokens[0];
    string owner = s.username;
    for (size_t i = 1; i + 1 < tokens.size(); ++i) {
        if (tokens[i] == "owner") owner = tokens[i + 1];
    }

    string location;
    string options;
    if (!afterLoc.empty() && afterLoc.front() == '\'') {
        size_t q2 = afterLoc.find('\'', 1);
        if (q2 == string::npos) {
            cout << "SQL syntax error: unterminated LOCATION string" << endl;
            return true;
        }
        location = afterLoc.substr(1, q2 - 1);
        afterLoc = trim(afterLoc.substr(q2 + 1));
    } else {
        size_t sp = afterLoc.find(' ');
        location = stripQuotes(sp == string::npos ? afterLoc : afterLoc.substr(0, sp));
        afterLoc = (sp == string::npos) ? "" : trim(afterLoc.substr(sp + 1));
    }
    if (afterLoc.substr(0, 4) == "with") options = trim(afterLoc.substr(4));
    if (location.empty()) {
        cout << "SQL syntax error: CREATE TABLESPACE requires LOCATION" << endl;
        return true;
    }

    auto spaces = loadTablespaces();
    if (spaces.count(name)) {
        if (ifNotExists) {
            cout << "Tablespace " << name << " already exists, skipping" << endl;
            return false;
        }
        cout << "Tablespace " << name << " already exists" << endl;
        return true;
    }
    try {
        std::filesystem::create_directories(location);
    } catch (...) {
        cout << "Could not create tablespace location: " << location << endl;
        return true;
    }
    spaces[name] = {name, owner, location, options};
    if (!saveTablespaces(spaces)) {
        cout << "Create tablespace failed" << endl;
        return true;
    }
    cout << "Tablespace " << name << " created" << endl;
    return false;
}

static bool handleAlterTablespace(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(16));
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 3) {
        cout << "SQL syntax error: ALTER TABLESPACE name RENAME TO newname | OWNER TO user | SET (...)" << endl;
        return true;
    }
    string name = tokens[0];
    auto spaces = loadTablespaces();
    auto it = spaces.find(name);
    if (it == spaces.end()) {
        cout << "Tablespace " << name << " not exist" << endl;
        return true;
    }
    if (tokens[1] == "rename" && tokens.size() >= 4 && tokens[2] == "to") {
        string newName = tokens[3];
        if (spaces.count(newName)) {
            cout << "Tablespace " << newName << " already exists" << endl;
            return true;
        }
        auto info = it->second;
        spaces.erase(it);
        info.name = newName;
        spaces[newName] = info;
        saveTablespaces(spaces);
        cout << "Tablespace " << name << " renamed to " << newName << endl;
        return false;
    }
    if (tokens[1] == "owner" && tokens.size() >= 4 && tokens[2] == "to") {
        it->second.owner = tokens[3];
        saveTablespaces(spaces);
        cout << "Tablespace " << name << " owner changed" << endl;
        return false;
    }
    if (tokens[1] == "set" || tokens[1] == "reset") {
        size_t actionPos = rest.find(tokens[1]);
        it->second.options = trim(rest.substr(actionPos + tokens[1].size()));
        saveTablespaces(spaces);
        cout << "Tablespace " << name << " options updated" << endl;
        return false;
    }
    cout << "SQL syntax error: ALTER TABLESPACE name RENAME TO newname | OWNER TO user | SET (...)" << endl;
    return true;
}

static bool handleDropTablespace(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    string rest = trim(sql.substr(15));
    bool ifExists = false;
    if (rest.substr(0, 9) == "if exists") {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    string name = trim(rest);
    if (name.empty()) {
        cout << "SQL syntax error: DROP TABLESPACE name" << endl;
        return true;
    }
    auto spaces = loadTablespaces();
    auto it = spaces.find(name);
    if (it == spaces.end()) {
        if (ifExists) {
            cout << "Tablespace " << name << " does not exist, skipping" << endl;
            return false;
        }
        cout << "Tablespace " << name << " not exist" << endl;
        return true;
    }
    spaces.erase(it);
    saveTablespaces(spaces);
    cout << "Tablespace " << name << " dropped" << endl;
    return false;
}

static bool handleCreateStatistics(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(17));
    bool ifNotExists = false;
    if (rest.substr(0, 13) == "if not exists") {
        ifNotExists = true;
        rest = trim(rest.substr(13));
    }
    size_t onPos = findTopLevelKeyword(rest, "on");
    size_t fromPos = (onPos == string::npos) ? string::npos : findTopLevelKeyword(rest, "from", onPos + 2);
    if (onPos == string::npos || fromPos == string::npos || fromPos <= onPos) {
        cout << "SQL syntax error: CREATE STATISTICS name [(kind,...)] ON col [, ...] FROM table" << endl;
        return true;
    }
    string header = trim(rest.substr(0, onPos));
    string name;
    vector<string> kinds;
    size_t lp = header.find('(');
    if (lp != string::npos) {
        size_t rp = header.rfind(')');
        if (rp == string::npos || rp <= lp) {
            cout << "SQL syntax error: malformed statistics kind list" << endl;
            return true;
        }
        name = trim(header.substr(0, lp));
        for (auto& k : splitTopLevelComma(header.substr(lp + 1, rp - lp - 1))) {
            k = trim(k);
            if (!k.empty()) kinds.push_back(k);
        }
    } else {
        name = trim(header);
    }
    if (kinds.empty()) kinds = {"ndistinct", "dependencies", "mcv"};
    vector<string> columns;
    for (auto& c : splitTopLevelComma(rest.substr(onPos + 2, fromPos - onPos - 2))) {
        c = trim(c);
        if (!c.empty()) columns.push_back(c);
    }
    string tableName = resolveTableName(s, trim(rest.substr(fromPos + 4)));
    if (name.empty() || columns.empty() || tableName.empty()) {
        cout << "SQL syntax error: CREATE STATISTICS name [(kind,...)] ON col [, ...] FROM table" << endl;
        return true;
    }
    if (!g_engine.tableExists(s.currentDB, tableName)) {
        cout << "Table " << tableName << " not exist" << endl;
        return true;
    }
    if (!tableHasColumns(s.currentDB, tableName, columns)) {
        cout << "CREATE STATISTICS references unknown column" << endl;
        return true;
    }
    auto stats = loadExtendedStats(s.currentDB);
    if (stats.count(name)) {
        if (ifNotExists) {
            cout << "Statistics " << name << " already exists, skipping" << endl;
            return false;
        }
        cout << "Statistics " << name << " already exists" << endl;
        return true;
    }
    stats[name] = {name, tableName, columns, kinds, -1};
    if (!saveExtendedStats(s.currentDB, stats)) {
        cout << "Create statistics failed" << endl;
        return true;
    }
    if (columns.size() >= 2) g_engine.analyzeMultiColumn(s.currentDB, tableName, columns);
    // dependencies kind: compute and report functional dependency degrees.
    bool wantsDeps = false;
    for (const auto& k : kinds) {
        if (toLower(k) == "dependencies") { wantsDeps = true; break; }
    }
    if (wantsDeps && columns.size() >= 2) {
        auto deps = g_engine.computeFunctionalDependencies(s.currentDB, tableName, columns);
        for (const auto& d : deps) {
            cout << "  dependency " << d.first << " degree "
                 << std::fixed << std::setprecision(6) << d.second << endl;
        }
    }
    cout << "Statistics " << name << " created" << endl;
    return false;
}

static bool handleAlterStatistics(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(16));
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 3) {
        cout << "SQL syntax error: ALTER STATISTICS name SET STATISTICS n | RENAME TO newname | OWNER TO user" << endl;
        return true;
    }
    string name = tokens[0];
    auto stats = loadExtendedStats(s.currentDB);
    auto it = stats.find(name);
    if (it == stats.end()) {
        cout << "Statistics " << name << " not exist" << endl;
        return true;
    }
    if (tokens[1] == "rename" && tokens.size() >= 4 && tokens[2] == "to") {
        string newName = tokens[3];
        if (stats.count(newName)) {
            cout << "Statistics " << newName << " already exists" << endl;
            return true;
        }
        auto info = it->second;
        stats.erase(it);
        info.name = newName;
        stats[newName] = info;
        saveExtendedStats(s.currentDB, stats);
        cout << "Statistics " << name << " renamed to " << newName << endl;
        return false;
    }
    if (tokens[1] == "set" && tokens.size() >= 4 && tokens[2] == "statistics") {
        try { it->second.target = stoi(tokens[3]); } catch (...) {
            cout << "Invalid statistics target" << endl;
            return true;
        }
        saveExtendedStats(s.currentDB, stats);
        cout << "Statistics " << name << " target updated" << endl;
        return false;
    }
    if (tokens[1] == "owner" && tokens.size() >= 4 && tokens[2] == "to") {
        cout << "Statistics " << name << " owner changed" << endl;
        return false;
    }
    cout << "SQL syntax error: ALTER STATISTICS name SET STATISTICS n | RENAME TO newname | OWNER TO user" << endl;
    return true;
}

static bool handleDropStatistics(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(15));
    bool ifExists = false;
    if (rest.substr(0, 9) == "if exists") {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    auto stats = loadExtendedStats(s.currentDB);
    bool allOk = true;
    for (auto name : splitTopLevelComma(rest)) {
        name = trim(name);
        if (name.empty()) continue;
        auto it = stats.find(name);
        if (it == stats.end()) {
            if (!ifExists) {
                cout << "Statistics " << name << " not exist" << endl;
                allOk = false;
            }
            continue;
        }
        removeMultiColumnStatsEntry(s.currentDB, it->second.tableName, it->second.columns);
        stats.erase(it);
        cout << "Statistics " << name << " dropped" << endl;
    }
    saveExtendedStats(s.currentDB, stats);
    return !allOk;
}

static bool handleCreateCollation(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(16));
    bool ifNotExists = false;
    if (rest.substr(0, 13) == "if not exists") {
        ifNotExists = true;
        rest = trim(rest.substr(13));
    }
    string name;
    string options;
    size_t parenPos = rest.find('(');
    size_t fromPos = findTopLevelKeyword(rest, "from");
    if (fromPos != string::npos && (parenPos == string::npos || fromPos < parenPos)) {
        name = trim(rest.substr(0, fromPos));
        options = "from=" + trim(rest.substr(fromPos + 4));
    } else if (parenPos != string::npos) {
        name = trim(rest.substr(0, parenPos));
        size_t rp = rest.rfind(')');
        if (rp == string::npos || rp <= parenPos) {
            cout << "SQL syntax error: CREATE COLLATION name (...)" << endl;
            return true;
        }
        options = trim(rest.substr(parenPos + 1, rp - parenPos - 1));
    } else {
        cout << "SQL syntax error: CREATE COLLATION name (...) | FROM existing" << endl;
        return true;
    }
    if (name.empty()) {
        cout << "SQL syntax error: CREATE COLLATION requires a name" << endl;
        return true;
    }
    auto collations = loadCollations(s.currentDB);
    if (collations.count(name)) {
        if (ifNotExists) {
            cout << "Collation " << name << " already exists, skipping" << endl;
            return false;
        }
        cout << "Collation " << name << " already exists" << endl;
        return true;
    }
    string provider = "default";
    string locale = "";
    for (auto item : splitTopLevelComma(options)) {
        size_t eq = item.find('=');
        if (eq == string::npos) continue;
        string key = trim(item.substr(0, eq));
        string val = stripQuotes(trim(item.substr(eq + 1)));
        if (key == "provider") provider = val;
        else if (key == "locale" || key == "lc_collate") locale = val;
    }
    collations[name] = {name, provider, locale, s.username, options,
                        inferCollationBehavior(name, options)};
    if (!saveCollations(s.currentDB, collations)) {
        cout << "Create collation failed" << endl;
        return true;
    }
    cout << "Collation " << name << " created" << endl;
    return false;
}

static bool handleAlterCollation(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(15));
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 3) {
        cout << "SQL syntax error: ALTER COLLATION name RENAME TO newname | OWNER TO user | REFRESH VERSION" << endl;
        return true;
    }
    string name = tokens[0];
    auto collations = loadCollations(s.currentDB);
    auto it = collations.find(name);
    if (it == collations.end()) {
        cout << "Collation " << name << " not exist" << endl;
        return true;
    }
    if (tokens[1] == "rename" && tokens.size() >= 4 && tokens[2] == "to") {
        string newName = tokens[3];
        if (collations.count(newName)) {
            cout << "Collation " << newName << " already exists" << endl;
            return true;
        }
        auto info = it->second;
        collations.erase(it);
        info.name = newName;
        info.behavior = inferCollationBehavior(newName, info.options);
        collations[newName] = info;
        saveCollations(s.currentDB, collations);
        cout << "Collation " << name << " renamed to " << newName << endl;
        return false;
    }
    if (tokens[1] == "owner" && tokens.size() >= 4 && tokens[2] == "to") {
        it->second.owner = tokens[3];
        saveCollations(s.currentDB, collations);
        cout << "Collation " << name << " owner changed" << endl;
        return false;
    }
    if (tokens[1] == "refresh" && tokens.size() >= 3 && tokens[2] == "version") {
        it->second.options += (it->second.options.empty() ? "" : ",") + string("refreshed=true");
        saveCollations(s.currentDB, collations);
        cout << "Collation " << name << " version refreshed" << endl;
        return false;
    }
    cout << "SQL syntax error: ALTER COLLATION name RENAME TO newname | OWNER TO user | REFRESH VERSION" << endl;
    return true;
}

static bool handleDropCollation(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(14));
    bool ifExists = false;
    if (rest.substr(0, 9) == "if exists") {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    auto collations = loadCollations(s.currentDB);
    bool allOk = true;
    for (auto name : splitTopLevelComma(rest)) {
        name = trim(name);
        if (name.empty()) continue;
        auto it = collations.find(name);
        if (it == collations.end()) {
            if (!ifExists) {
                cout << "Collation " << name << " not exist" << endl;
                allOk = false;
            }
            continue;
        }
        collations.erase(it);
        cout << "Collation " << name << " dropped" << endl;
    }
    saveCollations(s.currentDB, collations);
    return !allOk;
}

static bool handleCreateCast(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(11));
    if (rest.empty() || rest.front() != '(') {
        cout << "SQL syntax error: CREATE CAST (source AS target) WITH FUNCTION|WITHOUT FUNCTION|WITH INOUT" << endl;
        return true;
    }
    size_t rp = rest.find(')');
    if (rp == string::npos) {
        cout << "SQL syntax error: CREATE CAST missing ')'" << endl;
        return true;
    }
    string typeSpec = trim(rest.substr(1, rp - 1));
    size_t asPos = findTopLevelKeyword(typeSpec, "as");
    if (asPos == string::npos) {
        cout << "SQL syntax error: CREATE CAST requires source AS target" << endl;
        return true;
    }
    string sourceType = trim(typeSpec.substr(0, asPos));
    string targetType = trim(typeSpec.substr(asPos + 2));
    string after = trim(rest.substr(rp + 1));
    string method = "function";
    string functionName;
    if (after.substr(0, 13) == "with function") {
        method = "function";
        functionName = trim(after.substr(13));
    } else if (after.substr(0, 16) == "without function") {
        method = "without function";
    } else if (after.substr(0, 10) == "with inout") {
        method = "inout";
    } else {
        cout << "SQL syntax error: CREATE CAST requires WITH FUNCTION, WITHOUT FUNCTION, or WITH INOUT" << endl;
        return true;
    }
    string context = "explicit";
    if (after.find("as assignment") != string::npos) context = "assignment";
    if (after.find("as implicit") != string::npos) context = "implicit";
    auto casts = loadCasts(s.currentDB);
    string key = castKey(sourceType, targetType);
    if (casts.count(key)) {
        cout << "Cast " << sourceType << " AS " << targetType << " already exists" << endl;
        return true;
    }
    casts[key] = {sourceType, targetType, method, functionName, context};
    if (!saveCasts(s.currentDB, casts)) {
        cout << "Create cast failed" << endl;
        return true;
    }
    cout << "Cast " << sourceType << " AS " << targetType << " created" << endl;
    return false;
}

static bool handleDropCast(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(9));
    bool ifExists = false;
    if (rest.substr(0, 9) == "if exists") {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    if (rest.empty() || rest.front() != '(') {
        cout << "SQL syntax error: DROP CAST (source AS target)" << endl;
        return true;
    }
    size_t rp = rest.find(')');
    if (rp == string::npos) {
        cout << "SQL syntax error: DROP CAST missing ')'" << endl;
        return true;
    }
    string typeSpec = trim(rest.substr(1, rp - 1));
    size_t asPos = findTopLevelKeyword(typeSpec, "as");
    if (asPos == string::npos) {
        cout << "SQL syntax error: DROP CAST requires source AS target" << endl;
        return true;
    }
    string sourceType = trim(typeSpec.substr(0, asPos));
    string targetType = trim(typeSpec.substr(asPos + 2));
    auto casts = loadCasts(s.currentDB);
    string key = castKey(sourceType, targetType);
    auto it = casts.find(key);
    if (it == casts.end()) {
        if (ifExists) {
            cout << "Cast does not exist, skipping" << endl;
            return false;
        }
        cout << "Cast " << sourceType << " AS " << targetType << " not exist" << endl;
        return true;
    }
    casts.erase(it);
    saveCasts(s.currentDB, casts);
    cout << "Cast " << sourceType << " AS " << targetType << " dropped" << endl;
    return false;
}

static bool handleCreateConversion(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(17));
    bool isDefault = false;
    if (rest.substr(0, 7) == "default") {
        isDefault = true;
        rest = trim(rest.substr(7));
    }
    size_t forPos = findTopLevelKeyword(rest, "for");
    size_t toPos = (forPos == string::npos) ? string::npos : findTopLevelKeyword(rest, "to", forPos + 3);
    size_t fromPos = (toPos == string::npos) ? string::npos : findTopLevelKeyword(rest, "from", toPos + 2);
    if (forPos == string::npos || toPos == string::npos || fromPos == string::npos) {
        cout << "SQL syntax error: CREATE [DEFAULT] CONVERSION name FOR source TO dest FROM function" << endl;
        return true;
    }
    string name = trim(rest.substr(0, forPos));
    string sourceEncoding = stripQuotes(trim(rest.substr(forPos + 3, toPos - forPos - 3)));
    string destEncoding = stripQuotes(trim(rest.substr(toPos + 2, fromPos - toPos - 2)));
    string functionName = trim(rest.substr(fromPos + 4));
    if (name.empty() || sourceEncoding.empty() || destEncoding.empty() || functionName.empty()) {
        cout << "SQL syntax error: CREATE [DEFAULT] CONVERSION name FOR source TO dest FROM function" << endl;
        return true;
    }
    auto conversions = loadConversions(s.currentDB);
    if (conversions.count(name)) {
        cout << "Conversion " << name << " already exists" << endl;
        return true;
    }
    conversions[name] = {name, sourceEncoding, destEncoding, functionName, isDefault};
    if (!saveConversions(s.currentDB, conversions)) {
        cout << "Create conversion failed" << endl;
        return true;
    }
    cout << "Conversion " << name << " created" << endl;
    return false;
}

static bool handleAlterConversion(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(16));
    vector<string> tokens = tokenize(rest);
    if (tokens.size() < 4) {
        cout << "SQL syntax error: ALTER CONVERSION name RENAME TO newname | OWNER TO user" << endl;
        return true;
    }
    string name = tokens[0];
    auto conversions = loadConversions(s.currentDB);
    auto it = conversions.find(name);
    if (it == conversions.end()) {
        cout << "Conversion " << name << " not exist" << endl;
        return true;
    }
    if (tokens[1] == "rename" && tokens[2] == "to") {
        string newName = tokens[3];
        if (conversions.count(newName)) {
            cout << "Conversion " << newName << " already exists" << endl;
            return true;
        }
        auto info = it->second;
        conversions.erase(it);
        info.name = newName;
        conversions[newName] = info;
        saveConversions(s.currentDB, conversions);
        cout << "Conversion " << name << " renamed to " << newName << endl;
        return false;
    }
    if (tokens[1] == "owner" && tokens[2] == "to") {
        cout << "Conversion " << name << " owner changed" << endl;
        return false;
    }
    cout << "SQL syntax error: ALTER CONVERSION name RENAME TO newname | OWNER TO user" << endl;
    return true;
}

static bool handleDropConversion(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(15));
    bool ifExists = false;
    if (rest.substr(0, 9) == "if exists") {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    auto conversions = loadConversions(s.currentDB);
    bool allOk = true;
    for (auto name : splitTopLevelComma(rest)) {
        name = trim(name);
        if (name.empty()) continue;
        auto it = conversions.find(name);
        if (it == conversions.end()) {
            if (!ifExists) {
                cout << "Conversion " << name << " not exist" << endl;
                allOk = false;
            }
            continue;
        }
        conversions.erase(it);
        cout << "Conversion " << name << " dropped" << endl;
    }
    saveConversions(s.currentDB, conversions);
    return !allOk;
}

static bool handleAlterSequence(const string& sql, Session& s) {
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    string rest = trim(sql.substr(14)); // after "alter sequence"
    bool ifExists = false;
    if (startsWithKeyword(rest, "if exists")) {
        ifExists = true;
        rest = trim(rest.substr(9));
    }
    vector<string> tokens = tokenize(rest);
    if (tokens.empty()) {
        cout << "SQL syntax error: ALTER SEQUENCE name [RESTART [WITH] n] [INCREMENT BY n]" << endl;
        return true;
    }
    string seqName = tokens[0];
    if (!g_engine.sequenceExists(s.currentDB, seqName)) {
        if (ifExists) {
            cout << "Sequence " << seqName << " does not exist, skipping" << endl;
            return false;
        }
        cout << "Sequence " << seqName << " not exist" << endl;
        return true;
    }
    if (tokens.size() >= 4 && tokens[1] == "rename" && tokens[2] == "to") {
        string newName = tokens[3];
        if (g_engine.sequenceExists(s.currentDB, newName)) {
            cout << "Sequence " << newName << " already exists" << endl;
            return true;
        }
        auto oldPath = g_engine.dbPath(s.currentDB) / (seqName + ".seq");
        auto newPath = g_engine.dbPath(s.currentDB) / (newName + ".seq");
        try {
            std::filesystem::rename(oldPath, newPath);
        } catch (...) {
            cout << "Rename sequence failed" << endl;
            return true;
        }
        auto objects = loadCompatObjects(s.currentDB);
        auto oldKey = compatObjectKey("sequence_option", seqName);
        auto it = objects.find(oldKey);
        if (it != objects.end()) {
            auto info = it->second;
            objects.erase(it);
            info.name = newName;
            info.definition = sql;
            objects[compatObjectKey("sequence_option", newName)] = info;
            saveCompatObjects(s.currentDB, objects);
        }
        cout << "Sequence " << seqName << " renamed to " << newName << endl;
        return false;
    }

    bool hasRestart = false;
    bool hasIncrement = false;
    int64_t restart = 1;
    int64_t increment = 1;
    for (size_t i = 1; i < tokens.size(); ++i) {
        if (tokens[i] == "restart") {
            size_t j = i + 1;
            if (j < tokens.size() && tokens[j] == "with") ++j;
            if (j >= tokens.size()) {
                cout << "SQL syntax error: ALTER SEQUENCE RESTART requires a value" << endl;
                return true;
            }
            try {
                restart = stoll(tokens[j]);
                hasRestart = true;
            } catch (...) {
                cout << "Invalid restart value" << endl;
                return true;
            }
            i = j;
        } else if (tokens[i] == "increment") {
            size_t j = i + 1;
            if (j < tokens.size() && tokens[j] == "by") ++j;
            if (j >= tokens.size()) {
                cout << "SQL syntax error: ALTER SEQUENCE INCREMENT BY requires a value" << endl;
                return true;
            }
            try {
                increment = stoll(tokens[j]);
                hasIncrement = true;
            } catch (...) {
                cout << "Invalid increment value" << endl;
                return true;
            }
            i = j;
        }
    }
    auto objects = loadCompatObjects(s.currentDB);
    string options;
    if (hasRestart) options += "restart=" + to_string(restart);
    if (hasIncrement) {
        if (!options.empty()) options += ";";
        options += "increment=" + to_string(increment);
    }
    objects[compatObjectKey("sequence_option", seqName)] =
        {"sequence_option", seqName, s.username, sql, options};
    saveCompatObjects(s.currentDB, objects);

    if (!hasRestart && !hasIncrement) {
        cout << "Sequence " << seqName << " options recorded" << endl;
        return false;
    }
    auto res = g_engine.alterSequence(s.currentDB, seqName,
                                      hasRestart, restart,
                                      hasIncrement, increment);
    if (res != DBStatus::OK) {
        cout << "Alter sequence failed" << endl;
        return true;
    }
    cout << "Sequence " << seqName << " altered" << endl;
    return false;
}

static bool checkTablePermission(Session& s, const string& tname,
                                  dbms::StorageEngine::TablePrivilege priv) {
    if (s.permission == 1) return true; // admin bypass
    if (!g_engine.hasPermission(s.currentDB, tname, s.username, priv)) {
        // If there are column-level permissions for this privilege type,
        // don't reject here; let the column-level check handle it.
        if (priv == dbms::StorageEngine::TablePrivilege::Select ||
            priv == dbms::StorageEngine::TablePrivilege::Insert ||
            priv == dbms::StorageEngine::TablePrivilege::Update) {
            auto perms = g_engine.getUserPermissions(s.currentDB, tname, s.username);
            string target = (priv == dbms::StorageEngine::TablePrivilege::Select) ? "select" :
                           (priv == dbms::StorageEngine::TablePrivilege::Insert) ? "insert" : "update";
            for (const auto& p : perms) {
                if (p == target || p == "all") return true;
            }
        }
        cout << "permission denied on table " << tname << endl;
        return false;
    }
    return true;
}

// Check column-level SELECT permission. columnsStr is comma-separated column list (or "*").
static bool checkSelectColumnPermission(Session& s, const string& tname,
                                        const string& columnsStr) {
    if (s.permission == 1) return true;
    if (columnsStr == "*") {
        // SELECT * requires table-level SELECT or ALL columns allowed individually
        // For simplicity: if any column-level restrictions exist, deny SELECT *
        auto perms = g_engine.getUserPermissions(s.currentDB, tname, s.username);
        for (const auto& p : perms) {
            if (p == "select" || p == "all") {
                // Need to check if this is table-level or column-level
                // If table-level, hasPermission would have returned true already
                // So if we reach here, it's column-level - deny *
            }
        }
        // Actually: if hasPermission is true, we already passed table-level check
        // If hasPermission is false but hasColumnPermission might be true for some columns
        // For SELECT *, we need all columns or table-level permission
        if (!g_engine.hasPermission(s.currentDB, tname, s.username,
                                     dbms::StorageEngine::TablePrivilege::Select)) {
            // Check if ALL columns of the table are individually allowed
            auto schema = g_engine.getTableSchema(s.currentDB, tname);
            if (schema.len == 0) return true; // no columns = no restriction
            vector<string> allCols;
            for (size_t i = 0; i < schema.len; ++i) allCols.push_back(schema.cols[i].dataName);
            if (!g_engine.hasColumnPermission(s.currentDB, tname, s.username,
                                               dbms::StorageEngine::TablePrivilege::Select, allCols)) {
                cout << "permission denied: SELECT * requires access to all columns on table " << tname << endl;
                return false;
            }
        }
        return true;
    }
    // Parse explicit column list
    vector<string> cols;
    stringstream ss(columnsStr);
    string item;
    while (getline(ss, item, ',')) {
        string col = trim(item);
        // Strip alias prefix like "t.col" or aggregate expressions
        size_t dot = col.find('.');
        if (dot != string::npos) col = trim(col.substr(dot + 1));
        size_t lp = col.find('(');
        if (lp != string::npos) {
            // aggregate(col) - extract inner column name
            size_t rp = col.find(')', lp);
            if (rp != string::npos) {
                string inner = trim(col.substr(lp + 1, rp - lp - 1));
                if (inner != "*" && inner != "distinct") col = inner;
                else continue; // count(*) doesn't need column permission
            }
        }
        if (!col.empty() && col != "*") cols.push_back(col);
    }
    if (cols.empty()) return true;
    if (!g_engine.hasColumnPermission(s.currentDB, tname, s.username,
                                       dbms::StorageEngine::TablePrivilege::Select, cols)) {
        cout << "permission denied: SELECT on restricted columns of table " << tname << endl;
        return false;
    }
    return true;
}

// Check column-level UPDATE permission on SET columns.
static bool checkUpdateColumnPermission(Session& s, const string& tname,
                                        const string& setClause) {
    if (s.permission == 1) return true;
    vector<string> cols;
    stringstream ss(setClause);
    string item;
    while (getline(ss, item, ',')) {
        string part = trim(item);
        size_t eq = part.find('=');
        if (eq != string::npos) {
            string col = trim(part.substr(0, eq));
            size_t dot = col.find('.');
            if (dot != string::npos) col = trim(col.substr(dot + 1));
            if (!col.empty()) cols.push_back(col);
        }
    }
    if (cols.empty()) return true;
    if (!g_engine.hasColumnPermission(s.currentDB, tname, s.username,
                                       dbms::StorageEngine::TablePrivilege::Update, cols)) {
        cout << "permission denied: UPDATE on restricted columns of table " << tname << endl;
        return false;
    }
    return true;
}

// Check column-level INSERT permission on given columns (empty = all columns).
static bool checkInsertColumnPermission(Session& s, const string& tname,
                                        const vector<string>& cols) {
    if (s.permission == 1) return true;
    if (cols.empty()) {
        // INSERT INTO t VALUES (...) - check all columns
        auto schema = g_engine.getTableSchema(s.currentDB, tname);
        if (schema.len == 0) return true;
        vector<string> allCols;
        for (size_t i = 0; i < schema.len; ++i) allCols.push_back(schema.cols[i].dataName);
        if (!g_engine.hasColumnPermission(s.currentDB, tname, s.username,
                                           dbms::StorageEngine::TablePrivilege::Insert, allCols)) {
            cout << "permission denied: INSERT requires access to all columns on table " << tname << endl;
            return false;
        }
        return true;
    }
    if (!g_engine.hasColumnPermission(s.currentDB, tname, s.username,
                                       dbms::StorageEngine::TablePrivilege::Insert, cols)) {
        cout << "permission denied: INSERT on restricted columns of table " << tname << endl;
        return false;
    }
    return true;
}

bool checkDB(const Session& s) {
    if (s.currentDB == "information_schema") return true;
    if (!g_engine.databaseExists(s.currentDB)) {
        cout << "Invalid Database name:" << s.currentDB << endl;
        log(s.username, "invalid database name", getTime());
        return false;
    }
    return true;
}

// Lightweight auto_explain: log query plan description for slow queries
void autoExplainLog(const std::string& sql, double ms,
                    const std::string& username,
                    const std::string& dbname) {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));

    // Build a simple plan description based on SQL text
    std::string plan;
    std::string lsql = sql;
    if (lsql.size() >= 6 && lsql.substr(0, 6) == "select") {
        plan = "Seq Scan";
        size_t joinPos = lsql.find(" join ");
        if (joinPos != std::string::npos) {
            std::string joinType = "Nested Loop";
            if (lsql.find(" hash ") != std::string::npos ||
                lsql.find("hash join") != std::string::npos) {
                joinType = "Hash Join";
            } else if (lsql.find(" merge ") != std::string::npos ||
                       lsql.find("merge join") != std::string::npos) {
                joinType = "Merge Join";
            }
            plan = joinType;
        }
        size_t idxPos = lsql.find("where");
        if (idxPos != std::string::npos) {
            plan += " with Index Cond";
        }
    } else if (lsql.size() >= 6 && lsql.substr(0, 6) == "insert") {
        plan = "Insert";
    } else if (lsql.size() >= 6 && lsql.substr(0, 6) == "update") {
        plan = "Update";
    } else if (lsql.size() >= 6 && lsql.substr(0, 6) == "delete") {
        plan = "Delete";
    } else {
        plan = "Other";
    }

    std::ofstream ofs("auto_explain.log", std::ios::app);
    if (ofs) {
        ofs << buf << " [" << username << "] [" << dbname << "] ["
            << std::fixed << std::setprecision(2) << ms << "ms] "
            << plan << " | " << sql << "\n";
    }
}

void logSlowQuery(const std::string& sql, double ms,
                  const std::string& username,
                  const std::string& dbname) {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));

    // Write to file with enhanced format
    std::ofstream ofs("slow_query.log", std::ios::app);
    if (ofs) {
        ofs << buf << " [" << username << "] [" << dbname << "] ["
            << std::fixed << std::setprecision(2) << ms << "ms] " << sql << "\n";
    }

    // Buffer in memory for SHOW SLOW LOG
    std::lock_guard<std::mutex> lock(g_slowQueryMutex);
    SlowQueryEntry entry;
    entry.timestamp = buf;
    entry.username = username;
    entry.dbname = dbname;
    entry.ms = ms;
    entry.sql = sql;
    g_slowQueryBuffer.push_back(std::move(entry));
    if (g_slowQueryBuffer.size() > MAX_SLOW_LOG_ENTRIES) {
        g_slowQueryBuffer.erase(g_slowQueryBuffer.begin());
    }
}

// Determine SQL category for auditing: 1=DDL, 2=DML, 3=other
static int sqlAuditCategory(const string& sql) {
    if (sql.size() >= 6 && sql.substr(0, 6) == "create") return 1;
    if (sql.size() >= 4 && sql.substr(0, 4) == "drop") return 1;
    if (sql.size() >= 5 && sql.substr(0, 5) == "alter") return 1;
    if (sql.size() >= 8 && sql.substr(0, 8) == "truncate") return 1;
    if (sql.size() >= 6 && (sql.substr(0, 6) == "select" || sql.substr(0, 6) == "insert" ||
        sql.substr(0, 6) == "update" || sql.substr(0, 6) == "delete" ||
        sql.substr(0, 6) == "merge " || sql.substr(0, 6) == "replac")) return 2; // DML
    return 3; // other (DCL, etc.)
}

bool execute(const string& rawSql, Session& s) {
    // Check for pg_terminate_backend / pg_cancel_backend flags
    if (s.terminateRequested) {
        cout << "ERROR: session terminated" << endl;
        return true;
    }
    if (s.cancelRequested) {
        s.cancelRequested = false;
        cout << "ERROR: query cancelled" << endl;
        return true;
    }
    g_engine.setRLSUser(s.username);
    string sql = sqlProcessor(rawSql);
    // Handle pg_cancel_backend / pg_terminate_backend
    {
        string lsql = sql;
        while (!lsql.empty() && lsql.front() == ' ') lsql = lsql.substr(1);
        if (lsql.substr(0, 7) == "select ") {
            string rest = trim(lsql.substr(7));
            size_t lp = rest.find('(');
            size_t rp = rest.find(')');
            if (lp != string::npos && rp != string::npos && rp >= lp + 1) {
                string func = trim(rest.substr(0, lp));
                string arg = trim(rest.substr(lp + 1, rp - lp - 1));
                if (func == "pg_cancel_backend" || func == "pg_terminate_backend") {
                    if (!checkAdmin(s)) return true;
                    if (arg.empty()) {
                        cout << "Invalid pid" << endl;
                        return true;
                    }
                    uint64_t targetPid = 0;
                    try { targetPid = static_cast<uint64_t>(std::stoull(arg)); } catch (...) {}
                    if (targetPid == 0) {
                        cout << "Invalid pid" << endl;
                        return true;
                    }
                    bool ok;
                    if (func == "pg_cancel_backend") {
                        ok = dbms::cancelBackend(targetPid);
                    } else {
                        ok = dbms::terminateBackend(targetPid);
                    }
                    cout << (ok ? "t" : "f") << endl;
                    return false;
                }
                if (func == "pg_reload_conf") {
                    if (!checkAdmin(s)) return true;
                    if (g_config.load("dbms.conf")) {
                        g_slowQueryThresholdMs = g_config.slowQueryThresholdMs;
                        cout << "t" << endl;
                    } else {
                        cout << "f" << endl;
                    }
                    return false;
                }
                if (func == "nextval" || func == "currval") {
                    if (!checkDB(s)) return true;
                    string seqName = stripQuotes(arg);
                    if (!g_engine.sequenceExists(s.currentDB, seqName)) {
                        cout << "Sequence " << seqName << " not exist" << endl;
                        return true;
                    }
                    if (func == "nextval") {
                        int64_t value = g_engine.nextval(s.currentDB, seqName);
                        s.sequenceLastValues[seqName] = value;
                        cout << func << endl;
                        cout << value << endl;
                    } else {
                        auto it = s.sequenceLastValues.find(seqName);
                        if (it == s.sequenceLastValues.end()) {
                            cout << "currval of sequence " << seqName << " is not yet defined in this session" << endl;
                            return true;
                        }
                        cout << func << endl;
                        cout << it->second << endl;
                    }
                    return false;
                }
            }
        }
    }
    // Replace user variables @varname with their values
    // Skip CALL statements so OUT/INOUT parameter references remain intact
    if (sql.substr(0, 4) != "call") {
        // For SET @var = value, do not replace the target variable on the left side of '='
        size_t replaceStart = 0;
        if (sql.substr(0, 3) == "set") {
            size_t eqPos = sql.find('=');
            if (eqPos != string::npos) {
                string lhs = trim(sql.substr(3, eqPos - 3));
                if (!lhs.empty() && lhs[0] == '@') {
                    replaceStart = eqPos + 1;
                }
            }
        }
        for (const auto& kv : s.userVariables) {
            string varName = "@" + kv.first;
            size_t pos = replaceStart;
            while ((pos = sql.find(varName, pos)) != string::npos) {
                // Ensure it's a standalone token (not part of an identifier)
                bool okBefore = (pos == 0) || !isalnum(static_cast<unsigned char>(sql[pos - 1]));
                bool okAfter = (pos + varName.size() >= sql.size()) ||
                               !isalnum(static_cast<unsigned char>(sql[pos + varName.size()]));
                if (okBefore && okAfter) {
                    sql.replace(pos, varName.size(), kv.second);
                    pos += kv.second.size();
                } else {
                    ++pos;
                }
            }
        }
    }
    // Audit logging
    {
        int cat = sqlAuditCategory(sql);
        bool shouldAudit = false;
        if (g_config.auditLevel >= 3) shouldAudit = true;
        else if (g_config.auditLevel >= 2 && cat <= 2) shouldAudit = true;
        else if (g_config.auditLevel >= 1 && cat == 1) shouldAudit = true;
        if (shouldAudit) {
            auditLog(g_config.auditLevel, s.username, s.currentDB, rawSql, "EXEC");
        }
    }
    // Check pending notifications at the start of each command
    checkNotifications(s);

    // Phase 1: Parser integration — classify SQL command via AST parser
    dbms::SqlCommand parsedCmd = dbms::SQLParser::classify(sql);

    // Simple command dispatch using Parser classification.
    // Complex commands (CREATE/DROP/ALTER/SELECT/INSERT/UPDATE/DELETE)
    // fall through to the legacy string-based dispatch below.
    switch (parsedCmd) {
        case dbms::SqlCommand::Values:
            return executeValuesStatement(sql);

        case dbms::SqlCommand::UseDatabase: {
            string dbname = trim(sql.substr(13)); // skip "use database"
            if (dbname != "information_schema" && !g_engine.databaseExists(dbname)) {
                cout << "Database not found" << endl;
                s.currentDB = "";
                log(s.username, "use database error", getTime());
                return true;
            }
            s.currentDB = dbname;
            cout << "set Database to " << dbname << endl;
            log(s.username, "use database success", getTime());
            return false;
        }

        case dbms::SqlCommand::Listen: {
            string channel = trim(sql.substr(6));
            if (channel.empty()) {
                cout << "SQL syntax error: LISTEN channel" << endl;
                return true;
            }
            {
                std::lock_guard<std::mutex> lock(g_notifyMutex);
                g_listeners[channel].insert(s.username);
            }
            s.listenedChannels.insert(channel);
            cout << "LISTEN " << channel << endl;
            return false;
        }

        case dbms::SqlCommand::Notify: {
            string rest = trim(sql.substr(6));
            size_t commaPos = rest.find(',');
            string channel = trim(rest.substr(0, commaPos));
            string payload;
            if (commaPos != string::npos) {
                payload = trim(rest.substr(commaPos + 1));
                if (payload.size() >= 2 && payload.front() == '\'' && payload.back() == '\'') {
                    payload = payload.substr(1, payload.size() - 2);
                }
            }
            if (channel.empty()) {
                cout << "SQL syntax error: NOTIFY channel [, payload]" << endl;
                return true;
            }
            {
                std::lock_guard<std::mutex> lock(g_notifyMutex);
                auto it = g_listeners.find(channel);
                if (it != g_listeners.end()) {
                    for (const auto& uname : it->second) {
                        g_pendingNotifies[uname].push_back({channel, payload});
                    }
                }
            }
            cout << "NOTIFY " << channel << endl;
            return false;
        }

        case dbms::SqlCommand::Unlisten: {
            string channel = trim(sql.substr(8));
            if (channel == "*") {
                std::lock_guard<std::mutex> lock(g_notifyMutex);
                for (const auto& ch : s.listenedChannels) {
                    auto it = g_listeners.find(ch);
                    if (it != g_listeners.end()) {
                        it->second.erase(s.username);
                        if (it->second.empty()) g_listeners.erase(it);
                    }
                }
                s.listenedChannels.clear();
                cout << "UNLISTEN *" << endl;
            } else if (!channel.empty()) {
                {
                    std::lock_guard<std::mutex> lock(g_notifyMutex);
                    auto it = g_listeners.find(channel);
                    if (it != g_listeners.end()) {
                        it->second.erase(s.username);
                        if (it->second.empty()) g_listeners.erase(it);
                    }
                }
                s.listenedChannels.erase(channel);
                cout << "UNLISTEN " << channel << endl;
            } else {
                cout << "SQL syntax error: UNLISTEN channel | UNLISTEN *" << endl;
                return true;
            }
            return false;
        }

        case dbms::SqlCommand::Do:
            return handleDoBlock(sql, s);

        case dbms::SqlCommand::ImportForeignSchema:
            return handleImportForeignSchema(sql, s);

        case dbms::SqlCommand::Declare:
            return handleDeclareCursor(sql, s);

        case dbms::SqlCommand::Fetch:
            return handleFetchCursor(sql, s);

        case dbms::SqlCommand::Move:
            return handleMoveCursor(sql, s);

        case dbms::SqlCommand::Close:
            return handleCloseCursor(sql, s);

        case dbms::SqlCommand::Begin:
        case dbms::SqlCommand::StartTransaction:
            return handleBeginTransaction(sql, s);

        case dbms::SqlCommand::Commit:
        case dbms::SqlCommand::End:
            return handleCommitTransaction(sql, s);

        case dbms::SqlCommand::CommitPrepared:
            return handleCommitPrepared(sql, s);

        case dbms::SqlCommand::Rollback:
        case dbms::SqlCommand::Abort:
            return handleRollbackTransaction(sql, s);

        case dbms::SqlCommand::RollbackPrepared:
            return handleRollbackPrepared(sql, s);

        case dbms::SqlCommand::Savepoint:
            return handleSavepoint(sql, s);

        case dbms::SqlCommand::ReleaseSavepoint:
            return handleReleaseSavepoint(sql, s);

        case dbms::SqlCommand::RollbackToSavepoint:
            return handleRollbackToSavepoint(sql, s);

        case dbms::SqlCommand::Set:
            return handleSetCommand(sql, s);

        case dbms::SqlCommand::Reset:
            return handleResetCommand(sql, s);

        case dbms::SqlCommand::AlterSystem:
            return handleAlterSystem(sql, s);

        case dbms::SqlCommand::Comment:
            return handleCommentOn(sql, s, rawSql);

        case dbms::SqlCommand::Lock:
            return handleLockTable(sql, s);

        case dbms::SqlCommand::Analyze:
            return handleAnalyze(sql, s);

        case dbms::SqlCommand::RefreshMaterializedView:
            return handleRefreshMaterializedView(sql, s);

        case dbms::SqlCommand::Checkpoint:
            return handleCheckpoint(sql, s);

        case dbms::SqlCommand::Vacuum:
            return handleVacuum(sql, s);

        case dbms::SqlCommand::SecurityLabel:
            return handleSecurityLabel(sql, s);

        case dbms::SqlCommand::Truncate:
            return handleTruncate(sql, s);

        case dbms::SqlCommand::Reindex:
            return handleReindex(sql, s);

        case dbms::SqlCommand::Call:
            return handleCallProcedure(sql, s);

        case dbms::SqlCommand::Copy:
            return handleCopy(sql, s);

        case dbms::SqlCommand::Prepare:
            return handlePrepare(sql, s);

        case dbms::SqlCommand::Execute:
            return handleExecutePrepared(sql, s);

        case dbms::SqlCommand::Deallocate:
            return handleDeallocate(sql, s);

        case dbms::SqlCommand::Explain:
            return handleExplain(sql, s);

        case dbms::SqlCommand::Show:
        case dbms::SqlCommand::Grant:
        case dbms::SqlCommand::Revoke:
            // TODO: extract to helper functions (code blocks are very large)
            break;

        // Core DQL/DML — complex blocks remain in execute() for now
        case dbms::SqlCommand::Select:
        case dbms::SqlCommand::Insert:
        case dbms::SqlCommand::Update:
        case dbms::SqlCommand::Delete:
        case dbms::SqlCommand::Merge:
            break;

        // DDL — CREATE/DROP/ALTER complex blocks remain in execute() for now
        // Subcommands with dedicated handlers are routed below.
        case dbms::SqlCommand::CreateTable:
        case dbms::SqlCommand::CreateTableAs:
        case dbms::SqlCommand::CreateIndex:
        case dbms::SqlCommand::CreateView:
        case dbms::SqlCommand::CreateMaterializedView:
        case dbms::SqlCommand::CreateDatabase:
        case dbms::SqlCommand::CreateSchema:
        case dbms::SqlCommand::CreateSequence:
        case dbms::SqlCommand::CreateDomain:
        case dbms::SqlCommand::CreateType:
        case dbms::SqlCommand::CreateFunction:
        case dbms::SqlCommand::CreateProcedure:
        case dbms::SqlCommand::CreateTrigger:
        case dbms::SqlCommand::CreateRole:
        case dbms::SqlCommand::CreateUser:
        case dbms::SqlCommand::CreateTablespace:
        case dbms::SqlCommand::CreateStatistics:
        case dbms::SqlCommand::CreatePolicy:
        case dbms::SqlCommand::CreateRule:
        case dbms::SqlCommand::CreateEventTrigger:
        case dbms::SqlCommand::CreateExtension:
        case dbms::SqlCommand::CreatePublication:
        case dbms::SqlCommand::CreateSubscription:
        case dbms::SqlCommand::CreateForeignDataWrapper:
        case dbms::SqlCommand::CreateForeignTable:
        case dbms::SqlCommand::CreateServer:
        case dbms::SqlCommand::CreateUserMapping:
        case dbms::SqlCommand::CreateTextSearchConfiguration:
        case dbms::SqlCommand::CreateTextSearchDictionary:
        case dbms::SqlCommand::CreateTextSearchParser:
        case dbms::SqlCommand::CreateTextSearchTemplate:
        case dbms::SqlCommand::CreateCast:
        case dbms::SqlCommand::CreateCollation:
        case dbms::SqlCommand::CreateConversion:
        case dbms::SqlCommand::CreateOperator:
        case dbms::SqlCommand::CreateOperatorClass:
        case dbms::SqlCommand::CreateOperatorFamily:
        case dbms::SqlCommand::CreateAggregate:
        case dbms::SqlCommand::CreateTransform:
        case dbms::SqlCommand::CreateLanguage:
        case dbms::SqlCommand::CreateAccessMethod:
            break;

        case dbms::SqlCommand::DropTable:
        case dbms::SqlCommand::DropIndex:
        case dbms::SqlCommand::DropView:
        case dbms::SqlCommand::DropMaterializedView:
        case dbms::SqlCommand::DropDatabase:
        case dbms::SqlCommand::DropSchema:
        case dbms::SqlCommand::DropSequence:
        case dbms::SqlCommand::DropDomain:
        case dbms::SqlCommand::DropType:
        case dbms::SqlCommand::DropFunction:
        case dbms::SqlCommand::DropProcedure:
        case dbms::SqlCommand::DropTrigger:
        case dbms::SqlCommand::DropRole:
        case dbms::SqlCommand::DropUser:
        case dbms::SqlCommand::DropTablespace:
        case dbms::SqlCommand::DropStatistics:
        case dbms::SqlCommand::DropPolicy:
        case dbms::SqlCommand::DropRule:
        case dbms::SqlCommand::DropEventTrigger:
        case dbms::SqlCommand::DropExtension:
        case dbms::SqlCommand::DropPublication:
        case dbms::SqlCommand::DropSubscription:
        case dbms::SqlCommand::DropForeignDataWrapper:
        case dbms::SqlCommand::DropForeignTable:
        case dbms::SqlCommand::DropServer:
        case dbms::SqlCommand::DropUserMapping:
        case dbms::SqlCommand::DropTextSearchConfiguration:
        case dbms::SqlCommand::DropTextSearchDictionary:
        case dbms::SqlCommand::DropTextSearchParser:
        case dbms::SqlCommand::DropTextSearchTemplate:
        case dbms::SqlCommand::DropCast:
        case dbms::SqlCommand::DropCollation:
        case dbms::SqlCommand::DropConversion:
        case dbms::SqlCommand::DropOperator:
        case dbms::SqlCommand::DropOperatorClass:
        case dbms::SqlCommand::DropOperatorFamily:
        case dbms::SqlCommand::DropAggregate:
        case dbms::SqlCommand::DropTransform:
        case dbms::SqlCommand::DropLanguage:
        case dbms::SqlCommand::DropAccessMethod:
        case dbms::SqlCommand::DropOwned:
        case dbms::SqlCommand::DropLargeObject:
            break;

        case dbms::SqlCommand::AlterTable:
        case dbms::SqlCommand::AlterIndex:
        case dbms::SqlCommand::AlterView:
        case dbms::SqlCommand::AlterMaterializedView:
        case dbms::SqlCommand::AlterDatabase:
        case dbms::SqlCommand::AlterSchema:
        case dbms::SqlCommand::AlterSequence:
        case dbms::SqlCommand::AlterDomain:
        case dbms::SqlCommand::AlterType:
        case dbms::SqlCommand::AlterFunction:
        case dbms::SqlCommand::AlterProcedure:
        case dbms::SqlCommand::AlterRoutine:
        case dbms::SqlCommand::AlterTrigger:
        case dbms::SqlCommand::AlterRole:
        case dbms::SqlCommand::AlterUser:
        case dbms::SqlCommand::AlterTablespace:
        case dbms::SqlCommand::AlterStatistics:
        case dbms::SqlCommand::AlterPolicy:
        case dbms::SqlCommand::AlterRule:
        case dbms::SqlCommand::AlterEventTrigger:
        case dbms::SqlCommand::AlterExtension:
        case dbms::SqlCommand::AlterPublication:
        case dbms::SqlCommand::AlterSubscription:
        case dbms::SqlCommand::AlterForeignDataWrapper:
        case dbms::SqlCommand::AlterForeignTable:
        case dbms::SqlCommand::AlterServer:
        case dbms::SqlCommand::AlterUserMapping:
        case dbms::SqlCommand::AlterTextSearchConfiguration:
        case dbms::SqlCommand::AlterTextSearchDictionary:
        case dbms::SqlCommand::AlterTextSearchParser:
        case dbms::SqlCommand::AlterTextSearchTemplate:
        case dbms::SqlCommand::AlterCollation:
        case dbms::SqlCommand::AlterConversion:
        case dbms::SqlCommand::AlterOperator:
        case dbms::SqlCommand::AlterOperatorClass:
        case dbms::SqlCommand::AlterOperatorFamily:
        case dbms::SqlCommand::AlterAggregate:
        case dbms::SqlCommand::AlterTransform:
        case dbms::SqlCommand::AlterLanguage:
        case dbms::SqlCommand::AlterLargeObject:
        case dbms::SqlCommand::AlterDefaultPrivileges:
            break;

        default:
            break;
    }

    // Phase 4 Wave 0.3: DDL AST bridge — try AST-driven execution before legacy string dispatch.
    // tryDdlBridge returns on both success and error; if it did not handle the command,
    // execution falls through to the legacy string dispatch below.
    {
        bool handled = false;
        bool err = dbms::tryDdlBridge(sql, parsedCmd, s, handled);
        if (handled) {
            return err;
        }
    }

    if (sql.substr(0, 6) == "create") {
        if (sql.substr(7, 4) == "cast") {
            return handleCreateCast(sql, s);
        }
        if (sql.substr(7, 9) == "collation") {
            return handleCreateCollation(sql, s);
        }
        if (sql.substr(7, 10) == "conversion") {
            return handleCreateConversion(sql, s);
        }
        if (sql.substr(7, 10) == "tablespace") {
            return handleCreateTablespace(sql, s);
        }
        if (sql.substr(7, 10) == "statistics") {
            return handleCreateStatistics(sql, s);
        }
        if (isCompatObjectCreate(sql)) {
            return handleCreateCompatObject(sql, s);
        }
        if (sql.substr(7, 5) == "group") {
            return handleCreateGroup(sql, s);
        }
        if (sql.substr(7, 4) == "user") {
            if (!checkAdmin(s)) return true;
            string rest = trim(sql.substr(12));
            vector<string> parts;
            stringstream ss(rest);
            string part;
            while (ss >> part) parts.push_back(part);
            if (parts.size() < 3) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            user temp;
            temp.username = parts[0];
            temp.permission = parts[2];
            // Preserve password case from raw SQL (sqlProcessor lowercases everything)
            {
                string rawRest = rawSql;
                size_t p = 0;
                while (p < rawRest.size() && isspace(static_cast<unsigned char>(rawRest[p]))) ++p;
                rawRest = rawRest.substr(p);
                string lraw = toLower(rawRest);
                size_t cuPos = lraw.find("create user");
                if (cuPos != string::npos) rawRest = rawRest.substr(cuPos + 11);
                rawRest = trim(rawRest);
                vector<string> rawParts;
                stringstream rss(rawRest);
                string rp;
                while (rss >> rp) rawParts.push_back(rp);
                if (rawParts.size() >= 2) temp.password = rawParts[1];
                else temp.password = parts[1];
            }
            if (permissionQuery(temp.username) != -1) {
                cout << "error: user already exist" << endl;
                log(s.username, "error: user already exist", getTime());
                return true;
            }
            // Password strength check
            if (g_config.passwordPolicyLevel > 0) {
                int score = checkPasswordStrength(temp.password);
                std::string strength = passwordStrengthMessage(score);
                if (g_config.passwordPolicyLevel >= 3 && score < 80) {
                    cout << "ERROR: password too weak (" << strength << ", score=" << score
                         << "). Require strong password (score>=80)." << endl;
                    return true;
                }
                if (g_config.passwordPolicyLevel >= 2 && score < 50) {
                    cout << "ERROR: password too weak (" << strength << ", score=" << score
                         << "). Require medium password (score>=50)." << endl;
                    return true;
                }
                if (g_config.passwordPolicyLevel >= 1) {
                    cout << "Password strength: " << strength << " (score=" << score << ")" << endl;
                }
            }
            createUser(temp, g_config.passwordHashAlgorithm);
            cout << "create user  " << temp.username << "  succeeded" << endl;
            return false;
        }

        if (sql.substr(7, 4) == "role") {
            if (!checkAdmin(s)) return true;
            string rest = trim(sql.substr(12));
            if (rest.empty()) {
                cout << "SQL syntax error: CREATE ROLE role_name" << endl;
                return true;
            }
            string roleName = rest;
            // Check if role name conflicts with existing user
            if (permissionQuery(roleName) != -1) {
                cout << "error: role name conflicts with existing user" << endl;
                return true;
            }
            int res = createRole(roleName);
            if (res == -1) {
                cout << "error: role already exists" << endl;
                return true;
            }
            cout << "CREATE ROLE succeeded" << endl;
            return false;
        }

        if (sql.substr(7, 6) == "domain") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            string rest = trim(sql.substr(14));
            // Parse: domain_name AS base_type [DEFAULT 'val'] [CHECK (expr)]
            size_t asPos = rest.find(" as ");
            if (asPos == string::npos) {
                cout << "SQL syntax error: CREATE DOMAIN name AS type" << endl;
                return true;
            }
            string dname = trim(rest.substr(0, asPos));
            string afterAs = trim(rest.substr(asPos + 4));
            size_t defPos = findTopLevelSqlKeyword(afterAs, "default");
            size_t constraintPos = findTopLevelSqlKeyword(afterAs, "constraint");
            size_t checkPos = findTopLevelSqlKeyword(afterAs, "check");
            size_t typeEnd = afterAs.size();
            if (defPos != string::npos) typeEnd = std::min(typeEnd, defPos);
            if (constraintPos != string::npos) typeEnd = std::min(typeEnd, constraintPos);
            if (checkPos != string::npos) typeEnd = std::min(typeEnd, checkPos);
            string baseType = trim(afterAs.substr(0, typeEnd));
            string defVal;
            if (defPos != string::npos) {
                size_t defStart = defPos + 7;
                size_t defEnd = afterAs.size();
                if (constraintPos != string::npos && constraintPos > defPos) defEnd = std::min(defEnd, constraintPos);
                if (checkPos != string::npos && checkPos > defPos) defEnd = std::min(defEnd, checkPos);
                defVal = stripQuotes(trim(afterAs.substr(defStart, defEnd - defStart)));
            }
            string checkExpr;
            string constraintName;
            if (checkPos != string::npos) {
                size_t constraintNameStart = (constraintPos == string::npos) ? string::npos : constraintPos + 10;
                string beforeCheck = (constraintNameStart == string::npos || constraintNameStart > checkPos)
                    ? ""
                    : trim(afterAs.substr(constraintNameStart, checkPos - constraintNameStart));
                if (startsWithKeyword(beforeCheck, "constraint")) {
                    constraintName = firstCompatNameToken(trim(beforeCheck.substr(10)));
                } else if (!beforeCheck.empty()) {
                    constraintName = firstCompatNameToken(beforeCheck);
                }
                size_t lp = afterAs.find('(', checkPos);
                size_t rp = (lp == string::npos) ? string::npos : findMatchingParen(afterAs, lp);
                if (lp != string::npos && rp != string::npos) {
                    checkExpr = trim(afterAs.substr(lp + 1, rp - lp - 1));
                }
            }
            dbms::StorageEngine::DomainInfo info;
            info.name = dname;
            info.baseType = baseType;
            info.defaultValue = defVal;
            info.checkExpr = checkExpr;
            info.constraintName = constraintName;
            auto res = g_engine.createDomain(s.currentDB, info);
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Domain " << dname << " already exists" << endl;
                return true;
            }
            cout << "Domain " << dname << " created" << endl;
            return false;
        }

        if (sql.substr(7, 8) == "database") {
            if (!checkAdmin(s)) return true;
            if (g_engine.inTransaction()) {
                g_engine.commitTransaction();
                cout << "Note: DDL caused implicit commit of open transaction" << endl;
            }
            string rest = trim(sql.substr(16));
            string dbname = rest;
            string charset = "utf8";
            size_t csPos1 = rest.find(" character set ");
            size_t csPos2 = rest.find(" charset ");
            size_t csPos = string::npos;
            size_t csOffset = 0;
            if (csPos1 != string::npos) {
                csPos = csPos1;
                csOffset = 15;
            } else if (csPos2 != string::npos) {
                csPos = csPos2;
                csOffset = 9;
            }
            if (csPos != string::npos) {
                dbname = trim(rest.substr(0, csPos));
                charset = trim(rest.substr(csPos + csOffset));
            }
            auto res = g_engine.createDatabase(dbname, charset);
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Failed:Database " << dbname << " already exists" << endl;
                log(s.username, "create database error", getTime());
            } else {
                cout << "Create Database succeeded (charset=" << charset << ")" << endl;
                log(s.username, "create database succeeded", getTime());
            }
            return res != DBStatus::OK;
        }

        if (sql.substr(7, 9) == "temporary") {
            // create temporary table tname (...)
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            string rest = trim(sql.substr(17)); // skip "create temporary "
            if (rest.substr(0, 5) != "table") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            rest = trim(rest.substr(5));
            size_t sp = rest.find(' ');
            if (sp == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string origName = rest.substr(0, sp);
            string tmpName = tempTablePrefix(origName);
            TableSchema tbl = parseTableColumns(sql, 17 + 5 + 1 + sp + 1, s.currentDB);
            tbl.tablename = tmpName;
            auto res = g_engine.createTable(s.currentDB, tbl);
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Temporary table " << origName << " already exists" << endl;
                return true;
            }
            s.tempTables.insert(origName);
            cout << "Temporary table " << origName << " created" << endl;
            log(s.username, "temporary table create succeeded", getTime());
            return false;
        }

        bool isUnlogged = false;
        size_t tableKeywordPos = 7;
        if (sql.size() > 16 && sql.substr(7, 9) == "unlogged ") {
            isUnlogged = true;
            tableKeywordPos = 16;
        }
        if (sql.substr(tableKeywordPos, 5) == "table") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            // DDL implicitly commits any open transaction (PostgreSQL behavior)
            if (g_engine.inTransaction()) {
                g_engine.commitTransaction();
                cout << "Note: DDL caused implicit commit of open transaction" << endl;
            }
            size_t restOff = tableKeywordPos + 6;
            string rest = trim(sql.substr(restOff));
            // Determine table name end: stop at first '(', " as ", or space
            size_t parenStart = rest.find('(');
            size_t asPos = rest.find(" as ");
            size_t sp = rest.find(' ');
            size_t tnameEnd = rest.size();
            if (parenStart != string::npos) tnameEnd = std::min(tnameEnd, parenStart);
            if (asPos != string::npos) tnameEnd = std::min(tnameEnd, asPos);
            if (sp != string::npos && sp < tnameEnd) tnameEnd = sp;
            if (tnameEnd == 0 || tnameEnd == rest.size()) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = rest.substr(0, tnameEnd);
            tname = resolveTableName(s, tname);
            // CTAS: CREATE TABLE new_table AS SELECT ...
            if (parenStart == string::npos) parenStart = rest.find('(', tnameEnd);
            if (asPos == string::npos) asPos = rest.find(" as ", tnameEnd);
            // Only treat as CTAS if " as " appears before the column list paren
            if (asPos != string::npos && (parenStart == string::npos || asPos < parenStart)) {
                string newTname = tname;
                string selectPart = trim(rest.substr(asPos + 4));
                if (selectPart.size() >= 6 && selectPart.substr(0, 6) == "select") {
                    // Parse column names from SELECT part
                    size_t fromPos = findTopLevelKeyword(selectPart, "from");
                    if (fromPos == string::npos) {
                        cout << "SQL syntax error: CTAS requires SELECT ... FROM" << endl;
                        return true;
                    }
                    string columns = trim(selectPart.substr(6, fromPos - 6));
                    bool selectAll = (columns == "*");

                    // Parse source table name
                    size_t tnameEnd = selectPart.find(' ', fromPos + 5);
                    if (tnameEnd == string::npos) tnameEnd = selectPart.size();
                    string srcTname = trim(selectPart.substr(fromPos + 4, tnameEnd - fromPos - 4));
                    srcTname = resolveTableName(s, srcTname);
                    if (!g_engine.tableExists(s.currentDB, srcTname)) {
                        cout << "Source table not found" << endl;
                        return true;
                    }
                    TableSchema srcTbl = g_engine.getTableSchema(s.currentDB, srcTname);

                    vector<string> colNames;
                    if (selectAll) {
                        for (size_t i = 0; i < srcTbl.len; ++i)
                            colNames.push_back(srcTbl.cols[i].dataName);
                    } else {
                        stringstream css(columns);
                        string item;
                        while (getline(css, item, ',')) {
                            item = trim(item);
                            if (!item.empty()) colNames.push_back(item);
                        }
                    }
                    if (colNames.empty()) {
                        cout << "CTAS: no columns in SELECT" << endl;
                        return true;
                    }

                    // Build query conditions from WHERE clause
                    set<string> selectColsSet(colNames.begin(), colNames.end());
                    size_t wherePos = findTopLevelKeyword(selectPart, "where", fromPos);
                    vector<string> conditions;
                    if (wherePos != string::npos) {
                        string condStr = normalizeConditionStr(trim(selectPart.substr(wherePos + 5)));
                        // Split by AND for simple multi-condition support
                        size_t andPos = 0;
                        while (andPos < condStr.size()) {
                            size_t nextAnd = condStr.find("AND", andPos);
                            string singleCond = (nextAnd == string::npos)
                                ? trim(condStr.substr(andPos))
                                : trim(condStr.substr(andPos, nextAnd - andPos));
                            if (!singleCond.empty()) {
                                string mc = modifyLogic(singleCond);
                                if (!mc.empty()) conditions.push_back(mc);
                            }
                            if (nextAnd == string::npos) break;
                            andPos = nextAnd + 3;
                        }
                    }

                    auto rows = g_engine.query(s.currentDB, srcTname, conditions, selectColsSet, {});

                    // Build schema: infer types from first data row if available
                    TableSchema newTbl;
                    newTbl.tablename = newTname;
                    for (const auto& cname : colNames) {
                        dbms::Column col = dbms::makeVarCharColumn(cname, true, 255);
                        if (!rows.empty()) {
                            size_t idx = 0;
                            stringstream rss(rows[0]);
                            string val;
                            while (rss >> val && idx < colNames.size()) {
                                if (colNames[idx] == cname) break;
                                ++idx;
                            }
                            if (idx < colNames.size() && val != "NULL" && !val.empty()) {
                                bool isInt = true;
                                for (char ch : val) {
                                    if (!isdigit(static_cast<unsigned char>(ch))) { isInt = false; break; }
                                }
                                if (isInt) col = dbms::makeIntColumn(cname, true, 2);
                            }
                        }
                        newTbl.append(col);
                    }

                    auto res = g_engine.createTable(s.currentDB, newTbl);
                    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                        cout << "Table " << newTname << " already exists" << endl;
                        return true;
                    }

                    size_t rowCount = 0;
                    for (const auto& row : rows) {
                        vector<string> vals;
                        stringstream rss(row);
                        string v;
                        while (rss >> v) vals.push_back(v);
                        if (vals.size() != colNames.size()) continue;
                        std::map<string, string> values;
                        for (size_t j = 0; j < colNames.size(); ++j) {
                            if (vals[j] == "NULL") vals[j] = "";
                            values[colNames[j]] = vals[j];
                        }
                        g_engine.insert(s.currentDB, newTname, values);
                        ++rowCount;
                    }

                    cout << "Table created with " << rowCount << " rows" << endl;
                    log(s.username, "ctas succeeded", getTime());
                    return false;
                }
                cout << "SQL syntax error: CTAS requires SELECT" << endl;
                return true;
            }
            // Check for PARTITION OF clause (CREATE TABLE pname PARTITION OF parent)
            size_t poPos = rest.find("partition of");
            if (poPos != string::npos) {
                string pname = trim(rest.substr(0, poPos));
                string afterPo = trim(rest.substr(poPos + 12));
                size_t sp2 = afterPo.find(' ');
                string parentName = (sp2 == string::npos) ? afterPo : trim(afterPo.substr(0, sp2));
                parentName = resolveTableName(s, parentName);
                if (!g_engine.tableExists(s.currentDB, parentName)) {
                    cout << "Parent table not found" << endl;
                    return true;
                }
                TableSchema parentTbl = g_engine.getTableSchema(s.currentDB, parentName);
                if (parentTbl.partitionType == dbms::TableSchema::PartitionType::None) {
                    cout << "Parent table is not partitioned" << endl;
                    return true;
                }
                string spec = (sp2 == string::npos) ? "" : trim(afterPo.substr(sp2));
                TableSchema newTbl = parentTbl;
                newTbl.tablename = pname;
                newTbl.partitionType = dbms::TableSchema::PartitionType::None;
                newTbl.partitionKey.clear();
                newTbl.rangePartitions.clear();
                newTbl.listPartitions.clear();
                newTbl.hashPartitions = 0;
                newTbl.defaultPartitionName.clear();
                auto res = g_engine.createTable(s.currentDB, newTbl);
                if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                    cout << "Table " << pname << " already exists" << endl;
                    return true;
                }
                res = g_engine.attachPartition(s.currentDB, parentName, pname, spec);
                if (res != DBStatus::OK) {
                    g_engine.dropTable(s.currentDB, pname);
                    cout << "Failed to attach partition" << endl;
                    return true;
                }
                cout << "Partition " << pname << " created and attached to " << parentName << endl;
                return false;
            }

            // Check for INHERITS clause
            size_t inheritsPos = sql.find("inherits");
            string parentName;
            if (inheritsPos != string::npos) {
                string inheritsRest = trim(sql.substr(inheritsPos + 8));
                if (!inheritsRest.empty() && inheritsRest.front() == '(') {
                    size_t rp = inheritsRest.find(')');
                    if (rp != string::npos) {
                        parentName = trim(inheritsRest.substr(1, rp - 1));
                        parentName = resolveTableName(s, parentName);
                    }
                }
            }

            // Check for PARTITION BY clause
            size_t partPos = sql.find("partition by");
            string colsSql = sql;
            if (partPos != string::npos) colsSql = sql.substr(0, partPos);
            if (inheritsPos != string::npos && (partPos == string::npos || inheritsPos < partPos)) {
                colsSql = sql.substr(0, inheritsPos);
            }
            TableSchema tbl = parseTableColumns(colsSql, restOff + tnameEnd, s.currentDB);

            // Merge inherited columns from parent
            if (!parentName.empty()) {
                if (!g_engine.tableExists(s.currentDB, parentName)) {
                    cout << "Parent table " << parentName << " not exist" << endl;
                    return true;
                }
                TableSchema parentTbl = g_engine.getTableSchema(s.currentDB, parentName);
                // Prepend parent columns (skip duplicates by name)
                std::set<string> childCols;
                for (size_t i = 0; i < tbl.len; ++i) childCols.insert(tbl.cols[i].dataName);
                TableSchema merged;
                merged.tablename = tbl.tablename;
                merged.isUnlogged = tbl.isUnlogged;
                merged.partitionType = tbl.partitionType;
                merged.partitionKey = tbl.partitionKey;
                merged.rangePartitions = tbl.rangePartitions;
                merged.listPartitions = tbl.listPartitions;
                merged.hashPartitions = tbl.hashPartitions;
                merged.defaultPartitionName = tbl.defaultPartitionName;
                merged.pkColIndices = tbl.pkColIndices;
                merged.uniqueConstraints = tbl.uniqueConstraints;
                for (size_t i = 0; i < parentTbl.len; ++i) {
                    if (childCols.count(parentTbl.cols[i].dataName)) continue;
                    merged.append(parentTbl.cols[i]);
                }
                for (size_t i = 0; i < tbl.len; ++i) merged.append(tbl.cols[i]);
                for (size_t i = 0; i < tbl.fkLen; ++i) merged.appendFK(tbl.fks[i]);
                tbl = merged;
                // Record inheritance relationship
                auto inhPath = std::filesystem::path(g_engine.dbPath(s.currentDB)) / ".inherits";
                {
                    std::ofstream ofs(inhPath, std::ios::app);
                    if (ofs) ofs << parentName << "|" << tname << "\n";
                }
            }
            tbl.tablename = tname;
            tbl.isUnlogged = isUnlogged;
            // Parse partitioning
            if (partPos != string::npos) {
                string partRest = trim(sql.substr(partPos + 12)); // after "partition by"
                size_t lp = partRest.find('(');
                size_t rp = partRest.find(')', lp);
                if (lp != string::npos) {
                    string ptype = trim(partRest.substr(0, lp));
                    string pkey = trim(partRest.substr(lp + 1, rp - lp - 1));
                    tbl.partitionKey = pkey;
                    transform(ptype.begin(), ptype.end(), ptype.begin(), ::tolower);
                    if (ptype == "range") {
                        tbl.partitionType = dbms::TableSchema::PartitionType::Range;
                        size_t defsStart = partRest.find('(', rp);
                        if (defsStart != string::npos) {
                            size_t defsEnd = partRest.rfind(')');
                            string defs = partRest.substr(defsStart + 1, defsEnd - defsStart - 1);
                            size_t dp = 0;
                            while (dp < defs.size()) {
                                size_t nextPart = defs.find("partition ", dp + 1);
                                string def = trim(defs.substr(dp, nextPart - dp));
                                if (!def.empty()) {
                                    string defLower = def;
                                    transform(defLower.begin(), defLower.end(), defLower.begin(), ::tolower);
                                    size_t vp = defLower.find("values less than");
                                    if (vp != string::npos) {
                                        string pname = trim(def.substr(0, vp));
                                        if (pname.size() > 10 && pname.substr(0, 10) == "partition ") pname = trim(pname.substr(10));
                                        size_t blp = def.find('(', vp);
                                        size_t brp = def.find(')', blp);
                                        if (blp != string::npos && brp != string::npos) {
                                            string bound = trim(def.substr(blp + 1, brp - blp - 1));
                                            tbl.rangePartitions.push_back({pname, bound});
                                        }
                                    }
                                }
                                if (nextPart == string::npos) break;
                                dp = nextPart;
                            }
                        }
                    } else if (ptype == "list") {
                        tbl.partitionType = dbms::TableSchema::PartitionType::List;
                        size_t defsStart = partRest.find('(', rp);
                        if (defsStart != string::npos) {
                            size_t defsEnd = partRest.rfind(')');
                            string defs = partRest.substr(defsStart + 1, defsEnd - defsStart - 1);
                            // Split by "partition " keyword (not comma, which may appear inside values)
                            size_t dp = 0;
                            while (dp < defs.size()) {
                                size_t nextPart = defs.find("partition ", dp + 1);
                                string def = trim(defs.substr(dp, nextPart - dp));
                                if (!def.empty()) {
                                    string defLower = def;
                                    transform(defLower.begin(), defLower.end(), defLower.begin(), ::tolower);
                                    size_t vp = defLower.find("values in");
                                    size_t dpPos = defLower.find("default");
                                    if (vp != string::npos) {
                                        string pname = trim(def.substr(0, vp));
                                        if (pname.size() > 10 && pname.substr(0, 10) == "partition ") pname = trim(pname.substr(10));
                                        size_t blp = def.find('(', vp);
                                        size_t brp = def.find(')', blp);
                                        if (blp != string::npos && brp != string::npos) {
                                            string vals = def.substr(blp + 1, brp - blp - 1);
                                            vector<string> vlist;
                                            size_t cp = 0;
                                            while (cp < vals.size()) {
                                                size_t c = vals.find(',', cp);
                                                string v = trim(vals.substr(cp, c - cp));
                                                // Strip quotes from partition values
                                                if (v.size() >= 2 && ((v.front() == '\'' && v.back() == '\'') || (v.front() == '"' && v.back() == '"'))) {
                                                    v = v.substr(1, v.size() - 2);
                                                }
                                                vlist.push_back(v);
                                                if (c == string::npos) break;
                                                cp = c + 1;
                                            }
                                            tbl.listPartitions.push_back({pname, vlist});
                                        }
                                    } else if (dpPos != string::npos) {
                                        string pname = trim(def.substr(0, dpPos));
                                        if (pname.size() > 10 && pname.substr(0, 10) == "partition ") pname = trim(pname.substr(10));
                                        tbl.defaultPartitionName = pname;
                                    }
                                }
                                if (nextPart == string::npos) break;
                                dp = nextPart;
                            }
                        }
                    } else if (ptype == "hash") {
                        tbl.partitionType = dbms::TableSchema::PartitionType::Hash;
                        size_t np = partRest.find("partitions");
                        if (np != string::npos) {
                            string nstr = trim(partRest.substr(np + 10));
                            try { tbl.hashPartitions = stoul(nstr); } catch (...) {}
                        }
                        if (tbl.hashPartitions == 0) tbl.hashPartitions = 4;
                    }
                }
            }

            // Check for SUBPARTITION BY clause
            size_t subPartPos = sql.find("subpartition by");
            if (subPartPos != string::npos && partPos != string::npos) {
                string subRest = trim(sql.substr(subPartPos + 15)); // after "subpartition by"
                size_t lp = subRest.find('(');
                size_t rp = subRest.find(')', lp);
                if (lp != string::npos) {
                    string sptype = trim(subRest.substr(0, lp));
                    string spkey = trim(subRest.substr(lp + 1, rp - lp - 1));
                    tbl.subPartitionKey = spkey;
                    transform(sptype.begin(), sptype.end(), sptype.begin(), ::tolower);
                    if (sptype == "hash") {
                        tbl.subPartitionType = dbms::TableSchema::PartitionType::Hash;
                        size_t np = subRest.find("partitions");
                        if (np != string::npos) {
                            string nstr = trim(subRest.substr(np + 10));
                            try { tbl.subHashPartitions = stoul(nstr); } catch (...) {}
                        }
                        if (tbl.subHashPartitions == 0) tbl.subHashPartitions = 4;
                    }
                }
            }

            // Parse WITH (fillfactor=70, autovacuum_enabled=off) clause
            size_t withPos = sql.find("with ");
            if (withPos != string::npos) {
                size_t lp = sql.find('(', withPos);
                size_t rp = sql.find(')', lp);
                if (lp != string::npos && rp != string::npos && rp > lp) {
                    string inside = sql.substr(lp + 1, rp - lp - 1);
                    std::map<std::string, std::string> params;
                    size_t cp = 0;
                    while (cp < inside.size()) {
                        size_t c = inside.find(',', cp);
                        string kv = trim(inside.substr(cp, c - cp));
                        size_t eq = kv.find('=');
                        if (eq != string::npos) {
                            string k = trim(kv.substr(0, eq));
                            string v = trim(kv.substr(eq + 1));
                            // Strip quotes
                            if (v.size() >= 2 && ((v.front() == '\'' && v.back() == '\'') ||
                                (v.front() == '"' && v.back() == '"'))) {
                                v = v.substr(1, v.size() - 2);
                            }
                            params[k] = v;
                        }
                        if (c == string::npos) break;
                        cp = c + 1;
                    }
                    if (!params.empty()) {
                        g_engine.setStorageParams(s.currentDB, tname, params);
                    }
                }
            }

            auto res = g_engine.createTable(s.currentDB, tbl);
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Table " << tname << " already exists" << endl;
                log(s.username, "table already exists", getTime());
                return true;
            }
            // Apply default privileges for tables
            g_engine.applyDefaultPrivileges(s.currentDB, "public", "table", tname, s.username);
            cout << "Table create succeeded" << endl;
            log(s.username, "table create succeeded", getTime());
            return false;
        }

        if (sql.substr(7, 5) == "index" || sql.substr(7, 4) == "hash" || sql.substr(7, 6) == "unique") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            if (g_engine.inTransaction()) {
                g_engine.commitTransaction();
                cout << "Note: DDL caused implicit commit of open transaction" << endl;
            }
            bool isHash = false;
            bool isConcurrently = false;
            size_t restStart = 13;
            // Check for "create hash index"
            if (sql.substr(7, 4) == "hash") {
                isHash = true;
                restStart = 18; // after "create hash index "
            }
            // Check for "create unique index"
            if (sql.substr(7, 6) == "unique") {
                restStart = 20; // after "create unique index "
            }
            // Check for CONCURRENTLY keyword after "index"
            string afterCreate = trim(sql.substr(restStart));
            if (afterCreate.size() >= 13 && afterCreate.substr(0, 13) == "concurrently ") {
                isConcurrently = true;
                restStart += 13;
                afterCreate = trim(sql.substr(restStart));
            }
            string rest = afterCreate;
            size_t onPos = rest.find(" on ");
            if (onPos == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string idxName = trim(rest.substr(0, onPos));
            string afterOn = trim(rest.substr(onPos + 4));
            size_t lp = afterOn.find('(');
            size_t rp = afterOn.find(')');
            if (lp == string::npos || rp == string::npos || rp <= lp + 1) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tnameOrig = trim(afterOn.substr(0, lp));
            string tname = resolveTableName(s, tnameOrig);
            string colsStr = trim(afterOn.substr(lp + 1, rp - lp - 1));
            // Check for INCLUDE and WHERE clauses in the full afterOn (after the closing paren)
            vector<string> includeCols;
            string whereCondition;
            string afterParens = trim(afterOn.substr(rp + 1));
            // First extract WHERE if present (everything after WHERE)
            size_t wherePos = afterParens.find("where ");
            if (wherePos == string::npos) wherePos = afterParens.find("WHERE ");
            string beforeWhere = afterParens;
            if (wherePos != string::npos) {
                beforeWhere = trim(afterParens.substr(0, wherePos));
                whereCondition = trim(afterParens.substr(wherePos + 6));
            }
            size_t includePos = beforeWhere.find("include ");
            if (includePos == string::npos) includePos = beforeWhere.find("include(");
            if (includePos != string::npos) {
                string incPart = trim(beforeWhere.substr(includePos + 7)); // skip "include"
                if (!incPart.empty() && incPart.front() == '(') incPart = incPart.substr(1);
                if (!incPart.empty() && incPart.back() == ')') incPart.pop_back();
                stringstream icss(incPart);
                string ic;
                while (getline(icss, ic, ',')) {
                    string tc = trim(ic);
                    if (!tc.empty()) includeCols.push_back(tc);
                }
            }
            string keyColsStr = colsStr;
            // Parse comma-separated column list, supporting ASC/DESC and expressions
            vector<string> colnames;
            vector<bool> colAsc;
            vector<string> expressions; // expression string for each column (empty = regular column)
            size_t cpos = 0;
            while (cpos < keyColsStr.size()) {
                size_t comma = keyColsStr.find(',', cpos);
                string c = trim(keyColsStr.substr(cpos, comma - cpos));
                if (!c.empty()) {
                    bool asc = true;
                    string expr;
                    // Check for expression like UPPER(col) or LOWER(col)
                    size_t exprLp = c.find('(');
                    size_t exprRp = c.find(')');
                    if (exprLp != string::npos && exprRp != string::npos && exprRp > exprLp + 1) {
                        // This is an expression index
                        expr = c;
                        // Extract inner column for validation
                        string inner = trim(c.substr(exprLp + 1, exprRp - exprLp - 1));
                        // Check for ASC/DESC after the expression
                        string afterExpr = trim(c.substr(exprRp + 1));
                        if (!afterExpr.empty()) {
                            transform(afterExpr.begin(), afterExpr.end(), afterExpr.begin(), ::tolower);
                            if (afterExpr == "desc") asc = false;
                        }
                        c = inner; // use inner column for validation
                    } else {
                        size_t sp = c.find(' ');
                        if (sp != string::npos) {
                            string dir = trim(c.substr(sp + 1));
                            transform(dir.begin(), dir.end(), dir.begin(), ::tolower);
                            if (dir == "desc") asc = false;
                            c = trim(c.substr(0, sp));
                        }
                    }
                    colnames.push_back(c);
                    colAsc.push_back(asc);
                    expressions.push_back(expr);
                }
                if (comma == string::npos) break;
                cpos = comma + 1;
            }
            if (colnames.empty()) {
                cout << "SQL syntax error: no columns specified" << endl;
                return true;
            }
            DBStatus res;
            if (colnames.size() == 1) {
                if (isHash) {
                    res = g_engine.createHashIndex(s.currentDB, tname, colnames[0], isConcurrently);
                } else {
                    res = g_engine.createIndex(s.currentDB, tname, colnames[0], colAsc[0], includeCols, whereCondition, expressions[0], isConcurrently);
                }
            } else {
                if (isHash) {
                    cout << "Hash index only supports single-column" << endl;
                    return true;
                }
                // Expression indexes not supported for composite (simplified)
                res = g_engine.createCompositeIndex(s.currentDB, tname, colnames, idxName, includeCols, whereCondition, isConcurrently);
            }
            if (res != DBStatus::OK) {
                cout << "Create index failed" << endl;
                return true;
            }
            cout << "Index created" << (isConcurrently ? " concurrently" : "") << endl;
            return false;
        }

        if (sql.substr(7, 9) == "fulltext ") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            // create fulltext index idxname on tablename(column)
            string rest = trim(sql.substr(17));
            size_t onPos = rest.find(" on ");
            if (onPos == string::npos) {
                cout << "SQL syntax error: CREATE FULLTEXT INDEX idx ON t(col)" << endl;
                return true;
            }
            string idxName = trim(rest.substr(0, onPos));
            string afterOn = trim(rest.substr(onPos + 4));
            size_t lp = afterOn.find('(');
            size_t rp = afterOn.find(')');
            if (lp == string::npos || rp == string::npos || rp <= lp + 1) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = resolveTableName(s, trim(afterOn.substr(0, lp)));
            string colname = trim(afterOn.substr(lp + 1, rp - lp - 1));
            auto res = g_engine.createFullTextIndex(s.currentDB, tname, colname);
            if (res != DBStatus::OK) {
                cout << "Create fulltext index failed" << endl;
                return true;
            }
            cout << "Fulltext index created" << endl;
            return false;
        }

        if (sql.substr(7, 4) == "gin " || sql.substr(7, 5) == "gist " || sql.substr(7, 5) == "brin " || sql.substr(7, 7) == "spgist ") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            bool isGin = (sql.substr(7, 4) == "gin ");
            bool isGist = (sql.substr(7, 5) == "gist ");
            bool isSpgist = (sql.substr(7, 7) == "spgist ");
            size_t restStart = isGin ? 11 : (isSpgist ? 14 : 12); // after "create gin " or "create gist " / "create brin " / "create spgist "
            string rest = trim(sql.substr(restStart));
            // Remove "index " prefix if present
            if (rest.size() >= 6 && rest.substr(0, 6) == "index ") {
                rest = trim(rest.substr(6));
            }
            size_t onPos = rest.find(" on ");
            if (onPos == string::npos) {
                cout << "SQL syntax error: CREATE GIN/GiST/BRIN INDEX idx ON t(col)" << endl;
                return true;
            }
            string idxName = trim(rest.substr(0, onPos));
            string afterOn = trim(rest.substr(onPos + 4));
            size_t lp = afterOn.find('(');
            size_t rp = afterOn.find(')');
            if (lp == string::npos || rp == string::npos || rp <= lp + 1) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = resolveTableName(s, trim(afterOn.substr(0, lp)));
            string colname = trim(afterOn.substr(lp + 1, rp - lp - 1));
            DBStatus res;
            if (isGin) {
                res = g_engine.createGinIndex(s.currentDB, tname, colname);
            } else if (isGist) {
                res = g_engine.createGiSTIndex(s.currentDB, tname, colname);
            } else if (isSpgist) {
                res = g_engine.createSPGiSTIndex(s.currentDB, tname, colname);
            } else {
                res = g_engine.createBrinIndex(s.currentDB, tname, colname);
            }
            if (res != DBStatus::OK) {
                cout << "Create index failed" << endl;
                return true;
            }
            cout << "Index created" << endl;
            return false;
        }

        if (sql.substr(7, 8) == "sequence") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            string rest = trim(sql.substr(16));
            string seqName = rest;
            int64_t start = 1, increment = 1;
            size_t startPos = rest.find("start with");
            if (startPos != string::npos) {
                seqName = trim(rest.substr(0, startPos));
                string numStr = trim(rest.substr(startPos + 10));
                try { start = std::stoll(numStr); } catch (...) {}
            }
            size_t incPos = rest.find("increment by");
            if (incPos != string::npos) {
                if (startPos == string::npos) seqName = trim(rest.substr(0, incPos));
                string numStr = trim(rest.substr(incPos + 12));
                try { increment = std::stoll(numStr); } catch (...) {}
            }
            auto res = g_engine.createSequence(s.currentDB, seqName, start, increment);
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Sequence " << seqName << " already exists" << endl;
                return true;
            }
            cout << "Sequence " << seqName << " created" << endl;
            return false;
        }

        bool orReplace = false;
        size_t createOffset = 7;
        if (sql.substr(7, 10) == "or replace") {
            orReplace = true;
            createOffset = 18;
        }

        if (sql.substr(createOffset, 12) == "materialized") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            // create [or replace] materialized view viewname as select ...
            string rest = trim(sql.substr(createOffset + 18)); // after "create materialized view "
            size_t asPos = rest.find(" as ");
            if (asPos == string::npos) {
                cout << "SQL syntax error: MATERIALIZED VIEW requires AS clause" << endl;
                return true;
            }
            string viewname = trim(rest.substr(0, asPos));
            string viewSql = trim(rest.substr(asPos + 4));
            // Check if view already exists
            if (!orReplace && (g_engine.viewExists(s.currentDB, viewname) ||
                g_engine.isMaterializedView(s.currentDB, viewname))) {
                cout << "View " << viewname << " already exists" << endl;
                return true;
            }
            if (orReplace && g_engine.isMaterializedView(s.currentDB, viewname)) {
                g_engine.dropMaterializedView(s.currentDB, viewname);
            } else if (orReplace && g_engine.viewExists(s.currentDB, viewname)) {
                g_engine.dropView(s.currentDB, viewname);
            }
            // Execute the query to get column names and data
            // We need to extract column names from the SELECT
            // For simplicity, execute and use the first row to infer columns
            string lsql = toLower(viewSql);
            if (lsql.substr(0, 6) != "select") {
                cout << "Materialized view requires SELECT query" << endl;
                return true;
            }
            // Extract column names from SELECT clause
            size_t fromPos = lsql.find(" from ");
            if (fromPos == string::npos) {
                cout << "SQL syntax error: SELECT requires FROM" << endl;
                return true;
            }
            string colsPart = trim(viewSql.substr(6, fromPos - 6));
            vector<string> colNames;
            if (colsPart == "*") {
                // Need to infer from table - this is complex, skip for now
                cout << "Materialized view with SELECT * not supported, use explicit columns" << endl;
                return true;
            }
            // Parse comma-separated column names (support simple aliases)
            size_t cp = 0;
            while (cp < colsPart.size()) {
                size_t comma = colsPart.find(',', cp);
                string c = trim(colsPart.substr(cp, comma - cp));
                // Extract column name (handle alias: "col AS alias" or "col alias")
                size_t aliasPos = toLower(c).find(" as ");
                if (aliasPos != string::npos) {
                    c = trim(c.substr(aliasPos + 4));
                } else {
                    // Check for simple alias without AS
                    size_t sp = c.find(' ');
                    if (sp != string::npos) {
                        string before = trim(c.substr(0, sp));
                        string after = trim(c.substr(sp + 1));
                        // If after is not an operator, it's an alias
                        if (after != "" && after.find_first_of("+-*/=<>") == string::npos) {
                            c = after;
                        } else {
                            c = before;
                        }
                    }
                }
                // Remove table prefix if any (t.col -> col)
                size_t dotPos = c.find('.');
                if (dotPos != string::npos) c = c.substr(dotPos + 1);
                colNames.push_back(c);
                if (comma == string::npos) break;
                cp = comma + 1;
            }
            if (colNames.empty()) {
                cout << "Could not extract column names from SELECT" << endl;
                return true;
            }
            // Execute the query to get results
            string inner = trim(viewSql.substr(fromPos + 5)); // after " from "
            size_t wPos = toLower(inner).find(" where ");
            size_t oPos = toLower(inner).find(" order by ");
            size_t lPos = toLower(inner).find(" limit ");
            string tname = trim(inner.substr(0,
                min(wPos != string::npos ? wPos : inner.size(),
                    min(oPos != string::npos ? oPos : inner.size(),
                        lPos != string::npos ? lPos : inner.size()))));
            // Resolve table name (handle temp tables, etc.)
            tname = resolveTableName(s, tname);
            vector<string> conds;
            if (wPos != string::npos) {
                size_t condEnd = min(oPos != string::npos ? oPos : inner.size(),
                                    lPos != string::npos ? lPos : inner.size());
                string condStr = normalizeConditionStr(trim(inner.substr(wPos + 6, condEnd - wPos - 6)));
                if (!condStr.empty()) {
                    vector<string> rawConds = splitConds(condStr);
                    for (auto& c : rawConds) {
                        string mc = modifyLogic(c);
                        if (!mc.empty()) conds.push_back(mc);
                    }
                }
            }
            auto results = g_engine.query(s.currentDB, tname, conds, {}, {}, false, false);
            // Create backing table with VARCHAR columns
            string backingTable = dbms::StorageEngine::materializedViewPrefix(viewname);
            dbms::TableSchema tbl;
            tbl.tablename = backingTable;
            for (const auto& cname : colNames) {
                dbms::Column col;
                col.dataName = cname;
                col.dataType = "varchar";
                col.isVariableLength = true;
                col.dsize = 255;
                col.isNull = true;
                tbl.append(col);
            }
            // Drop existing backing table if any
            if (g_engine.tableExists(s.currentDB, backingTable)) {
                g_engine.dropTable(s.currentDB, backingTable);
            }
            auto res = g_engine.createTable(s.currentDB, tbl);
            if (res != DBStatus::OK) {
                cout << "Failed to create materialized view backing table" << endl;
                return true;
            }
            // Insert query results
            for (const auto& row : results) {
                stringstream ss(row);
                map<string, string> values;
                for (const auto& cname : colNames) {
                    string val;
                    ss >> val;
                    values[cname] = val;
                }
                g_engine.insert(s.currentDB, backingTable, values);
            }
            // Save SQL to .mview file
            auto mviewDir = g_engine.viewsDir(s.currentDB);
            if (!std::filesystem::exists(mviewDir)) {
                std::filesystem::create_directories(mviewDir);
            }
            auto mviewPath = mviewDir / (viewname + ".mview");
            {
                ofstream ofs(mviewPath);
                if (!ofs) {
                    cout << "Failed to save materialized view metadata" << endl;
                    return true;
                }
                ofs << viewSql;
            }
            cout << "Materialized view " << viewname << " created (" << results.size() << " rows)" << endl;
            return false;
        }

        if (sql.substr(7, 6) == "schema") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            string schemaname = trim(sql.substr(14));
            auto res = g_engine.createSchema(s.currentDB, schemaname);
            if (res != DBStatus::OK) {
                cout << "Create schema failed" << endl;
                return true;
            }
            cout << "Schema " << schemaname << " created" << endl;
            return false;
        }
    }

    if (sql.substr(0, 5) == "alter") {
        if (!checkAdmin(s)) return true;
        vector<string> tokens = tokenize(sql.substr(5));
        if (tokens.size() < 2) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        if (tokens[0] == "collation") {
            return handleAlterCollation(sql, s);
        }
        if (tokens[0] == "conversion") {
            return handleAlterConversion(sql, s);
        }
        if (tokens[0] == "tablespace") {
            return handleAlterTablespace(sql, s);
        }
        if (tokens[0] == "statistics") {
            return handleAlterStatistics(sql, s);
        }
        if (tokens[0] == "sequence") {
            return handleAlterSequence(sql, s);
        }
        if (tokens[0] == "role") {
            return handleAlterRole(sql, s);
        }
        if (tokens[0] == "group") {
            return handleAlterGroup(sql, s);
        }
        if (tokens[0] == "database") {
            return handleAlterDatabase(sql, s);
        }
        if (tokens[0] == "domain") {
            return handleAlterDomain(sql, s);
        }
        if (tokens[0] == "type") {
            return handleAlterType(sql, s);
        }
        if (tokens[0] == "policy") {
            return handleAlterPolicy(sql, s);
        }
        {
            string rest = trim(sql.substr(5));
            string compatKind, compatPhrase;
            if (consumeCompatPrefix(rest, compatAlterDropPrefixes(), compatKind, compatPhrase)) {
                return handleAlterCompatObject(sql, s);
            }
        }
        if (tokens[0] == "default" && tokens.size() >= 3 && tokens[1] == "privileges") {
            // ALTER DEFAULT PRIVILEGES [FOR ROLE owner] [IN SCHEMA schema] GRANT priv ON obj_type TO grantee
            string rest = trim(sql.substr(5 + 17)); // after "alter default privileges"
            string owner = s.username;
            string schema = "public";
            size_t forRolePos = rest.find("for role ");
            if (forRolePos != string::npos) {
                size_t nextSpace = rest.find(' ', forRolePos + 9);
                if (nextSpace == string::npos) nextSpace = rest.size();
                owner = trim(rest.substr(forRolePos + 9, nextSpace - forRolePos - 9));
                rest = trim(rest.substr(0, forRolePos) + rest.substr(nextSpace));
            }
            size_t inSchemaPos = rest.find("in schema ");
            if (inSchemaPos != string::npos) {
                size_t nextSpace = rest.find(' ', inSchemaPos + 10);
                if (nextSpace == string::npos) nextSpace = rest.size();
                schema = trim(rest.substr(inSchemaPos + 10, nextSpace - inSchemaPos - 10));
                rest = trim(rest.substr(0, inSchemaPos) + rest.substr(nextSpace));
            }
            size_t grantPos = rest.find("grant ");
            size_t onPos = rest.find(" on ");
            size_t toPos = rest.find(" to ");
            if (grantPos == string::npos || onPos == string::npos || toPos == string::npos) {
                cout << "SQL syntax error: ALTER DEFAULT PRIVILEGES [FOR ROLE owner] [IN SCHEMA schema] GRANT priv ON obj_type TO grantee" << endl;
                return true;
            }
            string privStr = trim(rest.substr(grantPos + 6, onPos - grantPos - 6));
            string objType = trim(rest.substr(onPos + 4, toPos - onPos - 4));
            string grantee = trim(rest.substr(toPos + 4));
            if (privStr.empty() || objType.empty() || grantee.empty()) {
                cout << "SQL syntax error: missing privilege, object type, or grantee" << endl;
                return true;
            }
            g_engine.addDefaultPrivilege(s.currentDB, owner, schema, objType, privStr, grantee);
            cout << "Default privilege set: " << owner << " in schema " << schema
                 << " grants " << privStr << " on " << objType << " to " << grantee << endl;
            return false;
        }
        if (tokens[0] == "user") {
            if (tokens.size() < 3) {
                cout << "SQL syntax error: ALTER USER username password newpassword" << endl;
                return true;
            }
            string uname = tokens[1];
            string newPw = tokens[2];
            {
                string rawRest = rawSql;
                size_t p = 0;
                while (p < rawRest.size() && isspace(static_cast<unsigned char>(rawRest[p]))) ++p;
                rawRest = rawRest.substr(p);
                string lraw = toLower(rawRest);
                size_t auPos = lraw.find("alter user");
                if (auPos != string::npos) rawRest = rawRest.substr(auPos + 10);
                rawRest = trim(rawRest);
                vector<string> rawParts;
                stringstream rss(rawRest);
                string rp;
                while (rss >> rp) rawParts.push_back(rp);
                if (rawParts.size() >= 2) newPw = rawParts[1];
            }
            ifstream infile("user.dat");
            vector<user> users;
            bool found = false;
            if (infile) {
                user temp;
                while (infile >> temp.username >> temp.password >> temp.permission) {
                    if (temp.username == uname) {
                        found = true;
                        if (isMd5Hash(temp.password))
                            temp.password = md5(newPw);
                        else
                            temp.password = sha256(newPw);
                    }
                    users.push_back(temp);
                }
            }
            if (!found) {
                cout << "User " << uname << " not exist" << endl;
                return true;
            }
            ofstream outfile("user.dat");
            for (size_t i = 0; i < users.size(); ++i) {
                if (i > 0) outfile << '\n';
                outfile << users[i].username << " " << users[i].password << " " << users[i].permission;
            }
            outfile << endl;
            cout << "User password updated" << endl;
            return false;
        }
        if (tokens[0] == "schema") {
            if (!checkDB(s)) return true;
            if (tokens.size() < 5 || tokens[2] != "rename" || tokens[3] != "to") {
                cout << "SQL syntax error: ALTER SCHEMA name RENAME TO newname" << endl;
                return true;
            }
            string oldName = tokens[1];
            string newName = tokens[4];
            auto res = g_engine.renameSchema(s.currentDB, oldName, newName);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Schema " << oldName << " not exist" << endl;
                return true;
            }
            if (res != DBStatus::OK) {
                cout << "Rename schema failed" << endl;
                return true;
            }
            cout << "Schema " << oldName << " renamed to " << newName << endl;
            return false;
        }
        if (tokens[0] == "view") {
            // ALTER VIEW viewname RENAME TO newname
            // ALTER VIEW viewname SET SCHEMA dbname
            if (!checkDB(s)) return true;
            if (tokens.size() < 5) {
                cout << "SQL syntax error: ALTER VIEW name RENAME TO newname / SET SCHEMA dbname" << endl;
                return true;
            }
            string vname = tokens[1];
            // Resolve schema-qualified view name
            size_t dotPos = vname.find('.');
            string actualVname = (dotPos != string::npos) ? vname.substr(dotPos + 1) : vname;
            if (tokens[2] == "rename" && tokens[3] == "to") {
                string newName = tokens[4];
                if (!g_engine.viewExists(s.currentDB, actualVname)) {
                    cout << "View " << actualVname << " not exist" << endl;
                    return true;
                }
                if (g_engine.viewExists(s.currentDB, newName)) {
                    cout << "View " << newName << " already exists" << endl;
                    return true;
                }
                // Read view SQL, drop old, create new with new name
                string viewSql = g_engine.getViewSQL(s.currentDB, actualVname);
                string checkOpt = g_engine.getViewCheckOption(s.currentDB, actualVname);
                g_engine.dropView(s.currentDB, actualVname);
                auto res = g_engine.createView(s.currentDB, newName, viewSql);
                if (res != DBStatus::OK) {
                    // Restore old view
                    g_engine.createView(s.currentDB, actualVname, viewSql);
                    cout << "Rename view failed" << endl;
                    return true;
                }
                cout << "View renamed" << endl;
                return false;
            }
            if (tokens[2] == "set" && tokens[3] == "schema") {
                string targetDb = tokens[4];
                if (!g_engine.databaseExists(targetDb)) {
                    cout << "Database " << targetDb << " not exist" << endl;
                    return true;
                }
                string viewSql = g_engine.getViewSQL(s.currentDB, actualVname);
                g_engine.dropView(s.currentDB, actualVname);
                auto res = g_engine.createView(targetDb, actualVname, viewSql);
                if (res != DBStatus::OK) {
                    g_engine.createView(s.currentDB, actualVname, viewSql);
                    cout << "Set schema failed" << endl;
                    return true;
                }
                cout << "View schema changed" << endl;
                return false;
            }
            cout << "SQL syntax error: ALTER VIEW name RENAME TO newname / SET SCHEMA dbname" << endl;
            return true;
        }
        if (!checkDB(s)) return true;
        if (tokens.size() < 4 || tokens[0] != "table") {
            cout << "SQL syntax error" << endl;
            return true;
        }
        if (g_engine.inTransaction()) {
            g_engine.commitTransaction();
            cout << "Note: DDL caused implicit commit of open transaction" << endl;
        }
        string tnameOrig = tokens[1];
        string tname = resolveTableName(s, tnameOrig);
        string op = tokens[2];
        if (op == "validate" && tokens.size() >= 5 && tokens[3] == "constraint") {
            string constrName = tokens[4];
            if (!g_engine.tableExists(s.currentDB, tname)) {
                cout << "Table not found" << endl;
                return true;
            }
            if (!knownConstraintExists(s.currentDB, tname, constrName)) {
                cout << "Constraint not found" << endl;
                return true;
            }
            if (!validateConstraintCompat(s.currentDB, tname, constrName, sql, s.username)) {
                cout << "Constraint metadata save failed" << endl;
                return true;
            }
            cout << "Constraint " << constrName << " validated" << endl;
            return false;
        }
        if (op == "alter" && tokens.size() >= 5 && tokens[3] == "constraint") {
            string constrName = tokens[4];
            if (!g_engine.tableExists(s.currentDB, tname)) {
                cout << "Table not found" << endl;
                return true;
            }
            if (!knownConstraintExists(s.currentDB, tname, constrName)) {
                cout << "Constraint not found" << endl;
                return true;
            }
            if (!alterConstraintOptionsCompat(s.currentDB, tname, constrName, sql, s.username)) {
                cout << "Constraint metadata save failed" << endl;
                return true;
            }
            cout << "Constraint " << constrName << " options updated" << endl;
            return false;
        }
        if (op == "add" && tokens.size() >= 4 && tokens[3] == "column") {
            // alter table tname add column colname:type [0|1]
            if (tokens.size() < 5) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string colDef = tokens[4];
            size_t colon = colDef.find(':');
            if (colon == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string cname = colDef.substr(0, colon);
            string ctype = colDef.substr(colon + 1);
            size_t sp = ctype.find(' ');
            string typeName = (sp == string::npos) ? ctype : trim(ctype.substr(0, sp));
            string nullFlag = (sp == string::npos) ? "" : trim(ctype.substr(sp + 1));
            bool isNull = (nullFlag != "0");

            Column col;
            if (typeName.substr(0, 8) == "interval") {
                col = makeIntervalColumn(cname, isNull);
            } else if (isPgStringBackedType(typeName)) {
                col = makePgStringBackedColumn(cname, typeName, isNull, false);
            } else if (typeName.substr(0, 3) == "int") {
                col = makeIntColumn(cname, isNull, 2);
            } else if (typeName.substr(0, 4) == "tiny") {
                col = makeIntColumn(cname, isNull, 1);
            } else if (typeName.substr(0, 8) == "smallint") {
                col = makeIntColumn(cname, isNull, 0);
            } else if (typeName.substr(0, 6) == "bigint") {
                col = makeIntColumn(cname, isNull, 3);
            } else if (typeName.substr(0, 4) == "long") {
                col = makeIntColumn(cname, isNull, 3);
            } else if (typeName.substr(0, 4) == "bool") {
                col = makeBooleanColumn(cname, isNull);
            } else if (typeName.substr(0, 3) == "bit") {
                col = makePgStringBackedColumn(cname, typeName.substr(0, 11) == "bit varying" ? "bit varying" : "bit", isNull, false);
            } else if (typeName.substr(0, 4) == "uuid") {
                col = makeUuidColumn(cname, isNull);
            } else if (typeName.substr(0, 4) == "date") {
                col = makeDateColumn(cname, isNull);
            } else if (typeName.substr(0, 12) == "timestamptz") {
                col = makeTimestamptzColumn(cname, isNull);
            } else if (typeName.substr(0, 9) == "timestamp") {
                col = makeTimestampColumn(cname, isNull);
            } else if (typeName.substr(0, 4) == "time") {
                col = makeTimeColumn(cname, isNull);
            } else if (typeName.substr(0, 8) == "datetime") {
                col = makeDateTimeColumn(cname, isNull);
            } else if (typeName.substr(0, 4) == "char") {
                size_t len = 0;
                for (size_t i = 4; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
                if (len == 0) len = 1;
                col = makeStringColumn(cname, isNull, len);
            } else if (typeName.substr(0, 7) == "varchar") {
                size_t len = 0;
                size_t start = 7;
                if (start < typeName.size() && typeName[start] == '(') ++start;
                for (size_t i = start; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
                if (len == 0) len = 1;
                col = makeVarCharColumn(cname, isNull, len);
            } else if (typeName.substr(0, 7) == "nvarchar") {
                size_t len = 0;
                size_t start = 7;
                if (start < typeName.size() && typeName[start] == '(') ++start;
                for (size_t i = start; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
                if (len == 0) len = 1;
                col = makeNVarCharColumn(cname, isNull, len);
            } else if (typeName.substr(0, 5) == "nchar") {
                size_t len = 0;
                size_t start = 5;
                if (start < typeName.size() && typeName[start] == '(') ++start;
                for (size_t i = start; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
                if (len == 0) len = 1;
                col = makeNCharColumn(cname, isNull, len);
            } else if (typeName.substr(0, 7) == "binary(") {
                size_t lp = typeName.find('(');
                size_t rp = typeName.find(')');
                size_t len = 0;
                if (lp != string::npos && rp != string::npos && rp > lp) {
                    try { len = stoul(typeName.substr(lp + 1, rp - lp - 1)); } catch (...) {}
                }
                if (len == 0) len = 1;
                col = makeBinaryColumn(cname, isNull, len);
            } else if (typeName.substr(0, 9) == "varbinary") {
                size_t lp = typeName.find('(');
                size_t rp = typeName.find(')');
                size_t len = 0;
                if (lp != string::npos && rp != string::npos && rp > lp) {
                    try { len = stoul(typeName.substr(lp + 1, rp - lp - 1)); } catch (...) {}
                }
                if (len == 0) len = 1;
                col = makeVarBinaryColumn(cname, isNull, len);
            } else if (typeName.substr(0, 4) == "blob") {
                col = makeBlobColumn(cname, isNull);
            } else if (typeName.substr(0, 4) == "text") {
                col = makeTextColumn(cname, isNull);
            } else if (typeName.substr(0, 5) == "jsonb") {
                col = makeJsonbColumn(cname, isNull);
            } else if (typeName.substr(0, 4) == "json") {
                col = makeJsonColumn(cname, isNull);
            } else if (typeName.substr(0, 3) == "xml") {
                col = makeXmlColumn(cname, isNull);
            } else if (typeName.substr(0, 6) == "pg_lsn") {
                col = makePgLsnColumn(cname, isNull);
            } else if (typeName.substr(0, 9) == "int4range") {
                col = makeInt4RangeColumn(cname, isNull);
            } else if (typeName.substr(0, 9) == "int8range") {
                col = makeInt8RangeColumn(cname, isNull);
            } else if (typeName.substr(0, 8) == "numrange") {
                col = makeNumRangeColumn(cname, isNull);
            } else if (typeName.substr(0, 7) == "tsrange") {
                col = makeTsRangeColumn(cname, isNull);
            } else if (typeName.substr(0, 8) == "tstzrang") {
                col = makeTstzRangeColumn(cname, isNull);
            } else if (typeName.substr(0, 9) == "daterange") {
                col = makeDateRangeColumn(cname, isNull);
            } else if (typeName.substr(0, 8) == "tsvector") {
                col = makeTsVectorColumn(cname, isNull);
            } else if (typeName.substr(0, 7) == "tsquery") {
                col = makeTsQueryColumn(cname, isNull);
            } else if (typeName.substr(0, 5) == "float") {
                col = makeFloatColumn(cname, isNull);
            } else if (typeName.substr(0, 6) == "double" || typeName.substr(0, 5) == "money") {
                col = makeDoubleColumn(cname, isNull);
            } else if (typeName.substr(0, 7) == "decimal") {
                col = makeDecimalColumn(cname, isNull, 10, 0);
            } else {
                cout << "Unknown data type" << endl;
                return true;
            }
            auto res = g_engine.alterTableAddColumn(s.currentDB, tname, col);
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Column already exists" << endl;
                return true;
            }
            cout << "Column added" << endl;
            return false;
        }
        if (op == "drop" && tokens.size() >= 4 && tokens[3] == "column") {
            if (tokens.size() < 5) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            auto res = g_engine.alterTableDropColumn(s.currentDB, tname, tokens[4]);
            if (res == DBStatus::INVALID_VALUE) {
                cout << "Column not found" << endl;
                return true;
            }
            cout << "Column dropped" << endl;
            return false;
        }
        if (op == "rename") {
            if (tokens.size() >= 5 && tokens[3] == "column" && tokens[5] == "to") {
                // alter table tname rename column old_name to new_name
                string oldName = tokens[4];
                string newName = tokens[6];
                auto res = g_engine.alterTableRenameColumn(s.currentDB, tname, oldName, newName);
                if (res == DBStatus::INVALID_VALUE) {
                    cout << "Column not found" << endl;
                    return true;
                }
                if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                    cout << "Column name already exists" << endl;
                    return true;
                }
                cout << "Column renamed" << endl;
                return false;
            }
            if (tokens.size() >= 4 && tokens[3] == "to") {
                // alter table tname rename to new_table_name
                string newTname = tokens[4];
                auto res = g_engine.alterTableRenameTable(s.currentDB, tname, newTname);
                if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                    cout << "Table name already exists" << endl;
                    return true;
                }
                cout << "Table renamed" << endl;
                return false;
            }
            // PostgreSQL shorthand: alter table tname rename old_name to new_name (for column)
            if (tokens.size() >= 5 && tokens[4] == "to") {
                string oldName = tokens[3];
                string newName = tokens[5];
                auto res = g_engine.alterTableRenameColumn(s.currentDB, tname, oldName, newName);
                if (res == DBStatus::INVALID_VALUE) {
                    cout << "Column not found" << endl;
                    return true;
                }
                if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                    cout << "Column name already exists" << endl;
                    return true;
                }
                cout << "Column renamed" << endl;
                return false;
            }
        }
        if (op == "alter" && tokens.size() >= 5 && tokens[3] == "column") {
            string cname = tokens[4];
            if (tokens.size() >= 7 && tokens[5] == "set" && tokens[6] == "default") {
                // alter table t alter column c set default value
                string defVal = (tokens.size() >= 8) ? tokens[7] : "";
                // Reconstruct multi-token default from raw SQL if needed
                if (tokens.size() > 8) {
                    for (size_t i = 8; i < tokens.size(); ++i) {
                        defVal += " " + tokens[i];
                    }
                }
                auto res = g_engine.alterTableSetDefault(s.currentDB, tname, cname, defVal);
                if (res == DBStatus::INVALID_VALUE) {
                    cout << "Column not found" << endl;
                    return true;
                }
                cout << "Default value set" << endl;
                return false;
            }
            if (tokens.size() >= 7 && tokens[5] == "drop" && tokens[6] == "default") {
                auto res = g_engine.alterTableDropDefault(s.currentDB, tname, cname);
                if (res == DBStatus::INVALID_VALUE) {
                    cout << "Column not found" << endl;
                    return true;
                }
                cout << "Default value dropped" << endl;
                return false;
            }
            if (tokens.size() >= 8 && tokens[5] == "set" && tokens[6] == "not" && tokens[7] == "null") {
                auto res = g_engine.alterTableSetNotNull(s.currentDB, tname, cname);
                if (res == DBStatus::INVALID_VALUE) {
                    cout << "Column not found" << endl;
                    return true;
                }
                cout << "Not null constraint set" << endl;
                return false;
            }
            if (tokens.size() >= 8 && tokens[5] == "drop" && tokens[6] == "not" && tokens[7] == "null") {
                auto res = g_engine.alterTableDropNotNull(s.currentDB, tname, cname);
                if (res == DBStatus::INVALID_VALUE) {
                    cout << "Column not found" << endl;
                    return true;
                }
                cout << "Not null constraint dropped" << endl;
                return false;
            }
        }
        if (op == "add" && tokens.size() >= 5 && tokens[3] == "constraint") {
            string constrName = tokens[4];
            if (!g_engine.tableExists(s.currentDB, tname)) {
                cout << "Table not found" << endl;
                return true;
            }
            if (constraintCompatExists(s.currentDB, tname, constrName)) {
                cout << "Constraint name already exists" << endl;
                return true;
            }
            size_t excludePos = findTopLevelKeyword(sql, "exclude");
            if (excludePos != string::npos) {
                ConstraintCompatFlags flags = parseConstraintFlags(sql, "exclusion");
                if (!recordExclusionConstraintCompat(s.currentDB, tname, constrName, sql, s.username, flags)) {
                    cout << "Constraint metadata save failed" << endl;
                    return true;
                }
                cout << "Exclusion constraint added" << endl;
                return false;
            }
            // CHECK constraint
            size_t checkPos = findTopLevelKeyword(sql, "check");
            if (checkPos != string::npos) {
                size_t lp = sql.find('(', checkPos);
                size_t rp = sql.find(')', lp);
                if (lp != string::npos && rp != string::npos) {
                    string expr = trim(sql.substr(lp + 1, rp - lp - 1));
                    auto res = g_engine.alterTableAddCheckConstraint(s.currentDB, tname, constrName, expr);
                    if (res == DBStatus::TABLE_NOT_FOUND) {
                        cout << "Table not found" << endl;
                        return true;
                    }
                    if (res == DBStatus::INVALID_VALUE) {
                        cout << "Invalid check constraint: no referenced column found" << endl;
                        return true;
                    }
                    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                        cout << "Constraint name already exists" << endl;
                        return true;
                    }
                    ConstraintCompatFlags flags = parseConstraintFlags(sql, "check");
                    if (!recordConstraintCompat(s.currentDB, tname, constrName, "check", sql, s.username, flags)) {
                        cout << "Constraint metadata save failed" << endl;
                        return true;
                    }
                    cout << "Check constraint added" << endl;
                    return false;
                }
            }
            // UNIQUE constraint
            size_t uniquePos = findTopLevelKeyword(sql, "unique");
            if (uniquePos != string::npos) {
                size_t lp = sql.find('(', uniquePos);
                size_t rp = sql.find(')', lp);
                if (lp != string::npos && rp != string::npos) {
                    string colsStr = trim(sql.substr(lp + 1, rp - lp - 1));
                    vector<string> cols;
                    stringstream css(colsStr);
                    string c;
                    while (getline(css, c, ',')) {
                        c = trim(c);
                        if (!c.empty()) cols.push_back(c);
                    }
                    auto res = g_engine.alterTableAddUniqueConstraint(s.currentDB, tname, constrName, cols);
                    if (res == DBStatus::TABLE_NOT_FOUND) {
                        cout << "Table not found" << endl;
                        return true;
                    }
                    if (res == DBStatus::INVALID_VALUE) {
                        cout << "Column not found" << endl;
                        return true;
                    }
                    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                        cout << "Constraint name already exists" << endl;
                        return true;
                    }
                    ConstraintCompatFlags flags = parseConstraintFlags(sql, "unique");
                    if (!recordConstraintCompat(s.currentDB, tname, constrName, "unique", sql, s.username, flags)) {
                        cout << "Constraint metadata save failed" << endl;
                        return true;
                    }
                    cout << "Unique constraint added" << endl;
                    return false;
                }
            }
            // FOREIGN KEY constraint
            size_t fkPos = findTopLevelKeyword(sql, "foreign key");
            if (fkPos != string::npos) {
                size_t lp1 = sql.find('(', fkPos);
                size_t rp1 = sql.find(')', lp1);
                size_t refPos = findTopLevelKeyword(sql, "references",
                                                     rp1 == string::npos ? fkPos : rp1 + 1);
                if (lp1 != string::npos && rp1 != string::npos && refPos != string::npos) {
                    string localColsStr = trim(sql.substr(lp1 + 1, rp1 - lp1 - 1));
                    vector<string> localCols;
                    stringstream lcss(localColsStr);
                    string c;
                    while (getline(lcss, c, ',')) {
                        c = trim(c);
                        if (!c.empty()) localCols.push_back(c);
                    }
                    string refRest = trim(sql.substr(refPos + 10));
                    size_t lp2 = refRest.find('(');
                    size_t rp2 = refRest.find(')', lp2);
                    string refTable = (lp2 != string::npos) ? trim(refRest.substr(0, lp2)) : refRest;
                    vector<string> refCols;
                    if (lp2 != string::npos && rp2 != string::npos) {
                        string refColsStr = trim(refRest.substr(lp2 + 1, rp2 - lp2 - 1));
                        stringstream rcss(refColsStr);
                        while (getline(rcss, c, ',')) {
                            c = trim(c);
                            if (!c.empty()) refCols.push_back(c);
                        }
                    }
                    string onDelete = "restrict";
                    string onUpdate = "restrict";
                    size_t odPos = sql.find("on delete");
                    if (odPos != string::npos) {
                        string odStr = trim(sql.substr(odPos + 9));
                        size_t sp = odStr.find(' ');
                        if (sp != string::npos) {
                            onDelete = trim(odStr.substr(0, sp));
                            if (onDelete == "set" && odStr.substr(sp + 1, 4) == "null") onDelete = "setnull";
                        } else {
                            onDelete = odStr;
                        }
                    }
                    size_t ouPos = sql.find("on update");
                    if (ouPos != string::npos) {
                        string ouStr = trim(sql.substr(ouPos + 9));
                        size_t sp = ouStr.find(' ');
                        if (sp != string::npos) {
                            onUpdate = trim(ouStr.substr(0, sp));
                            if (onUpdate == "set" && ouStr.substr(sp + 1, 4) == "null") onUpdate = "setnull";
                        } else {
                            onUpdate = ouStr;
                        }
                    }
                    auto res = g_engine.alterTableAddFKConstraint(s.currentDB, tname, constrName,
                                                                  localCols, refTable, refCols,
                                                                  onDelete, onUpdate);
                    if (res == DBStatus::INVALID_VALUE) {
                        cout << "Invalid foreign key definition" << endl;
                        return true;
                    }
                    if (res == DBStatus::TABLE_NOT_FOUND) {
                        cout << "Referenced table not found" << endl;
                        return true;
                    }
                    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                        cout << "Constraint name already exists" << endl;
                        return true;
                    }
                    ConstraintCompatFlags flags = parseConstraintFlags(sql, "foreign_key");
                    if (!recordConstraintCompat(s.currentDB, tname, constrName, "foreign_key", sql, s.username, flags)) {
                        cout << "Constraint metadata save failed" << endl;
                        return true;
                    }
                    cout << "Foreign key constraint added" << endl;
                    return false;
                }
            }
            cout << "SQL syntax error: unsupported constraint type" << endl;
            return true;
        }
        if (op == "drop" && tokens.size() >= 4 && tokens[3] == "constraint") {
            if (tokens.size() < 5) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string constrName = tokens[4];
            auto res = g_engine.alterTableDropConstraint(s.currentDB, tname, constrName);
            bool droppedMeta = removeConstraintCompat(s.currentDB, tname, constrName);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Table not found" << endl;
                return true;
            }
            if (res == DBStatus::INVALID_VALUE && !droppedMeta) {
                cout << "Constraint not found" << endl;
                return true;
            }
            cout << "Constraint dropped" << endl;
            return false;
        }
        // ALTER TABLE ... ENABLE/DISABLE/FORCE ROW LEVEL SECURITY
        if (sql.find("enable row level security") != string::npos ||
            sql.find("disable row level security") != string::npos ||
            sql.find("force row level security") != string::npos) {
            if (sql.find("disable") != string::npos) {
                auto res = g_engine.disableRowLevelSecurity(s.currentDB, tname);
                if (res == DBStatus::OK) {
                    cout << "Row level security disabled on " << tname << endl;
                } else {
                    cout << "Table not found" << endl;
                }
            } else {
                bool force = (sql.find("force") != string::npos);
                auto res = g_engine.enableRowLevelSecurity(s.currentDB, tname, force);
                if (res == DBStatus::OK) {
                    cout << "Row level security " << (force ? "forced" : "enabled") << " on " << tname << endl;
                } else {
                    cout << "Table not found" << endl;
                }
            }
            return false;
        }
        // ALTER TABLE ... ATTACH/DETACH PARTITION
        if (sql.find("attach partition") != string::npos) {
            // ALTER TABLE tname ATTACH PARTITION pname [FOR VALUES ...]
            // Hash partitions don't need FOR VALUES (just the partition name)
            size_t attachPos = sql.find("attach partition");
            string afterAttach = trim(sql.substr(attachPos + 16)); // after "attach partition"
            size_t sp = afterAttach.find(' ');
            string pname;
            string spec;
            if (sp == string::npos) {
                // No spec (hash partition attach)
                pname = afterAttach;
                spec = "";
            } else {
                pname = afterAttach.substr(0, sp);
                spec = trim(afterAttach.substr(sp + 1));
            }
            auto res = g_engine.attachPartition(s.currentDB, tname, pname, spec);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Table not found" << endl;
            } else if (res == DBStatus::INVALID_VALUE) {
                cout << "Table is not partitioned or invalid partition specification" << endl;
            } else if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Partition " << pname << " already exists" << endl;
            } else if (res == DBStatus::SYNTAX_ERROR) {
                cout << "SQL syntax error: invalid partition specification" << endl;
            } else {
                cout << "Partition " << pname << " attached to " << tname << endl;
            }
            return false;
        }
        if (sql.find("detach partition") != string::npos) {
            // ALTER TABLE tname DETACH PARTITION pname
            size_t detachPos = sql.find("detach partition");
            string afterDetach = trim(sql.substr(detachPos + 16)); // after "detach partition"
            string pname = afterDetach;
            auto res = g_engine.detachPartition(s.currentDB, tname, pname);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Table not found" << endl;
            } else if (res == DBStatus::INVALID_VALUE) {
                cout << "Partition " << pname << " not found or table is not partitioned" << endl;
            } else {
                cout << "Partition " << pname << " detached from " << tname << endl;
            }
            return false;
        }
        // ALTER TABLE ... ENABLE/DISABLE TRIGGER
        if (sql.find("enable trigger") != string::npos || sql.find("disable trigger") != string::npos) {
            bool enable = (sql.find("enable trigger") != string::npos);
            size_t trigPos = sql.find(" trigger");
            string trigName = trim(sql.substr(trigPos + 8));
            auto res = enable ? g_engine.enableTrigger(s.currentDB, trigName)
                              : g_engine.disableTrigger(s.currentDB, trigName);
            if (res == DBStatus::OK) {
                cout << "Trigger " << trigName << (enable ? " enabled" : " disabled") << endl;
            } else {
                cout << "Trigger not found" << endl;
            }
            return false;
        }
        // ALTER TABLE ... SET SCHEMA
        if (sql.find("set schema") != string::npos) {
            size_t schemaPos = sql.find("set schema");
            string targetDb = trim(sql.substr(schemaPos + 10));
            auto res = g_engine.alterTableSetSchema(s.currentDB, tname, targetDb);
            if (res == DBStatus::DATABASE_NOT_FOUND) {
                cout << "Target database not found" << endl;
                return true;
            }
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Table not found" << endl;
                return true;
            }
            if (res == DBStatus::TABLE_ALREADY_EXISTS) {
                cout << "Table already exists in target database" << endl;
                return true;
            }
            cout << "Table " << tname << " moved to database " << targetDb << endl;
            return false;
        }
        cout << "SQL syntax error" << endl;
        return true;
    }

    // MERGE INTO: simplified syntax
    // merge into target using source on target.id = source.id
    //   update set name = source.name, age = source.age
    //   insert (id, name, age) values (source.id, source.name, source.age)
    if (sql.substr(0, 10) == "merge into") {
        if (!checkDB(s)) return true;
        // Parse: merge into target using source on target.col = source.col
        size_t usingPos = sql.find("using");
        size_t onPos = sql.find(" on ");
        size_t updatePos = sql.find("update set");
        size_t insertPos = sql.find("insert");
        if (usingPos == string::npos || onPos == string::npos ||
            updatePos == string::npos || insertPos == string::npos) {
            cout << "SQL syntax error: MERGE INTO target USING source ON condition UPDATE SET ... INSERT ..." << endl;
            return true;
        }
        string targetName = trim(sql.substr(10, usingPos - 10));
        string sourceName = trim(sql.substr(usingPos + 5, onPos - usingPos - 5));
        string onClause = trim(sql.substr(onPos + 4, updatePos - onPos - 4));
        string updateClause = trim(sql.substr(updatePos + 10, insertPos - updatePos - 10));
        string insertClause = trim(sql.substr(insertPos + 6));

        targetName = resolveTableName(s, targetName);
        sourceName = resolveTableName(s, sourceName);
        if (!g_engine.tableExists(s.currentDB, targetName)) {
            cout << "Table " << targetName << " not exist" << endl;
            return true;
        }
        if (!g_engine.tableExists(s.currentDB, sourceName)) {
            cout << "Table " << sourceName << " not exist" << endl;
            return true;
        }

        // Parse ON clause: target.col = source.col
        size_t eqPos = onClause.find('=');
        if (eqPos == string::npos) {
            cout << "SQL syntax error: ON clause requires =" << endl;
            return true;
        }
        string leftOn = trim(onClause.substr(0, eqPos));
        string rightOn = trim(onClause.substr(eqPos + 1));
        size_t leftDot = leftOn.find('.');
        size_t rightDot = rightOn.find('.');
        string targetOnCol = (leftDot != string::npos) ? trim(leftOn.substr(leftDot + 1)) : leftOn;
        string sourceOnCol = (rightDot != string::npos) ? trim(rightOn.substr(rightDot + 1)) : rightOn;

        // Parse UPDATE SET: col = source.col, ...
        map<string, string> updateMap; // targetCol -> sourceCol
        {
            vector<string> parts;
            int depth = 0;
            string cur;
            for (char c : updateClause) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                if (c == ',' && depth == 0) {
                    parts.push_back(trim(cur));
                    cur.clear();
                } else cur += c;
            }
            if (!trim(cur).empty()) parts.push_back(trim(cur));
            for (const string& part : parts) {
                size_t eq = part.find('=');
                if (eq == string::npos) continue;
                string tcol = trim(part.substr(0, eq));
                string scol = trim(part.substr(eq + 1));
                size_t sd = scol.find('.');
                if (sd != string::npos) scol = trim(scol.substr(sd + 1));
                updateMap[tcol] = scol;
            }
        }

        // Parse INSERT: (cols) VALUES (source.cols)
        vector<string> insertCols;
        vector<string> insertSourceCols;
        {
            size_t lp = insertClause.find('(');
            size_t rp = insertClause.find(')');
            if (lp != string::npos && rp != string::npos && rp > lp) {
                string colsPart = trim(insertClause.substr(lp + 1, rp - lp - 1));
                string valsPart;
                size_t valsPos = insertClause.find("values", rp);
                if (valsPos != string::npos) {
                    size_t vlp = insertClause.find('(', valsPos);
                    size_t vrp = insertClause.find(')', vlp);
                    if (vlp != string::npos && vrp != string::npos) {
                        valsPart = trim(insertClause.substr(vlp + 1, vrp - vlp - 1));
                    }
                }
                // Split cols and vals
                stringstream css(colsPart);
                string item;
                while (getline(css, item, ',')) insertCols.push_back(trim(item));
                stringstream vss(valsPart);
                while (getline(vss, item, ',')) {
                    string sc = trim(item);
                    size_t sd = sc.find('.');
                    if (sd != string::npos) sc = trim(sc.substr(sd + 1));
                    insertSourceCols.push_back(sc);
                }
            }
        }

        // Read source table schema and data
        TableSchema sourceTbl = g_engine.getTableSchema(s.currentDB, sourceName);
        TableSchema targetTbl = g_engine.getTableSchema(s.currentDB, targetName);
        vector<map<string, string>> sourceRows;
        g_engine.forEachRow(s.currentDB, sourceName, [&](uint32_t, uint16_t, const char* data, size_t len) {
            map<string, string> row;
            string rowStr(data, len);
            for (size_t i = 0; i < sourceTbl.len; ++i) {
                row[sourceTbl.cols[i].dataName] = dbms::StorageEngine::extractColumnValueStatic(rowStr, sourceTbl, i);
            }
            sourceRows.push_back(std::move(row));
        });

        int updated = 0, inserted = 0;
        for (const auto& srow : sourceRows) {
            auto it = srow.find(sourceOnCol);
            if (it == srow.end()) continue;
            string matchVal = it->second;
            if (matchVal.empty()) continue;

            // Check if target has matching row
            vector<string> conds = {"=" + targetOnCol + " " + matchVal};
            set<string> qcols;
            auto matchRows = g_engine.query(s.currentDB, targetName, conds, qcols);

            if (!matchRows.empty()) {
                // UPDATE matched row
                map<string, string> updates;
                for (const auto& kv : updateMap) {
                    auto sit = srow.find(kv.second);
                    if (sit != srow.end()) updates[kv.first] = sit->second;
                }
                if (!updates.empty()) {
                    g_engine.update(s.currentDB, targetName, updates, conds);
                    updated++;
                }
            } else {
                // INSERT new row
                map<string, string> values;
                for (size_t i = 0; i < insertCols.size() && i < insertSourceCols.size(); ++i) {
                    auto sit = srow.find(insertSourceCols[i]);
                    if (sit != srow.end()) values[insertCols[i]] = sit->second;
                }
                if (!values.empty()) {
                    auto res = g_engine.insert(s.currentDB, targetName, values);
                    if (res == dbms::DBStatus::OK) inserted++;
                }
            }
        }
        cout << "MERGE completed: " << updated << " updated, " << inserted << " inserted" << endl;
        return false;
    }

    bool isReplace = false;
    if (sql.substr(0, 13) == "replace into ") {
        isReplace = true;
        sql = "insert into " + sql.substr(13);
    }

    if (sql.substr(0, 12) == "insert into ") {
        if (!checkDB(s)) return true;
        vector<string> tokens = tokenize(sql.substr(12));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        string resolvedName = resolveTableName(s, tname);
        // Check for INSTEAD OF triggers on view (takes precedence over base table rewriting)
        if (!g_engine.tableExists(s.currentDB, resolvedName) &&
            g_engine.viewExists(s.currentDB, tname)) {
            map<string, string> newValues;
            // Parse VALUES to build NEW values
            size_t valuesPos = sql.find("values");
            size_t valStart = (valuesPos != string::npos) ? sql.find('(', valuesPos) : string::npos;
            size_t valEnd = (valStart != string::npos) ? findMatchingParen(sql, valStart) : string::npos;
            if (valStart != string::npos && valEnd != string::npos) {
                string valsStr = trim(sql.substr(valStart + 1, valEnd - valStart - 1));
                vector<string> vals = splitValues(valsStr);
                // Try to find column list (before VALUES)
                size_t colStart = sql.find('(', 12);
                if (colStart != string::npos && colStart < valStart) {
                    size_t colEnd = sql.find(')', colStart);
                    if (colEnd != string::npos && colEnd < valStart) {
                        string colsStr = trim(sql.substr(colStart + 1, colEnd - colStart - 1));
                        vector<string> cols = splitValues(colsStr);
                        for (size_t i = 0; i < cols.size() && i < vals.size(); ++i) {
                            newValues[trim(cols[i])] = trim(vals[i]);
                        }
                    }
                }
            }
            if (executeInsteadOfTrigger(s, tname, "insert", newValues, {})) {
                cout << "INSTEAD OF INSERT trigger executed on view " << tname << endl;
                return false;
            }
        }
        // Check for updatable view
        string viewBaseTable;
        string viewCheckOpt;
        if (!g_engine.tableExists(s.currentDB, resolvedName) &&
            g_engine.viewExists(s.currentDB, tname)) {
            viewBaseTable = g_engine.getViewBaseTable(s.currentDB, tname);
            viewCheckOpt = g_engine.getViewCheckOption(s.currentDB, tname);
            if (!viewBaseTable.empty()) {
                // Rewrite SQL to use base table
                string rewritten = rawSql;
                size_t pos = rewritten.find(tname);
                if (pos != string::npos) {
                    rewritten = rewritten.substr(0, pos) + viewBaseTable + rewritten.substr(pos + tname.size());
                    if (!viewCheckOpt.empty()) {
                        // Pre-validate WITH CHECK OPTION for INSERT INTO ... VALUES
                        size_t valuesPos = sql.find("values");
                        size_t selectPos = sql.find("select");
                        bool isValuesInsert = (valuesPos != string::npos);
                        bool isSelectInsert = (selectPos != string::npos);
                        if (isValuesInsert && !isSelectInsert) {
                            size_t colStart = sql.find('(', 12);
                            size_t colEnd = sql.find(')', colStart);
                            size_t valStart = sql.find('(', colEnd);
                            size_t valEnd = findMatchingParen(sql, valStart);
                            if (colStart != string::npos && colEnd != string::npos &&
                                valStart != string::npos && valEnd != string::npos) {
                                string colsStr = trim(sql.substr(colStart + 1, colEnd - colStart - 1));
                                string valsStr = trim(sql.substr(valStart + 1, valEnd - valStart - 1));
                                vector<string> insertCols;
                                {
                                    stringstream css(colsStr);
                                    string item;
                                    while (getline(css, item, ',')) insertCols.push_back(trim(item));
                                }
                                vector<string> insertVals = splitValues(valsStr);
                                for (auto& v : insertVals) v = stripQuotes(v);
                                if (insertCols.size() == insertVals.size()) {
                                    map<string, string> colVals;
                                    for (size_t i = 0; i < insertCols.size(); ++i)
                                        colVals[insertCols[i]] = insertVals[i];
                                    if (!g_engine.validateViewCheckOption(s.currentDB, tname, colVals)) {
                                        cout << "ERROR: new row violates WITH CHECK OPTION on view \"" << tname << "\"" << endl;
                                        return true;
                                    }
                                }
                            }
                        } else if (isSelectInsert) {
                            cout << "ERROR: INSERT INTO view WITH CHECK OPTION does not support INSERT ... SELECT" << endl;
                            return true;
                        }
                    }
                    return execute(rewritten, s);
                }
            }
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!g_engine.tableExists(s.currentDB, resolvedName)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Insert)) return true;

        // Find column list (optional - can omit columns)
        size_t valuesPos = sql.find("values");
        size_t selectPosCheck = sql.find("select", 12);
        bool isSelectInsert = (selectPosCheck != string::npos && (valuesPos == string::npos || selectPosCheck < valuesPos));
        if (valuesPos == string::npos && !isSelectInsert) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        string colsStr;
        size_t colStart = sql.find('(', 12);
        size_t colEnd = 0;
        bool hasExplicitCols = (colStart != string::npos && colStart < valuesPos);

        if (hasExplicitCols) {
            colEnd = sql.find(')', colStart);
            if (colEnd == string::npos || colEnd > valuesPos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            colsStr = trim(sql.substr(colStart + 1, colEnd - colStart - 1));
        } else {
            // Omit column list: use all columns from table schema
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
            if (tbl.len == 0) {
                cout << "Table has no columns" << endl;
                return true;
            }
            for (size_t i = 0; i < tbl.len; ++i) {
                if (i > 0) colsStr += ",";
                colsStr += tbl.cols[i].dataName;
            }
            colEnd = valuesPos;
        }
        // Column-level permission check
        {
            vector<string> insertCols;
            stringstream css(colsStr);
            string item;
            while (getline(css, item, ',')) {
                string c = trim(item);
                if (!c.empty()) insertCols.push_back(c);
            }
            if (!isTempTable(s, tname) && !checkInsertColumnPermission(s, tname, insertCols)) return true;
        }

        // Check for INSERT INTO ... SELECT
        size_t selectPos = sql.find("select", colEnd);
        if (selectPos != string::npos) {
            // Parse INSERT INTO ... SELECT
            vector<string> cols;
            {
                stringstream css(colsStr);
                string item;
                while (getline(css, item, ',')) cols.push_back(trim(item));
            }

            string selectSql = trim(sql.substr(selectPos));
            // Parse the SELECT: extract FROM table, columns, WHERE
            size_t fromPos = selectSql.find("from");
            if (fromPos == string::npos) {
                cout << "SQL syntax error: SELECT requires FROM" << endl;
                return true;
            }
            string selectColsStr = trim(selectSql.substr(6, fromPos - 6));
            size_t wherePos = selectSql.find("where", fromPos);
            size_t orderPos = selectSql.find("order by", fromPos);
            string srcTname = trim(selectSql.substr(fromPos + 4,
                (wherePos != string::npos) ? (wherePos - fromPos - 4)
                : (orderPos != string::npos) ? (orderPos - fromPos - 4)
                : (selectSql.size() - fromPos - 4)));

            vector<string> selectCols;
            {
                stringstream scs(selectColsStr);
                string item;
                while (getline(scs, item, ',')) selectCols.push_back(trim(item));
            }
            if (cols.size() != selectCols.size()) {
                cout << "SQL syntax error: column count mismatch" << endl;
                return true;
            }

            vector<string> conds;
            if (wherePos != string::npos) {
                size_t condEnd = (orderPos != string::npos) ? orderPos : selectSql.size();
                string condStr = normalizeConditionStr(trim(selectSql.substr(wherePos + 5, condEnd - wherePos - 5)));
                if (!condStr.empty()) {
                    vector<string> rawConds = splitConds(condStr);
                    for (auto& c : rawConds) {
                        string mc = modifyLogic(c);
                        if (!mc.empty()) conds.push_back(mc);
                    }
                }
            }

            TableSchema srcTbl = g_engine.getTableSchema(s.currentDB, srcTname);
            auto parsedConds = dbms::StorageEngine::parseConditions(conds);

            int inserted = 0;
            g_engine.forEachRow(s.currentDB, srcTname, [&](uint32_t, uint16_t, const char* data, size_t len) {
                std::string row(data, len);
                bool match = true;
                for (const auto& c : parsedConds) {
                    if (!dbms::StorageEngine::evalConditionOnRow(c, row, srcTbl)) {
                        match = false; break;
                    }
                }
                if (!match) return;

                std::map<string, string> values;
                for (size_t i = 0; i < cols.size(); ++i) {
                    size_t colIdx = srcTbl.len;
                    for (size_t j = 0; j < srcTbl.len; ++j) {
                        if (srcTbl.cols[j].dataName == selectCols[i]) { colIdx = j; break; }
                    }
                    if (colIdx < srcTbl.len) {
                        values[cols[i]] = dbms::StorageEngine::extractColumnValueStatic(row, srcTbl, colIdx);
                    }
                }
                auto res = g_engine.insert(s.currentDB, resolvedName, values);
                if (res == dbms::DBStatus::OK) ++inserted;
            });
            cout << inserted << " rows inserted" << endl;
            return false;
        }

        // Regular INSERT INTO ... VALUES
        size_t valStart = sql.find('(', colEnd);
        if (valStart == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        size_t valEnd = findMatchingParen(sql, valStart);
        if (valEnd == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        // Parse all value rows (multi-row INSERT)
        vector<string> allValStrs;
        allValStrs.push_back(trim(sql.substr(valStart + 1, valEnd - valStart - 1)));
        size_t nextPos = valEnd + 1;
        while (nextPos < sql.size()) {
            while (nextPos < sql.size() && isspace(static_cast<unsigned char>(sql[nextPos]))) ++nextPos;
            if (nextPos >= sql.size() || sql[nextPos] != ',') break;
            ++nextPos;
            while (nextPos < sql.size() && isspace(static_cast<unsigned char>(sql[nextPos]))) ++nextPos;
            if (nextPos >= sql.size() || sql[nextPos] != '(') break;
            size_t nextValEnd = findMatchingParen(sql, nextPos);
            if (nextValEnd == string::npos) break;
            allValStrs.push_back(trim(sql.substr(nextPos + 1, nextValEnd - nextPos - 1)));
            nextPos = nextValEnd + 1;
        }

        vector<string> cols;
        {
            stringstream css(colsStr);
            string item;
            while (getline(css, item, ',')) cols.push_back(trim(item));
        }

        // Parse ON CONFLICT clause for UPSERT
        size_t onConflictPos = sql.find("on conflict", valEnd);
        string conflictCol;
        map<string, string> upsertUpdates;
        if (onConflictPos != string::npos) {
            size_t conflictColStart = sql.find('(', onConflictPos);
            size_t conflictColEnd = sql.find(')', conflictColStart);
            if (conflictColStart != string::npos && conflictColEnd != string::npos) {
                conflictCol = trim(sql.substr(conflictColStart + 1, conflictColEnd - conflictColStart - 1));
            }
            size_t doUpdatePos = sql.find("do update set", conflictColEnd);
            if (doUpdatePos != string::npos) {
                string setClause = trim(sql.substr(doUpdatePos + 13));
                vector<string> setParts;
                {
                    stringstream sss(setClause);
                    string item;
                    while (getline(sss, item, ',')) setParts.push_back(trim(item));
                }
                for (const string& part : setParts) {
                    size_t eq = part.find('=');
                    if (eq != string::npos) {
                        upsertUpdates[trim(part.substr(0, eq))] = stripQuotes(trim(part.substr(eq + 1)));
                    }
                }
            }
        }

        // Parse RETURNING clause
        auto [returningCols, returningAll] = parseReturningClause(sql, valEnd);

        int inserted = 0;
        vector<string> returnedRows;
        for (const string& valsStr : allValStrs) {
            vector<string> vals;
            {
                string item;
                size_t i = 0;
                while (i < valsStr.size()) {
                    while (i < valsStr.size() && isspace(static_cast<unsigned char>(valsStr[i]))) ++i;
                    if (i >= valsStr.size()) break;
                    string val;
                    if (valsStr[i] == '\'') {
                        val += valsStr[i++];
                        while (i < valsStr.size() && valsStr[i] != '\'') val += valsStr[i++];
                        if (i < valsStr.size()) val += valsStr[i++];
                    } else if (valsStr[i] == '\"') {
                        val += valsStr[i++];
                        while (i < valsStr.size() && valsStr[i] != '\"') val += valsStr[i++];
                        if (i < valsStr.size()) val += valsStr[i++];
                    } else {
                        while (i < valsStr.size() && valsStr[i] != ',') val += valsStr[i++];
                    }
                    vals.push_back(trim(val));
                    if (i < valsStr.size() && valsStr[i] == ',') ++i;
                }
            }
            if (cols.size() != vals.size()) {
                cout << "SQL syntax error: column count mismatch" << endl;
                return true;
            }

            map<string, string> values;
            for (size_t i = 0; i < cols.size(); ++i) {
                string val = stripQuotes(vals[i]);
                string lval = toLower(val);
                if (lval.size() >= 7 && lval.substr(0, 7) == "nextval") {
                    size_t lp = val.find('(');
                    size_t rp = val.find(')', lp);
                    if (lp != string::npos && rp != string::npos) {
                        string seqName = stripQuotes(trim(val.substr(lp + 1, rp - lp - 1)));
                        int64_t nextValue = g_engine.nextval(s.currentDB, seqName);
                        s.sequenceLastValues[seqName] = nextValue;
                        val = std::to_string(nextValue);
                    }
                } else if (lval.size() >= 7 && lval.substr(0, 7) == "currval") {
                    size_t lp = val.find('(');
                    size_t rp = val.find(')', lp);
                    if (lp != string::npos && rp != string::npos) {
                        string seqName = stripQuotes(trim(val.substr(lp + 1, rp - lp - 1)));
                        auto it = s.sequenceLastValues.find(seqName);
                        if (it == s.sequenceLastValues.end()) {
                            cout << "currval of sequence " << seqName << " is not yet defined in this session" << endl;
                            return true;
                        }
                        val = std::to_string(it->second);
                    }
                }
                values[cols[i]] = val;
            }

            // Validate ENUM constraints
            {
                TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
                for (size_t ci = 0; ci < tbl.len; ++ci) {
                    if (tbl.cols[ci].enumValues.empty()) continue;
                    auto it = values.find(tbl.cols[ci].dataName);
                    if (it == values.end()) continue;
                    const string& v = it->second;
                    if (v.empty()) continue;
                    bool found = false;
                    for (const auto& ev : tbl.cols[ci].enumValues) {
                        if (ev == v) { found = true; break; }
                    }
                    if (!found) {
                        cout << "Invalid enum value '" << v << "' for column " << tbl.cols[ci].dataName << endl;
                        return true;
                    }
                }
            }

            auto res = g_engine.insert(s.currentDB, resolvedName, values);
            if (res == DBStatus::DUPLICATE_KEY) {
                if (isReplace) {
                    // REPLACE INTO: delete conflicting row(s), then re-insert
                    TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
                    if (tbl.hasPrimaryKey()) {
                        string pkVal = tbl.buildPKValue(values);
                        if (!pkVal.empty()) {
                            vector<string> whereConds;
                            string pkColName;
                            if (!tbl.pkColIndices.empty()) {
                                for (size_t idx : tbl.pkColIndices) {
                                    if (idx < tbl.len) { pkColName = tbl.cols[idx].dataName; break; }
                                }
                            } else {
                                for (size_t i = 0; i < tbl.len; ++i) {
                                    if (tbl.cols[i].isPrimaryKey) { pkColName = tbl.cols[i].dataName; break; }
                                }
                            }
                            if (!pkColName.empty()) {
                                whereConds.push_back("=" + pkColName + " " + pkVal);
                                g_engine.remove(s.currentDB, resolvedName, whereConds);
                            }
                        }
                    }
                    // Also check unique constraints
                    for (size_t ci = 0; ci < tbl.len; ++ci) {
                        if (!tbl.cols[ci].isUnique) continue;
                        auto it = values.find(tbl.cols[ci].dataName);
                        if (it == values.end() || it->second.empty()) continue;
                        vector<string> whereConds;
                        whereConds.push_back("=" + tbl.cols[ci].dataName + " " + it->second);
                        g_engine.remove(s.currentDB, resolvedName, whereConds);
                    }
                    // Re-insert after deletion
                    res = g_engine.insert(s.currentDB, resolvedName, values);
                    if (res == DBStatus::OK) {
                        ++inserted;
                        if (!returningCols.empty() || returningAll) {
                            vector<string> whereConds;
                            for (auto& kv : values) {
                                if (!kv.second.empty()) whereConds.push_back("=" + kv.first + " " + kv.second);
                            }
                            auto rr = g_engine.query(s.currentDB, resolvedName, whereConds,
                                                     returningAll ? set<string>() : returningCols, {});
                            for (auto& r : rr) returnedRows.push_back(r);
                        }
                        continue;
                    }
                }
                if (conflictCol.empty()) {
                    cout << "Duplicate key" << endl;
                    return true;
                }
                // UPSERT: perform UPDATE on conflict
                auto it = values.find(conflictCol);
                if (it == values.end()) {
                    cout << "Conflict column not in INSERT values" << endl;
                    return true;
                }
                string condVal = it->second;
                vector<string> whereConds;
                if (!condVal.empty()) whereConds.push_back("=" + conflictCol + " " + condVal);
                auto ures = g_engine.update(s.currentDB, resolvedName, upsertUpdates, whereConds);
                if (ures != DBStatus::OK) {
                    cout << "UPSERT update failed" << endl;
                    return true;
                }
                ++inserted;
                // RETURNING for UPSERT: query updated row
                if (!returningCols.empty() || returningAll) {
                    auto rr = g_engine.query(s.currentDB, resolvedName, whereConds,
                                             returningAll ? set<string>() : returningCols, {});
                    for (auto& r : rr) returnedRows.push_back(r);
                }
                continue;
            }
            if (res != DBStatus::OK) {
                cout << "Invalid data, please check" << endl;
                return true;
            }
            ++inserted;
            // RETURNING: query inserted row
            if (!returningCols.empty() || returningAll) {
                vector<string> whereConds;
                for (auto& kv : values) {
                    if (!kv.second.empty()) whereConds.push_back("=" + kv.first + " " + kv.second);
                }
                auto rr = g_engine.query(s.currentDB, resolvedName, whereConds,
                                         returningAll ? set<string>() : returningCols, {});
                for (auto& r : rr) returnedRows.push_back(r);
            }
        }
        if (isReplace) {
            cout << inserted << " row(s) replaced" << endl;
            log(s.username, to_string(inserted) + " row(s) replaced", getTime());
        } else {
            cout << inserted << " row(s) inserted" << endl;
            log(s.username, to_string(inserted) + " row(s) inserted", getTime());
        }
        for (auto& row : returnedRows) cout << row << endl;
        if (inserted > 0) g_engine.analyzeTable(s.currentDB, resolvedName);
        return false;
    }

    // DELETE ... USING ... (multi-table delete via JOIN)
    if (sql.substr(0, 12) == "delete from ") {
        if (!checkDB(s)) return true;
        string delRest = expandSubqueries(sql.substr(12), s);
        size_t usingPos = findTopLevelKeyword(delRest, "using");
        size_t wherePos = findTopLevelKeyword(delRest, "where",
            (usingPos != string::npos) ? usingPos : 0);
        string targetPart = trim(delRest.substr(0,
            (usingPos != string::npos) ? usingPos
            : (wherePos != string::npos) ? wherePos
            : delRest.size()));
        // Check for INSTEAD OF DELETE triggers on view
        {
            string ioTname = targetPart;
            string ioResolved = resolveTableName(s, ioTname);
            if (!g_engine.tableExists(s.currentDB, ioResolved) &&
                g_engine.viewExists(s.currentDB, ioTname)) {
                map<string, string> oldValues;
                // Parse WHERE clause for OLD values
                if (wherePos != string::npos) {
                    string whereClause = trim(delRest.substr(wherePos + 5));
                    vector<string> conds = splitConds(normalizeConditionStr(whereClause));
                    for (const auto& c : conds) {
                        string mc = modifyLogic(c);
                        if (!mc.empty() && mc[0] == '=') {
                            size_t sp = mc.find(' ', 1);
                            if (sp != string::npos) {
                                string col = trim(mc.substr(1, sp - 1));
                                string val = trim(mc.substr(sp + 1));
                                oldValues[col] = val;
                            }
                        }
                    }
                }
                if (executeInsteadOfTrigger(s, ioTname, "delete", {}, oldValues)) {
                    cout << "INSTEAD OF DELETE trigger executed on view " << ioTname << endl;
                    return false;
                }
            }
        }
        if (usingPos != string::npos) {
            string tname = targetPart;
            string sourceName = trim(delRest.substr(usingPos + 5,
                (wherePos != string::npos) ? (wherePos - usingPos - 5)
                : (delRest.size() - usingPos - 5)));
            string resolvedSource = resolveTableName(s, sourceName);
            string resolvedTarget = resolveTableName(s, tname);
            if (!g_engine.tableExists(s.currentDB, resolvedSource)) {
                cout << "Table " << sourceName << " not exist" << endl;
                return true;
            }
            if (!g_engine.tableExists(s.currentDB, resolvedTarget)) {
                cout << "Table " << tname << " not exist" << endl;
                return true;
            }
            if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Delete)) return true;
            // Build JOIN query: separate join conditions (ON) from filter conditions (WHERE)
            string joinSql = "select * from " + tname + " inner join " + sourceName;
            if (wherePos != string::npos) {
                string whereClause = trim(delRest.substr(wherePos + 5));
                vector<string> conds;
                {
                    size_t p = 0;
                    while (p < whereClause.size()) {
                        size_t ap = whereClause.find(" and ", p);
                        if (ap == string::npos) {
                            conds.push_back(trim(whereClause.substr(p)));
                            break;
                        }
                        conds.push_back(trim(whereClause.substr(p, ap - p)));
                        p = ap + 5;
                    }
                }
                string onClause, whereFilter;
                string targetPrefix = tname + ".";
                string sourcePrefix = sourceName + ".";
                for (const auto& c : conds) {
                    bool hasTarget = (c.find(targetPrefix) != string::npos);
                    bool hasSource = (c.find(sourcePrefix) != string::npos);
                    if (hasTarget && hasSource) {
                        if (!onClause.empty()) onClause += " and ";
                        onClause += c;
                    } else {
                        if (!whereFilter.empty()) whereFilter += " and ";
                        whereFilter += c;
                    }
                }
                if (!onClause.empty()) {
                    joinSql += " on " + onClause;
                }
                if (!whereFilter.empty()) {
                    joinSql += " where " + whereFilter;
                }
            }
            Session tmpS = s;
            tmpS.preparedStmts.clear();
            auto* oldBuf = std::cout.rdbuf();
            std::stringstream joinOutput;
            std::cout.rdbuf(joinOutput.rdbuf());
            execute(joinSql, tmpS);
            std::cout.rdbuf(oldBuf);
            vector<string> joinLines;
            string line;
            while (getline(joinOutput, line)) {
                string tl = trim(line);
                if (tl.size() >= 3 && (tl.substr(0, 3) == "Mon" || tl.substr(0, 3) == "Tue" ||
                    tl.substr(0, 3) == "Wed" || tl.substr(0, 3) == "Thu" || tl.substr(0, 3) == "Fri" ||
                    tl.substr(0, 3) == "Sat" || tl.substr(0, 3) == "Sun")) continue;
                joinLines.push_back(tl);
            }
            if (joinLines.size() <= 1) {
                cout << "Delete done (0 rows)" << endl;
                return false;
            }
            TableSchema targetTbl = g_engine.getTableSchema(s.currentDB, resolvedTarget);
            size_t pkIdx = targetTbl.len;
            for (size_t i = 0; i < targetTbl.len; ++i) {
                if (targetTbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
            }
            if (pkIdx >= targetTbl.len) {
                cout << "DELETE USING requires target table to have a primary key" << endl;
                return true;
            }
            string pkCol = targetTbl.cols[pkIdx].dataName;
            size_t deletedCount = 0;
            for (size_t li = 1; li < joinLines.size(); ++li) {
                stringstream ss(joinLines[li]);
                string val;
                vector<string> cols;
                while (ss >> val) cols.push_back(val);
                if (cols.size() <= pkIdx) continue;
                string pkVal = cols[pkIdx];
                vector<string> pkCond = {"=" + pkCol + " " + pkVal};
                auto res = g_engine.remove(s.currentDB, resolvedTarget, pkCond);
                if (res == DBStatus::OK) deletedCount++;
            }
            cout << "Delete done (" << deletedCount << " row(s))" << endl;
            return false;
        }
        // Parse LIMIT clause from original SQL before tokenizing
        size_t delLimitPos = sql.find("limit", 12);
        int delLimitVal = -1;
        if (delLimitPos != string::npos) {
            string limitStr = trim(sql.substr(delLimitPos + 5));
            try { delLimitVal = stoi(limitStr); } catch (...) { delLimitVal = -1; }
            // Remove limit from delRest for tokenization
            size_t limitInDelRest = delRest.find("limit");
            if (limitInDelRest != string::npos) {
                delRest = trim(delRest.substr(0, limitInDelRest));
            }
        }

        vector<string> tokens = tokenize(normalizeConditionStr(delRest));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        string resolvedName = resolveTableName(s, tname);
        // Check for updatable view
        if (!g_engine.tableExists(s.currentDB, resolvedName) &&
            g_engine.viewExists(s.currentDB, tname)) {
            string viewBaseTable = g_engine.getViewBaseTable(s.currentDB, tname);
            if (!viewBaseTable.empty()) {
                string rewritten = rawSql;
                size_t pos = rewritten.find(tname);
                if (pos != string::npos) {
                    rewritten = rewritten.substr(0, pos) + viewBaseTable + rewritten.substr(pos + tname.size());
                    return execute(rewritten, s);
                }
            }
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!g_engine.tableExists(s.currentDB, resolvedName)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Delete)) return true;

        // Parse RETURNING clause from original SQL (before normalization)
        auto [returningCols, returningAll] = parseReturningClause(sql, 12);

        tokens.erase(tokens.begin());
        if (tokens.empty()) {
            // No WHERE clause
            if (delLimitVal > 0) {
                TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
                size_t pkIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
                }
                if (pkIdx >= tbl.len) {
                    cout << "DELETE ... LIMIT requires table to have a primary key" << endl;
                    return true;
                }
                string pkCol = tbl.cols[pkIdx].dataName;
                size_t deletedCount = 0;
                vector<string> returnedRows;
                auto matchRows = g_engine.query(s.currentDB, resolvedName, {}, {}, {});
                for (size_t ri = 0; ri < matchRows.size() && deletedCount < static_cast<size_t>(delLimitVal); ++ri) {
                    stringstream ss(matchRows[ri]);
                    string val;
                    vector<string> cols;
                    while (ss >> val) cols.push_back(val);
                    if (cols.size() <= pkIdx) continue;
                    string pkVal = cols[pkIdx];
                    vector<string> pkCond = {"=" + pkCol + " " + pkVal};
                    if (!returningCols.empty() || returningAll) {
                        auto rr = g_engine.query(s.currentDB, resolvedName, pkCond,
                                                 returningAll ? set<string>() : returningCols, {});
                        for (auto& r : rr) returnedRows.push_back(r);
                    }
                    auto res = g_engine.remove(s.currentDB, resolvedName, pkCond);
                    if (res == DBStatus::OK) ++deletedCount;
                }
                cout << "Delete done (" << deletedCount << " row(s))" << endl;
                for (auto& row : returnedRows) cout << row << endl;
                log(s.username, "delete done", getTime());
                g_engine.analyzeTable(s.currentDB, resolvedName);
                return false;
            }
            // RETURNING: query all rows before delete
            vector<string> returnedRows;
            if (!returningCols.empty() || returningAll) {
                auto rr = g_engine.query(s.currentDB, resolvedName, {},
                                         returningAll ? set<string>() : returningCols, {});
                returnedRows = rr;
            }
            auto res = g_engine.remove(s.currentDB, resolvedName, {});
            if (res != DBStatus::OK) {
                cout << "Delete failed: foreign key constraint violation or other error" << endl;
                return true;
            }
            cout << "Delete done" << endl;
            for (auto& row : returnedRows) cout << row << endl;
            log(s.username, "delete done", getTime());
            return false;
        }
        if (tokens.size() == 1) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        // Wrap in parens and convert conditions
        tokens.insert(tokens.begin(), "(");
        tokens.push_back(")");
        for (auto& t : tokens) t = modifyLogic(t);
        auto groups = breakDownConditions(tokens);

        // If LIMIT is specified, query matching rows and delete by PK
        if (delLimitVal > 0) {
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
            size_t pkIdx = tbl.len;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
            }
            if (pkIdx >= tbl.len) {
                cout << "DELETE ... LIMIT requires table to have a primary key" << endl;
                return true;
            }
            string pkCol = tbl.cols[pkIdx].dataName;
            size_t deletedCount = 0;
            vector<string> returnedRows;
            for (const auto& g : groups) {
                auto matchRows = g_engine.query(s.currentDB, resolvedName, g, {}, {});
                for (size_t ri = 0; ri < matchRows.size() && deletedCount < static_cast<size_t>(delLimitVal); ++ri) {
                    stringstream ss(matchRows[ri]);
                    string val;
                    vector<string> cols;
                    while (ss >> val) cols.push_back(val);
                    if (cols.size() <= pkIdx) continue;
                    string pkVal = cols[pkIdx];
                    vector<string> pkCond = {"=" + pkCol + " " + pkVal};
                    if (!returningCols.empty() || returningAll) {
                        auto rr = g_engine.query(s.currentDB, resolvedName, pkCond,
                                                 returningAll ? set<string>() : returningCols, {});
                        for (auto& r : rr) returnedRows.push_back(r);
                    }
                    auto res = g_engine.remove(s.currentDB, resolvedName, pkCond);
                    if (res == DBStatus::OK) ++deletedCount;
                }
            }
            cout << "Delete done (" << deletedCount << " row(s))" << endl;
            for (auto& row : returnedRows) cout << row << endl;
            log(s.username, "delete done", getTime());
            g_engine.analyzeTable(s.currentDB, resolvedName);
            return false;
        }

        // RETURNING: query rows before delete
        vector<string> returnedRows;
        if (!returningCols.empty() || returningAll) {
            for (const auto& g : groups) {
                auto rr = g_engine.query(s.currentDB, resolvedName, g,
                                         returningAll ? set<string>() : returningCols, {});
                for (auto& r : rr) returnedRows.push_back(r);
            }
        }
        for (const auto& g : groups) {
            auto res = g_engine.remove(s.currentDB, resolvedName, g);
            if (res != DBStatus::OK) {
                cout << "Delete failed: foreign key constraint violation or other error" << endl;
                return true;
            }
        }
        cout << "Delete done" << endl;
        for (auto& row : returnedRows) cout << row << endl;
        log(s.username, "delete done", getTime());
        g_engine.analyzeTable(s.currentDB, resolvedName);
        return false;
    }

    // UPDATE ... FROM ... (multi-table update via JOIN)
    if (sql.substr(0, 6) == "update") {
        if (!checkDB(s)) return true;
        size_t setPos = sql.find("set");
        if (setPos == std::string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = trim(sql.substr(6, setPos - 6));
        // Check for INSTEAD OF UPDATE triggers on view
        {
            string ioResolved = resolveTableName(s, tname);
            if (!g_engine.tableExists(s.currentDB, ioResolved) &&
                g_engine.viewExists(s.currentDB, tname)) {
                map<string, string> newValues;
                map<string, string> oldValues;
                // Parse SET clause for NEW values
                auto updates = parseSetClause(sql, setPos + 3, sql.size());
                for (const auto& kv : updates) newValues[kv.first] = kv.second;
                // Parse WHERE clause for OLD values (simplified: just pass condition string)
                size_t wherePos = findTopLevelKeyword(sql, "where", setPos);
                if (wherePos != string::npos) {
                    string whereClause = trim(sql.substr(wherePos + 5));
                    // Try to extract OLD values from WHERE conditions like "id = 1"
                    vector<string> conds = splitConds(normalizeConditionStr(whereClause));
                    for (const auto& c : conds) {
                        string mc = modifyLogic(c);
                        if (!mc.empty() && mc[0] == '=') {
                            size_t sp = mc.find(' ', 1);
                            if (sp != string::npos) {
                                string col = trim(mc.substr(1, sp - 1));
                                string val = trim(mc.substr(sp + 1));
                                oldValues[col] = val;
                            }
                        }
                    }
                }
                if (executeInsteadOfTrigger(s, tname, "update", newValues, oldValues)) {
                    cout << "INSTEAD OF UPDATE trigger executed on view " << tname << endl;
                    return false;
                }
            }
        }
        size_t fromPos = findTopLevelKeyword(sql, "from", setPos);
        if (fromPos != std::string::npos) {
            // Multi-table update: UPDATE target SET ... FROM source WHERE ...
            size_t wherePos = findTopLevelKeyword(sql, "where", fromPos);
            string sourceName = trim(sql.substr(fromPos + 4,
                (wherePos != std::string::npos) ? (wherePos - fromPos - 4)
                : (sql.size() - fromPos - 4)));
            string resolvedSource = resolveTableName(s, sourceName);
            if (!g_engine.tableExists(s.currentDB, resolvedSource)) {
                cout << "Table " << sourceName << " not exist" << endl;
                return true;
            }
            string resolvedTarget = resolveTableName(s, tname);
            if (!g_engine.tableExists(s.currentDB, resolvedTarget)) {
                cout << "Table " << tname << " not exist" << endl;
                return true;
            }
            // Parse SET clause
            auto updates = parseSetClause(sql, setPos + 3, fromPos);
            if (updates.empty()) {
                cout << "SQL syntax error: invalid SET clause" << endl;
                return true;
            }
            // Build JOIN query: separate join conditions (ON) from filter conditions (WHERE)
            string joinSql = "select * from " + tname + " inner join " + sourceName;
            if (wherePos != std::string::npos) {
                string whereClause = trim(sql.substr(wherePos + 5));
                vector<string> conds;
                {
                    int depth = 0;
                    string cur;
                    for (char c : whereClause) {
                        if (c == '(') depth++;
                        else if (c == ')') depth--;
                        if (c == ' ' && depth == 0) {
                            if (cur == "and" || cur == "or") {
                                if (!cur.empty()) {
                                    conds.push_back(trim(cur));
                                    cur.clear();
                                }
                            } else {
                                cur += c;
                            }
                        } else {
                            cur += c;
                        }
                    }
                    if (!trim(cur).empty()) conds.push_back(trim(cur));
                }
                // Simpler split by " and "
                conds.clear();
                {
                    size_t p = 0;
                    while (p < whereClause.size()) {
                        size_t ap = whereClause.find(" and ", p);
                        if (ap == string::npos) {
                            conds.push_back(trim(whereClause.substr(p)));
                            break;
                        }
                        conds.push_back(trim(whereClause.substr(p, ap - p)));
                        p = ap + 5;
                    }
                }
                string onClause, whereFilter;
                string targetPrefix = tname + ".";
                string sourcePrefix = sourceName + ".";
                for (const auto& c : conds) {
                    bool hasTarget = (c.find(targetPrefix) != string::npos);
                    bool hasSource = (c.find(sourcePrefix) != string::npos);
                    if (hasTarget && hasSource) {
                        if (!onClause.empty()) onClause += " and ";
                        onClause += c;
                    } else {
                        if (!whereFilter.empty()) whereFilter += " and ";
                        whereFilter += c;
                    }
                }
                if (!onClause.empty()) {
                    joinSql += " on " + onClause;
                }
                if (!whereFilter.empty()) {
                    joinSql += " where " + whereFilter;
                }
            }
            // Execute JOIN query to get matching target rows
            Session tmpS = s;
            tmpS.preparedStmts.clear();
            auto* oldBuf = std::cout.rdbuf();
            std::stringstream joinOutput;
            std::cout.rdbuf(joinOutput.rdbuf());
            execute(joinSql, tmpS);
            std::cout.rdbuf(oldBuf);
            // Parse output: first line is header, rest are data rows (skip timestamp lines)
            vector<string> joinLines;
            string line;
            while (getline(joinOutput, line)) {
                string tl = trim(line);
                // Skip timestamp lines that start with day-of-week
                if (tl.size() >= 3 && (tl.substr(0, 3) == "Mon" || tl.substr(0, 3) == "Tue" ||
                    tl.substr(0, 3) == "Wed" || tl.substr(0, 3) == "Thu" || tl.substr(0, 3) == "Fri" ||
                    tl.substr(0, 3) == "Sat" || tl.substr(0, 3) == "Sun")) continue;
                joinLines.push_back(tl);
            }
            if (joinLines.size() <= 1) {
                cout << "Update done (0 rows)" << endl;
                return false;
            }
            // Get PK column of target table
            TableSchema targetTbl = g_engine.getTableSchema(s.currentDB, resolvedTarget);
            size_t pkIdx = targetTbl.len;
            for (size_t i = 0; i < targetTbl.len; ++i) {
                if (targetTbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
            }
            if (pkIdx >= targetTbl.len) {
                cout << "UPDATE FROM requires target table to have a primary key" << endl;
                return true;
            }
            string pkCol = targetTbl.cols[pkIdx].dataName;
            // For each joined row, extract target PK and update
            size_t updatedCount = 0;
            for (size_t li = 1; li < joinLines.size(); ++li) {
                stringstream ss(joinLines[li]);
                string val;
                vector<string> cols;
                while (ss >> val) cols.push_back(val);
                if (cols.size() <= pkIdx) continue;
                string pkVal = cols[pkIdx];
                vector<string> pkCond = {"=" + pkCol + " " + pkVal};
                auto res = g_engine.update(s.currentDB, resolvedTarget, updates, pkCond);
                if (res == DBStatus::OK) updatedCount++;
            }
            cout << "Update done (" << updatedCount << " row(s))" << endl;
            return false;
        }
        string resolvedName = resolveTableName(s, tname);
        // Check for updatable view
        if (!g_engine.tableExists(s.currentDB, resolvedName) &&
            g_engine.viewExists(s.currentDB, tname)) {
            string viewBaseTable = g_engine.getViewBaseTable(s.currentDB, tname);
            string viewCheckOpt = g_engine.getViewCheckOption(s.currentDB, tname);
            if (!viewBaseTable.empty()) {
                if (!viewCheckOpt.empty()) {
                    cout << "ERROR: UPDATE on view WITH CHECK OPTION not yet supported" << endl;
                    return true;
                }
                string rewritten = rawSql;
                size_t pos = rewritten.find(tname);
                if (pos != string::npos) {
                    rewritten = rewritten.substr(0, pos) + viewBaseTable + rewritten.substr(pos + tname.size());
                    return execute(rewritten, s);
                }
            }
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!g_engine.tableExists(s.currentDB, resolvedName)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Update)) return true;
        {
            size_t wPos = sql.find("where", setPos);
            size_t lPos = sql.find("limit", setPos);
            size_t setEnd = sql.size();
            if (wPos != string::npos) setEnd = wPos;
            if (lPos != string::npos && lPos < setEnd) setEnd = lPos;
            string setClause = trim(sql.substr(setPos + 3, setEnd - setPos - 3));
            if (!isTempTable(s, tname) && !checkUpdateColumnPermission(s, tname, setClause)) return true;
        }

        size_t wherePos = sql.find("where", setPos);
        size_t setEndPos = (wherePos == std::string::npos) ? sql.size() : wherePos;
        // Strip LIMIT from set clause range before parsing
        {
            size_t tmpLimitPos = sql.find("limit", setPos);
            if (tmpLimitPos != string::npos && tmpLimitPos < setEndPos) {
                setEndPos = tmpLimitPos;
            }
        }
        auto updates = parseSetClause(sql, setPos + 3, setEndPos);
        if (updates.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        // Parse RETURNING clause
        auto [returningCols, returningAll] = parseReturningClause(sql, setPos);

        // Parse LIMIT clause
        size_t limitPos = string::npos;
        int limitVal = -1;
        {
            size_t searchStart = (wherePos != string::npos) ? wherePos : setPos;
            limitPos = sql.find("limit", searchStart);
            if (limitPos != string::npos) {
                string limitStr = trim(sql.substr(limitPos + 5));
                try { limitVal = stoi(limitStr); } catch (...) { limitVal = -1; }
            }
        }

        vector<string> conds;
        vector<string> returnedRows;
        if (wherePos != std::string::npos) {
            string whereClause = trim(sql.substr(wherePos + 5));
            // Strip LIMIT from whereClause if present
            if (limitPos != string::npos && limitPos > wherePos) {
                whereClause = trim(sql.substr(wherePos + 5, limitPos - wherePos - 5));
            }
            whereClause = expandSubqueries(whereClause, s);
            whereClause = normalizeConditionStr(whereClause);
            vector<string> tokens = tokenize(whereClause);
            tokens.insert(tokens.begin(), "(");
            tokens.push_back(")");
            for (auto& t : tokens) t = modifyLogic(t);
            auto groups = breakDownConditions(tokens);

            // If LIMIT is specified, query matching rows and update by PK
            if (limitVal > 0) {
                TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
                size_t pkIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
                }
                if (pkIdx >= tbl.len) {
                    cout << "UPDATE ... LIMIT requires table to have a primary key" << endl;
                    return true;
                }
                string pkCol = tbl.cols[pkIdx].dataName;
                size_t updatedCount = 0;
                for (const auto& g : groups) {
                    auto matchRows = g_engine.query(s.currentDB, resolvedName, g, {}, {});
                    for (size_t ri = 0; ri < matchRows.size() && updatedCount < static_cast<size_t>(limitVal); ++ri) {
                        stringstream ss(matchRows[ri]);
                        string val;
                        vector<string> cols;
                        while (ss >> val) cols.push_back(val);
                        if (cols.size() <= pkIdx) continue;
                        string pkVal = cols[pkIdx];
                        vector<string> pkCond = {"=" + pkCol + " " + pkVal};
                        cerr << "DEBUG pkCond='" << pkCond[0] << "'" << endl;
                        auto res = g_engine.update(s.currentDB, resolvedName, updates, pkCond);
                        cerr << "DEBUG update res=" << static_cast<int>(res) << endl;
                        if (res == DBStatus::OK) {
                            ++updatedCount;
                            if (!returningCols.empty() || returningAll) {
                                auto rr = g_engine.query(s.currentDB, resolvedName, pkCond,
                                                         returningAll ? set<string>() : returningCols, {});
                                for (auto& r : rr) returnedRows.push_back(r);
                            }
                        }
                    }
                }
                cout << "Update done (" << updatedCount << " row(s))" << endl;
                for (auto& row : returnedRows) cout << row << endl;
                log(s.username, "update done", getTime());
                g_engine.analyzeTable(s.currentDB, resolvedName);
                return false;
            }

            for (const auto& g : groups) {
                auto res = g_engine.update(s.currentDB, resolvedName, updates, g);
                if (res != DBStatus::OK) {
                    cout << "Update failed" << endl;
                    return true;
                }
            }
            // RETURNING: query updated rows
            if (!returningCols.empty() || returningAll) {
                for (const auto& g : groups) {
                    auto rr = g_engine.query(s.currentDB, resolvedName, g,
                                             returningAll ? set<string>() : returningCols, {});
                    for (auto& r : rr) returnedRows.push_back(r);
                }
            }
        } else {
            // No WHERE clause
            if (limitVal > 0) {
                TableSchema tbl = g_engine.getTableSchema(s.currentDB, resolvedName);
                size_t pkIdx = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].isPrimaryKey) { pkIdx = i; break; }
                }
                if (pkIdx >= tbl.len) {
                    cout << "UPDATE ... LIMIT requires table to have a primary key" << endl;
                    return true;
                }
                string pkCol = tbl.cols[pkIdx].dataName;
                size_t updatedCount = 0;
                auto matchRows = g_engine.query(s.currentDB, resolvedName, {}, {}, {});
                for (size_t ri = 0; ri < matchRows.size() && updatedCount < static_cast<size_t>(limitVal); ++ri) {
                    stringstream ss(matchRows[ri]);
                    string val;
                    vector<string> cols;
                    while (ss >> val) cols.push_back(val);
                    if (cols.size() <= pkIdx) continue;
                    string pkVal = cols[pkIdx];
                    vector<string> pkCond = {"=" + pkCol + " " + pkVal};
                    auto res = g_engine.update(s.currentDB, resolvedName, updates, pkCond);
                    if (res == DBStatus::OK) {
                        ++updatedCount;
                        if (!returningCols.empty() || returningAll) {
                            auto rr = g_engine.query(s.currentDB, resolvedName, pkCond,
                                                     returningAll ? set<string>() : returningCols, {});
                            for (auto& r : rr) returnedRows.push_back(r);
                        }
                    }
                }
                cout << "Update done (" << updatedCount << " row(s))" << endl;
                for (auto& row : returnedRows) cout << row << endl;
                log(s.username, "update done", getTime());
                g_engine.analyzeTable(s.currentDB, resolvedName);
                return false;
            }
            auto res = g_engine.update(s.currentDB, resolvedName, updates, {});
            if (res != DBStatus::OK) {
                cout << "Update failed" << endl;
                return true;
            }
            if (!returningCols.empty() || returningAll) {
                auto rr = g_engine.query(s.currentDB, resolvedName, {},
                                         returningAll ? set<string>() : returningCols, {});
                for (auto& r : rr) returnedRows.push_back(r);
            }
        }
        cout << "Update done" << endl;
        for (auto& row : returnedRows) cout << row << endl;
        log(s.username, "update done", getTime());
        g_engine.analyzeTable(s.currentDB, resolvedName);
        return false;
    }

    // DUMP DATABASE dbname TO 'file.sql'
    if (sql.substr(0, 4) == "dump") {
        if (!checkAdmin(s)) return true;
        string rest = trim(sql.substr(4));
        size_t toPos = rest.find("to ");
        if (toPos == string::npos) {
            cout << "SQL syntax error: DUMP database_name TO 'file.sql'" << endl;
            return true;
        }
        string dbname = trim(rest.substr(0, toPos));
        string filePath = stripQuotes(trim(rest.substr(toPos + 3)));
        if (filePath.empty()) {
            cout << "SQL syntax error: missing file path" << endl;
            return true;
        }
        ofstream out(filePath);
        if (!out) {
            cout << "Cannot open file: " << filePath << endl;
            return true;
        }
        auto tables = g_engine.getTableNames(dbname);
        for (const string& tname : tables) {
            TableSchema tbl = g_engine.getTableSchema(dbname, tname);
            out << "CREATE TABLE " << tname << " (";
            for (size_t i = 0; i < tbl.len; ++i) {
                if (i > 0) out << ", ";
                out << tbl.cols[i].dataName << " " << tbl.cols[i].dataType;
                if (tbl.cols[i].dsize > 0 && tbl.cols[i].dataType == "char") {
                    out << "(" << tbl.cols[i].dsize << ")";
                } else if (tbl.cols[i].dsize > 0 && tbl.cols[i].dataType == "varchar") {
                    out << "(" << tbl.cols[i].dsize << ")";
                }
                if (tbl.cols[i].isPrimaryKey) out << " PRIMARY KEY";
                if (!tbl.cols[i].isNull) out << " NOT NULL";
                if (tbl.cols[i].isUnique) out << " UNIQUE";
                if (tbl.cols[i].isAutoIncrement) out << " AUTO_INCREMENT";
            }
            out << ");\n";
            vector<string> colNames;
            for (size_t i = 0; i < tbl.len; ++i) colNames.push_back(tbl.cols[i].dataName);
            g_engine.forEachRow(dbname, tname, [&](uint32_t, uint16_t, const char* data, size_t len) {
                std::string row(data, len);
                out << "INSERT INTO " << tname << " (";
                for (size_t i = 0; i < colNames.size(); ++i) {
                    if (i > 0) out << ", ";
                    out << colNames[i];
                }
                out << ") VALUES (";
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (i > 0) out << ", ";
                    string val = dbms::StorageEngine::extractColumnValueStatic(row, tbl, i);
                    if (val.empty()) {
                        out << "NULL";
                    } else {
                        string escaped;
                        for (char c : val) {
                            if (c == '\'') escaped += "''";
                            else escaped += c;
                        }
                        out << "'" << escaped << "'";
                    }
                }
                out << ");\n";
            });
        }
        cout << "Dumped " << tables.size() << " tables to " << filePath << endl;
        return false;
    }

    // RESTORE DATABASE dbname FROM 'file.sql'
    if (sql.substr(0, 7) == "restore") {
        if (!checkAdmin(s)) return true;
        string rest = trim(sql.substr(7));
        size_t fromPos = rest.find("from ");
        if (fromPos == string::npos) {
            cout << "SQL syntax error: RESTORE database_name FROM 'file.sql'" << endl;
            return true;
        }
        string dbname = trim(rest.substr(0, fromPos));
        string filePath = stripQuotes(trim(rest.substr(fromPos + 5)));
        if (filePath.empty()) {
            cout << "SQL syntax error: missing file path" << endl;
            return true;
        }
        ifstream in(filePath);
        if (!in) {
            cout << "Cannot open file: " << filePath << endl;
            return true;
        }
        if (!g_engine.databaseExists(dbname)) {
            g_engine.createDatabase(dbname);
        }
        string line, stmt;
        int count = 0;
        while (getline(in, line)) {
            stmt += line;
            size_t semi = stmt.find(';');
            if (semi != string::npos) {
                string cmd = trim(stmt.substr(0, semi));
                stmt = stmt.substr(semi + 1);
                if (!cmd.empty()) {
                    Session tmpS = s;
                    tmpS.currentDB = dbname;
                    execute(cmd, tmpS);
                    ++count;
                }
            }
            stmt += " ";
        }
        cout << "Restored " << count << " statements to " << dbname << endl;
        return false;
    }

    if (sql.substr(0, 14) == "backup database") {
        if (!checkAdmin(s)) return true;
        string rest = trim(sql.substr(14));
        size_t toPos = rest.find("to ");
        if (toPos == string::npos) {
            cout << "SQL syntax error: BACKUP DATABASE dbname TO 'path'" << endl;
            return true;
        }
        string dbname = trim(rest.substr(0, toPos));
        string backupPath = stripQuotes(trim(rest.substr(toPos + 3)));
        if (backupPath.empty()) {
            cout << "SQL syntax error: missing backup path" << endl;
            return true;
        }
        if (g_engine.physicalBackup(dbname, backupPath)) {
            cout << "Backup completed: " << dbname << " -> " << backupPath << endl;
            log(s.username, "backup " + dbname + " to " + backupPath, getTime());
        } else {
            cout << "Backup failed" << endl;
            return true;
        }
        return false;
    }

    if (sql.substr(0, 15) == "restore database") {
        if (!checkAdmin(s)) return true;
        string rest = trim(sql.substr(15));
        size_t fromPos = rest.find("from ");
        if (fromPos == string::npos) {
            cout << "SQL syntax error: RESTORE DATABASE dbname FROM 'path'" << endl;
            return true;
        }
        string dbname = trim(rest.substr(0, fromPos));
        string backupPath = stripQuotes(trim(rest.substr(fromPos + 5)));
        if (backupPath.empty()) {
            cout << "SQL syntax error: missing backup path" << endl;
            return true;
        }
        if (g_engine.physicalRestore(dbname, backupPath)) {
            cout << "Restore completed: " << backupPath << " -> " << dbname << endl;
            log(s.username, "restore " + dbname + " from " + backupPath, getTime());
        } else {
            cout << "Restore failed" << endl;
            return true;
        }
        return false;
    }

    if (sql.substr(0, 16) == "clear plan cache") {
        std::lock_guard<std::mutex> lock(g_planCacheMutex);
        size_t cleared = g_queryPlanCache.size();
        g_queryPlanCache.clear();
        g_planCacheHits = 0;
        g_planCacheMisses = 0;
        cout << "Cleared " << cleared << " plan cache entries" << endl;
        return false;
    }

    // DROP OWNED BY owner
    if (sql.substr(0, 11) == "drop owned ") {
        if (!checkAdmin(s)) return true;
        string rest = trim(sql.substr(11));
        if (rest.substr(0, 3) != "by ") {
            cout << "SQL syntax error: DROP OWNED BY owner" << endl;
            return true;
        }
        string owner = trim(rest.substr(3));
        int count = 0;
        for (const auto& dbname : g_engine.getDatabaseNames()) {
            auto ppath = g_engine.permPath(dbname);
            if (!std::filesystem::exists(ppath)) continue;
            std::vector<std::string> lines;
            {
                std::ifstream ifs(ppath);
                std::string line;
                while (std::getline(ifs, line)) {
                    if (line.empty()) continue;
                    std::stringstream ss(line);
                    std::string u, t, p;
                    ss >> u >> t >> p;
                    if (u == owner) {
                        count++;
                        continue;
                    }
                    lines.push_back(line);
                }
            }
            std::ofstream ofs(ppath);
            for (const auto& l : lines) ofs << l << "\n";
        }
        cout << "Dropped " << count << " permissions for " << owner << endl;
        return false;
    }

    if (startsWithKeyword(sql, "drop database")) {
        return handleDropDatabaseGlobal(sql, s);
    }

    if (startsWithKeyword(sql, "drop role")) {
        return handleDropRoleGlobal(sql, s);
    }

    if (startsWithKeyword(sql, "drop group")) {
        return handleDropGroup(sql, s);
    }

    if (sql.substr(0, 15) == "drop tablespace") {
        return handleDropTablespace(sql, s);
    }

    if (sql.substr(0, 15) == "drop statistics") {
        return handleDropStatistics(sql, s);
    }

    if (sql.substr(0, 9) == "drop cast") {
        return handleDropCast(sql, s);
    }

    if (sql.substr(0, 14) == "drop collation") {
        return handleDropCollation(sql, s);
    }

    if (sql.substr(0, 15) == "drop conversion") {
        return handleDropConversion(sql, s);
    }

    if (sql.substr(0, 12) == "drop policy ") {
        return handleDropPolicy(sql, s);
    }

    {
        string rest = trim(sql.substr(4));
        string compatKind, compatPhrase;
        if (startsWithKeyword(sql, "drop") &&
            consumeCompatPrefix(rest, compatDropPrefixes(), compatKind, compatPhrase)) {
            return handleDropCompatObject(sql, s);
        }
    }

    if (sql.substr(0, 4) == "drop") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        if (g_engine.inTransaction()) {
            g_engine.commitTransaction();
            cout << "Note: DDL caused implicit commit of open transaction" << endl;
        }
        vector<string> tokens = tokenize(sql.substr(4));
        if (tokens.size() < 2) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string op = tokens[0];
        string name = tokens[1];
        if (op == "temporary") {
            if (tokens.size() < 3 || tokens[1] != "table") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string origName = tokens[2];
            if (!s.tempTables.count(origName)) {
                cout << "Temporary table " << origName << " not exist" << endl;
                return true;
            }
            string tmpName = tempTablePrefix(origName);
            auto res = g_engine.dropTable(s.currentDB, tmpName);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Temporary table " << origName << " not exist" << endl;
                return true;
            }
            s.tempTables.erase(origName);
            cout << "Temporary table dropped" << endl;
            return false;
        }
        if (op == "table") {
            // Check if it's a temp table first
            if (s.tempTables.count(name)) {
                string tmpName = tempTablePrefix(name);
                auto res = g_engine.dropTable(s.currentDB, tmpName);
                if (res != DBStatus::OK) {
                    cout << "Table " << name << " not exist" << endl;
                    return true;
                }
                s.tempTables.erase(name);
                cout << "Table dropped" << endl;
                return false;
            }
            // Check for foreign key references before dropping
            {
                auto allTables = g_engine.getTableNames(s.currentDB);
                for (const auto& tbl : allTables) {
                    if (tbl == name) continue;
                    auto schema = g_engine.getTableSchema(s.currentDB, tbl);
                    for (size_t i = 0; i < schema.fkLen; ++i) {
                        if (schema.fks[i].refTable == name) {
                            cout << "Cannot drop table " << name
                                 << ": referenced by foreign key in table " << tbl << endl;
                            return true;
                        }
                    }
                }
            }
            auto res = g_engine.dropTable(s.currentDB, name);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Table " << name << " not exist" << endl;
                return true;
            }
            cout << "Table dropped" << endl;
            return false;
        }
        if (op == "domain") {
            auto res = g_engine.dropDomain(s.currentDB, name);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Domain " << name << " not exist" << endl;
                return true;
            }
            cout << "Domain dropped" << endl;
            return false;
        }
        if (op == "type") {
            auto res = g_engine.dropCompositeType(s.currentDB, name);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Type " << name << " not exist" << endl;
                return true;
            }
            cout << "Type dropped" << endl;
            return false;
        }
        if (op == "sequence") {
            auto res = g_engine.dropSequence(s.currentDB, name);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Sequence " << name << " not exist" << endl;
                return true;
            }
            cout << "Sequence dropped" << endl;
            return false;
        }
        if (op == "user") {
            ifstream infile("user.dat");
            vector<user> users;
            bool found = false;
            if (infile) {
                user temp;
                while (infile >> temp.username >> temp.password >> temp.permission) {
                    if (temp.username == name) found = true;
                    else users.push_back(temp);
                }
            }
            ofstream outfile("user.dat");
            for (size_t i = 0; i < users.size(); ++i) {
                if (i > 0) outfile << '\n';
                outfile << users[i].username << " " << users[i].password << " " << users[i].permission;
            }
            outfile << endl;
            if (!found) {
                cout << "User " << name << " not exist" << endl;
                return true;
            }
            cout << "User dropped" << endl;
            return false;
        }
        if (op == "role") {
            if (!dropRole(name)) {
                cout << "Role " << name << " not exist" << endl;
                return true;
            }
            cout << "Role dropped" << endl;
            return false;
        }
        if (op == "schema") {
            bool cascade = false;
            if (tokens.size() >= 3 && tokens[2] == "cascade") cascade = true;
            auto res = g_engine.dropSchema(s.currentDB, name, cascade);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "Schema " << name << " not exist" << endl;
                return true;
            }
            cout << "Schema " << name << " dropped" << (cascade ? " (cascade)" : "") << endl;
            return false;
        }
        if (op == "database") {
            auto res = g_engine.dropDatabase(name);
            if (res == DBStatus::DATABASE_NOT_FOUND) {
                cout << "Database " << name << " not exist" << endl;
                return true;
            }
            auto dbOptions = loadDatabaseOptions();
            if (dbOptions.erase(name) > 0) saveDatabaseOptions(dbOptions);
            if (s.currentDB == name) s.currentDB.clear();
            cout << "Database dropped" << endl;
            log(s.username, "database dropped", getTime());
            return false;
        }
        if (op == "index") {
            // drop index idxname on tname
            if (tokens.size() < 3 || tokens[2] != "on") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tnameOrig = tokens[3];
            string tname = resolveTableName(s, tnameOrig);
            // Check GIN/GiST/BRIN/SP-GiST first (they have explicit has* checks)
            if (g_engine.hasGinIndex(s.currentDB, tname, name)) {
                g_engine.dropGinIndex(s.currentDB, tname, name);
                cout << "Index dropped" << endl;
                return false;
            }
            if (g_engine.hasGiSTIndex(s.currentDB, tname, name)) {
                g_engine.dropGiSTIndex(s.currentDB, tname, name);
                cout << "Index dropped" << endl;
                return false;
            }
            if (g_engine.hasBrinIndex(s.currentDB, tname, name)) {
                g_engine.dropBrinIndex(s.currentDB, tname, name);
                cout << "Index dropped" << endl;
                return false;
            }
            if (g_engine.hasSPGiSTIndex(s.currentDB, tname, name)) {
                g_engine.dropSPGiSTIndex(s.currentDB, tname, name);
                cout << "Index dropped" << endl;
                return false;
            }
            // Check if table exists
            if (!g_engine.tableExists(s.currentDB, tname)) {
                cout << "Table " << tname << " not exist" << endl;
                return true;
            }
            // For composite/single-column/hash indexes, check metadata first
            bool indexExists = false;
            auto idxMeta = g_engine.getIndexedColumns(s.currentDB, tname);
            for (const auto& entry : idxMeta) {
                if (entry == name) { indexExists = true; break; }
            }
            if (!indexExists) {
                // Try composite index name match
                auto compIdx = g_engine.getCompositeIndexes(s.currentDB, tname);
                for (const auto& e : compIdx) {
                    if (e.name == name) { indexExists = true; break; }
                }
            }
            if (!indexExists) {
                cout << "Index " << name << " not exist on table " << tname << endl;
                return true;
            }
            // Try composite index, single-column index, hash index
            auto res = g_engine.dropCompositeIndex(s.currentDB, tname, name);
            if (res != DBStatus::OK) {
                res = g_engine.dropIndex(s.currentDB, tname, name);
            }
            if (res != DBStatus::OK) {
                res = g_engine.dropHashIndex(s.currentDB, tname, name);
            }
            if (res != DBStatus::OK) {
                cout << "Drop index failed" << endl;
                return true;
            }
            cout << "Index dropped" << endl;
            return false;
        }
        if (op == "fulltext") {
            if (tokens.size() < 3 || tokens[1] != "index" || tokens.size() < 5 || tokens[3] != "on") {
                cout << "SQL syntax error: DROP FULLTEXT INDEX idx ON t" << endl;
                return true;
            }
            string idxName = tokens[2];
            string tname = resolveTableName(s, tokens[4]);
            if (!g_engine.hasFullTextIndex(s.currentDB, tname, idxName)) {
                cout << "Index " << idxName << " not exist on table " << tname << endl;
                return true;
            }
            g_engine.dropFullTextIndex(s.currentDB, tname, idxName);
            cout << "Fulltext index dropped" << endl;
            return false;
        }
        if (op == "gin" || op == "gist" || op == "brin" || op == "spgist") {
            if (tokens.size() < 3 || tokens[1] != "index" || tokens.size() < 5 || tokens[3] != "on") {
                cout << "SQL syntax error: DROP GIN/GiST/BRIN/SP-GiST INDEX idx ON t" << endl;
                return true;
            }
            string idxName = tokens[2];
            string tname = resolveTableName(s, tokens[4]);
            bool hasIdx = false;
            if (op == "gin") hasIdx = g_engine.hasGinIndex(s.currentDB, tname, idxName);
            else if (op == "gist") hasIdx = g_engine.hasGiSTIndex(s.currentDB, tname, idxName);
            else if (op == "brin") hasIdx = g_engine.hasBrinIndex(s.currentDB, tname, idxName);
            else if (op == "spgist") hasIdx = g_engine.hasSPGiSTIndex(s.currentDB, tname, idxName);
            if (!hasIdx) {
                cout << "Index " << idxName << " not exist on table " << tname << endl;
                return true;
            }
            if (op == "gin") {
                g_engine.dropGinIndex(s.currentDB, tname, idxName);
            } else if (op == "gist") {
                g_engine.dropGiSTIndex(s.currentDB, tname, idxName);
            } else if (op == "brin") {
                g_engine.dropBrinIndex(s.currentDB, tname, idxName);
            } else {
                g_engine.dropSPGiSTIndex(s.currentDB, tname, idxName);
            }
            cout << "Index dropped" << endl;
            return false;
        }
        if (op == "view") {
            auto res = g_engine.dropView(s.currentDB, name);
            if (res == DBStatus::TABLE_NOT_FOUND) {
                cout << "View " << name << " not exist" << endl;
                return true;
            }
            cout << "View dropped" << endl;
            return false;
        }
        if (op == "materialized") {
            if (tokens.size() < 3 || tokens[1] != "view") {
                cout << "SQL syntax error: DROP MATERIALIZED VIEW viewname" << endl;
                return true;
            }
            string viewname = tokens[2];
            if (!g_engine.isMaterializedView(s.currentDB, viewname)) {
                cout << "Materialized view " << viewname << " not exist" << endl;
                return true;
            }
            g_engine.dropMaterializedView(s.currentDB, viewname);
            cout << "Materialized view " << viewname << " dropped" << endl;
            return false;
        }
        if (op == "trigger") {
            auto res = g_engine.dropTrigger(s.currentDB, name);
            if (res != DBStatus::OK) {
                cout << "Trigger " << name << " not exist" << endl;
                return true;
            }
            cout << "Trigger dropped" << endl;
            return false;
        }
        if (op == "procedure") {
            if (!g_engine.procedureExists(s.currentDB, name)) {
                cout << "Procedure " << name << " not exist" << endl;
                return true;
            }
            g_engine.dropProcedure(s.currentDB, name);
            cout << "Procedure " << name << " dropped" << endl;
            return false;
        }
        if (op == "function") {
            if (g_engine.udfExists(s.currentDB, name)) {
                g_engine.dropUDF(s.currentDB, name);
                cout << "Function " << name << " dropped" << endl;
                return false;
            }
            if (g_engine.tvfExists(s.currentDB, name)) {
                g_engine.dropTVF(s.currentDB, name);
                cout << "Function " << name << " dropped" << endl;
                return false;
            }
            cout << "Function " << name << " not exist" << endl;
            return true;
        }
        cout << "SQL syntax error" << endl;
        return true;
    }

    // REFRESH MATERIALIZED VIEW viewname
    if (sql.substr(0, 7) == "refresh") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(7));
        if (rest.substr(0, 12) == "materialized") {
            string viewname = trim(rest.substr(12));
            size_t vPos = toLower(viewname).find("view ");
            if (vPos != string::npos) {
                viewname = trim(viewname.substr(vPos + 5));
            }
            // Optional CONCURRENTLY (we always refresh synchronously).
            if (toLower(viewname).substr(0, 13) == "concurrently ") {
                viewname = trim(viewname.substr(13));
            }
            // Optional trailing WITH [NO] DATA.
            bool refreshWithData = true;
            {
                string lv = toLower(viewname);
                if (lv.size() >= 12 && lv.substr(lv.size() - 12) == "with no data") {
                    refreshWithData = false;
                    viewname = trim(viewname.substr(0, viewname.size() - 12));
                } else if (lv.size() >= 9 && lv.substr(lv.size() - 9) == "with data") {
                    viewname = trim(viewname.substr(0, viewname.size() - 9));
                }
            }
            if (!g_engine.isMaterializedView(s.currentDB, viewname)) {
                cout << "Materialized view " << viewname << " not exist" << endl;
                return true;
            }
            string viewSql = g_engine.getMaterializedViewSQL(s.currentDB, viewname);
            if (viewSql.empty()) {
                cout << "Materialized view SQL not found" << endl;
                return true;
            }
            // Parse and re-execute the query. Column resolution is derived from
            // the backing table schema below (robust for SELECT * and aliases),
            // so here we only need the source table + WHERE conditions.
            string lsql = toLower(viewSql);
            size_t fromPos = lsql.find(" from ");
            if (fromPos == string::npos) {
                cout << "Invalid materialized view SQL" << endl;
                return true;
            }
            string inner = trim(viewSql.substr(fromPos + 5));
            size_t wPos = toLower(inner).find(" where ");
            size_t oPos = toLower(inner).find(" order by ");
            size_t lPos = toLower(inner).find(" limit ");
            string tname = trim(inner.substr(0,
                min(wPos != string::npos ? wPos : inner.size(),
                    min(oPos != string::npos ? oPos : inner.size(),
                        lPos != string::npos ? lPos : inner.size()))));
            tname = resolveTableName(s, tname);
            vector<string> conds;
            if (wPos != string::npos) {
                size_t condEnd = min(oPos != string::npos ? oPos : inner.size(),
                                    lPos != string::npos ? lPos : inner.size());
                string condStr = normalizeConditionStr(trim(inner.substr(wPos + 6, condEnd - wPos - 6)));
                if (!condStr.empty()) {
                    vector<string> rawConds = splitConds(condStr);
                    for (auto& c : rawConds) {
                        string mc = modifyLogic(c);
                        if (!mc.empty()) conds.push_back(mc);
                    }
                }
            }
            string backingTable = dbms::StorageEngine::materializedViewPrefix(viewname);
            // Derive the view's columns from the backing table schema (created
            // with the correct projected source column names) and map query
            // output (source schema order) to those columns.
            TableSchema backingSchema = g_engine.getTableSchema(s.currentDB, backingTable);
            set<string> queryCols;
            for (size_t i = 0; i < backingSchema.len; ++i)
                queryCols.insert(backingSchema.cols[i].dataName);
            TableSchema srcSchema = g_engine.getTableSchema(s.currentDB, tname);
            vector<string> orderedCols;
            for (size_t i = 0; i < srcSchema.len; ++i)
                if (queryCols.count(srcSchema.cols[i].dataName))
                    orderedCols.push_back(srcSchema.cols[i].dataName);

            // Clear backing table and (unless WITH NO DATA) re-populate.
            g_engine.remove(s.currentDB, backingTable, {});
            size_t refreshed = 0;
            if (refreshWithData) {
                auto results = g_engine.query(s.currentDB, tname, conds, queryCols, {}, false, false);
                for (const auto& row : results) {
                    stringstream ss(row);
                    map<string, string> values;
                    string val;
                    size_t idx = 0;
                    while (ss >> val && idx < orderedCols.size()) {
                        values[orderedCols[idx]] = val;
                        ++idx;
                    }
                    if (idx != orderedCols.size()) continue;
                    if (g_engine.insert(s.currentDB, backingTable, values) == DBStatus::OK) ++refreshed;
                }
            }
            cout << "Materialized view " << viewname << " refreshed (" << refreshed << " rows)" << endl;
            return false;
        }
        cout << "SQL syntax error: REFRESH MATERIALIZED VIEW viewname" << endl;
        return true;
    }

    // UNION / UNION ALL — skip matches inside parentheses (e.g., WITH clauses)
    auto findSetOp = [&](const string& kw) -> size_t {
        size_t p = sql.find(kw);
        while (p != string::npos) {
            int depth = 0;
            for (size_t i = 0; i < p; ++i) {
                if (sql[i] == '(') depth++;
                else if (sql[i] == ')') depth--;
            }
            if (depth == 0) return p;
            p = sql.find(kw, p + 1);
        }
        return string::npos;
    };

    size_t unionAllPos = findSetOp("union all");
    size_t unionPos = findSetOp("union");
    bool isUnionAll = false;
    size_t actualUnionPos = string::npos;
    if (unionAllPos != string::npos) {
        isUnionAll = true;
        actualUnionPos = unionAllPos;
    } else if (unionPos != string::npos) {
        actualUnionPos = unionPos;
    }
    if (actualUnionPos != string::npos) {
        if (!checkDB(s)) return true;
        string leftSql = trim(sql.substr(0, actualUnionPos));
        string rightSql = trim(sql.substr(actualUnionPos + (isUnionAll ? 9 : 5)));
        if (leftSql.empty() || rightSql.empty()) {
            cout << "SQL syntax error: invalid UNION" << endl;
            return true;
        }
        // Execute left query, capture output
        auto* oldBuf = cout.rdbuf();
        stringstream leftSs;
        cout.rdbuf(leftSs.rdbuf());
        execute(leftSql, s);
        cout.rdbuf(oldBuf);
        // Execute right query, capture output
        stringstream rightSs;
        cout.rdbuf(rightSs.rdbuf());
        execute(rightSql, s);
        cout.rdbuf(oldBuf);
        // Parse outputs: first line is header, rest are data rows
        vector<string> leftLines, rightLines;
        string line;
        while (getline(leftSs, line)) leftLines.push_back(line);
        while (getline(rightSs, line)) rightLines.push_back(line);
        // Print header from left query
        if (!leftLines.empty()) {
            cout << leftLines[0] << endl;
        } else if (!rightLines.empty()) {
            cout << rightLines[0] << endl;
        }
        // Merge data rows (skip headers)
        set<string> seen;
        for (size_t i = 1; i < leftLines.size(); ++i) {
            if (isUnionAll || seen.insert(leftLines[i]).second) {
                cout << leftLines[i] << endl;
            }
        }
        for (size_t i = 1; i < rightLines.size(); ++i) {
            if (isUnionAll || seen.insert(rightLines[i]).second) {
                cout << rightLines[i] << endl;
            }
        }
        return false;
    }

    // INTERSECT
    size_t intersectPos = findSetOp("intersect");
    if (intersectPos != string::npos) {
        if (!checkDB(s)) return true;
        string leftSql = trim(sql.substr(0, intersectPos));
        string rightSql = trim(sql.substr(intersectPos + 9));
        if (leftSql.empty() || rightSql.empty()) {
            cout << "SQL syntax error: invalid INTERSECT" << endl;
            return true;
        }
        auto* oldBuf = cout.rdbuf();
        stringstream leftSs;
        cout.rdbuf(leftSs.rdbuf());
        execute(leftSql, s);
        cout.rdbuf(oldBuf);
        stringstream rightSs;
        cout.rdbuf(rightSs.rdbuf());
        execute(rightSql, s);
        cout.rdbuf(oldBuf);
        vector<string> leftLines, rightLines;
        string line;
        while (getline(leftSs, line)) leftLines.push_back(line);
        while (getline(rightSs, line)) rightLines.push_back(line);
        if (!leftLines.empty()) cout << leftLines[0] << endl;
        else if (!rightLines.empty()) cout << rightLines[0] << endl;
        set<string> rightSet;
        for (size_t i = 1; i < rightLines.size(); ++i) rightSet.insert(rightLines[i]);
        for (size_t i = 1; i < leftLines.size(); ++i) {
            if (rightSet.find(leftLines[i]) != rightSet.end()) {
                cout << leftLines[i] << endl;
            }
        }
        return false;
    }

    // EXCEPT / MINUS
    size_t exceptPos = findSetOp("except");
    size_t minusPos = findSetOp("minus");
    size_t actualExceptPos = string::npos;
    if (exceptPos != string::npos) actualExceptPos = exceptPos;
    else if (minusPos != string::npos) actualExceptPos = minusPos;
    if (actualExceptPos != string::npos) {
        if (!checkDB(s)) return true;
        string leftSql = trim(sql.substr(0, actualExceptPos));
        string rightSql = trim(sql.substr(actualExceptPos + (exceptPos != string::npos ? 6 : 5)));
        if (leftSql.empty() || rightSql.empty()) {
            cout << "SQL syntax error: invalid EXCEPT" << endl;
            return true;
        }
        auto* oldBuf = cout.rdbuf();
        stringstream leftSs;
        cout.rdbuf(leftSs.rdbuf());
        execute(leftSql, s);
        cout.rdbuf(oldBuf);
        stringstream rightSs;
        cout.rdbuf(rightSs.rdbuf());
        execute(rightSql, s);
        cout.rdbuf(oldBuf);
        vector<string> leftLines, rightLines;
        string line;
        while (getline(leftSs, line)) leftLines.push_back(line);
        while (getline(rightSs, line)) rightLines.push_back(line);
        if (!leftLines.empty()) cout << leftLines[0] << endl;
        else if (!rightLines.empty()) cout << rightLines[0] << endl;
        set<string> rightSet;
        for (size_t i = 1; i < rightLines.size(); ++i) rightSet.insert(rightLines[i]);
        for (size_t i = 1; i < leftLines.size(); ++i) {
            if (rightSet.find(leftLines[i]) == rightSet.end()) {
                cout << leftLines[i] << endl;
            }
        }
        return false;
    }

    // SHOW CONNECTIONS / SHOW STATUS
    if (sql.substr(0, 5) == "show ") {
        string rest = trim(sql.substr(5));
        if (rest == "connections") {
            auto& s = dbms::getServerStats();
            cout << "active_connections: " << s.activeConnections.load() << endl;
            cout << "total_connections: " << s.totalConnections.load() << endl;
            cout << "max_connections: " << s.maxConnections.load() << endl;
            cout << "rejected_connections: " << s.rejectedConnections.load() << endl;
            return false;
        }
        if (rest == "processlist") {
            auto procs = dbms::getProcessList();
            cout << "id user host db command time state info" << endl;
            for (const auto& p : procs) {
                cout << p.id << " " << p.user << " " << p.host << " "
                     << p.db << " " << p.command << " " << std::fixed << std::setprecision(2)
                     << p.timeSec << " " << p.state << " " << p.info << endl;
            }
            return false;
        }
        if (rest == "statements") {
            auto stats = getSqlStats(s.currentDB);
            cout << "query calls total_time min_time max_time mean_time dbname " << endl;
            for (const auto& st : stats) {
                std::string q = st.sql;
                std::string compact;
                bool lastSpace = false;
                for (char c : q) {
                    if (isspace(static_cast<unsigned char>(c))) {
                        if (!lastSpace) compact += ' ';
                        lastSpace = true;
                    } else {
                        compact += c;
                        lastSpace = false;
                    }
                }
                if (compact.size() > 60) compact = compact.substr(0, 57) + "...";
                cout << compact << " " << st.calls << " "
                     << std::fixed << std::setprecision(2) << st.totalTimeMs << " "
                     << st.minTimeMs << " " << st.maxTimeMs << " " << st.meanTimeMs << " "
                     << st.dbname << endl;
            }
            return false;
        }
        if (rest == "status") {
            auto& s = dbms::getServerStats();
            cout << "active_connections " << s.activeConnections.load() << endl;
            cout << "total_connections " << s.totalConnections.load() << endl;
            cout << "max_connections " << s.maxConnections.load() << endl;
            cout << "rejected_connections " << s.rejectedConnections.load() << endl;
            auto bpStats = g_engine.getBufferPoolStats();
            cout << "buffer_pool_hits " << bpStats.totalHits << endl;
            cout << "buffer_pool_misses " << bpStats.totalMisses << endl;
            cout << "buffer_pool_hit_rate " << std::fixed << std::setprecision(2) << bpStats.hitRate << "%" << endl;
            {
                std::lock_guard<std::mutex> lock(g_planCacheMutex);
                cout << "plan_cache_size " << g_queryPlanCache.size() << endl;
                cout << "plan_cache_hits " << g_planCacheHits << endl;
                cout << "plan_cache_misses " << g_planCacheMisses << endl;
                size_t total = g_planCacheHits + g_planCacheMisses;
                cout << "plan_cache_hit_rate " << std::fixed << std::setprecision(2)
                     << (total == 0 ? 0.0 : 100.0 * static_cast<double>(g_planCacheHits) / static_cast<double>(total)) << "%" << endl;
            }
            return false;
        }
        if (rest == "plan cache") {
            std::lock_guard<std::mutex> lock(g_planCacheMutex);
            if (g_queryPlanCache.empty()) {
                cout << "Plan cache is empty" << endl;
                return false;
            }
            cout << "sql plan" << endl;
            for (const auto& kv : g_queryPlanCache) {
                cout << kv.first << endl;
            }
            return false;
        }
        if (rest == "variables") {
            g_config.printAll();
            return false;
        }
        if (startsWithKeyword(rest, "compat objects") ||
            startsWithKeyword(rest, "compatibility objects") ||
            startsWithKeyword(rest, "pg compat objects")) {
            showCompatObjects(s, rest);
            return false;
        }
        if (rest == "domains") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getDomainNames(s.currentDB);
            if (names.empty()) {
                cout << "No domains found" << endl;
                return false;
            }
            cout << "domain_name base_type default check constraint" << endl;
            for (const auto& n : names) {
                auto d = g_engine.getDomain(s.currentDB, n);
                cout << d.name << " " << d.baseType << " "
                     << (d.defaultValue.empty() ? "-" : d.defaultValue) << " "
                     << (d.checkExpr.empty() ? "-" : d.checkExpr) << " "
                     << (d.constraintName.empty() ? "-" : d.constraintName) << endl;
            }
            return false;
        }
        if (rest == "types") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getCompositeTypeNames(s.currentDB);
            if (names.empty()) {
                cout << "No types found" << endl;
                return false;
            }
            cout << "type_name attributes" << endl;
            for (const auto& n : names) {
                auto ct = g_engine.getCompositeType(s.currentDB, n);
                vector<string> attrs;
                for (const auto& f : ct.fields) attrs.push_back(f.first + ":" + f.second);
                cout << ct.name << " " << joinStrings(attrs, ",") << endl;
            }
            return false;
        }
        if (rest.substr(0, 11) == "dead tuples") {
            string tname = trim(rest.substr(11));
            if (tname.substr(0, 5) == "from ") tname = trim(tname.substr(5));
            if (tname.empty()) {
                cout << "Usage: SHOW DEAD TUPLES FROM table_name" << endl;
                return true;
            }
            string resolved = resolveTableName(s, tname);
            if (!g_engine.tableExists(s.currentDB, resolved)) {
                cout << "Table " << tname << " not exist" << endl;
                return true;
            }
            size_t count = g_engine.getDeadTupleCount(s.currentDB, resolved);
            cout << "dead_tuples " << count << endl;
            cout << "auto_vacuum_threshold " << g_config.autoVacuumThreshold << endl;
            return false;
        }
        if (rest == "deadlocks") {
            auto entries = g_engine.getLockManager().getDeadlockLog();
            if (entries.empty()) {
                cout << "No deadlocks recorded" << endl;
                return false;
            }
            cout << "timestamp description" << endl;
            for (const auto& e : entries) {
                cout << e.timestamp << " " << e.description << endl;
            }
            return false;
        }
        if (rest == "locks") {
            auto holds = g_engine.getLockManager().getLockHolds();
            auto waits = g_engine.getLockManager().getLockWaits();
            cout << "resource holder mode" << endl;
            for (const auto& h : holds) {
                cout << h.resource << " " << h.holderTid << " " << h.mode << endl;
            }
            if (!waits.empty()) {
                cout << "--- waits ---" << endl;
                cout << "waiter holders" << endl;
                for (const auto& w : waits) {
                    cout << w.waiterTid << " ";
                    for (const auto& h : w.holderTids) cout << h << " ";
                    cout << endl;
                }
            }
            return false;
        }
        if (rest == "databases") {
            auto names = g_engine.getDatabaseNames();
            for (const auto& n : names) cout << n << endl;
            return false;
        }
        if (rest.substr(0, 6) == "grants") {
            string targetUser = s.username;
            string after = trim(rest.substr(6));
            if (after.substr(0, 3) == "for") {
                string reqUser = trim(after.substr(3));
                if (!reqUser.empty()) {
                    if (!checkAdmin(s)) return true;
                    targetUser = reqUser;
                }
            }
            if (!checkDB(s)) return true;
            auto tables = g_engine.getTableNames(s.currentDB);
            bool hasAny = false;
            for (const auto& tname : tables) {
                auto perms = g_engine.getUserPermissions(s.currentDB, tname, targetUser);
                if (!perms.empty()) {
                    hasAny = true;
                    for (const auto& p : perms) {
                        cout << "GRANT " << p << " ON " << tname << " TO " << targetUser << endl;
                    }
                }
            }
            if (!hasAny) {
                cout << "No grants found for user " << targetUser << endl;
            }
            return false;
        }
        if (rest == "tables") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getTableNames(s.currentDB);
            for (const auto& n : names) cout << n << endl;
            return false;
        }
        if (rest == "views") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getViewNames(s.currentDB);
            for (const auto& n : names) {
                string baseTable = g_engine.getViewBaseTable(s.currentDB, n);
                cout << n;
                if (!baseTable.empty()) cout << " (updatable -> " << baseTable << ")";
                cout << endl;
            }
            return false;
        }
        if (rest == "materialized views") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getMaterializedViewNames(s.currentDB);
            for (const auto& n : names) cout << n << endl;
            return false;
        }
        if (rest == "procedures") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getProcedureNames(s.currentDB);
            for (const auto& n : names) {
                auto stmts = g_engine.getProcedureStatements(s.currentDB, n);
                cout << n << " (" << stmts.size() << " statements)" << endl;
            }
            return false;
        }
        if (rest == "functions") {
            if (!checkDB(s)) return true;
            auto udfNames = g_engine.getUDFNames(s.currentDB);
            for (const auto& n : udfNames) {
                auto udf = g_engine.getUDF(s.currentDB, n);
                string sig;
                for (size_t i = 0; i < udf.paramNames.size(); ++i) {
                    if (i > 0) sig += ", ";
                    sig += udf.paramNames[i];
                    if (i < udf.paramTypes.size() && !udf.paramTypes[i].empty()) sig += " " + udf.paramTypes[i];
                }
                if (sig.empty()) sig = udf.paramName;
                cout << n << "(" << sig << ") = " << udf.expression << endl;
            }
            auto tvfNames = g_engine.getTVFNames(s.currentDB);
            for (const auto& n : tvfNames) {
                auto param = g_engine.getTVFParam(s.currentDB, n);
                cout << n << "(" << param << ") RETURNS TABLE" << endl;
            }
            return false;
        }
        if (rest.substr(0, 7) == "columns") {
            if (!checkDB(s)) return true;
            string colsRest = trim(rest.substr(7));
            size_t fromPos = colsRest.find("from ");
            size_t inPos = colsRest.find("in ");
            string tname;
            if (fromPos != string::npos) tname = trim(colsRest.substr(fromPos + 5));
            else if (inPos != string::npos) tname = trim(colsRest.substr(inPos + 3));
            else tname = colsRest;
            tname = resolveTableName(s, tname);
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
            if (tbl.len == 0) {
                cout << "Table not exist" << endl;
                return true;
            }
            cout << "field type null key default extra" << endl;
            for (size_t i = 0; i < tbl.len; ++i) {
                const auto& c = tbl.cols[i];
                cout << c.dataName << ' '
                     << c.dataType << ' '
                     << (c.isNull ? "yes" : "no") << ' '
                     << (c.isPrimaryKey ? "pri" : "") << ' '
                     << c.defaultValue << ' '
                     << (c.isAutoIncrement ? "auto_increment" : "")
                     << endl;
            }
            return false;
        }
        if (rest == "indexes") {
            if (!checkDB(s)) return true;
            cout << "table column type include where" << endl;
            auto tables = g_engine.getTableNames(s.currentDB);
            for (const auto& tname : tables) {
                auto idxMeta = g_engine.getIndexMetadata(s.currentDB, tname);
                for (const auto& meta : idxMeta) {
                    string incStr;
                    for (size_t i = 0; i < meta.includeCols.size(); ++i) {
                        if (i > 0) incStr += ",";
                        incStr += meta.includeCols[i];
                    }
                    string idxType = meta.isExpression ? "expression" : "bptree";
                    cout << tname << " " << meta.name << " " << idxType << " " << incStr;
                    if (!meta.whereCondition.empty()) cout << " " << meta.whereCondition;
                    cout << endl;
                }
                auto hashIdx = g_engine.getHashIndexedColumns(s.currentDB, tname);
                for (const auto& col : hashIdx) {
                    cout << tname << " " << col << " hash " << endl;
                }
                auto compIdx = g_engine.getCompositeIndexes(s.currentDB, tname);
                for (const auto& ci : compIdx) {
                    string cols;
                    for (size_t i = 0; i < ci.columns.size(); ++i) {
                        if (i > 0) cols += ",";
                        cols += ci.columns[i];
                    }
                    cout << tname << " " << cols << " composite ";
                    if (!ci.whereCondition.empty()) cout << ci.whereCondition;
                    cout << endl;
                }
                auto ftCols = g_engine.getFullTextIndexedColumns(s.currentDB, tname);
                for (const auto& col : ftCols) {
                    cout << tname << " " << col << " fulltext " << endl;
                }
            }
            return false;
        }
        if (rest == "triggers") {
            if (!checkDB(s)) return true;
            auto triggers = g_engine.getAllTriggers(s.currentDB);
            if (triggers.empty()) {
                cout << "No triggers" << endl;
                return false;
            }
            cout << "name timing event table action" << endl;
            for (const auto& trg : triggers) {
                cout << trg.name << ' '
                     << trg.timing << ' '
                     << trg.event << ' '
                     << trg.tableName << ' '
                     << trg.action << endl;
            }
            return false;
        }
        if (rest == "slow log") {
            std::lock_guard<std::mutex> lock(g_slowQueryMutex);
            if (g_slowQueryBuffer.empty()) {
                cout << "No slow queries recorded" << endl;
                return false;
            }
            cout << "timestamp user db ms sql" << endl;
            for (const auto& e : g_slowQueryBuffer) {
                cout << e.timestamp << ' '
                     << e.username << ' '
                     << e.dbname << ' '
                     << std::fixed << std::setprecision(2) << e.ms << ' '
                     << e.sql << endl;
            }
            return false;
        }
        if (rest.substr(0, 5) == "stats") {
            if (!checkDB(s)) return true;
            string statsRest = trim(rest.substr(5));
            size_t forPos = statsRest.find("for ");
            string tname = (forPos != string::npos) ? trim(statsRest.substr(forPos + 4)) : statsRest;
            tname = resolveTableName(s, tname);
            size_t rowCount = g_engine.getTableRowCount(s.currentDB, tname);
            cout << "table: " << tname << "  rows: " << rowCount << endl;
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
            for (size_t i = 0; i < tbl.len; ++i) {
                auto cs = g_engine.getColumnStats(s.currentDB, tname, tbl.cols[i].dataName);
                cout << "  column: " << tbl.cols[i].dataName
                     << "  cardinality: " << cs.cardinality
                     << "  min: " << cs.minVal
                     << "  max: " << cs.maxVal;
                if (!cs.mcv.empty()) {
                    cout << "  mcv: ";
                    for (size_t j = 0; j < cs.mcv.size() && j < 5; ++j) {
                        if (j > 0) cout << ", ";
                        cout << cs.mcv[j].first << "(" << cs.mcv[j].second << ")";
                    }
                }
                cout << endl;
            }
            // Show multi-column stats
            auto schema = g_engine.getTableSchema(s.currentDB, tname);
            auto spath = g_engine.statsPath(s.currentDB);
            if (std::filesystem::exists(spath)) {
                std::ifstream ifs(spath);
                std::string line;
                bool hasMulti = false;
                while (std::getline(ifs, line)) {
                    if (line.empty()) continue;
                    std::stringstream ss(line);
                    std::string t, tag, key;
                    ss >> t >> tag >> key;
                    if (t == tname && tag == "__multi__") {
                        if (!hasMulti) { cout << "  multi-column stats:" << endl; hasMulti = true; }
                        auto mcs = g_engine.getMultiColumnStats(s.currentDB, tname, key);
                        cout << "    " << key << "  cardinality: " << mcs.cardinality;
                        if (!mcs.mcv.empty()) {
                            cout << "  mcv: ";
                            for (size_t j = 0; j < mcs.mcv.size() && j < 5; ++j) {
                                if (j > 0) cout << ", ";
                                cout << mcs.mcv[j].first << "(" << mcs.mcv[j].second << ")";
                            }
                        }
                        cout << endl;
                    }
                }
            }
            return false;
        }
        if (rest.substr(0, 12) == "create table") {
            if (!checkDB(s)) return true;
            string tname = trim(rest.substr(12));
            tname = resolveTableName(s, tname);
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
            if (tbl.len == 0) {
                cout << "Table not exist" << endl;
                return true;
            }
            string ddl = "CREATE TABLE " + tname + " (";
            for (size_t i = 0; i < tbl.len; ++i) {
                if (i > 0) ddl += ", ";
                const auto& c = tbl.cols[i];
                ddl += c.dataName + " " + c.dataType;
                if (!c.isNull) ddl += " NOT NULL";
                if (c.isPrimaryKey) ddl += " PRIMARY KEY";
                if (c.isUnique) ddl += " UNIQUE";
                if (c.isAutoIncrement) ddl += " AUTO_INCREMENT";
                if (!c.defaultValue.empty()) ddl += " DEFAULT '" + c.defaultValue + "'";
            }
            // Primary key
            vector<string> pkCols;
            for (size_t i = 0; i < tbl.len; ++i) {
                if (tbl.cols[i].isPrimaryKey) pkCols.push_back(tbl.cols[i].dataName);
            }
            if (!pkCols.empty()) {
                ddl += ", PRIMARY KEY (";
                for (size_t i = 0; i < pkCols.size(); ++i) {
                    if (i > 0) ddl += ", ";
                    ddl += pkCols[i];
                }
                ddl += ")";
            }
            // Foreign keys
            for (size_t fi = 0; fi < tbl.fkLen; ++fi) {
                const auto& fk = tbl.fks[fi];
                ddl += ", FOREIGN KEY (";
                for (size_t i = 0; i < fk.colNames.size(); ++i) {
                    if (i > 0) ddl += ", ";
                    ddl += fk.colNames[i];
                }
                ddl += ") REFERENCES " + fk.refTable + " (";
                for (size_t i = 0; i < fk.refCols.size(); ++i) {
                    if (i > 0) ddl += ", ";
                    ddl += fk.refCols[i];
                }
                ddl += ")";
                if (!fk.onDelete.empty()) ddl += " ON DELETE " + fk.onDelete;
                if (!fk.onUpdate.empty() && fk.onUpdate != "restrict") ddl += " ON UPDATE " + fk.onUpdate;
            }
            ddl += ")";
            cout << ddl << endl;
            return false;
        }
        if (rest.substr(0, 5) == "index") {
            if (!checkDB(s)) return true;
            string idxRest = trim(rest.substr(5));
            size_t fromPos = idxRest.find("from ");
            size_t inPos = idxRest.find("in ");
            string tname;
            if (fromPos != string::npos) tname = trim(idxRest.substr(fromPos + 5));
            else if (inPos != string::npos) tname = trim(idxRest.substr(inPos + 3));
            else tname = idxRest;
            tname = resolveTableName(s, tname);
            if (!g_engine.tableExists(s.currentDB, tname)) {
                cout << "Table not exist" << endl;
                return true;
            }
            cout << "table index_name column_name seq" << endl;
            // Single-column indexes
            auto idxCols = g_engine.getIndexedColumns(s.currentDB, tname);
            for (const auto& cname : idxCols) {
                cout << tname << " " << cname << "_idx " << cname << " 1" << endl;
            }
            // Composite indexes
            auto compIdxs = g_engine.getCompositeIndexes(s.currentDB, tname);
            for (const auto& ci : compIdxs) {
                for (size_t seq = 0; seq < ci.columns.size(); ++seq) {
                    cout << tname << " " << ci.name << " " << ci.columns[seq] << " " << (seq + 1) << endl;
                }
            }
            return false;
        }
        if (rest == "users") {
            if (!checkAdmin(s)) return true;
            std::ifstream infile("user.dat");
            if (!infile) {
                cout << "No users found" << endl;
                return false;
            }
            user temp;
            cout << "username permission" << endl;
            while (infile >> temp.username >> temp.password >> temp.permission) {
                cout << temp.username << " " << temp.permission << endl;
            }
            return false;
        }
        if (rest == "roles") {
            if (!checkAdmin(s)) return true;
            std::ifstream infile("role.dat");
            if (!infile) {
                cout << "No roles found" << endl;
                return false;
            }
            std::string r, u;
            cout << "role_name" << endl;
            while (infile >> r >> u) {
                if (u == "__ROLE__") {
                    cout << r << endl;
                }
            }
            return false;
        }
        cout << "Unknown SHOW command" << endl;
        return true;
    }

    // GRANT privilege[(col1,col2)] ON table TO user [WITH GRANT OPTION]
    if (sql.substr(0, 6) == "grant ") {
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(6));
        size_t onPos = rest.find(" on ");
        size_t toPos = rest.find(" to ");
        if (onPos == string::npos && toPos != string::npos) {
            if (!checkAdmin(s)) return true;
            // GRANT role_name TO user_name
            string roleName = trim(rest.substr(0, toPos));
            string username = trim(rest.substr(toPos + 4));
            int res = grantRoleToUser(roleName, username);
            if (res == -1) {
                cout << "error: role does not exist" << endl;
                return true;
            }
            if (res == -2) {
                cout << "error: role already granted to user" << endl;
                return true;
            }
            cout << "Granted role " << roleName << " to " << username << endl;
            return false;
        }
        if (onPos == string::npos || toPos == string::npos) {
            cout << "SQL syntax error: GRANT privilege[(cols)] ON table TO user [WITH GRANT OPTION]" << endl;
            return true;
        }
        string privPart = trim(rest.substr(0, onPos));
        string onPart = trim(rest.substr(onPos + 4, toPos - onPos - 4));
        string afterTo = trim(rest.substr(toPos + 4));
        // Check for WITH GRANT OPTION
        bool withGrantOpt = false;
        size_t wgoPos = afterTo.find("with grant option");
        string uname;
        if (wgoPos != string::npos) {
            withGrantOpt = true;
            uname = trim(afterTo.substr(0, wgoPos));
        } else {
            uname = afterTo;
        }
        // Parse privilege and optional column list: select(col1,col2)
        string privStr;
        vector<string> colList;
        size_t lp = privPart.find('(');
        size_t rp = privPart.find(')');
        if (lp != string::npos && rp != string::npos && rp > lp + 1) {
            privStr = trim(privPart.substr(0, lp));
            string cols = privPart.substr(lp + 1, rp - lp - 1);
            stringstream css(cols);
            string c;
            while (getline(css, c, ',')) {
                string tc = trim(c);
                if (!tc.empty()) colList.push_back(tc);
            }
        } else {
            privStr = privPart;
        }
        dbms::StorageEngine::TablePrivilege priv;
        if (privStr == "select") priv = dbms::StorageEngine::TablePrivilege::Select;
        else if (privStr == "insert") priv = dbms::StorageEngine::TablePrivilege::Insert;
        else if (privStr == "update") priv = dbms::StorageEngine::TablePrivilege::Update;
        else if (privStr == "delete") priv = dbms::StorageEngine::TablePrivilege::Delete;
        else if (privStr == "all") priv = dbms::StorageEngine::TablePrivilege::All;
        else if (privStr == "usage") priv = dbms::StorageEngine::TablePrivilege::Usage;
        else if (privStr == "execute") priv = dbms::StorageEngine::TablePrivilege::Execute;
        else {
            cout << "Unknown privilege: " << privStr << endl;
            return true;
        }
        // Detect object type: schema, sequence, function, or table
        string tname = onPart;
        string objectType = "table";
        if (onPart.substr(0, 7) == "schema ") {
            objectType = "schema";
            tname = "schema:" + trim(onPart.substr(7));
        } else if (onPart.substr(0, 9) == "sequence ") {
            objectType = "sequence";
            tname = "sequence:" + trim(onPart.substr(9));
        } else if (onPart.substr(0, 9) == "function ") {
            objectType = "function";
            tname = "function:" + trim(onPart.substr(9));
        }
        // Validate object existence
        if (objectType == "table" && tname != "*" && !g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        // Permission check: admin OR user with grant option for this privilege
        bool canGrant = (s.permission == 1 || userIsAdminViaRole(s.username));
        if (!canGrant) {
            canGrant = g_engine.hasGrantOption(s.currentDB, tname, s.username, priv);
        }
        if (!canGrant) {
            cout << "permission denied: you do not have GRANT OPTION for " << privStr << " on " << tname << endl;
            return true;
        }
        g_engine.grant(s.currentDB, tname, uname, priv, colList, withGrantOpt, s.username);
        string scope;
        if (tname == "*") scope = "database " + s.currentDB;
        else if (objectType == "schema") scope = "schema " + onPart.substr(7);
        else if (objectType == "sequence") scope = "sequence " + onPart.substr(9);
        else if (objectType == "function") scope = "function " + onPart.substr(9);
        else scope = "table " + tname;
        string goStr = withGrantOpt ? " WITH GRANT OPTION" : "";
        if (!colList.empty()) {
            string cols;
            for (size_t i = 0; i < colList.size(); ++i) {
                if (i > 0) cols += ",";
                cols += colList[i];
            }
            cout << "Granted " << privStr << "(" << cols << ") on " << scope << " to " << uname << goStr << endl;
        } else {
            cout << "Granted " << privStr << " on " << scope << " to " << uname << goStr << endl;
        }
        return false;
    }

    // REVOKE privilege[(col1,col2)] ON table FROM user
    if (sql.substr(0, 7) == "revoke ") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(7));
        size_t onPos = rest.find(" on ");
        size_t fromPos = rest.find(" from ");
        if (onPos == string::npos && fromPos != string::npos) {
            // REVOKE role_name FROM user_name
            string roleName = trim(rest.substr(0, fromPos));
            string username = trim(rest.substr(fromPos + 6));
            if (!revokeRoleFromUser(roleName, username)) {
                cout << "error: role not granted to user" << endl;
                return true;
            }
            cout << "Revoked role " << roleName << " from " << username << endl;
            return false;
        }
        if (onPos == string::npos || fromPos == string::npos) {
            cout << "SQL syntax error: REVOKE privilege[(cols)] ON table FROM user" << endl;
            return true;
        }
        string privPart = trim(rest.substr(0, onPos));
        string onPart = trim(rest.substr(onPos + 4, fromPos - onPos - 4));
        string afterFrom = trim(rest.substr(fromPos + 6));
        bool cascade = false;
        string uname;
        size_t cascadePos = afterFrom.find("cascade");
        if (cascadePos != string::npos) {
            cascade = true;
            uname = trim(afterFrom.substr(0, cascadePos));
        } else {
            uname = afterFrom;
        }
        string privStr;
        vector<string> colList;
        size_t lp = privPart.find('(');
        size_t rp = privPart.find(')');
        if (lp != string::npos && rp != string::npos && rp > lp + 1) {
            privStr = trim(privPart.substr(0, lp));
            string cols = privPart.substr(lp + 1, rp - lp - 1);
            stringstream css(cols);
            string c;
            while (getline(css, c, ',')) {
                string tc = trim(c);
                if (!tc.empty()) colList.push_back(tc);
            }
        } else {
            privStr = privPart;
        }
        dbms::StorageEngine::TablePrivilege priv;
        if (privStr == "select") priv = dbms::StorageEngine::TablePrivilege::Select;
        else if (privStr == "insert") priv = dbms::StorageEngine::TablePrivilege::Insert;
        else if (privStr == "update") priv = dbms::StorageEngine::TablePrivilege::Update;
        else if (privStr == "delete") priv = dbms::StorageEngine::TablePrivilege::Delete;
        else if (privStr == "all") priv = dbms::StorageEngine::TablePrivilege::All;
        else if (privStr == "usage") priv = dbms::StorageEngine::TablePrivilege::Usage;
        else if (privStr == "execute") priv = dbms::StorageEngine::TablePrivilege::Execute;
        else {
            cout << "Unknown privilege: " << privStr << endl;
            return true;
        }
        // Detect object type: schema, sequence, function, or table
        string tname = onPart;
        string objectType = "table";
        if (onPart.substr(0, 7) == "schema ") {
            objectType = "schema";
            tname = "schema:" + trim(onPart.substr(7));
        } else if (onPart.substr(0, 9) == "sequence ") {
            objectType = "sequence";
            tname = "sequence:" + trim(onPart.substr(9));
        } else if (onPart.substr(0, 9) == "function ") {
            objectType = "function";
            tname = "function:" + trim(onPart.substr(9));
        }
        // Validate object existence
        if (objectType == "table" && tname != "*" && !g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        g_engine.revoke(s.currentDB, tname, uname, priv, colList, cascade);
        string scope;
        if (tname == "*") scope = "database " + s.currentDB;
        else if (objectType == "schema") scope = "schema " + onPart.substr(7);
        else if (objectType == "sequence") scope = "sequence " + onPart.substr(9);
        else if (objectType == "function") scope = "function " + onPart.substr(9);
        else scope = "table " + tname;
        string casStr = cascade ? " CASCADE" : "";
        if (!colList.empty()) {
            string cols;
            for (size_t i = 0; i < colList.size(); ++i) {
                if (i > 0) cols += ",";
                cols += colList[i];
            }
            cout << "Revoked " << privStr << "(" << cols << ") on " << scope << " from " << uname << casStr << endl;
        } else {
            cout << "Revoked " << privStr << " on " << scope << " from " << uname << casStr << endl;
        }
        return false;
    }

    // REASSIGN OWNED BY old_owner TO new_owner
    if (sql.substr(0, 15) == "reassign owned ") {
        if (!checkAdmin(s)) return true;
        string rest = trim(sql.substr(15));
        if (rest.substr(0, 3) != "by ") {
            cout << "SQL syntax error: REASSIGN OWNED BY old_owner TO new_owner" << endl;
            return true;
        }
        rest = trim(rest.substr(3));
        size_t toPos = rest.find(" to ");
        if (toPos == string::npos) {
            cout << "SQL syntax error: REASSIGN OWNED BY old_owner TO new_owner" << endl;
            return true;
        }
        string oldOwner = trim(rest.substr(0, toPos));
        string newOwner = trim(rest.substr(toPos + 4));
        int count = 0;
        for (const auto& dbname : g_engine.getDatabaseNames()) {
            auto ppath = g_engine.permPath(dbname);
            if (!std::filesystem::exists(ppath)) continue;
            std::vector<std::string> lines;
            {
                std::ifstream ifs(ppath);
                std::string line;
                while (std::getline(ifs, line)) {
                    if (line.empty()) continue;
                    std::stringstream ss(line);
                    std::string u, t, p, cols;
                    ss >> u >> t >> p;
                    std::getline(ss, cols);
                    if (u == oldOwner) {
                        u = newOwner;
                        count++;
                    }
                    std::string newline = u + " " + t + " " + p + cols;
                    lines.push_back(newline);
                }
            }
            std::ofstream ofs(ppath);
            for (const auto& l : lines) ofs << l << "\n";
        }
        cout << "Reassigned " << count << " permissions from " << oldOwner << " to " << newOwner << endl;
        return false;
    }

    // DROP POLICY name ON table
    if (sql.substr(0, 12) == "drop policy ") {
        return handleDropPolicy(sql, s);
    }

    // SET variable = value
    if (sql.substr(0, 4) == "set ") {
        string rest = trim(sql.substr(4));
        size_t eqPos = rest.find('=');
        if (eqPos == string::npos) {
            cout << "SQL syntax error: SET var=value" << endl;
            return true;
        }
        string var = trim(rest.substr(0, eqPos));
        string val = trim(rest.substr(eqPos + 1));
        if (var == "slow_query_threshold") {
            try {
                g_slowQueryThresholdMs = std::stod(val);
                cout << "slow_query_threshold set to " << g_slowQueryThresholdMs << "ms" << endl;
            } catch (...) {
                cout << "Invalid value for slow_query_threshold" << endl;
                return true;
            }
        } else if (var == "checkpoint_interval") {
            try {
                g_checkpointInterval = std::stoi(val);
                cout << "checkpoint_interval set to " << g_checkpointInterval << endl;
            } catch (...) {
                cout << "Invalid value for checkpoint_interval" << endl;
                return true;
            }
        } else if (var == "statement_timeout") {
            try {
                s.statementTimeoutMs = std::stoi(val);
                cout << "statement_timeout set to " << s.statementTimeoutMs << "ms" << endl;
            } catch (...) {
                cout << "Invalid value for statement_timeout" << endl;
                return true;
            }
        } else if (var == "password_policy_level") {
            try {
                g_config.passwordPolicyLevel = std::stoi(val);
                cout << "password_policy_level set to " << g_config.passwordPolicyLevel << endl;
            } catch (...) {
                cout << "Invalid value for password_policy_level" << endl;
                return true;
            }
        } else if (var == "password_hash_algorithm") {
            g_config.passwordHashAlgorithm = val;
            cout << "password_hash_algorithm set to " << g_config.passwordHashAlgorithm << endl;
        } else if (var == "audit_level") {
            try {
                g_config.auditLevel = std::stoi(val);
                cout << "audit_level set to " << g_config.auditLevel << endl;
            } catch (...) {
                cout << "Invalid value for audit_level" << endl;
                return true;
            }
        } else if (var == "lock_timeout") {
            try {
                int ms = std::stoi(val);
                g_config.lockTimeoutMs = ms;
                g_engine.getLockManager().setLockTimeout(ms);
                cout << "lock_timeout set to " << ms << "ms" << endl;
            } catch (...) {
                cout << "Invalid value for lock_timeout" << endl;
                return true;
            }
        } else if (var == "deadlock_timeout") {
            try {
                int ms = std::stoi(val);
                g_config.deadlockTimeoutMs = ms;
                g_engine.getLockManager().setDeadlockTimeout(ms);
                cout << "deadlock_timeout set to " << ms << "ms" << endl;
            } catch (...) {
                cout << "Invalid value for deadlock_timeout" << endl;
                return true;
            }
        } else {
            cout << "Unknown variable: " << var << endl;
            return true;
        }
        return false;
    }


    // DISCARD ALL: reset session state
    if (sql == "discard all") {
        // Drop all temporary tables
        for (const auto& t : s.tempTables) {
            g_engine.dropTable(s.currentDB, tempTablePrefix(t));
        }
        s.tempTables.clear();
        // Clear prepared statements
        s.preparedStmts.clear();
        // Reset session variables
        s.timezoneOffsetMinutes = 0;
        s.statementTimeoutMs = 0;
        s.isolationLevel = 2; // REPEATABLE READ default
        cout << "Session state discarded" << endl;
        return false;
    }

    // LOAD DATA INFILE: import CSV
    if (sql.substr(0, 17) == "load data infile ") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(17));
        // Parse: 'file.csv' into table tname
        size_t q1 = rest.find('\'');
        if (q1 == string::npos) {
            cout << "SQL syntax error: missing filename" << endl;
            return true;
        }
        size_t q2 = rest.find('\'', q1 + 1);
        if (q2 == string::npos) {
            cout << "SQL syntax error: unclosed filename" << endl;
            return true;
        }
        string filename = rest.substr(q1 + 1, q2 - q1 - 1);
        string afterFile = trim(rest.substr(q2 + 1));
        if (afterFile.substr(0, 11) != "into table ") {
            cout << "SQL syntax error: expected INTO TABLE" << endl;
            return true;
        }
        string tname = trim(afterFile.substr(11));
        if (!g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        ifstream csvIn(filename);
        if (!csvIn) {
            cout << "Cannot open file: " << filename << endl;
            return true;
        }
        size_t imported = 0, skipped = 0;
        string line;
        bool firstLine = true;
        while (getline(csvIn, line)) {
            if (trim(line).empty()) continue;
            auto fields = parseCSVLine(line);
            if (fields.size() != tbl.len) {
                // Try treating first line as header
                if (firstLine) { firstLine = false; continue; }
                skipped++;
                continue;
            }
            firstLine = false;
            map<string, string> values;
            for (size_t i = 0; i < tbl.len; ++i) {
                values[tbl.cols[i].dataName] = trim(fields[i]);
            }
            auto res = g_engine.insert(s.currentDB, tname, values);
            if (res == DBStatus::OK) imported++;
            else skipped++;
        }
        cout << "Imported " << imported << " rows, skipped " << skipped << endl;
        return false;
    }

    if (startsWithKeyword(sql, "load")) {
        return handleLoadSharedLibrary(sql, s);
    }

    // pg_advisory_lock / pg_advisory_unlock (intercept before SELECT)
    if (sql.find("pg_advisory") != string::npos) {
        if (!checkDB(s)) return true;
        size_t lockPos = sql.find("pg_advisory_lock(");
        size_t unlockPos = sql.find("pg_advisory_unlock(");
        size_t lockSharedPos = sql.find("pg_advisory_lock_shared(");
        size_t unlockSharedPos = sql.find("pg_advisory_unlock_shared(");
        auto extractKey = [](const string& sql, size_t pos) -> int64_t {
            size_t lp = sql.find('(', pos);
            size_t rp = sql.find(')', lp);
            if (lp == string::npos || rp == string::npos) return -1;
            try { return stoll(trim(sql.substr(lp + 1, rp - lp - 1))); } catch (...) { return -1; }
        };
        if (lockPos != string::npos) {
            int64_t key = extractKey(sql, lockPos);
            if (key < 0) { cout << "SQL syntax error" << endl; return true; }
            bool ok = g_engine.advisoryLock(key);
            cout << (ok ? "Lock acquired" : "Lock not available") << endl;
            return false;
        }
        if (unlockPos != string::npos) {
            int64_t key = extractKey(sql, unlockPos);
            if (key < 0) { cout << "SQL syntax error" << endl; return true; }
            bool ok = g_engine.advisoryUnlock(key);
            cout << (ok ? "Lock released" : "Lock not held") << endl;
            return false;
        }
        if (lockSharedPos != string::npos) {
            int64_t key = extractKey(sql, lockSharedPos);
            if (key < 0) { cout << "SQL syntax error" << endl; return true; }
            bool ok = g_engine.advisoryLockShared(key);
            cout << (ok ? "Shared lock acquired" : "Lock not available") << endl;
            return false;
        }
        if (unlockSharedPos != string::npos) {
            int64_t key = extractKey(sql, unlockSharedPos);
            if (key < 0) { cout << "SQL syntax error" << endl; return true; }
            bool ok = g_engine.advisoryUnlockShared(key);
            cout << (ok ? "Shared lock released" : "Lock not held") << endl;
            return false;
        }
    }

    if (startsWithKeyword(sql, "select")) {
        bool handledSelectInto = false;
        bool failedSelectInto = handleSelectIntoTable(sql, s, handledSelectInto);
        if (handledSelectInto) return failedSelectInto;
        string expr = trim(sql.substr(6));
        if (findTopLevelKeyword(expr, "from") == string::npos) {
            string funcName;
            if (startsWithKeyword(expr, "nextval")) funcName = "nextval";
            else if (startsWithKeyword(expr, "currval")) funcName = "currval";
            if (!funcName.empty()) {
                if (!checkDB(s)) return true;
                size_t lp = expr.find('(');
                size_t rp = expr.find(')', lp);
                if (lp == string::npos || rp == string::npos || rp <= lp + 1) {
                    cout << "SQL syntax error: SELECT " << funcName << "('sequence')" << endl;
                    return true;
                }
                string seqName = stripQuotes(trim(expr.substr(lp + 1, rp - lp - 1)));
                if (!g_engine.sequenceExists(s.currentDB, seqName)) {
                    cout << "Sequence " << seqName << " not exist" << endl;
                    return true;
                }
                if (funcName == "nextval") {
                    int64_t value = g_engine.nextval(s.currentDB, seqName);
                    s.sequenceLastValues[seqName] = value;
                    cout << funcName << endl;
                    cout << value << endl;
                } else {
                    auto it = s.sequenceLastValues.find(seqName);
                    if (it == s.sequenceLastValues.end()) {
                        cout << "currval of sequence " << seqName << " is not yet defined in this session" << endl;
                        return true;
                    }
                    cout << funcName << endl;
                    cout << it->second << endl;
                }
                return false;
            }
        }
    }

    if (sql.substr(0, 6) == "select" || sql.substr(0, 5) == "with ") {
        if (!checkDB(s)) return true;

        // Process CTEs: WITH cte AS (SELECT ...)
        sql = processCTEs(sql, s);

        // Process derived tables: (SELECT ...) AS alias
        sql = processDerivedTables(sql, s);
        // Process LATERAL JOINs: materialize into temp tables
        sql = processLateralJoins(sql, s);

        // Parse FOR UPDATE / FOR SHARE / NOWAIT / SKIP LOCKED
        bool forUpdate = false;
        bool noWait = false;
        bool skipLocked = false;
        {
            size_t skPos = sql.find("skip locked");
            if (skPos != string::npos) {
                skipLocked = true;
                sql = trim(sql.substr(0, skPos));
            }
            size_t nwPos = sql.find("nowait");
            if (nwPos != string::npos) {
                noWait = true;
                sql = trim(sql.substr(0, nwPos));
            }
            size_t fuPos = sql.find("for update");
            if (fuPos != string::npos) {
                forUpdate = true;
                sql = trim(sql.substr(0, fuPos));
            }
            size_t fsPos = sql.find("for share");
            if (fsPos != string::npos) {
                sql = trim(sql.substr(0, fsPos));
            }
        }

        // RAII guard to drop temp tables created for CTEs and derived tables
        struct TempTableGuard {
            Session* ps;
            TempTableGuard(Session* s) : ps(s) {}
            ~TempTableGuard() {
                for (const auto& t : ps->tempTables) {
                    g_engine.dropTable(ps->currentDB, tempTablePrefix(t));
                }
                ps->tempTables.clear();
            }
        } guard(&s);

        // Check for INTO OUTFILE clause
        string outfile;
        size_t intoPos = sql.find("into outfile");
        if (intoPos != string::npos) {
            size_t q1 = sql.find('\'', intoPos);
            if (q1 != string::npos) {
                size_t q2 = sql.find('\'', q1 + 1);
                if (q2 != string::npos) {
                    outfile = sql.substr(q1 + 1, q2 - q1 - 1);
                    sql = trim(sql.substr(0, intoPos));
                }
            }
        }

        size_t fromPos = findTopLevelKeyword(sql, "from");
        if (fromPos == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string columns = trim(sql.substr(6, fromPos - 6));
        bool isDistinct = false;
        vector<string> distinctOnCols;
        if (columns.size() >= 12 && columns.substr(0, 12) == "distinct on(") {
            isDistinct = true;
            size_t rp = columns.find(')', 12);
            if (rp != string::npos) {
                string inner = columns.substr(12, rp - 12);
                stringstream ss(inner);
                string part;
                while (getline(ss, part, ',')) {
                    string c = trim(part);
                    if (!c.empty()) distinctOnCols.push_back(c);
                }
                columns = trim(columns.substr(rp + 1));
            }
        } else if (columns.size() >= 13 && columns.substr(0, 13) == "distinct on (") {
            isDistinct = true;
            size_t rp = columns.find(')', 13);
            if (rp != string::npos) {
                string inner = columns.substr(13, rp - 13);
                stringstream ss(inner);
                string part;
                while (getline(ss, part, ',')) {
                    string c = trim(part);
                    if (!c.empty()) distinctOnCols.push_back(c);
                }
                columns = trim(columns.substr(rp + 1));
            }
        } else if (columns.size() >= 9 && columns.substr(0, 9) == "distinct ") {
            isDistinct = true;
            columns = trim(columns.substr(9));
        }

        size_t wherePos = findTopLevelKeyword(sql, "where", fromPos);
        size_t groupPos = findTopLevelKeyword(sql, "group by", fromPos);
        size_t havingPos = findTopLevelKeyword(sql, "having", fromPos);
        size_t orderPos = findTopLevelKeyword(sql, "order by", fromPos);
        size_t limitPos = findTopLevelKeyword(sql, "limit", fromPos);
        size_t offsetPos = findTopLevelKeyword(sql, "offset", fromPos);

        auto parseLimitOffset = [&](size_t& limitVal, size_t& offsetVal) {
            limitVal = 0; offsetVal = 0;
            if (limitPos != string::npos) {
                size_t limEnd = (offsetPos != string::npos) ? offsetPos
                              : sql.size();
                string lstr = trim(sql.substr(limitPos + 5, limEnd - limitPos - 5));
                try { limitVal = static_cast<size_t>(std::stoull(lstr)); } catch (...) {}
            }
            if (offsetPos != string::npos) {
                string ostr = trim(sql.substr(offsetPos + 6));
                try { offsetVal = static_cast<size_t>(std::stoull(ostr)); } catch (...) {}
            }
        };

        // Check for JOIN
        size_t leftJoinPos = sql.find("left join", fromPos);
        size_t rightJoinPos = sql.find("right join", fromPos);
        size_t fullOuterJoinPos = sql.find("full outer join", fromPos);
        size_t crossJoinPos = sql.find("cross join", fromPos);
        size_t innerJoinPos = sql.find("inner join", fromPos);
        size_t joinPos = sql.find("join", fromPos);

        enum class JoinType { Inner, Left, Right, FullOuter, Cross };
        JoinType jt = JoinType::Inner;
        size_t actualJoinPos = joinPos;

        if (leftJoinPos != string::npos &&
            (wherePos == string::npos || leftJoinPos < wherePos) &&
            (orderPos == string::npos || leftJoinPos < orderPos)) {
            jt = JoinType::Left;
            actualJoinPos = leftJoinPos;
        } else if (rightJoinPos != string::npos &&
                   (wherePos == string::npos || rightJoinPos < wherePos) &&
                   (orderPos == string::npos || rightJoinPos < orderPos)) {
            jt = JoinType::Right;
            actualJoinPos = rightJoinPos;
        } else if (fullOuterJoinPos != string::npos &&
                   (wherePos == string::npos || fullOuterJoinPos < wherePos) &&
                   (orderPos == string::npos || fullOuterJoinPos < orderPos)) {
            jt = JoinType::FullOuter;
            actualJoinPos = fullOuterJoinPos;
        } else if (crossJoinPos != string::npos &&
                   (wherePos == string::npos || crossJoinPos < wherePos) &&
                   (orderPos == string::npos || crossJoinPos < orderPos)) {
            jt = JoinType::Cross;
            actualJoinPos = crossJoinPos;
        } else if (innerJoinPos != string::npos &&
                   (wherePos == string::npos || innerJoinPos < wherePos) &&
                   (orderPos == string::npos || innerJoinPos < orderPos)) {
            jt = JoinType::Inner;
            actualJoinPos = innerJoinPos;
        } else if (joinPos != string::npos &&
                   (wherePos == string::npos || joinPos < wherePos) &&
                   (orderPos == string::npos || joinPos < orderPos)) {
            jt = JoinType::Inner;
            actualJoinPos = joinPos;
        } else {
            actualJoinPos = string::npos;
        }

        bool isJoin = (actualJoinPos != string::npos);

        if (isJoin) {
            string leftTableOrig = trim(sql.substr(fromPos + 4, actualJoinPos - fromPos - 4));
            bool isCrossJoin = (jt == JoinType::Cross);
            size_t onPos = sql.find("on", actualJoinPos);
            size_t tableNameStart = actualJoinPos;
            if (jt == JoinType::Left) tableNameStart += 9;
            else if (jt == JoinType::Right) tableNameStart += 10;
            else if (jt == JoinType::FullOuter) tableNameStart += 15;
            else if (jt == JoinType::Cross) tableNameStart += 10;
            else if (actualJoinPos + 10 <= sql.size() && sql.substr(actualJoinPos, 10) == "inner join") tableNameStart += 10;
            else tableNameStart += 4;

            size_t clauseEnd = (wherePos != string::npos) ? wherePos
                             : (orderPos != string::npos) ? orderPos : sql.size();
            string rightTableOrig;
            string leftOnCol, rightOnCol;

            if (isCrossJoin) {
                rightTableOrig = trim(sql.substr(tableNameStart, clauseEnd - tableNameStart));
            } else {
                if (onPos == string::npos) {
                    cout << "SQL syntax error: missing ON clause" << endl;
                    return true;
                }
                rightTableOrig = trim(sql.substr(tableNameStart, onPos - tableNameStart));
                string onClause = normalizeConditionStr(trim(sql.substr(onPos + 2, clauseEnd - onPos - 2)));
                size_t eqPos = onClause.find('=');
                if (eqPos == string::npos) {
                    cout << "SQL syntax error: invalid ON clause" << endl;
                    return true;
                }
                leftOnCol = trim(onClause.substr(0, eqPos));
                rightOnCol = trim(onClause.substr(eqPos + 1));
                size_t dot = leftOnCol.find('.');
                if (dot != string::npos) leftOnCol = leftOnCol.substr(dot + 1);
                dot = rightOnCol.find('.');
                if (dot != string::npos) rightOnCol = rightOnCol.substr(dot + 1);
            }

            // Extract table name and alias from "table alias" format
            auto extractTableAndAlias = [](const string& s) -> pair<string, string> {
                size_t sp = s.find(' ');
                if (sp == string::npos) return {s, ""};
                return {trim(s.substr(0, sp)), trim(s.substr(sp + 1))};
            };
            auto [leftTableName, leftAlias] = extractTableAndAlias(leftTableOrig);
            auto [rightTableName, rightAlias] = extractTableAndAlias(rightTableOrig);

            string leftTable = resolveTableName(s, leftTableName);
            string rightTable = resolveTableName(s, rightTableName);
            if (!g_engine.tableExists(s.currentDB, leftTable) ||
                !g_engine.tableExists(s.currentDB, rightTable)) {
                cout << "Table not exist" << endl;
                return true;
            }
            if (!isTempTable(s, leftTableName) && !checkTablePermission(s, leftTableName, dbms::StorageEngine::TablePrivilege::Select)) return true;
            if (!isTempTable(s, rightTableName) && !checkTablePermission(s, rightTableName, dbms::StorageEngine::TablePrivilege::Select)) return true;
            // Column-level permission check for JOIN: verify all selected columns
            if (!isTempTable(s, leftTableName) && !checkSelectColumnPermission(s, leftTableName, columns)) return true;
            if (!isTempTable(s, rightTableName) && !checkSelectColumnPermission(s, rightTableName, columns)) return true;

            set<string> selectCols;
            bool selectAll = (columns == "*");
            if (!selectAll) {
                for (const auto& item : splitSelectColumns(columns)) {
                    string col = trim(item);
                    // Strip table aliases from SELECT columns
                    for (const auto& alias : {leftAlias, rightAlias}) {
                        if (alias.empty()) continue;
                        string prefix = alias + ".";
                        if (col.size() > prefix.size() && col.substr(0, prefix.size()) == prefix) {
                            col = col.substr(prefix.size());
                        }
                    }
                    selectCols.insert(col);
                }
            }

            vector<string> condTokens;
            if (wherePos != string::npos) {
                size_t condEnd = (orderPos != string::npos) ? orderPos : sql.size();
                string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
                whereClause = expandSubqueries(whereClause, s);
                // Strip table aliases from WHERE clause before normalization
                if (!leftAlias.empty() || !rightAlias.empty()) {
                    for (const auto& alias : {leftAlias, rightAlias}) {
                        if (alias.empty()) continue;
                        string prefix = alias + ".";
                        size_t pos = 0;
                        while ((pos = whereClause.find(prefix, pos)) != string::npos) {
                            whereClause = whereClause.substr(0, pos) + whereClause.substr(pos + prefix.size());
                        }
                    }
                }
                string condStr = normalizeConditionStr(whereClause);
                condTokens = tokenize(condStr);
            }

            if (forUpdate) { cout << "FOR UPDATE not supported with JOIN" << endl; return true; }

            TableSchema leftTbl = g_engine.getTableSchema(s.currentDB, leftTable);
            TableSchema rightTbl = g_engine.getTableSchema(s.currentDB, rightTable);
            string leftPrefix = leftAlias.empty() ? leftTableName : leftAlias;
            string rightPrefix = rightAlias.empty() ? rightTableName : rightAlias;
            if (selectAll) {
                for (size_t i = 0; i < leftTbl.len; ++i)
                    cout << leftPrefix << "." << leftTbl.cols[i].dataName << ' ';
                for (size_t i = 0; i < rightTbl.len; ++i)
                    cout << rightPrefix << "." << rightTbl.cols[i].dataName << ' ';
            } else {
                for (const auto& c : selectCols) cout << c << ' ';
            }
            cout << '\n';

            // Strip table aliases from WHERE condition tokens
            if (!condTokens.empty() && (!leftAlias.empty() || !rightAlias.empty())) {
                for (auto& tok : condTokens) {
                    for (const auto& alias : {leftAlias, rightAlias}) {
                        if (alias.empty()) continue;
                        string prefix = alias + ".";
                        if (tok.size() > prefix.size() && tok.substr(0, prefix.size()) == prefix) {
                            tok = tok.substr(prefix.size());
                        }
                    }
                }
            }

            vector<string> answers;
            auto runJoin = [&](const vector<string>& conds) -> vector<string> {
                if (jt == JoinType::Left) {
                    return g_engine.leftJoin(s.currentDB, leftTable, rightTable,
                                              leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::Right) {
                    return g_engine.rightJoin(s.currentDB, leftTable, rightTable,
                                               leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::FullOuter) {
                    return g_engine.fullOuterJoin(s.currentDB, leftTable, rightTable,
                                                   leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::Cross) {
                    return g_engine.crossJoin(s.currentDB, leftTable, rightTable, conds, selectCols);
                } else {
                    return g_engine.join(s.currentDB, leftTable, rightTable,
                                          leftOnCol, rightOnCol, conds, selectCols);
                }
            };

            if (condTokens.empty()) {
                answers = runJoin({});
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = runJoin(g);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
            if (isDistinct) {
                vector<string> deduped;
                set<string> seen;
                for (const auto& row : answers) {
                    if (seen.insert(row).second) deduped.push_back(row);
                }
                answers = std::move(deduped);
            }
            size_t jlim = 0, joff = 0;
            parseLimitOffset(jlim, joff);
            if (joff < answers.size()) {
                if (jlim > 0 && joff + jlim < answers.size())
                    answers.erase(answers.begin() + joff + jlim, answers.end());
                if (joff > 0)
                    answers.erase(answers.begin(), answers.begin() + joff);
            }
            for (const auto& row : answers) {
                cout << row << endl;
                log(s.username, row, getTime());
            }
            return false;
        }

        // Non-JOIN query
        size_t tnameEnd = (wherePos != string::npos) ? wherePos
                         : (groupPos != string::npos) ? groupPos
                         : (havingPos != string::npos) ? havingPos
                         : (orderPos != string::npos) ? orderPos
                         : (limitPos != string::npos) ? limitPos
                         : (offsetPos != string::npos) ? offsetPos : sql.size();
        string tnameOrig = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));
        string tname = resolveTableName(s, tnameOrig);

        // Support dbname.tablename syntax for special catalogs
        string queryDb = s.currentDB;
        size_t dotPos = tnameOrig.find('.');
        if (dotPos != string::npos) {
            string maybeDb = trim(tnameOrig.substr(0, dotPos));
            if (maybeDb == "pg_catalog" || maybeDb == "information_schema") {
                queryDb = maybeDb;
                tname = trim(tnameOrig.substr(dotPos + 1));
                tnameOrig = tname;
            }
        }

        // Table-valued function expansion: FROM func(arg) -> FROM (sql_with_arg) AS __tvf_func
        size_t tvfLp = tnameOrig.find('(');
        if (tvfLp != string::npos) {
            string tvfName = trim(tnameOrig.substr(0, tvfLp));
            size_t tvfRp = tnameOrig.find(')', tvfLp);
            if (tvfRp != string::npos && g_engine.tvfExists(s.currentDB, tvfName)) {
                string tvfArg = trim(tnameOrig.substr(tvfLp + 1, tvfRp - tvfLp - 1));
                // Remove quotes from argument if present
                if (tvfArg.size() >= 2 && tvfArg.front() == '\'' && tvfArg.back() == '\'') {
                    tvfArg = tvfArg.substr(1, tvfArg.size() - 2);
                }
                string tvfSql = g_engine.getTVFSQL(s.currentDB, tvfName);
                string tvfParam = g_engine.getTVFParam(s.currentDB, tvfName);
                if (!tvfSql.empty()) {
                    // Substitute parameter with argument
                    // Replace $1 with argument value
                    size_t p1 = tvfSql.find("$1");
                    if (p1 != string::npos) {
                        tvfSql = tvfSql.substr(0, p1) + tvfArg + tvfSql.substr(p1 + 2);
                    }
                    // Also replace param name if used
                    if (!tvfParam.empty()) {
                        size_t pp = tvfSql.find(tvfParam);
                        while (pp != string::npos) {
                            tvfSql = tvfSql.substr(0, pp) + tvfArg + tvfSql.substr(pp + tvfParam.size());
                            pp = tvfSql.find(tvfParam);
                        }
                    }
                    string expanded = rawSql;
                    string pattern = "from " + tnameOrig;
                    size_t fp = expanded.find(pattern);
                    if (fp != string::npos) {
                        string subq = "from (" + tvfSql + ") as __tvf_" + tvfName;
                        expanded = expanded.substr(0, fp) + subq + expanded.substr(fp + pattern.size());
                        return execute(expanded, s);
                    }
                }
            }
        }

        // pg_stat_* virtual tables
        if (tname == "pg_stat_database" || tname == "pg_stat_tables" || tname == "pg_stat_statements" || tname == "pg_seclabels" || tname == "pg_buffercache" || tname == "pg_locks" || tname == "pg_stat_wait_events" || tname == "pg_stat_activity" || tname == "pg_database" || tname == "pg_tables" || tname == "pg_indexes" || tname == "pg_settings" || tname == "pg_roles" || tname == "pg_namespace" || tname == "pg_class" || tname == "pg_type") {
            auto bpStats = g_engine.getBufferPoolStats();
            if (tname == "pg_stat_database") {
                cout << "datname numbackends blks_read blks_hit tup_returned " << endl;
                for (const auto& dbname : g_engine.getDatabaseNames()) {
                    cout << dbname << " 0 " << bpStats.totalMisses << " " << bpStats.totalHits << " 0 " << endl;
                }
            } else if (tname == "pg_stat_tables") {
                cout << "relname seq_scan idx_scan n_tup_ins n_tup_upd n_tup_del " << endl;
                for (const auto& t : g_engine.getTableNames(s.currentDB)) {
                    cout << t << " 0 0 0 0 0 " << endl;
                }
            } else if (tname == "pg_stat_statements") {
                cout << "query calls total_time min_time max_time mean_time dbname " << endl;
                auto stats = getSqlStats(s.currentDB);
                for (const auto& st : stats) {
                    std::string q = st.sql;
                    // Replace spaces with single space for compact display
                    std::string compact;
                    bool lastSpace = false;
                    for (char c : q) {
                        if (isspace(static_cast<unsigned char>(c))) {
                            if (!lastSpace) compact += ' ';
                            lastSpace = true;
                        } else {
                            compact += c;
                            lastSpace = false;
                        }
                    }
                    if (compact.size() > 50) compact = compact.substr(0, 47) + "...";
                    cout << compact << " " << st.calls << " "
                         << std::fixed << std::setprecision(2) << st.totalTimeMs << " "
                         << st.minTimeMs << " " << st.maxTimeMs << " " << st.meanTimeMs << " "
                         << st.dbname << endl;
                }
            } else if (tname == "pg_seclabels") {
                cout << "objtype objname label " << endl;
                auto labels = g_engine.getAllSecurityLabels(s.currentDB);
                for (const auto& [ot, on, lab] : labels) {
                    cout << ot << " " << on << " " << lab << endl;
                }
            } else if (tname == "pg_buffercache") {
                cout << "relname pageid dirty pincount " << endl;
                auto entries = g_engine.getBufferCacheEntries();
                for (const auto& e : entries) {
                    cout << e.relname << " " << e.pageId << " "
                         << (e.dirty ? "t" : "f") << " " << e.pinCount << endl;
                }
            } else if (tname == "pg_locks") {
                cout << "locktype database relation mode granted " << endl;
                auto holds = g_engine.getLockManager().getLockHolds();
                for (const auto& h : holds) {
                    cout << "relation " << s.currentDB << " " << h.resource << " " << h.mode << " t" << endl;
                }
                auto waits = g_engine.getLockManager().getLockWaits();
                for (const auto& w : waits) {
                    cout << "relation " << s.currentDB << " " << w.resource << " " << "wait" << " f" << endl;
                }
            } else if (tname == "pg_stat_wait_events") {
                cout << "event_type event count " << endl;
                // Static snapshot of common PostgreSQL wait event categories
                cout << "Client ClientRead 1" << endl;
                cout << "Client ClientWrite 0" << endl;
                cout << "Lock relation 0" << endl;
                cout << "Lock tuple 0" << endl;
                cout << "BufferPin BufferPin 0" << endl;
                cout << "IO DataFileRead 0" << endl;
                cout << "IO DataFileWrite 0" << endl;
                cout << "IO WALWrite 0" << endl;
                cout << "IO WALSync 0" << endl;
                cout << "Timeout Timeout 0" << endl;
                cout << "Activity ArchiverMain 0" << endl;
                cout << "Activity AutoVacuumMain 0" << endl;
                cout << "Activity BgWriterHibernate 0" << endl;
                cout << "Activity BgWriterMain 0" << endl;
                cout << "Activity WalWriterMain 0" << endl;
            } else if (tname == "pg_stat_activity") {
                cout << "pid usename datname state query " << endl;
                auto procs = dbms::getProcessList();
                for (const auto& p : procs) {
                    cout << p.id << " " << p.user << " " << p.db << " " << p.state << " " << p.info << endl;
                }
            } else if (tname == "pg_database") {
                cout << "datname encoding datcollate datctype " << endl;
                for (const auto& dbname : g_engine.getDatabaseNames()) {
                    cout << dbname << " UTF8 en_US.UTF-8 en_US.UTF-8 " << endl;
                }
            } else if (tname == "pg_tables") {
                cout << "schemaname tablename tableowner " << endl;
                if (queryDb != "information_schema" && queryDb != "pg_catalog") {
                    for (const auto& t : g_engine.getTableNames(queryDb)) {
                        cout << "public " << t << " " << s.username << " " << endl;
                    }
                }
            } else if (tname == "pg_indexes") {
                cout << "schemaname tablename indexname " << endl;
                if (queryDb != "information_schema" && queryDb != "pg_catalog") {
                    for (const auto& tblName : g_engine.getTableNames(queryDb)) {
                        auto idxMeta = g_engine.getIndexMetadata(queryDb, tblName);
                        for (const auto& meta : idxMeta) {
                            cout << "public " << tblName << " " << meta.name << " " << endl;
                        }
                    }
                }
            } else if (tname == "pg_settings") {
                cout << "name setting unit " << endl;
                cout << "max_connections " << g_config.maxConnections << " " << endl;
                cout << "shared_buffers " << g_config.bufferPoolFrames << " 8kB" << endl;
                cout << "work_mem " << g_config.workMemKb << " kB" << endl;
                cout << "checkpoint_timeout " << g_config.checkpointInterval << " s" << endl;
                cout << "statement_timeout " << g_config.statementTimeoutMs << " ms" << endl;
                cout << "lock_timeout " << g_config.lockTimeoutMs << " ms" << endl;
                cout << "deadlock_timeout " << g_config.deadlockTimeoutMs << " ms" << endl;
                cout << "slow_query_threshold_ms " << g_config.slowQueryThresholdMs << " ms" << endl;
                cout << "enable_seq_scan " << (g_config.enableSeqScan ? "on" : "off") << " " << endl;
                cout << "enable_hash_join " << (g_config.enableHashJoin ? "on" : "off") << " " << endl;
                cout << "enable_merge_join " << (g_config.enableMergeJoin ? "on" : "off") << " " << endl;
                cout << "auto_explain " << (g_config.autoExplainEnabled ? "on" : "off") << " " << endl;
                cout << "auto_explain.log_min_duration " << g_config.autoExplainThresholdMs << " ms" << endl;
                cout << "auto_vacuum " << (g_config.autoVacuumEnabled ? "on" : "off") << " " << endl;
                cout << "auto_analyze " << (g_config.autoAnalyzeEnabled ? "on" : "off") << " " << endl;
                cout << "password_policy_level " << g_config.passwordPolicyLevel << " " << endl;
                cout << "audit_level " << g_config.auditLevel << " " << endl;
            } else if (tname == "pg_roles") {
                cout << "rolname rolsuper rolcreatedb rolcanlogin " << endl;
                // Read users from user.dat
                {
                    ifstream uf("user.dat");
                    string line;
                    while (getline(uf, line)) {
                        stringstream ss(line);
                        string u, p;
                        int perm = 0;
                        ss >> u >> p >> perm;
                        if (u.empty()) continue;
                        cout << u << " " << (perm == 1 ? "t" : "f") << " t t " << endl;
                    }
                }
                // Read roles from role.dat
                {
                    ifstream rf("role.dat");
                    string line;
                    while (getline(rf, line)) {
                        string r = trim(line);
                        if (r.empty()) continue;
                        cout << r << " f f f " << endl;
                    }
                }
            } else if (tname == "pg_namespace") {
                cout << "oid nspname nspowner " << endl;
                if (!queryDb.empty()) {
                    try {
                        for (const auto& ns : g_engine.catalogService().get(queryDb).listNamespaces()) {
                            cout << ns.oid << " " << ns.nspname << " " << ns.nspowner << " " << endl;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "WARNING: pg_namespace lookup failed: " << e.what() << std::endl;
                    }
                }
            } else if (tname == "pg_class") {
                cout << "oid relname relnamespace relkind relnatts relpersistence relowner " << endl;
                if (!queryDb.empty()) {
                    try {
                        for (const auto& cls : g_engine.catalogService().get(queryDb).listClasses()) {
                            cout << cls.oid << " " << cls.relname << " "
                                 << cls.relnamespace << " " << cls.relkind << " "
                                 << cls.relnatts << " " << cls.relpersistence << " "
                                 << cls.relowner << " " << endl;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "WARNING: pg_class lookup failed: " << e.what() << std::endl;
                    }
                }
            } else if (tname == "pg_type") {
                cout << "oid typname typnamespace typtype typlen " << endl;
                if (!queryDb.empty()) {
                    try {
                        for (const auto& typ : g_engine.catalogService().get(queryDb).listTypes()) {
                            cout << typ.oid << " " << typ.typname << " "
                                 << typ.typnamespace << " " << typ.typtype << " "
                                 << typ.typlen << " " << endl;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "WARNING: pg_type lookup failed: " << e.what() << std::endl;
                    }
                }
            }
            return false;
        }

        if (queryDb != "information_schema" && queryDb != "pg_catalog" &&
            !g_engine.tableExists(queryDb, tname)) {
            if (g_engine.viewExists(queryDb, tnameOrig)) {
                string viewSql = g_engine.getViewSQL(queryDb, tnameOrig);
                if (!viewSql.empty()) {
                    // Strip BASE_TABLE metadata line for expansion
                    string expandedSql = viewSql;
                    size_t btPos = expandedSql.find("\nBASE_TABLE:");
                    if (btPos != string::npos) expandedSql = expandedSql.substr(0, btPos);
                    // View expansion: replace view reference with derived table
                    // Replace FROM viewname with FROM (view_sql) AS __view_name
                    string expanded = rawSql;
                    string pattern = "from " + tnameOrig;
                    size_t fp = expanded.find(pattern);
                    if (fp != string::npos) {
                        string subq = "from (" + expandedSql + ") as __view_" + tnameOrig;
                        expanded = expanded.substr(0, fp) + subq + expanded.substr(fp + pattern.size());
                        return execute(expanded, s);
                    }
                    // Fallback: execute view standalone
                    execute(expandedSql, s);
                    return false;
                }
            }
            cout << "Table " << tnameOrig << " not exist" << endl;
            return true;
        }
        if (queryDb != "pg_catalog" && queryDb != "information_schema" &&
            !isTempTable(s, tnameOrig) && !checkTablePermission(s, tnameOrig, dbms::StorageEngine::TablePrivilege::Select)) return true;
        if (queryDb != "pg_catalog" && queryDb != "information_schema" &&
            !isTempTable(s, tnameOrig) && !checkSelectColumnPermission(s, tnameOrig, columns)) return true;

        vector<dbms::StorageEngine::OrderBySpec> orderBySpecs;
        vector<dbms::StorageEngine::OrderBySpec> exprOrderBySpecs; // expressions sorted post-query
        if (orderPos != string::npos) {
            size_t orderEnd = (limitPos != string::npos) ? limitPos
                            : (offsetPos != string::npos) ? offsetPos : sql.size();
            string orderRest = trim(sql.substr(orderPos + 8, orderEnd - orderPos - 8));
            vector<string> orderParts;
            {
                // Split by comma at paren depth 0
                int depth = 0;
                string cur;
                for (char c : orderRest) {
                    if (c == '(') depth++;
                    else if (c == ')') depth--;
                    if (c == ',' && depth == 0) {
                        orderParts.push_back(trim(cur));
                        cur.clear();
                    } else {
                        cur += c;
                    }
                }
                if (!trim(cur).empty()) orderParts.push_back(trim(cur));
            }
            for (const string& part : orderParts) {
                string sortItem = part;
                bool asc = true;
                bool nullsFirst = false;
                // Detect NULLS FIRST / NULLS LAST
                size_t nullsPos = sortItem.find("nulls");
                if (nullsPos != string::npos) {
                    string afterNulls = trim(sortItem.substr(nullsPos + 5));
                    if (afterNulls == "first") nullsFirst = true;
                    // else "last" is default
                    sortItem = trim(sortItem.substr(0, nullsPos));
                }
                if (sortItem.size() >= 5 && sortItem.substr(sortItem.size() - 4) == "desc") {
                    asc = false;
                    sortItem = trim(sortItem.substr(0, sortItem.size() - 4));
                }
                // Detect COLLATE
                string collation;
                size_t collatePos = sortItem.find("collate");
                if (collatePos != string::npos) {
                    string afterCollate = trim(sortItem.substr(collatePos + 7));
                    // Remove quotes if present
                    if (!afterCollate.empty() && (afterCollate.front() == '\'' || afterCollate.front() == '"')) {
                        afterCollate = afterCollate.substr(1);
                    }
                    if (!afterCollate.empty() && (afterCollate.back() == '\'' || afterCollate.back() == '"')) {
                        afterCollate.pop_back();
                    }
                    collation = resolveCollationForSort(queryDb, trim(afterCollate));
                    sortItem = trim(sortItem.substr(0, collatePos));
                }
                size_t lp = sortItem.find('(');
                size_t rp = sortItem.rfind(')');
                if (lp != string::npos && rp != string::npos && rp > lp) {
                    // Expression: func(arg)
                    string func = toLower(trim(sortItem.substr(0, lp)));
                    string arg = trim(sortItem.substr(lp + 1, rp - lp - 1));
                    if (isScalarFunc(func)) {
                        dbms::StorageEngine::OrderBySpec spec;
                        spec.isExpression = true;
                        spec.exprFunc = func;
                        spec.exprArg = arg;
                        spec.ascending = asc;
                        spec.nullsFirst = nullsFirst;
                        spec.collation = collation;
                        exprOrderBySpecs.push_back(spec);
                        continue;
                    }
                }
                // Detect arithmetic expression: col + num or col - num
                {
                    size_t plusPos = sortItem.find('+');
                    size_t minusPos = sortItem.find('-');
                    size_t opPos = (plusPos != string::npos) ? plusPos : minusPos;
                    if (opPos != string::npos) {
                        string left = trim(sortItem.substr(0, opPos));
                        string right = trim(sortItem.substr(opPos + 1));
                        if (!left.empty() && !right.empty()) {
                            dbms::StorageEngine::OrderBySpec spec;
                            spec.isExpression = true;
                            spec.exprFunc = (plusPos != string::npos) ? "add" : "sub";
                            spec.exprArg = left;
                            spec.exprArg2 = right;
                            spec.ascending = asc;
                            spec.nullsFirst = nullsFirst;
                            spec.collation = collation;
                            exprOrderBySpecs.push_back(spec);
                            continue;
                        }
                    }
                }
                // Simple column
                dbms::StorageEngine::OrderBySpec spec;
                spec.colName = sortItem;
                spec.ascending = asc;
                spec.nullsFirst = nullsFirst;
                spec.collation = collation;
                orderBySpecs.push_back(spec);
            }
        }

        vector<string> groupByCols;
        vector<vector<string>> groupingSets;
        bool isGroupingSets = false;
        if (groupPos != string::npos) {
            size_t groupEnd = (havingPos != string::npos) ? havingPos
                            : (orderPos != string::npos) ? orderPos
                            : (limitPos != string::npos) ? limitPos
                            : (offsetPos != string::npos) ? offsetPos : sql.size();
            string groupRest = trim(sql.substr(groupPos + 8, groupEnd - groupPos - 8));
            // Detect ROLLUP / CUBE / GROUPING SETS
            if (groupRest.size() > 7 && groupRest.substr(0, 7) == "rollup(") {
                string inner = groupRest.substr(7);
                if (!inner.empty() && inner.back() == ')') inner.pop_back();
                stringstream gss(inner);
                string part;
                while (getline(gss, part, ',')) groupByCols.push_back(trim(part));
                // Generate grouping sets: (a,b,c), (a,b), (a), ()
                for (size_t i = groupByCols.size(); i > 0; --i) {
                    vector<string> set;
                    for (size_t j = 0; j < i; ++j) set.push_back(groupByCols[j]);
                    groupingSets.push_back(set);
                }
                groupingSets.push_back({});
                isGroupingSets = true;
            } else if (groupRest.size() > 5 && groupRest.substr(0, 5) == "cube(") {
                string inner = groupRest.substr(5);
                if (!inner.empty() && inner.back() == ')') inner.pop_back();
                stringstream gss(inner);
                string part;
                while (getline(gss, part, ',')) groupByCols.push_back(trim(part));
                size_t n = groupByCols.size();
                for (size_t mask = 0; mask < (1u << n); ++mask) {
                    vector<string> set;
                    for (size_t i = 0; i < n; ++i) {
                        if (mask & (1u << i)) set.push_back(groupByCols[i]);
                    }
                    groupingSets.push_back(set);
                }
                isGroupingSets = true;
            } else if (groupRest.size() > 14 && groupRest.substr(0, 14) == "grouping sets(") {
                string inner = groupRest.substr(14);
                if (!inner.empty() && inner.back() == ')') inner.pop_back();
                size_t pos = 0;
                set<string> seenCols;
                while (pos < inner.size()) {
                    while (pos < inner.size() && isspace(static_cast<unsigned char>(inner[pos]))) ++pos;
                    if (pos >= inner.size()) break;
                    if (inner[pos] == '(') {
                        size_t end = pos + 1;
                        while (end < inner.size() && inner[end] != ')') ++end;
                        string setInner = inner.substr(pos + 1, end - pos - 1);
                        vector<string> set;
                        stringstream sss(setInner);
                        string p;
                        while (getline(sss, p, ',')) {
                            string tp = trim(p);
                            if (!tp.empty()) { set.push_back(tp); seenCols.insert(tp); }
                        }
                        groupingSets.push_back(set);
                        pos = end + 1;
                        while (pos < inner.size() && isspace(static_cast<unsigned char>(inner[pos]))) ++pos;
                        if (pos < inner.size() && inner[pos] == ',') ++pos;
                    } else {
                        size_t end = pos;
                        while (end < inner.size() && inner[end] != ',') ++end;
                        string col = trim(inner.substr(pos, end - pos));
                        if (!col.empty()) {
                            groupingSets.push_back({col});
                            seenCols.insert(col);
                        }
                        pos = end + 1;
                    }
                }
                // Preserve first-seen order for allGroupByCols
                for (const auto& set : groupingSets) {
                    for (const auto& c : set) {
                        if (seenCols.count(c)) {
                            groupByCols.push_back(c);
                            seenCols.erase(c);
                        }
                    }
                }
                isGroupingSets = true;
            } else {
                stringstream gss(groupRest);
                string part;
                while (getline(gss, part, ',')) groupByCols.push_back(trim(part));
            }
        }

        if (havingPos != string::npos && groupPos == string::npos) {
            cout << "SQL syntax error: HAVING without GROUP BY" << endl;
            return true;
        }

        vector<string> havingConds;
        if (havingPos != string::npos) {
            size_t havingEnd = (orderPos != string::npos) ? orderPos
                             : (limitPos != string::npos) ? limitPos
                             : (offsetPos != string::npos) ? offsetPos : sql.size();
            string havingClause = normalizeConditionStr(trim(sql.substr(havingPos + 6, havingEnd - havingPos - 6)));
            size_t pos = 0;
            while (pos < havingClause.size()) {
                size_t andPos = havingClause.find("and", pos);
                if (andPos == string::npos) {
                    havingConds.push_back(trim(havingClause.substr(pos)));
                    break;
                }
                havingConds.push_back(trim(havingClause.substr(pos, andPos - pos)));
                pos = andPos + 3;
            }
        }

        TableSchema tbl = g_engine.getTableSchema(queryDb, tname);
        set<string> selectCols;
        bool selectAll = (columns == "*");

        // Detect aggregate functions, window functions, and scalar functions in columns
        vector<dbms::StorageEngine::AggItem> aggItems;
        vector<WindowFunc> windowFuncs;
        vector<dbms::StorageEngine::SelectExpr> selectExprs;
        vector<int> exprTypes; // 0=normal, 1=agg, 2=window, 3=scalar
        bool hasAgg = false;
        bool hasWindow = false;
        bool hasScalar = false;
        {
            for (const auto& itemRaw : splitSelectColumns(columns)) {
                string item = trim(itemRaw);
                if (item == "current_user" || item == "session_user") {
                    dbms::StorageEngine::SelectExpr expr;
                    expr.displayName = item;
                    expr.isScalar = true;
                    expr.funcName = item;
                    expr.sessionUser = (item == "current_user")
                        ? (s.currentRole.empty() ? s.username : s.currentRole)
                        : s.username;
                    selectExprs.push_back(expr);
                    hasScalar = true;
                    exprTypes.push_back(3);
                    continue;
                }
                WindowFunc wf;
                if (parseWindowFunc(item, wf)) {
                    windowFuncs.push_back(wf);
                    hasWindow = true;
                    exprTypes.push_back(2);
                    dbms::StorageEngine::AggItem ai;
                    ai.func = wf.name;
                    ai.arg = wf.arg;
                    aggItems.push_back(ai);
                    dbms::StorageEngine::SelectExpr expr;
                    expr.displayName = item;
                    expr.isScalar = false;
                    expr.colName = item;
                    selectExprs.push_back(expr);
                } else if (item.size() >= 9 && item.front() == '(' && item.back() == ')' &&
                           item.substr(1, 7) == "select ") {
                    // Scalar subquery in SELECT: (SELECT col FROM t [WHERE ...])
                    dbms::StorageEngine::SelectExpr expr;
                    expr.displayName = item;
                    expr.isScalar = true;
                    expr.funcName = "subquery";
                    expr.funcArgs.push_back(item.substr(1, item.size() - 2));
                    selectExprs.push_back(expr);
                    hasScalar = true;
                    exprTypes.push_back(3);
                } else {
                    // Detect FILTER (WHERE condition) after aggregate function
                    vector<string> filterConds;
                    string itemBase = item;
                    size_t filterPos = item.find("filter (where ");
                    if (filterPos == string::npos) filterPos = item.find("filter(where ");
                    if (filterPos != string::npos) {
                        size_t filterStart = item.find("where ", filterPos);
                        if (filterStart != string::npos) {
                            // Find the closing ')' of FILTER clause
                            size_t filterEnd = item.find(')', filterStart);
                            if (filterEnd == string::npos) filterEnd = item.size();
                            string filterStr = trim(item.substr(filterStart + 6, filterEnd - filterStart - 6));
                            string ms = modifyLogic(filterStr);
                            string parseStr = ms.empty() ? filterStr : ms;
                            // Normalize spaces: replace multiple spaces with single space
                            {
                                string normalized;
                                bool inSpace = false;
                                for (char c : parseStr) {
                                    if (isspace(static_cast<unsigned char>(c))) {
                                        if (!inSpace) { normalized += ' '; inSpace = true; }
                                    } else {
                                        normalized += c; inSpace = false;
                                    }
                                }
                                parseStr = trim(normalized);
                            }
                            filterConds.push_back(parseStr);
                        }
                        itemBase = trim(item.substr(0, filterPos));
                    }
                    size_t lp = itemBase.find('(');
                    size_t rp = itemBase.find(')');
                    if (lp != string::npos && rp != string::npos && rp > lp) {
                        string func = itemBase.substr(0, lp);
                        string arg = itemBase.substr(lp + 1, rp - lp - 1);
                        // Preprocess cast: "expr as type" → "expr,type"
                        if (func == "cast") {
                            size_t asPos = arg.find(" as ");
                            if (asPos != string::npos) {
                                string expr = trim(arg.substr(0, asPos));
                                string type = trim(arg.substr(asPos + 4));
                                arg = expr + "," + type;
                            }
                        }
                        // Normalize count(distinct col) → "distinct col"
                        if (func == "count") {
                            arg = trim(arg);
                            if (arg.size() > 9 && arg.substr(0, 9) == "distinct ") {
                                arg = "distinct " + trim(arg.substr(9));
                            }
                        }
                        bool isUDF = (!s.currentDB.empty() && g_engine.udfExists(s.currentDB, func));
                        if (isScalarFunc(func) || isUDF) {
                            dbms::StorageEngine::SelectExpr expr;
                            expr.displayName = item;
                            expr.isScalar = true;
                            expr.funcName = func;
                            expr.funcArgs = splitFuncArgs(arg);
                            selectExprs.push_back(expr);
                            hasScalar = true;
                            exprTypes.push_back(3);
                            // Add referenced columns to selectCols for fetching
                            for (const auto& a : expr.funcArgs) {
                                string ta = trim(a);
                                if (ta.size() >= 2 && ta.front() == '\'' && ta.back() == '\'') continue;
                                for (size_t ci = 0; ci < tbl.len; ++ci) {
                                    if (tbl.cols[ci].dataName == ta) {
                                        selectCols.insert(ta);
                                        break;
                                    }
                                }
                            }
                        } else {
                            dbms::StorageEngine::AggItem ai;
                            ai.func = func;
                            ai.arg = arg;
                            ai.filterConds = filterConds;
                            aggItems.push_back(ai);
                            hasAgg = true;
                            exprTypes.push_back(1);
                            dbms::StorageEngine::SelectExpr expr;
                            expr.displayName = item;
                            expr.isScalar = false;
                            expr.colName = item;
                            selectExprs.push_back(expr);
                        }
                    } else {
                        dbms::StorageEngine::AggItem ai;
                        ai.func = "";
                        ai.arg = item;
                        aggItems.push_back(ai);
                        exprTypes.push_back(0);
                        dbms::StorageEngine::SelectExpr expr;
                        expr.displayName = item;
                        expr.isScalar = false;
                        expr.colName = item;
                        selectExprs.push_back(expr);
                        if (!selectAll) {
                            bool found = false;
                            for (size_t i = 0; i < tbl.len; ++i) {
                                if (tbl.cols[i].dataName == item) { found = true; break; }
                            }
                            if (!found) {
                                cout << "Invalid column name " << item << endl;
                                return true;
                            }
                            selectCols.insert(item);
                        }
                    }
                }
            }
        }

        // WHERE clause
        vector<string> condTokens;
        if (wherePos != string::npos) {
            size_t condEnd = (groupPos != string::npos) ? groupPos
                           : (havingPos != string::npos) ? havingPos
                           : (orderPos != string::npos) ? orderPos
                           : (limitPos != string::npos) ? limitPos
                           : (offsetPos != string::npos) ? offsetPos : sql.size();
            string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
            whereClause = expandSubqueries(whereClause, s);
            string condStr = normalizeConditionStr(whereClause);
            condTokens = tokenize(condStr);
        }

        vector<string> answers;
        if (!groupByCols.empty()) {
            if (forUpdate) { cout << "FOR UPDATE not supported with GROUP BY" << endl; return true; }
            for (const auto& gc : groupByCols) cout << gc << ' ';
            vector<dbms::StorageEngine::AggItem> pureAgg;
            for (const auto& it : aggItems) {
                if (!it.func.empty()) {
                    cout << it.func << '(' << it.arg << ") ";
                    pureAgg.push_back(it);
                }
            }
            cout << '\n';
            if (isGroupingSets) {
                if (condTokens.empty()) {
                    answers = g_engine.groupAggregateSets(s.currentDB, tname, {}, pureAgg, groupByCols, groupingSets, havingConds);
                } else {
                    condTokens.insert(condTokens.begin(), "(");
                    condTokens.push_back(")");
                    for (auto& t : condTokens) t = modifyLogic(t);
                    auto groups = breakDownConditions(condTokens);
                    set<string> seen;
                    for (const auto& g : groups) {
                        auto part = g_engine.groupAggregateSets(s.currentDB, tname, g, pureAgg, groupByCols, groupingSets, havingConds);
                        for (const auto& row : part) {
                            if (seen.insert(row).second) answers.push_back(row);
                        }
                    }
                }
            } else {
                if (condTokens.empty()) {
                    answers = g_engine.groupAggregate(s.currentDB, tname, {}, pureAgg, groupByCols, havingConds);
                } else {
                    condTokens.insert(condTokens.begin(), "(");
                    condTokens.push_back(")");
                    for (auto& t : condTokens) t = modifyLogic(t);
                    auto groups = breakDownConditions(condTokens);
                    set<string> seen;
                    for (const auto& g : groups) {
                        auto part = g_engine.groupAggregate(s.currentDB, tname, g, pureAgg, groupByCols, havingConds);
                        for (const auto& row : part) {
                            if (seen.insert(row).second) answers.push_back(row);
                        }
                    }
                }
            }
        } else if (hasAgg) {
            for (const auto& it : aggItems) {
                if (it.func.empty()) cout << it.arg << ' ';
                else cout << it.func << '(' << it.arg << ") ";
            }
            cout << '\n';
            vector<dbms::StorageEngine::AggItem> pureAgg;
            for (const auto& it : aggItems) {
                if (!it.func.empty()) pureAgg.push_back(it);
            }
            if (pureAgg.empty()) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            if (forUpdate) { cout << "FOR UPDATE not supported with aggregate" << endl; return true; }
            if (condTokens.empty()) {
                answers = g_engine.aggregate(s.currentDB, tname, {}, pureAgg);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.aggregate(s.currentDB, tname, g, pureAgg);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
        } else if (hasWindow) {
            if (forUpdate) { cout << "FOR UPDATE not supported with window functions" << endl; return true; }
            // Output header
            for (size_t i = 0; i < aggItems.size(); ++i) {
                if (exprTypes[i] == 2) {
                    cout << aggItems[i].func << "(" << aggItems[i].arg << ") over (";
                    bool hasPart = !windowFuncs[0].partitionByCols.empty();
                    if (hasPart) {
                        cout << "partition by ";
                        for (size_t pi = 0; pi < windowFuncs[0].partitionByCols.size(); ++pi) {
                            if (pi > 0) cout << ",";
                            cout << windowFuncs[0].partitionByCols[pi];
                        }
                    }
                    if (!windowFuncs[0].orderByCol.empty()) {
                        if (hasPart) cout << " ";
                        cout << "order by " << windowFuncs[0].orderByCol;
                    }
                    cout << ") ";
                } else if (exprTypes[i] == 1) {
                    cout << aggItems[i].func << "(" << aggItems[i].arg << ") ";
                } else {
                    cout << aggItems[i].arg << " ";
                }
            }
            cout << '\n';

            // Fetch all data (need all columns for window function computation)
            set<string> allCols;
            vector<string> rawAnswers;
            if (condTokens.empty()) {
                rawAnswers = g_engine.query(s.currentDB, tname, {}, allCols, {});
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.query(s.currentDB, tname, g, allCols, {});
                    for (const auto& row : part) {
                        if (seen.insert(row).second) rawAnswers.push_back(row);
                    }
                }
            }

            // Parse rows into column name -> value maps
            vector<map<string, string>> rows;
            for (const auto& rowStr : rawAnswers) {
                map<string, string> rowData;
                stringstream ss(rowStr);
                string val;
                for (size_t i = 0; i < tbl.len && ss >> val; ++i) {
                    rowData[tbl.cols[i].dataName] = val;
                }
                rows.push_back(rowData);
            }

            // Sort by partition columns first, then by ORDER BY column
            const WindowFunc& wf0 = windowFuncs[0];
            std::stable_sort(rows.begin(), rows.end(), [&](const auto& a, const auto& b) {
                // Compare partition columns first
                for (const auto& pcol : wf0.partitionByCols) {
                    auto itA = a.find(pcol);
                    auto itB = b.find(pcol);
                    if (itA == a.end() || itB == b.end()) continue;
                    if (itA->second != itB->second) {
                        try {
                            int64_t va = stoll(itA->second);
                            int64_t vb = stoll(itB->second);
                            return va < vb;
                        } catch (...) {
                            return itA->second < itB->second;
                        }
                    }
                }
                // Then compare ORDER BY column
                if (wf0.orderByCol.empty()) return false;
                auto itA = a.find(wf0.orderByCol);
                auto itB = b.find(wf0.orderByCol);
                if (itA == a.end() || itB == b.end()) return false;
                try {
                    int64_t va = stoll(itA->second);
                    int64_t vb = stoll(itB->second);
                    return wf0.orderByAsc ? (va < vb) : (va > vb);
                } catch (...) {
                    return wf0.orderByAsc ? (itA->second < itB->second) : (itA->second > itB->second);
                }
            });

            // Helper: check if two rows are in the same partition
            auto samePartition = [&](size_t i, size_t j, const WindowFunc& wf) -> bool {
                if (wf.partitionByCols.empty()) return true;
                for (const auto& pcol : wf.partitionByCols) {
                    auto itA = rows[i].find(pcol);
                    auto itB = rows[j].find(pcol);
                    if (itA == rows[i].end() || itB == rows[j].end()) continue;
                    if (itA->second != itB->second) return false;
                }
                return true;
            };

            // Pre-compute partition boundaries for aggregate window functions
            vector<map<size_t, string>> aggCache(windowFuncs.size());
            for (size_t wi = 0; wi < windowFuncs.size(); ++wi) {
                const auto& wf = windowFuncs[wi];
                if (!wf.isAggregate) continue;
                // Group row indices by partition key
                map<string, vector<size_t>> partRows;
                for (size_t ri = 0; ri < rows.size(); ++ri) {
                    string key;
                    for (const auto& pcol : wf.partitionByCols) {
                        auto it = rows[ri].find(pcol);
                        key += (it != rows[ri].end() ? it->second : "") + "\x01";
                    }
                    partRows[key].push_back(ri);
                }
                // Compute aggregate per partition
                for (const auto& pr : partRows) {
                    const auto& idxs = pr.second;
                    // For RANGE/GROUPS frames, pre-sort by ORDER BY within partition
                    vector<size_t> sortedIdxs = idxs;
                    if (wf.hasFrame && wf.frameType != WindowFunc::FrameType::ROWS && !wf.orderByCol.empty()) {
                        sort(sortedIdxs.begin(), sortedIdxs.end(), [&](size_t a, size_t b) {
                            auto itA = rows[a].find(wf.orderByCol);
                            auto itB = rows[b].find(wf.orderByCol);
                            string va = (itA != rows[a].end()) ? itA->second : "";
                            string vb = (itB != rows[b].end()) ? itB->second : "";
                            try { return stoll(va) < stoll(vb); } catch (...) { return va < vb; }
                        });
                    }
                    const auto& effIdxs = (wf.hasFrame && wf.frameType != WindowFunc::FrameType::ROWS) ? sortedIdxs : idxs;
                    // Pre-compute peer group boundaries for GROUPS
                    vector<size_t> groupStart; // start index of each peer group in effIdxs
                    if (wf.hasFrame && wf.frameType == WindowFunc::FrameType::GROUPS && !wf.orderByCol.empty()) {
                        for (size_t gi = 0; gi < effIdxs.size(); ) {
                            groupStart.push_back(gi);
                            size_t gj = gi + 1;
                            while (gj < effIdxs.size()) {
                                auto itA = rows[effIdxs[gi]].find(wf.orderByCol);
                                auto itB = rows[effIdxs[gj]].find(wf.orderByCol);
                                if ((itA == rows[effIdxs[gi]].end()) != (itB == rows[effIdxs[gj]].end())) break;
                                if (itA != rows[effIdxs[gi]].end() && itA->second != itB->second) break;
                                ++gj;
                            }
                            gi = gj;
                        }
                    }
                    // Map from original row index to position in effIdxs
                    unordered_map<size_t, size_t> posInEff;
                    for (size_t ei = 0; ei < effIdxs.size(); ++ei) posInEff[effIdxs[ei]] = ei;
                    for (size_t ii = 0; ii < idxs.size(); ++ii) {
                        size_t ri = idxs[ii];
                        // Determine frame boundaries within this partition
                        size_t fStart = 0, fEnd = effIdxs.size();
                        if (wf.hasFrame) {
                            if (wf.frameType == WindowFunc::FrameType::ROWS) {
                                if (wf.frameStartOffset >= 0) {
                                    size_t pos = posInEff.count(ri) ? posInEff[ri] : ii;
                                    fStart = (pos >= static_cast<size_t>(wf.frameStartOffset)) ? (pos - wf.frameStartOffset) : 0;
                                }
                                if (wf.frameEndOffset >= 0) {
                                    size_t pos = posInEff.count(ri) ? posInEff[ri] : ii;
                                    fEnd = pos + wf.frameEndOffset + 1;
                                    if (fEnd > effIdxs.size()) fEnd = effIdxs.size();
                                }
                            } else if (wf.frameType == WindowFunc::FrameType::RANGE && !wf.orderByCol.empty()) {
                                auto itCur = rows[ri].find(wf.orderByCol);
                                if (itCur != rows[ri].end() && !itCur->second.empty()) {
                                    try {
                                        int64_t curVal = stoll(itCur->second);
                                        // Find start: first row with value >= curVal - startOffset (or unbounded)
                                        if (wf.frameStartOffset == -1) {
                                            fStart = 0;
                                        } else if (wf.frameStartOffset == 0) {
                                            // current row means value == curVal
                                            for (size_t k = 0; k < effIdxs.size(); ++k) {
                                                auto itK = rows[effIdxs[k]].find(wf.orderByCol);
                                                if (itK != rows[effIdxs[k]].end()) {
                                                    try {
                                                        if (stoll(itK->second) >= curVal) { fStart = k; break; }
                                                    } catch (...) {}
                                                }
                                            }
                                        } else {
                                            int64_t bound = curVal - wf.frameStartOffset;
                                            for (size_t k = 0; k < effIdxs.size(); ++k) {
                                                auto itK = rows[effIdxs[k]].find(wf.orderByCol);
                                                if (itK != rows[effIdxs[k]].end()) {
                                                    try {
                                                        if (stoll(itK->second) >= bound) { fStart = k; break; }
                                                    } catch (...) {}
                                                }
                                            }
                                        }
                                        // Find end: last row with value <= curVal + endOffset (or unbounded)
                                        if (wf.frameEndOffset == -1) {
                                            fEnd = effIdxs.size();
                                        } else if (wf.frameEndOffset == 0) {
                                            for (size_t k = effIdxs.size(); k-- > 0; ) {
                                                auto itK = rows[effIdxs[k]].find(wf.orderByCol);
                                                if (itK != rows[effIdxs[k]].end()) {
                                                    try {
                                                        if (stoll(itK->second) <= curVal) { fEnd = k + 1; break; }
                                                    } catch (...) {}
                                                }
                                            }
                                        } else {
                                            int64_t bound = curVal + wf.frameEndOffset;
                                            for (size_t k = effIdxs.size(); k-- > 0; ) {
                                                auto itK = rows[effIdxs[k]].find(wf.orderByCol);
                                                if (itK != rows[effIdxs[k]].end()) {
                                                    try {
                                                        if (stoll(itK->second) <= bound) { fEnd = k + 1; break; }
                                                    } catch (...) {}
                                                }
                                            }
                                        }
                                    } catch (...) {}
                                }
                            } else if (wf.frameType == WindowFunc::FrameType::GROUPS && !groupStart.empty()) {
                                size_t pos = posInEff.count(ri) ? posInEff[ri] : ii;
                                // Find which group current row belongs to
                                size_t curGroup = 0;
                                for (size_t g = 0; g < groupStart.size(); ++g) {
                                    if (groupStart[g] <= pos && (g + 1 >= groupStart.size() || groupStart[g + 1] > pos)) {
                                        curGroup = g; break;
                                    }
                                }
                                size_t startGroup = (wf.frameStartOffset == -1) ? 0 :
                                    (curGroup >= static_cast<size_t>(wf.frameStartOffset) ? curGroup - wf.frameStartOffset : 0);
                                size_t endGroup = (wf.frameEndOffset == -1) ? groupStart.size() - 1 :
                                    (curGroup + wf.frameEndOffset);
                                if (endGroup >= groupStart.size()) endGroup = groupStart.size() - 1;
                                fStart = groupStart[startGroup];
                                fEnd = (endGroup + 1 < groupStart.size()) ? groupStart[endGroup + 1] : effIdxs.size();
                            }
                        }
                        int64_t sum = 0, count = 0;
                        bool hasMax = false, hasMin = false;
                        int64_t maxVal = 0, minVal = 0;
                        for (size_t fj = fStart; fj < fEnd; ++fj) {
                            size_t rj = effIdxs[fj];
                            if (wf.name == "count") {
                                count++;
                                continue;
                            }
                            auto it = rows[rj].find(wf.arg);
                            if (it == rows[rj].end()) continue;
                            if (it->second.empty()) continue;
                            try {
                                int64_t v = stoll(it->second);
                                if (wf.name == "sum" || wf.name == "avg") { sum += v; count++; }
                                if (wf.name == "max") { if (!hasMax || v > maxVal) { maxVal = v; hasMax = true; } }
                                if (wf.name == "min") { if (!hasMin || v < minVal) { minVal = v; hasMin = true; } }
                            } catch (...) {}
                        }
                        string aggVal;
                        if (wf.name == "sum") aggVal = to_string(sum);
                        else if (wf.name == "count") aggVal = to_string(count);
                        else if (wf.name == "avg") aggVal = (count == 0 ? "0" : to_string(static_cast<double>(sum) / count));
                        else if (wf.name == "max") aggVal = hasMax ? to_string(maxVal) : "NULL";
                        else if (wf.name == "min") aggVal = hasMin ? to_string(minVal) : "NULL";
                        aggCache[wi][ri] = aggVal;
                    }
                }
            }

            // Compute window function values
            for (size_t i = 0; i < rows.size(); ++i) {
                for (size_t wi = 0; wi < windowFuncs.size(); ++wi) {
                    const auto& wf = windowFuncs[wi];
                    string val;
                    // Determine partition start index for current row
                    size_t partStart = i;
                    while (partStart > 0 && samePartition(partStart - 1, i, wf)) partStart--;
                    size_t partIdx = i - partStart; // 0-based index within partition

                    if (wf.isAggregate) {
                        auto it = aggCache[wi].find(i);
                        val = (it != aggCache[wi].end()) ? it->second : "NULL";
                    } else if (wf.name == "row_number") {
                        val = to_string(partIdx + 1);
                    } else if (wf.name == "rank") {
                        if (partIdx == 0) {
                            val = "1";
                        } else {
                            auto itCur = rows[i].find(wf.orderByCol);
                            auto itPrev = rows[i - 1].find(wf.orderByCol);
                            if (itCur != rows[i].end() && itPrev != rows[i - 1].end()
                                && itCur->second == itPrev->second) {
                                val = rows[i - 1]["_win_" + to_string(wi)];
                            } else {
                                val = to_string(partIdx + 1);
                            }
                        }
                    } else if (wf.name == "dense_rank") {
                        if (partIdx == 0) {
                            val = "1";
                        } else {
                            auto itCur = rows[i].find(wf.orderByCol);
                            auto itPrev = rows[i - 1].find(wf.orderByCol);
                            if (itCur != rows[i].end() && itPrev != rows[i - 1].end()
                                && itCur->second == itPrev->second) {
                                val = rows[i - 1]["_win_" + to_string(wi)];
                            } else {
                                int64_t prevRank = 0;
                                try { prevRank = stoll(rows[i - 1]["_win_" + to_string(wi)]); }
                                catch (...) { prevRank = 0; }
                                val = to_string(prevRank + 1);
                            }
                        }
                    } else if (wf.name == "lag") {
                        if (partIdx == 0) val = "NULL";
                        else {
                            auto it = rows[i - 1].find(wf.arg);
                            val = (it != rows[i - 1].end()) ? it->second : "NULL";
                        }
                    } else if (wf.name == "lead") {
                        // Check if next row is in same partition
                        if (i + 1 < rows.size() && samePartition(i + 1, i, wf)) {
                            auto it = rows[i + 1].find(wf.arg);
                            val = (it != rows[i + 1].end()) ? it->second : "NULL";
                        } else {
                            val = "NULL";
                        }
                    } else if (wf.name == "first_value") {
                        auto it = rows[partStart].find(wf.arg);
                        val = (it != rows[partStart].end()) ? it->second : "NULL";
                    } else if (wf.name == "last_value") {
                        // Find last row in same partition
                        size_t partEnd = i;
                        while (partEnd + 1 < rows.size() && samePartition(partEnd + 1, i, wf)) partEnd++;
                        auto it = rows[partEnd].find(wf.arg);
                        val = (it != rows[partEnd].end()) ? it->second : "NULL";
                    } else if (wf.name == "ntile") {
                        size_t partEnd = i;
                        while (partEnd + 1 < rows.size() && samePartition(partEnd + 1, i, wf)) partEnd++;
                        size_t partitionSize = partEnd - partStart + 1;
                        int n = 1;
                        try { n = std::stoi(wf.arg); } catch (...) { n = 1; }
                        if (n <= 0) n = 1;
                        size_t bucket = (partIdx * static_cast<size_t>(n)) / partitionSize + 1;
                        val = to_string(bucket);
                    } else if (wf.name == "percent_rank") {
                        size_t partEnd = i;
                        while (partEnd + 1 < rows.size() && samePartition(partEnd + 1, i, wf)) partEnd++;
                        size_t partitionSize = partEnd - partStart + 1;
                        if (partitionSize <= 1) {
                            val = "0";
                        } else {
                            // Compute RANK of current row within partition
                            int64_t rankVal = 1;
                            for (size_t j = partStart; j <= i; ++j) {
                                if (j == partStart) continue;
                                auto itCur = rows[j].find(wf.orderByCol);
                                auto itPrev = rows[j - 1].find(wf.orderByCol);
                                if (itCur != rows[j].end() && itPrev != rows[j - 1].end()
                                    && itCur->second == itPrev->second) {
                                    // Same rank as previous
                                } else {
                                    rankVal = static_cast<int64_t>(j - partStart + 1);
                                }
                            }
                            double pr = static_cast<double>(rankVal - 1) / static_cast<double>(partitionSize - 1);
                            std::ostringstream oss;
                            oss << std::fixed << std::setprecision(4) << pr;
                            val = oss.str();
                        }
                    } else if (wf.name == "cume_dist") {
                        size_t partEnd = i;
                        while (partEnd + 1 < rows.size() && samePartition(partEnd + 1, i, wf)) partEnd++;
                        size_t partitionSize = partEnd - partStart + 1;
                        // Count rows with value <= current row's value
                        auto itCur = rows[i].find(wf.orderByCol);
                        size_t countLe = 0;
                        if (itCur != rows[i].end()) {
                            for (size_t j = partStart; j <= partEnd; ++j) {
                                auto itJ = rows[j].find(wf.orderByCol);
                                if (itJ != rows[j].end()) {
                                    try {
                                        int64_t vCur = stoll(itCur->second);
                                        int64_t vJ = stoll(itJ->second);
                                        if (vJ <= vCur) countLe++;
                                    } catch (...) {
                                        if (itJ->second <= itCur->second) countLe++;
                                    }
                                }
                            }
                        }
                        double cd = partitionSize > 0 ? static_cast<double>(countLe) / static_cast<double>(partitionSize) : 0.0;
                        std::ostringstream oss;
                        oss << std::fixed << std::setprecision(4) << cd;
                        val = oss.str();
                    } else if (wf.name == "nth_value") {
                        size_t partEnd = i;
                        while (partEnd + 1 < rows.size() && samePartition(partEnd + 1, i, wf)) partEnd++;
                        size_t partitionSize = partEnd - partStart + 1;
                        // Parse "col,n" from wf.arg
                        string nthCol;
                        int n = 1;
                        size_t commaPos = wf.arg.find(',');
                        if (commaPos != string::npos) {
                            nthCol = trim(wf.arg.substr(0, commaPos));
                            try { n = std::stoi(trim(wf.arg.substr(commaPos + 1))); } catch (...) { n = 1; }
                        } else {
                            nthCol = wf.arg;
                        }
                        if (n <= 0) n = 1;
                        if (static_cast<size_t>(n) <= partitionSize) {
                            auto it = rows[partStart + n - 1].find(nthCol);
                            val = (it != rows[partStart + n - 1].end()) ? it->second : "NULL";
                        } else {
                            val = "NULL";
                        }
                    }
                    rows[i]["_win_" + to_string(wi)] = val;
                }
            }

            // Format results as strings
            vector<string> winAnswers;
            for (const auto& row : rows) {
                string line;
                for (size_t i = 0; i < aggItems.size(); ++i) {
                    if (exprTypes[i] == 2) {
                        size_t wi = 0;
                        for (size_t j = 0; j < i; ++j) {
                            if (exprTypes[j] == 2) wi++;
                        }
                        auto it = row.find("_win_" + to_string(wi));
                        if (it != row.end()) line += it->second + " ";
                    } else if (exprTypes[i] == 0) {
                        auto it = row.find(aggItems[i].arg);
                        if (it != row.end()) line += it->second + " ";
                    }
                }
                if (!line.empty() && line.back() == ' ') line.pop_back();
                winAnswers.push_back(line);
            }
            // (debug removed)

            // DISTINCT
            if (isDistinct) {
                vector<string> deduped;
                set<string> seen;
                for (const auto& row : winAnswers) {
                    if (seen.insert(row).second) deduped.push_back(row);
                }
                winAnswers = std::move(deduped);
            }

            // LIMIT / OFFSET
            size_t wlim = 0, woff = 0;
            parseLimitOffset(wlim, woff);
            if (woff < winAnswers.size()) {
                if (wlim > 0 && woff + wlim < winAnswers.size())
                    winAnswers.erase(winAnswers.begin() + woff + wlim, winAnswers.end());
                if (woff > 0)
                    winAnswers.erase(winAnswers.begin(), winAnswers.begin() + woff);
            }

            // Output
            for (const auto& row : winAnswers) {
                cout << row << endl;
                log(s.username, row, getTime());
            }
            return false;
        } else if (hasScalar) {
            if (forUpdate) { cout << "FOR UPDATE not supported with scalar functions" << endl; return true; }
            for (const auto& expr : selectExprs) {
                cout << expr.displayName << ' ';
            }
            cout << '\n';
            if (condTokens.empty()) {
                answers = g_engine.queryExpr(s.currentDB, tname, {}, selectExprs, orderBySpecs);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.queryExpr(s.currentDB, tname, g, selectExprs, orderBySpecs);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
        } else {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (!selectAll && selectCols.find(tbl.cols[i].dataName) == selectCols.end()) continue;
                cout << tbl.cols[i].dataName << ' ';
            }
            cout << '\n';
            if (condTokens.empty()) {
                answers = g_engine.query(queryDb, tname, {}, selectCols, orderBySpecs, forUpdate, noWait, skipLocked, s.timezoneOffsetMinutes, distinctOnCols);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.query(queryDb, tname, g, selectCols, orderBySpecs, forUpdate, noWait, skipLocked, s.timezoneOffsetMinutes, distinctOnCols);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
            // Inheritance: UNION rows from child tables
            if (queryDb != "information_schema" && queryDb != "pg_catalog") {
                auto children = g_engine.getInheritedChildren(queryDb, tname);
                if (!children.empty()) {
                    set<string> childSelectCols = selectCols;
                    if (selectAll) {
                        for (size_t i = 0; i < tbl.len; ++i) childSelectCols.insert(tbl.cols[i].dataName);
                    }
                    for (const auto& childName : children) {
                        if (!g_engine.tableExists(queryDb, childName)) continue;
                        if (condTokens.empty()) {
                            auto childRows = g_engine.query(queryDb, childName, {}, childSelectCols, orderBySpecs, forUpdate, noWait, skipLocked, s.timezoneOffsetMinutes, distinctOnCols);
                            for (const auto& row : childRows) answers.push_back(row);
                        } else {
                            // Re-tokenize conditions for each child (they were modified above)
                            size_t condEnd = (groupPos != string::npos) ? groupPos
                                           : (havingPos != string::npos) ? havingPos
                                           : (orderPos != string::npos) ? orderPos
                                           : (limitPos != string::npos) ? limitPos
                                           : (offsetPos != string::npos) ? offsetPos : sql.size();
                            string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
                            whereClause = expandSubqueries(whereClause, s);
                            string condStr = normalizeConditionStr(whereClause);
                            auto childCondTokens = tokenize(condStr);
                            childCondTokens.insert(childCondTokens.begin(), "(");
                            childCondTokens.push_back(")");
                            for (auto& t : childCondTokens) t = modifyLogic(t);
                            auto childGroups = breakDownConditions(childCondTokens);
                            set<string> childSeen;
                            for (const auto& g : childGroups) {
                                auto part = g_engine.query(queryDb, childName, g, childSelectCols, orderBySpecs, forUpdate, noWait, skipLocked, s.timezoneOffsetMinutes, distinctOnCols);
                                for (const auto& row : part) {
                                    if (childSeen.insert(row).second) answers.push_back(row);
                                }
                            }
                        }
                    }
                }
            }
        }
        // Post-query expression sorting
        if (!exprOrderBySpecs.empty()) {
            answers = g_engine.sortByExpression(s.currentDB, tname, std::move(answers), exprOrderBySpecs);
        }
        // Post-query DISTINCT deduplication (skip if DISTINCT ON already handled in query())
        if (isDistinct && distinctOnCols.empty()) {
            vector<string> deduped;
            set<string> seen;
            for (const auto& row : answers) {
                if (seen.insert(row).second) deduped.push_back(row);
            }
            answers = std::move(deduped);
        }
        size_t nlim = 0, noff = 0;
        parseLimitOffset(nlim, noff);
        if (noff < answers.size()) {
            if (nlim > 0 && noff + nlim < answers.size())
                answers.erase(answers.begin() + noff + nlim, answers.end());
            if (noff > 0)
                answers.erase(answers.begin(), answers.begin() + noff);
        }
        if (!outfile.empty()) {
            ofstream ofs(outfile);
            if (!ofs) {
                cout << "Cannot open file for writing: " << outfile << endl;
                return true;
            }
            // Write header based on column selection
            if (!groupByCols.empty()) {
                bool first = true;
                for (const auto& gc : groupByCols) {
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(gc);
                }
                for (const auto& it : aggItems) {
                    if (!it.func.empty()) {
                        ofs << "," << escapeCSVField(it.func + "(" + it.arg + ")");
                    }
                }
                ofs << "\n";
            } else if (hasAgg) {
                bool first = true;
                for (const auto& it : aggItems) {
                    if (!first) ofs << ",";
                    first = false;
                    if (it.func.empty()) ofs << escapeCSVField(it.arg);
                    else ofs << escapeCSVField(it.func + "(" + it.arg + ")");
                }
                ofs << "\n";
            } else if (hasScalar) {
                bool first = true;
                for (const auto& expr : selectExprs) {
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(expr.displayName);
                }
                ofs << "\n";
            } else {
                bool first = true;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (!selectAll && selectCols.find(tbl.cols[i].dataName) == selectCols.end()) continue;
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(tbl.cols[i].dataName);
                }
                ofs << "\n";
            }
            for (const auto& row : answers) {
                // Row values are space-separated; split and re-join as CSV
                stringstream rss(row);
                string val;
                bool first = true;
                while (rss >> val) {
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(val);
                }
                ofs << "\n";
            }
            ofs.close();
            cout << "Query result saved to " << outfile << " (" << answers.size() << " rows)" << endl;
        } else {
            for (const auto& row : answers) {
                cout << row << endl;
                log(s.username, row, getTime());
            }
        }
        return false;
    }

    if (sql.substr(0, 5) == "desc " || sql.substr(0, 9) == "describe ") {
        if (!checkDB(s)) return true;
        string tname = (sql.substr(0, 5) == "desc ") ? trim(sql.substr(5)) : trim(sql.substr(9));
        tname = resolveTableName(s, tname);
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        if (tbl.len == 0) {
            cout << "Table not exist" << endl;
            return true;
        }
        cout << "field type null key default extra" << endl;
        for (size_t i = 0; i < tbl.len; ++i) {
            const auto& c = tbl.cols[i];
            cout << c.dataName << ' '
                 << c.dataType << ' '
                 << (c.isNull ? "yes" : "no") << ' '
                 << (c.isPrimaryKey ? "pri" : "") << ' '
                 << c.defaultValue << ' '
                 << (c.isAutoIncrement ? "auto_increment" : "")
                 << endl;
        }
        return false;
    }

    if (sql.substr(0, 4) == "view") {
        string rest = trim(sql.substr(4));
        vector<string> tokens = tokenize(rest);
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string op = tokens[0];
        if (op == "table") {
            if (tokens.size() < 2) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = resolveTableName(s, tokens[1]);
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
            if (tbl.len == 0) {
                cout << "Table " << tokens[1] << " not exist" << endl;
                return true;
            }
            tbl.print();
            return false;
        }
        if (op == "database") {
            auto names = g_engine.getTableNames(s.currentDB);
            for (const auto& n : names) cout << n << endl;
            log(s.username, "view database", getTime());
            return false;
        }
        cout << "SQL syntax error" << endl;
        return true;
    }

    cout << "SQL syntax error" << endl;
    return true;
}

// ========================================================================
// Main
// ========================================================================
dbms::Config g_config;

int main(int argc, char* argv[]) {
    // Set locale for Unicode support
    std::setlocale(LC_CTYPE, "");

    // Load runtime configuration
    if (g_config.load("dbms.conf")) {
        g_slowQueryThresholdMs = g_config.slowQueryThresholdMs;
        g_checkpointInterval = g_config.checkpointInterval;
        g_engine.getLockManager().setLockTimeout(g_config.lockTimeoutMs);
        g_engine.getLockManager().setDeadlockTimeout(g_config.deadlockTimeoutMs);
    }

    // Server mode: ./dbms_main --server PORT
    if (argc >= 3 && std::string(argv[1]) == "--server") {
        int port = std::stoi(argv[2]);
        dbms::startServer(port);
        return 0;
    }

    Session s;
    s.statementTimeoutMs = g_config.statementTimeoutMs;
    s.defaultStatementTimeoutMs = g_config.statementTimeoutMs;
    // Register trigger executor callback
    g_engine.setTriggerExecutor([&](const std::string& actionSql) -> bool {
        Session triggerSession = s;
        triggerSession.preparedStmts.clear();
        return execute(actionSql, triggerSession);
    });
    // Register WHEN condition evaluator for triggers
    g_engine.setWhenConditionEvaluator(
        [&](const std::string& condition,
            const std::map<std::string, std::string>& /*newValues*/,
            const std::map<std::string, std::string>& /*oldValues*/) -> bool {
            // Simple expression evaluator for trigger WHEN conditions.
            // Supports numeric comparisons: val1 op val2 where op is ==, !=, <, >, <=, >=
            // Supports string comparisons with quotes.
            std::string cond = condition;
            // Normalize spaces
            std::string c;
            bool lastSpace = false;
            for (char ch : cond) {
                if (isspace(static_cast<unsigned char>(ch))) {
                    if (!lastSpace) c += ' ';
                    lastSpace = true;
                } else {
                    c += ch;
                    lastSpace = false;
                }
            }
            // Extract operator
            std::string ops[] = {"==", "!=", "<=", ">=", "<", ">"};
            std::string foundOp;
            size_t opPos = std::string::npos;
            for (const auto& op : ops) {
                opPos = c.find(op);
                if (opPos != std::string::npos) {
                    foundOp = op;
                    break;
                }
            }
            if (foundOp.empty()) {
                // No operator found; treat as truthy if non-empty and not "0" or "false"
                return !c.empty() && c != "0" && c != "false";
            }
            std::string left = trim(c.substr(0, opPos));
            std::string right = trim(c.substr(opPos + foundOp.size()));
            // Remove surrounding quotes for strings
            auto unquote = [](const std::string& s) {
                if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') return s.substr(1, s.size() - 2);
                return s;
            };
            std::string l = unquote(left);
            std::string r = unquote(right);
            // Try numeric comparison
            bool isNum = true;
            double lv = 0, rv = 0;
            try {
                lv = std::stod(l);
                rv = std::stod(r);
            } catch (...) {
                isNum = false;
            }
            if (isNum) {
                if (foundOp == "==") return lv == rv;
                if (foundOp == "!=") return lv != rv;
                if (foundOp == "<") return lv < rv;
                if (foundOp == ">") return lv > rv;
                if (foundOp == "<=") return lv <= rv;
                if (foundOp == ">=") return lv >= rv;
            } else {
                // String comparison
                if (foundOp == "==") return l == r;
                if (foundOp == "!=") return l != r;
                if (foundOp == "<") return l < r;
                if (foundOp == ">") return l > r;
                if (foundOp == "<=") return l <= r;
                if (foundOp == ">=") return l >= r;
            }
            return false;
        });
    string username, password;
    cout << "login" << endl;
    cin >> username >> password;
    if (login(username, password)) {
        s.username = username;
        s.authenticatedUser = username;
        s.originalRole = username;
        log(s.username, "login", getTime());
        s.permission = permissionQuery(username);
        s.authenticatedPermission = s.permission;
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
        // Register interactive session in process list
        uint64_t pid = dbms::registerProcess(s.username, "localhost", s.currentDB);
        s.pid = pid;
        int sqlCount = 0;
        while (true) {
            string sql;
            if (!getline(cin, sql)) break;
            if (trim(sql) == "exit") break;
            dbms::updateProcessInfo(pid, "Query", "executing", sql);
            auto start = std::chrono::steady_clock::now();
            bool ok = false;
            bool timedOut = false;
            if (s.statementTimeoutMs > 0) {
                auto future = std::async(std::launch::async, [&]() { return execute(sql, s); });
                if (future.wait_for(std::chrono::milliseconds(s.statementTimeoutMs))
                    == std::future_status::timeout) {
                    timedOut = true;
                    cout << "ERROR: statement timeout" << endl;
                } else {
                    ok = future.get();
                }
            } else {
                ok = execute(sql, s);
            }
            dbms::updateProcessDb(pid, s.currentDB);
            dbms::updateProcessInfo(pid, "Sleep", "", "");
            // Check if session was terminated
            if (s.terminateRequested) {
                cout << "Session terminated" << endl;
                break;
            }
            auto end = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            if (timedOut) {
                logSlowQuery(sql, ms, s.username, s.currentDB);
            } else if (ms > g_slowQueryThresholdMs) {
                logSlowQuery(sql, ms, s.username, s.currentDB);
            }
            if (g_config.autoExplainEnabled && ms >= g_config.autoExplainThresholdMs) {
                autoExplainLog(sql, ms, s.username, s.currentDB);
            }
            recordSqlStat(sql, ms, s.currentDB);
            if (ok && !s.currentDB.empty() && g_checkpointInterval > 0) {
                if (++sqlCount >= g_checkpointInterval) {
                    g_engine.checkpoint(s.currentDB);
                    sqlCount = 0;
                }
            }
        }
        dbms::unregisterProcess(pid);
    } else {
        cout << "wrong username or password" << endl;
    }
    return 0;
}
