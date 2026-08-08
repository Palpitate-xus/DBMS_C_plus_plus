#include "SqlStats.h"

#include <algorithm>
#include <cctype>
#include <map>
#include <mutex>

namespace dbms {
namespace {

std::map<std::string, SqlStatEntry> g_sqlStats;
std::mutex g_sqlStatsMutex;

bool isIdentifierChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '$';
}

bool startsLiteralNumber(const std::string& sql, size_t i) {
    if (i >= sql.size()) return false;
    const char c = sql[i];
    if (std::isdigit(static_cast<unsigned char>(c))) {
        return i == 0 || !isIdentifierChar(sql[i - 1]);
    }
    return c == '-' && i + 1 < sql.size() &&
           std::isdigit(static_cast<unsigned char>(sql[i + 1])) &&
           (i == 0 || !isIdentifierChar(sql[i - 1]));
}

void appendSpaceIfNeeded(std::string& out, bool& pendingSpace) {
    if (pendingSpace && !out.empty() && out.back() != ' ') out.push_back(' ');
    pendingSpace = false;
}

void skipQuotedLiteral(const std::string& sql, size_t& i) {
    ++i; // opening quote
    while (i < sql.size()) {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
            i += 2;
            continue;
        }
        if (sql[i] != '\'') {
            ++i;
            continue;
        }
        if (i + 1 < sql.size() && sql[i + 1] == '\'') {
            i += 2; // SQL escaped quote: ''
            continue;
        }
        ++i;
        break;
    }
}

void skipQuotedIdentifier(const std::string& sql, size_t& i, std::string& out) {
    out.push_back('"');
    ++i;
    while (i < sql.size()) {
        out.push_back(sql[i]);
        if (sql[i] == '"') {
            if (i + 1 < sql.size() && sql[i + 1] == '"') {
                out.push_back('"');
                i += 2;
                continue;
            }
            ++i;
            break;
        }
        ++i;
    }
}

void skipNumber(const std::string& sql, size_t& i) {
    if (sql[i] == '-') ++i;
    while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) ++i;
    if (i < sql.size() && sql[i] == '.') {
        ++i;
        while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) ++i;
    }
    if (i < sql.size() && (sql[i] == 'e' || sql[i] == 'E')) {
        size_t exponent = i++;
        if (i < sql.size() && (sql[i] == '+' || sql[i] == '-')) ++i;
        size_t digits = i;
        while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) ++i;
        if (digits == i) i = exponent; // leave malformed exponent visible
    }
}

} // namespace

std::string normalizeSqlForStats(const std::string& sql) {
    std::string out;
    bool pendingSpace = false;
    for (size_t i = 0; i < sql.size();) {
        const char c = sql[i];
        if (std::isspace(static_cast<unsigned char>(c))) {
            pendingSpace = true;
            ++i;
            continue;
        }
        if (c == '\'') {
            appendSpaceIfNeeded(out, pendingSpace);
            out.push_back('?');
            skipQuotedLiteral(sql, i);
            continue;
        }
        if (c == '"') {
            appendSpaceIfNeeded(out, pendingSpace);
            skipQuotedIdentifier(sql, i, out);
            continue;
        }
        if (startsLiteralNumber(sql, i)) {
            appendSpaceIfNeeded(out, pendingSpace);
            out.push_back('?');
            skipNumber(sql, i);
            continue;
        }
        appendSpaceIfNeeded(out, pendingSpace);
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        ++i;
    }
    while (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

void recordSqlStat(const std::string& sql, double elapsedMs,
                   const std::string& dbname) {
    const std::string normalized = normalizeSqlForStats(sql);
    if (normalized.empty()) return;
    std::lock_guard<std::mutex> lock(g_sqlStatsMutex);
    const std::string key = dbname + "\x1f" + normalized;
    auto& entry = g_sqlStats[key];
    if (entry.calls == 0) {
        entry.sql = sql;
        entry.dbname = dbname;
        entry.minTimeMs = elapsedMs;
        entry.maxTimeMs = elapsedMs;
    } else {
        entry.minTimeMs = std::min(entry.minTimeMs, elapsedMs);
        entry.maxTimeMs = std::max(entry.maxTimeMs, elapsedMs);
    }
    ++entry.calls;
    entry.totalTimeMs += elapsedMs;
    entry.meanTimeMs = entry.totalTimeMs / static_cast<double>(entry.calls);
}

std::vector<SqlStatEntry> getSqlStats(const std::string& dbFilter) {
    std::lock_guard<std::mutex> lock(g_sqlStatsMutex);
    std::vector<SqlStatEntry> result;
    for (const auto& item : g_sqlStats) {
        if (dbFilter.empty() || item.second.dbname == dbFilter) result.push_back(item.second);
    }
    std::sort(result.begin(), result.end(), [](const SqlStatEntry& a, const SqlStatEntry& b) {
        if (a.totalTimeMs != b.totalTimeMs) return a.totalTimeMs > b.totalTimeMs;
        return a.sql < b.sql;
    });
    return result;
}

void resetSqlStats() {
    std::lock_guard<std::mutex> lock(g_sqlStatsMutex);
    g_sqlStats.clear();
}

} // namespace dbms
