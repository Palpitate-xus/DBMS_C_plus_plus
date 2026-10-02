#pragma once

#include "common/SqlTrivia.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace dbms {

inline bool sqlIdentifierContinuation(unsigned char c) {
    return std::isalnum(c) || c == '_' || c == '$' || c >= 0x80;
}

// Mark quoted/comment bytes without changing the SQL, datum, or byte offsets.
// This is a lexical boundary adapter, not a replacement for grammar analysis.
inline std::vector<bool> sqlProtectedBytes(const std::string& sql) {
    std::vector<bool> protectedBytes(sql.size(), false);
    for (size_t i = 0; i < sql.size();) {
        const size_t begin = i;
        if (sql.compare(i, 2, "--") == 0 || sql.compare(i, 2, "/*") == 0) {
            const size_t after = skipLeadingSqlTrivia(sql, i);
            i = after == std::string::npos ? sql.size() : after;
        } else if (sql[i] == '\'' || sql[i] == '"') {
            const char quote = sql[i];
            const bool escaped = quote == '\'' && i > 0 &&
                (sql[i - 1] == 'e' || sql[i - 1] == 'E') &&
                (i < 2 || !sqlIdentifierContinuation(static_cast<unsigned char>(sql[i - 2])));
            ++i;
            while (i < sql.size()) {
                if (escaped && sql[i] == '\\') {
                    i += std::min<size_t>(2, sql.size() - i);
                } else if (sql[i] == quote) {
                    ++i;
                    if (i < sql.size() && sql[i] == quote) ++i;
                    else break;
                } else {
                    ++i;
                }
            }
        } else if (sql[i] == '$' &&
                   (i == 0 || !sqlIdentifierContinuation(static_cast<unsigned char>(sql[i - 1])))) {
            size_t end = i + 1;
            if (end < sql.size() &&
                (std::isalpha(static_cast<unsigned char>(sql[end])) || sql[end] == '_')) {
                while (end < sql.size() &&
                       (std::isalnum(static_cast<unsigned char>(sql[end])) || sql[end] == '_')) ++end;
            }
            if (end >= sql.size() || sql[end] != '$') {
                ++i;
                continue;
            }
            const std::string delimiter = sql.substr(i, end - i + 1);
            const size_t close = sql.find(delimiter, end + 1);
            i = close == std::string::npos ? sql.size() : close + delimiter.size();
        } else {
            ++i;
            continue;
        }
        std::fill(protectedBytes.begin() + begin, protectedBytes.begin() + i, true);
    }
    return protectedBytes;
}

inline size_t findTopLevelSqlKeyword(const std::string& sql, const std::string& keyword) {
    if (keyword.empty()) return std::string::npos;
    const auto protectedBytes = sqlProtectedBytes(sql);
    size_t depth = 0;
    for (size_t i = 0; i < sql.size(); ++i) {
        if (protectedBytes[i]) continue;
        if (sql[i] == '(' || sql[i] == '[') { ++depth; continue; }
        if (sql[i] == ')' || sql[i] == ']') { if (depth) --depth; continue; }
        if (depth || keyword.size() > sql.size() - i) continue;
        if (i && sqlIdentifierContinuation(static_cast<unsigned char>(sql[i - 1]))) continue;
        const size_t after = i + keyword.size();
        if (after < sql.size() && sqlIdentifierContinuation(static_cast<unsigned char>(sql[after]))) continue;
        bool equal = true;
        for (size_t j = 0; j < keyword.size(); ++j) {
            if (protectedBytes[i + j] ||
                std::tolower(static_cast<unsigned char>(sql[i + j])) !=
                std::tolower(static_cast<unsigned char>(keyword[j]))) {
                equal = false;
                break;
            }
        }
        if (equal) return i;
    }
    return std::string::npos;
}

} // namespace dbms
