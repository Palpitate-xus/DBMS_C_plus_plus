#pragma once

#include <cctype>
#include <string>

namespace dbms {

// Return the first non-trivia byte without rewriting literals or identifiers.
// Unterminated block comments return npos; line comments may end at EOF.
inline size_t skipLeadingSqlTrivia(const std::string& sql, size_t position = 0) {
    while (true) {
        while (position < sql.size() &&
               std::isspace(static_cast<unsigned char>(sql[position]))) ++position;
        if (position + 1 >= sql.size()) return position;
        if (sql[position] == '-' && sql[position + 1] == '-') {
            position += 2;
            while (position < sql.size() && sql[position] != '\n' && sql[position] != '\r') ++position;
            continue;
        }
        if (sql[position] != '/' || sql[position + 1] != '*') return position;
        position += 2;
        size_t depth = 1;
        while (position < sql.size() && depth != 0) {
            if (position + 1 < sql.size() && sql[position] == '/' && sql[position + 1] == '*') {
                ++depth;
                position += 2;
            } else if (position + 1 < sql.size() && sql[position] == '*' && sql[position + 1] == '/') {
                --depth;
                position += 2;
            } else {
                ++position;
            }
        }
        if (depth != 0) return std::string::npos;
    }
}

} // namespace dbms
