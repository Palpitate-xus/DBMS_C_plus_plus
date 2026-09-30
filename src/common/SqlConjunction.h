#pragma once

#include <cctype>
#include <string>
#include <vector>

namespace dbms {

// Split top-level conjuncts, not the bound separator of BETWEEN. This is
// only lexical separation: Boolean trees and expression binding belong to
// the SQL parser. Preserve quoted text and nested expressions verbatim.
inline std::vector<std::string> splitSqlConjunction(const std::string& text) {
    std::vector<std::string> result;
    size_t start = 0;
    size_t depth = 0;
    bool between = false;
    char quote = '\0';
    const auto append = [&](size_t end) {
        size_t first = start;
        while (first < end && std::isspace(static_cast<unsigned char>(text[first]))) ++first;
        while (end > first && std::isspace(static_cast<unsigned char>(text[end - 1]))) --end;
        result.push_back(text.substr(first, end - first));
    };
    for (size_t i = 0; i < text.size();) {
        const char ch = text[i];
        if (quote != '\0') {
            if (ch == quote) {
                if (i + 1 < text.size() && text[i + 1] == quote) {
                    i += 2;
                    continue;
                }
                quote = '\0';
            }
            ++i;
            continue;
        }
        if (ch == '\'' || ch == '"') {
            quote = ch;
            ++i;
            continue;
        }
        if (ch == '(') { ++depth; ++i; continue; }
        if (ch == ')') { if (depth > 0) --depth; ++i; continue; }
        if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_') {
            const size_t wordStart = i++;
            while (i < text.size() &&
                   (std::isalnum(static_cast<unsigned char>(text[i])) ||
                    text[i] == '_' || text[i] == '$')) ++i;
            if (depth != 0) continue;
            std::string word = text.substr(wordStart, i - wordStart);
            for (char& letter : word)
                letter = static_cast<char>(std::tolower(static_cast<unsigned char>(letter)));
            if (word == "between") between = true;
            else if (word == "and") {
                if (between) between = false;
                else { append(wordStart); start = i; }
            }
            continue;
        }
        ++i;
    }
    append(text.size());
    return result;
}

} // namespace dbms
