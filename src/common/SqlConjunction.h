#pragma once

#include <cctype>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace dbms {

// Split top-level conjuncts, not the bound separator of BETWEEN. This is
// only lexical separation: Boolean trees and expression binding belong to
// the SQL parser. Preserve quoted text and nested expressions verbatim.
inline std::vector<std::string> splitSqlBooleanConnective(
        const std::string& text, const std::string& connective) {
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
            else if (word == "and" && between) between = false;
            else if (word == connective) {
                append(wordStart);
                start = i;
            }
            continue;
        }
        ++i;
    }
    append(text.size());
    return result;
}

inline std::vector<std::string> splitSqlConjunction(const std::string& text) {
    return splitSqlBooleanConnective(text, "and");
}

// Preserve complete legacy predicate atoms while lowering AND/OR grouping.
// This does not bind expressions or implement unary NOT. Bound expansion so
// a compact Boolean expression cannot allocate an unbounded DNF plan.
inline std::vector<std::vector<std::string>> sqlBooleanGroups(
        const std::string& text, size_t maxBranches = 1024,
        size_t maxReferences = 4096) {
    using Groups = std::vector<std::vector<std::string>>;
    const auto trimText = [](std::string value) {
        const auto first = value.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) return std::string();
        return value.substr(first, value.find_last_not_of(" \t\r\n") - first + 1);
    };
    const auto checkSize = [&](const Groups& groups) {
        if (groups.size() > maxBranches)
            throw std::length_error("EXPLAIN Boolean expression has too many branches");
        size_t references = 0;
        for (const auto& branch : groups) {
            if (branch.size() > maxReferences - references)
                throw std::length_error("EXPLAIN Boolean expression has too many predicates");
            references += branch.size();
        }
    };
    std::function<Groups(std::string, size_t)> parse;
    parse = [&](std::string expression, size_t nesting) -> Groups {
        if (nesting > 128)
            throw std::length_error("EXPLAIN Boolean expression is nested too deeply");
        expression = trimText(std::move(expression));
        if (expression.empty())
            throw std::invalid_argument("empty EXPLAIN Boolean predicate");
        size_t depth = 0, firstClose = std::string::npos;
        char quote = '\0';
        for (size_t i = 0; i < expression.size(); ++i) {
            const char ch = expression[i];
            if (quote != '\0') {
                if (ch == quote) {
                    if (i + 1 < expression.size() && expression[i + 1] == quote) ++i;
                    else quote = '\0';
                }
                continue;
            }
            if (ch == '\'' || ch == '"') quote = ch;
            else if (ch == '(') ++depth;
            else if (ch == ')') {
                if (depth == 0)
                    throw std::invalid_argument("unbalanced EXPLAIN Boolean parentheses");
                if (--depth == 0 && firstClose == std::string::npos) firstClose = i;
            }
        }
        if (depth != 0 || quote != '\0')
            throw std::invalid_argument("unbalanced EXPLAIN Boolean expression");
        if (expression.front() == '(' && firstClose == expression.size() - 1)
            return parse(expression.substr(1, expression.size() - 2), nesting + 1);

        const auto alternatives = splitSqlBooleanConnective(expression, "or");
        if (alternatives.size() > 1) {
            Groups result;
            for (const auto& alternative : alternatives) {
                Groups right = parse(alternative, nesting + 1);
                if (right.size() > maxBranches - result.size())
                    throw std::length_error("EXPLAIN Boolean expression has too many branches");
                result.insert(result.end(), right.begin(), right.end());
                checkSize(result);
            }
            return result;
        }
        const auto conjuncts = splitSqlConjunction(expression);
        if (conjuncts.size() > 1) {
            Groups result(1);
            for (const auto& conjunct : conjuncts) {
                const Groups right = parse(conjunct, nesting + 1);
                if (right.size() != 0 && result.size() > maxBranches / right.size())
                    throw std::length_error("EXPLAIN Boolean expression has too many branches");
                Groups combined;
                size_t references = 0;
                for (const auto& leftBranch : result) {
                    for (const auto& rightBranch : right) {
                        if (leftBranch.size() > maxReferences - rightBranch.size() ||
                            leftBranch.size() + rightBranch.size() > maxReferences - references)
                            throw std::length_error("EXPLAIN Boolean expression has too many predicates");
                        references += leftBranch.size() + rightBranch.size();
                        auto branch = leftBranch;
                        branch.insert(branch.end(), rightBranch.begin(), rightBranch.end());
                        combined.push_back(std::move(branch));
                    }
                }
                checkSize(combined);
                result = std::move(combined);
            }
            return result;
        }
        Groups atom{{std::move(expression)}};
        checkSize(atom);
        return atom;
    };
    return parse(text, 0);
}

} // namespace dbms
