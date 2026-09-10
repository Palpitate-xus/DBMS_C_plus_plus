// ============================================================================
// PostgreSQL-style prepared statement helpers.
//
// PREPARE name [(type, ...)] AS <statement with $n>
// EXECUTE name [(expr, ...)]
// DEALLOCATE [PREPARE] name | DEALLOCATE ALL
//
// The main.cpp SQL dispatcher uses these to split/stitch statement text;
// keeping them here (a linked unit, unlike main.cpp) makes them unit-testable.
// ============================================================================

#include "utils/prepared_stmts.h"

#include <algorithm>
#include <cctype>
#include <limits>

namespace dbms {

namespace {

struct DollarParameterRef {
    size_t begin = 0;
    size_t end = 0;
    size_t number = 0;
};

bool collectDollarParameters(const std::string& sql,
                             std::vector<DollarParameterRef>& refs,
                             std::string& error) {
    refs.clear();
    error.clear();
    bool singleQuoted = false;
    bool escapeSingleQuoted = false;
    bool doubleQuoted = false;
    bool lineComment = false;
    size_t blockCommentDepth = 0;
    std::string dollarDelimiter;

    for (size_t i = 0; i < sql.size(); ++i) {
        const char c = sql[i];
        if (lineComment) {
            if (c == '\n' || c == '\r') lineComment = false;
            continue;
        }
        if (blockCommentDepth != 0) {
            if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
                ++blockCommentDepth;
                ++i;
            } else if (c == '*' && i + 1 < sql.size() && sql[i + 1] == '/') {
                --blockCommentDepth;
                ++i;
            }
            continue;
        }
        if (!dollarDelimiter.empty()) {
            if (sql.compare(i, dollarDelimiter.size(), dollarDelimiter) == 0) {
                i += dollarDelimiter.size() - 1;
                dollarDelimiter.clear();
            }
            continue;
        }
        if (singleQuoted) {
            if (escapeSingleQuoted && c == '\\' && i + 1 < sql.size()) {
                ++i;
            } else if (c == '\'' && i + 1 < sql.size() && sql[i + 1] == '\'') {
                ++i;
            } else if (c == '\'') {
                singleQuoted = false;
                escapeSingleQuoted = false;
            }
            continue;
        }
        if (doubleQuoted) {
            if (c == '"' && i + 1 < sql.size() && sql[i + 1] == '"') {
                ++i;
            } else if (c == '"') {
                doubleQuoted = false;
            }
            continue;
        }
        if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
            lineComment = true;
            ++i;
            continue;
        }
        if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
            blockCommentDepth = 1;
            ++i;
            continue;
        }
        if (c == '\'') {
            singleQuoted = true;
            escapeSingleQuoted = i > 0 &&
                (sql[i - 1] == 'e' || sql[i - 1] == 'E') &&
                (i < 2 || (!std::isalnum(static_cast<unsigned char>(sql[i - 2])) &&
                           sql[i - 2] != '_' && sql[i - 2] != '$'));
            continue;
        }
        if (c == '"') {
            doubleQuoted = true;
            continue;
        }
        if (c != '$' || i + 1 >= sql.size()) continue;

        if (std::isdigit(static_cast<unsigned char>(sql[i + 1]))) {
            size_t number = 0;
            size_t end = i + 1;
            while (end < sql.size() &&
                   std::isdigit(static_cast<unsigned char>(sql[end]))) {
                const size_t digit = static_cast<size_t>(sql[end] - '0');
                if (number > (std::numeric_limits<size_t>::max() - digit) / 10) {
                    error = "parameter number is too large";
                    return false;
                }
                number = number * 10 + digit;
                ++end;
            }
            if (number == 0) {
                error = "there is no parameter $0";
                return false;
            }
            refs.push_back({i, end, number});
            i = end - 1;
            continue;
        }

        // $tag$...$tag$ and $$...$$ are string literals, not parameters.
        size_t delimiterEnd = i + 1;
        if (sql[delimiterEnd] != '$' &&
            !std::isalpha(static_cast<unsigned char>(sql[delimiterEnd])) &&
            sql[delimiterEnd] != '_') {
            continue;
        }
        if (sql[delimiterEnd] != '$') {
            ++delimiterEnd;
            while (delimiterEnd < sql.size() &&
                   (std::isalnum(static_cast<unsigned char>(sql[delimiterEnd])) ||
                    sql[delimiterEnd] == '_')) {
                ++delimiterEnd;
            }
        }
        if (delimiterEnd < sql.size() && sql[delimiterEnd] == '$') {
            dollarDelimiter = sql.substr(i, delimiterEnd - i + 1);
            i = delimiterEnd;
        }
    }
    return true;
}

} // namespace

std::string ps_trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

// Find the top-level " AS " separating the PREPARE head from the statement.
// Quote-, paren- and dollar-quote-aware. npos when absent.
size_t ps_findPrepareAs(const std::string& rest) {
    bool inS = false;
    int depth = 0;
    for (size_t i = 0; i < rest.size(); ++i) {
        char c = rest[i];
        if (inS) {
            if (c == '\'') {
                if (i + 1 < rest.size() && rest[i + 1] == '\'') ++i;
                else inS = false;
            }
            continue;
        }
        if (c == '\'') { inS = true; continue; }
        if (c == '$' && i + 1 < rest.size()
            && (std::isalpha(static_cast<unsigned char>(rest[i + 1])) || rest[i + 1] == '$')) {
            // dollar-quote: find $tag$ ... $tag$
            size_t j = i + 1;
            while (j < rest.size() && (std::isalnum(static_cast<unsigned char>(rest[j])) || rest[j] == '_')) ++j;
            if (j < rest.size() && rest[j] == '$') {
                std::string delim = rest.substr(i, j - i + 1);
                size_t close = rest.find(delim, j + 1);
                if (close != std::string::npos) i = close + delim.size() - 1;
            }
            continue;
        }
        if (c == '(') { ++depth; continue; }
        if (c == ')') { if (depth > 0) --depth; continue; }
        if (depth == 0 && c == ' ' && i + 3 < rest.size()
            && (rest.compare(i + 1, 3, "as ") == 0 || rest.compare(i + 1, 3, "As ") == 0
                || rest.compare(i + 1, 3, "aS ") == 0 || rest.compare(i + 1, 3, "AS ") == 0)) {
            return i;
        }
    }
    return std::string::npos;
}

// Parse "name" or "name ( type, type )" into name + declared types.
bool ps_parsePrepareHead(const std::string& head, std::string& name,
                         std::vector<std::string>& paramTypes) {
    name.clear();
    paramTypes.clear();
    size_t op = head.find('(');
    if (op != std::string::npos) {
        size_t cp = head.rfind(')');
        if (cp == std::string::npos || cp < op ||
            !ps_trim(head.substr(cp + 1)).empty()) return false;
        name = ps_trim(head.substr(0, op));
        std::string types = head.substr(op + 1, cp - op - 1);
        if (!ps_trim(types).empty()) {
            paramTypes = ps_splitExecuteArgs(types);
            for (const auto& type : paramTypes) {
                if (type.empty()) return false;
            }
        }
    } else {
        name = ps_trim(head);
    }
    return !name.empty();
}

bool ps_analyzeDollarParams(const std::string& sql, size_t& count,
                            std::string& error) {
    std::vector<DollarParameterRef> refs;
    if (!collectDollarParameters(sql, refs, error)) return false;
    count = 0;
    for (const auto& ref : refs) count = std::max(count, ref.number);
    return true;
}

// Split an EXECUTE argument list on top-level commas (quote/paren aware).
std::vector<std::string> ps_splitExecuteArgs(const std::string& in) {
    std::vector<std::string> out;
    std::string cur;
    bool inS = false, inD = false;
    int depth = 0;
    for (size_t i = 0; i < in.size(); ++i) {
        char c = in[i];
        if (inS) {
            cur += c;
            if (c == '\'') {
                if (i + 1 < in.size() && in[i + 1] == '\'') { cur += in[i + 1]; ++i; }
                else inS = false;
            }
            continue;
        }
        if (inD) {
            cur += c;
            if (c == '\\') { if (i + 1 < in.size()) cur += in[++i]; }
            else if (c == '"') inD = false;
            continue;
        }
        if (c == '\'') { inS = true; cur += c; continue; }
        if (c == '"') { inD = true; cur += c; continue; }
        if (c == '(' || c == '[') { ++depth; cur += c; continue; }
        if (c == ')' || c == ']') { if (depth > 0) --depth; cur += c; continue; }
        if (c == ',' && depth == 0) { out.push_back(ps_trim(cur)); cur.clear(); continue; }
        cur += c;
    }
    if (!cur.empty() || !out.empty()) out.push_back(ps_trim(cur));
    return out;
}

// Replace actual $n parameter tokens with values (1-based).  Text resembling
// a parameter inside a literal, quoted identifier or comment is data and must
// remain untouched.
bool ps_substituteDollarParams(const std::string& in,
                               const std::vector<std::string>& values,
                               std::string& out, std::string& error) {
    std::vector<DollarParameterRef> refs;
    if (!collectDollarParameters(in, refs, error)) return false;
    out.clear();
    out.reserve(in.size());
    size_t copied = 0;
    for (const auto& ref : refs) {
        if (ref.number > values.size()) {
            error = "there is no parameter $" + std::to_string(ref.number);
            return false;
        }
        out.append(in, copied, ref.begin - copied);
        out += values[ref.number - 1];
        copied = ref.end;
    }
    out.append(in, copied, std::string::npos);
    return true;
}

} // namespace dbms
