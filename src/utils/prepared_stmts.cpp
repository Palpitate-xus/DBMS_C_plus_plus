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

#include <cctype>
#include <sstream>

namespace dbms {

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
        if (cp == std::string::npos || cp < op) return false;
        name = ps_trim(head.substr(0, op));
        std::string types = head.substr(op + 1, cp - op - 1);
        std::stringstream tss(types);
        std::string t;
        while (std::getline(tss, t, ',')) {
            t = ps_trim(t);
            if (!t.empty()) paramTypes.push_back(t);
        }
    } else {
        name = ps_trim(head);
    }
    return !name.empty();
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

// Replace $n place-holders with values (1-based). Returns false (and sets
// error) when a $n is out of range. $n inside single-quoted strings is still
// substituted — PostgreSQL also substitutes inside the prepared text.
bool ps_substituteDollarParams(const std::string& in,
                               const std::vector<std::string>& values,
                               std::string& out, std::string& error) {
    out.clear();
    error.clear();
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '$' && i + 1 < in.size()
            && std::isdigit(static_cast<unsigned char>(in[i + 1]))) {
            size_t j = i + 1;
            long n = 0;
            while (j < in.size() && std::isdigit(static_cast<unsigned char>(in[j]))) {
                n = n * 10 + (in[j] - '0');
                ++j;
            }
            if (n < 1 || n > static_cast<long>(values.size())) {
                error = "prepared statement parameter $" + std::to_string(n) + " out of range";
                return false;
            }
            out += values[static_cast<size_t>(n - 1)];
            i = j - 1;
        } else {
            out += in[i];
        }
    }
    return true;
}

} // namespace dbms
