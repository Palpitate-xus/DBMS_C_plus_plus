#include "collation.h"

#include <algorithm>
#include <cctype>
#include <locale>
#include <set>

namespace dbms {
namespace collation {

namespace {

std::string toLower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

bool isBinaryCollation(const std::string& name) {
    return name.empty() || name == "default" || name == "c" || name == "posix" ||
           name == "ucs_basic";
}

int caseInsensitiveCompare(const std::string& a, const std::string& b) {
    size_t n = std::max(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) {
        unsigned char ca = (i < a.size()) ? static_cast<unsigned char>(a[i]) : 0;
        unsigned char cb = (i < b.size()) ? static_cast<unsigned char>(b[i]) : 0;
        int la = std::tolower(ca);
        int lb = std::tolower(cb);
        if (la != lb) return la - lb;
    }
    return 0;
}

int localeCompare(const std::string& a, const std::string& b, const std::string& locName) {
    try {
        const std::locale loc(locName.empty() ? "C" : locName.c_str());
        const std::collate<char>& coll = std::use_facet<std::collate<char>>(loc);
        return coll.compare(a.data(), a.data() + a.size(), b.data(), b.data() + b.size());
    } catch (...) {
        // Locale unavailable: fall back to binary comparison.
        return a.compare(b);
    }
}

} // anonymous namespace

std::string normalizeName(const std::string& name) {
    std::string s = toLower(name);
    // Strip surrounding quotes if any.
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                             (s.front() == '\"' && s.back() == '\"'))) {
        s = s.substr(1, s.size() - 2);
    }
    // Map common variants.
    if (s == "en_us" || s == "en_us.utf-8" || s == "en-us" || s == "english") {
        return "en_us.utf8";
    }
    if (s == "case_insensitive" || s == "nocase") return "nocase";
    if (s == "reverse") return "reverse";
    return s;
}

bool isValid(const std::string& name) {
    static const std::set<std::string> kBuiltins = {
        "default", "c", "posix", "ucs_basic",
        "en_us.utf8", "en_us",
        "nocase", "reverse"};
    return kBuiltins.count(normalizeName(name)) > 0;
}

bool isBinary(const std::string& name) {
    return isBinaryCollation(normalizeName(name));
}

int compare(const std::string& a, const std::string& b, const std::string& collation) {
    std::string coll = normalizeName(collation);
    if (isBinaryCollation(coll)) {
        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
    }
    if (coll == "nocase") return caseInsensitiveCompare(a, b);
    if (coll == "reverse") {
        if (b < a) return -1;
        if (b > a) return 1;
        return 0;
    }
    if (coll == "en_us" || coll == "en_us.utf8") {
        return localeCompare(a, b, "en_US.UTF-8");
    }
    // Unknown collation: treat as binary.
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

std::vector<std::string> listBuiltins() {
    return {"default", "C", "POSIX", "ucs_basic", "en_US.utf8", "en_US", "nocase", "reverse"};
}

} // namespace collation
} // namespace dbms
