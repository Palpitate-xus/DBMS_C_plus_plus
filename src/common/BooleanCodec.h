#pragma once

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>

namespace dbms {

// PostgreSQL boolean input accepts 1/0 and unambiguous, case-insensitive
// prefixes of true/false, yes/no, and on/off.  Keep this rule in one codec so
// SQL casts, heap writes, predicates, COPY and protocol parameters cannot
// silently disagree about the same value.
inline std::optional<bool> parsePostgresBoolean(std::string input) {
    const auto notSpace = [](unsigned char c) { return !std::isspace(c); };
    input.erase(input.begin(), std::find_if(input.begin(), input.end(), notSpace));
    input.erase(std::find_if(input.rbegin(), input.rend(), notSpace).base(),
                input.end());
    std::transform(input.begin(), input.end(), input.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });
    if (input == "1") return true;
    if (input == "0") return false;
    if (input.empty()) return std::nullopt;

    static constexpr struct {
        const char* name;
        bool value;
    } accepted[] = {
        {"true", true}, {"false", false}, {"yes", true},
        {"no", false}, {"on", true}, {"off", false},
    };
    std::optional<bool> result;
    size_t matches = 0;
    for (const auto& candidate : accepted) {
        const std::string name(candidate.name);
        if (input.size() <= name.size() &&
            name.compare(0, input.size(), input) == 0) {
            result = candidate.value;
            ++matches;
        }
    }
    return matches == 1 ? result : std::nullopt;
}

inline const char* postgresBooleanOutput(bool value) {
    return value ? "t" : "f";
}

}  // namespace dbms
