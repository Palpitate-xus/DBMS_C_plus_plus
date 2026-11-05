#pragma once

#include "expression/SqlPattern.h"
#include <optional>

namespace dbms::planner_pattern {

// A necessary byte prefix for the existing case-sensitive text LIKE matcher.
// Validate the whole pattern, including escapes after the first wildcard, so
// planning never changes the executor's demand for malformed-pattern errors.
inline std::optional<std::string> likePrefix(const std::string& pattern,
                                             const std::string& escape = "\\") {
    sql_pattern::Characters source, escapes;
    try {
        source = sql_pattern::characters(pattern);
        escapes = sql_pattern::characters(escape);
    } catch (const DbError&) {
        return std::nullopt;
    }
    if (escapes.size() > 1) return std::nullopt;
    std::string prefix;
    bool fixed = true;
    for (size_t i = 0; i < source.size(); ++i) {
        auto character = source[i];
        if (!escapes.empty() && character == escapes.front()) {
            if (++i == source.size()) return std::nullopt;
            character = source[i];
        } else if (character == '%' || character == '_') {
            fixed = false;
            continue;
        }
        if (fixed) {
            char bytes[4];
            int32_t size = 0;
            U8_APPEND_UNSAFE(bytes, size, character);
            prefix.append(bytes, size);
        }
    }
    return prefix.empty() ? std::nullopt : std::optional<std::string>(std::move(prefix));
}

// A valid UTF8 exclusive ceiling in byte order. Incrementing an arbitrary
// trailing byte can produce invalid text (e.g. U+00FF ends in 0xBF), which also
// corrupts EXPLAIN's text/JSON description. Carry across maximum codepoints
// and skip surrogates. An inclusive overlap may return the boundary itself;
// the original LIKE still rechecks that harmless extra candidate.
inline std::optional<std::string> prefixCeiling(std::string prefix) {
    sql_pattern::Characters points;
    try { points = sql_pattern::characters(prefix); }
    catch (const DbError&) { return std::nullopt; }
    while (!points.empty()) {
        if (points.back() == 0x10ffff) {
            points.pop_back();
            continue;
        }
        if (++points.back() == 0xd800) points.back() = 0xe000;
        prefix.clear();
        for (const auto point : points) {
            char bytes[4];
            int32_t size = 0;
            U8_APPEND_UNSAFE(bytes, size, point);
            prefix.append(bytes, size);
        }
        return prefix;
    }
    return std::nullopt;
}

} // namespace dbms::planner_pattern
