#pragma once

#include "common/SqlSyntax.h"

#include <string>
#include <string_view>

namespace dbms {

// This boundary is interpreted only for columns with the corresponding
// schema flag. The expression remains valid for readers of older schemas.
inline constexpr std::string_view storedDomainCheckPrefix = "/*DBMS domain CHECK*/(";

inline std::string annotateStoredDomainCheck(const std::string& expression) {
    return std::string(storedDomainCheckPrefix) + expression + ")";
}

inline std::string copyCheckWithoutDomain(const std::string& check) {
    const auto start = check.find_first_not_of('(');
    if (start == std::string::npos ||
        check.compare(start, storedDomainCheckPrefix.size(), storedDomainCheckPrefix))
        return check;
    const auto open = start + storedDomainCheckPrefix.size() - 1;
    const auto protectedBytes = sqlProtectedBytes(check);
    size_t depth = 1;
    for (size_t i = open + 1; i < check.size(); ++i) {
        if (protectedBytes[i]) continue;
        if (check[i] == '(') ++depth;
        if (check[i] != ')' || --depth != 0) continue;
        if (start == 0 && i + 1 == check.size()) return {};
        auto residual = check;
        residual.replace(start, i + 1 - start, "TRUE");
        return residual;
    }
    return check;
}

} // namespace dbms
