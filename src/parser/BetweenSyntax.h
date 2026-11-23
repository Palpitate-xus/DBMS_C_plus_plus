#pragma once

#include "parser/ast.h"
#include <string_view>

namespace dbms {

struct BetweenSyntax {
    bool symmetric;
    bool negated;
    bool explicitMode;
};

inline std::optional<BetweenSyntax> betweenSyntax(const FunctionCallExpr& call) {
    if (!call.schema.empty()) return {};
    const std::string_view name = call.funcName;
    const bool negated = name.substr(0, 4) == "NOT ";
    const auto predicate = name.substr(negated ? 4 : 0);
    if (predicate == "BETWEEN") return BetweenSyntax{false, negated, false};
    if (predicate == "BETWEEN ASYMMETRIC") return BetweenSyntax{false, negated, true};
    if (predicate == "BETWEEN SYMMETRIC") return BetweenSyntax{true, negated, true};
    return {};
}

} // namespace dbms
