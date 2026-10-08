#pragma once

#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/query_binding.h"
#include "parser/parser.h"
#include "utils/interval.h"

namespace dbms {
namespace assignment_input_detail {

inline bool unknownLiteral(const LiteralExpr* literal) {
    return literal && !literal->preparedSubquery && literal->typeName.empty();
}

inline void intervalLiteral(const LiteralExpr& literal) {
    // Decode one actual SQL literal only. No expression folding, stored
    // function lookup, parameters, rows, or scalar children are evaluated.
    ExprEvaluator evaluator;
    CastExpr conversion;
    conversion.typeName = "interval";
    conversion.operand = std::make_unique<LiteralExpr>(literal);
    // Reuse actual interval input, including valid symbolic infinity, rather
    // than imposing a second finite-only parser contract. The operand is
    // still one genuine literal: no row/routine/query is folded here.
    (void)evaluator.eval(&conversion, RowContext{});
}

inline void declaredIntervalLiteral(const Expr* source) {
    if (!source || source->preparedSubquery) return;
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(source)) {
        if (ExprHelper::canonicalResultTypeName(literal->typeName) == "interval")
            intervalLiteral(*literal);
        return;
    }
    const Expr* operand = nullptr;
    std::string target;
    if (const auto* cast = dynamic_cast<const CastExpr*>(source)) {
        operand = cast->operand.get(); target = cast->typeName;
    } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(source);
               binary && SQLParser::toLower(binary->op) == "::") {
        const auto* type = dynamic_cast<const LiteralExpr*>(binary->right.get());
        if (!type) return;
        operand = binary->left.get(); target = type->value;
    } else return;
    // Only unknown-to-interval input conversion belongs to analysis. A CAST
    // of an already typed expression is not evaluated here: even a pure
    // numeric narrowing/division can have a later planning-time error.
    if (ExprHelper::canonicalResultTypeName(target) == "interval") {
        const auto* literal = dynamic_cast<const LiteralExpr*>(operand);
        if (unknownLiteral(literal)) intervalLiteral(*literal);
    }
}

} // namespace assignment_input_detail

// Metadata-only scalar INTERVAL assignment input analysis. The caller must
// resolve all siblings of one VALUES row before this callback, and must not
// bind a later row first. CTE/derived unknown output types must be finalized
// to text; only a direct source leaf retains contextual unknown input.
// Other target types and arrays deliberately retain their existing contract.
inline void validateAssignmentInput(const QueryOutputColumn& target,
                                    const Expr* source,
                                    const std::string& sourceType) {
    if (ExprHelper::canonicalResultTypeName(target.type) != "interval") return;
    const auto type = ExprHelper::canonicalResultTypeName(sourceType);
    if (type.empty() || type == "unknown") {
        const auto* literal = dynamic_cast<const LiteralExpr*>(source);
        if (assignment_input_detail::unknownLiteral(literal)) {
            if (SQLParser::toLower(literal->value) == "default") return;
            assignment_input_detail::intervalLiteral(*literal);
        }
        return;
    }
    if (type != "interval")
        throw DbError("42804", "column \"" + target.name +
            "\" is of type interval but expression is of type " + type);
    assignment_input_detail::declaredIntervalLiteral(source);
}

} // namespace dbms
