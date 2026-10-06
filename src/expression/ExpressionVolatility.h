#pragma once

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"

namespace dbms {

// Metadata-only analysis. Looking at declared volatility must never invoke a
// function merely to decide where its result projection belongs in a plan.
inline bool expressionContainsVolatileFunction(
    const Expr* expression, const ExprEvaluator& evaluator,
    StorageEngine* engine = nullptr) {
    if (!expression) return false;
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
        if (evaluator.scalarFunctionVolatility(call, engine) == 'v') return true;
        for (const auto& arg : call->args)
            if (expressionContainsVolatileFunction(arg.get(), evaluator, engine)) return true;
        for (const auto& arg : call->namedArgs)
            if (expressionContainsVolatileFunction(arg.value.get(), evaluator, engine)) return true;
        if (expressionContainsVolatileFunction(call->filter.get(), evaluator, engine)) return true;
        for (const auto& part : call->over.partitionBy)
            if (expressionContainsVolatileFunction(part.get(), evaluator, engine)) return true;
        for (const auto& order : call->over.orderBy)
            if (expressionContainsVolatileFunction(order.first.get(), evaluator, engine)) return true;
        return expressionContainsVolatileFunction(call->over.frameStart.get(), evaluator, engine) ||
               expressionContainsVolatileFunction(call->over.frameEnd.get(), evaluator, engine);
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression))
        return expressionContainsVolatileFunction(unary->operand.get(), evaluator, engine);
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression))
        return expressionContainsVolatileFunction(binary->left.get(), evaluator, engine) ||
               (binary->op != "::" && expressionContainsVolatileFunction(binary->right.get(), evaluator, engine));
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        return expressionContainsVolatileFunction(cast->operand.get(), evaluator, engine);
    if (const auto* conditional = dynamic_cast<const CaseExpr*>(expression)) {
        if (expressionContainsVolatileFunction(conditional->switchExpr.get(), evaluator, engine)) return true;
        for (const auto& arm : conditional->whenClauses)
            if (expressionContainsVolatileFunction(arm.first.get(), evaluator, engine) ||
                expressionContainsVolatileFunction(arm.second.get(), evaluator, engine)) return true;
        return expressionContainsVolatileFunction(conditional->elseExpr.get(), evaluator, engine);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expression)) {
        for (const auto& element : array->elements)
            if (expressionContainsVolatileFunction(element.get(), evaluator, engine)) return true;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expression)) {
        for (const auto& element : row->elements)
            if (expressionContainsVolatileFunction(element.get(), evaluator, engine)) return true;
    }
    // Query children need their own planning boundary, not an assumption that
    // an unrecognized child can be evaluated below Sort as an immutable value.
    return expression->type == ExprType::Subquery;
}

} // namespace dbms
