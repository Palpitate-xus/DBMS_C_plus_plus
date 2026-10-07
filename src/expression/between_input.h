#pragma once

#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <algorithm>
#include <set>

namespace dbms::between_input_detail {

// A structural proof, not a value/type guess. Quoted/custom/qualified types
// retain their real owner; no domain codec, parameter, routine or SQL child
// is admitted to the pure primitive evaluator below.
inline bool primitiveType(const std::string& spelling) {
    if (spelling.find_first_of("\".()") != std::string::npos) return false;
    static const std::set<std::string> names = {
        "bit", "bit varying", "text", "boolean", "smallint", "integer", "bigint"
    };
    return names.count(ExprHelper::canonicalResultTypeName(spelling));
}

inline bool literal(const Expr* expression) {
    const auto* value = dynamic_cast<const LiteralExpr*>(expression);
    if (!value || value->preparedSubquery ||
        (!value->typeName.empty() && !primitiveType(value->typeName))) return false;
    const auto tokens = SQLParser::tokenize(value->value);
    if (tokens.size() != 1 || tokens[0].empty() || tokens[0] == "*") return false;
    const auto word = SQLParser::toLower(tokens[0]);
    if (word == "null" || word == "true" || word == "false") return true;
    if (tokens[0].front() == '\'' && tokens[0].back() == '\'') return true;
    return std::all_of(tokens[0].begin(), tokens[0].end(), [](unsigned char c) {
        return (c >= '0' && c <= '9') || c == '.';
    });
}

inline bool constant(const Expr* expression) {
    if (!expression || expression->preparedSubquery) return false;
    if (literal(expression)) return true;
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        return cast->typeMods.empty() && primitiveType(cast->typeName) && constant(cast->operand.get());
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression))
        return (unary->op == "+" || unary->op == "-") && constant(unary->operand.get());
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
        if (binary->op == "::") {
            const auto* target = dynamic_cast<const LiteralExpr*>(binary->right.get());
            return target && primitiveType(target->value) && constant(binary->left.get());
        }
        static const std::set<std::string> arithmetic = {"+", "-", "*", "/", "%"};
        return arithmetic.count(binary->op) && constant(binary->left.get()) && constant(binary->right.get());
    }
    return false;
}

inline bool nullConstant(const Expr* expression) {
    if (!expression || expression->preparedSubquery) return false;
    if (const auto* value = dynamic_cast<const LiteralExpr*>(expression))
        return literal(expression) && SQLParser::toLower(value->value) == "null";
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        return cast->typeMods.empty() && primitiveType(cast->typeName) && nullConstant(cast->operand.get());
    if (const auto* cast = dynamic_cast<const BinaryOpExpr*>(expression); cast && cast->op == "::") {
        const auto* target = dynamic_cast<const LiteralExpr*>(cast->right.get());
        return target && primitiveType(target->value) && nullConstant(cast->left.get());
    }
    return false;
}

// Transform only a direct UNKNOWN SQL string passed to a primitive CAST.
// This is analysis-time input conversion, not arithmetic constant folding:
// an unreachable 1/0 stays unreachable, but 'bad'::int still reports 22P02.
inline void validateLiteralCasts(const Expr* expression) {
    if (!expression || expression->preparedSubquery) return;
    const auto value = [&](const ExprPtr& child) { validateLiteralCasts(child.get()); };
    const auto conversion = [&](const Expr* operand, const std::string& target) {
        const auto* input = dynamic_cast<const LiteralExpr*>(operand);
        if (!input || !input->typeName.empty() || !literal(input) || !primitiveType(target)) return;
        const auto tokens = SQLParser::tokenize(input->value);
        if (tokens[0].front() != '\'') return;
        CastExpr cast; cast.typeName = target;
        cast.operand = std::make_unique<LiteralExpr>(*input);
        ExprEvaluator pure; (void)pure.eval(&cast, RowContext{});
    };
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
        value(cast->operand);
        if (cast->typeMods.empty()) conversion(cast->operand.get(), cast->typeName);
    } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
        value(binary->left);
        if (binary->op == "::") {
            if (const auto* target = dynamic_cast<const LiteralExpr*>(binary->right.get()))
                conversion(binary->left.get(), target->value);
        } else value(binary->right);
    } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) value(unary->operand);
    else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
        for (const auto& argument : call->args) value(argument);
        for (const auto& argument : call->namedArgs) value(argument.value);
        value(call->filter);
        for (const auto& argument : call->over.partitionBy) value(argument);
        for (const auto& argument : call->over.orderBy) value(argument.first);
        value(call->over.frameStart); value(call->over.frameEnd);
    } else if (const auto* conditional = dynamic_cast<const CaseExpr*>(expression)) {
        value(conditional->switchExpr); value(conditional->elseExpr);
        for (const auto& arm : conditional->whenClauses) { value(arm.first); value(arm.second); }
    } else if (const auto* array = dynamic_cast<const ArrayExpr*>(expression)) {
        for (const auto& item : array->elements) value(item);
    } else if (const auto* row = dynamic_cast<const RowExpr*>(expression)) {
        for (const auto& item : row->elements) value(item);
    } else if (const auto* quantified = dynamic_cast<const QuantifiedComparisonExpr*>(expression)) {
        value(quantified->left); value(quantified->right);
    }
}

} // namespace dbms::between_input_detail
