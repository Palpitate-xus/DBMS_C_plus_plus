#pragma once

#include "parser/parser.h"

#include <map>
#include <set>
#include <string_view>
#include <utility>

namespace dbms {
namespace local_expression_syntax_detail {
struct SyntaxCloneUnsupported {};

inline ExprPtr cloneSyntaxExpression(const Expr* source) {
    if (!source) return {};
    if (source->preparedSubquery) throw SyntaxCloneUnsupported{};
    ExprPtr result;
    const auto copy = [&](const ExprPtr& node) { return cloneSyntaxExpression(node.get()); };
    const auto window = [&](const WindowDef& source, WindowDef& target) {
        target.name = source.name; target.frameMode = source.frameMode;
        target.frameExclusion = source.frameExclusion;
        target.frameStartKind = source.frameStartKind;
        target.frameEndKind = source.frameEndKind;
        target.frameBetween = source.frameBetween;
        target.orderNullsFirst = source.orderNullsFirst;
        target.orderDirectionSpecified = source.orderDirectionSpecified;
        target.referenceName = source.referenceName;
        target.partitionComparisons = source.partitionComparisons;
        target.orderComparisons = source.orderComparisons;
        for (const auto& expr : source.partitionBy) target.partitionBy.push_back(copy(expr));
        for (const auto& expr : source.orderBy) target.orderBy.emplace_back(copy(expr.first),expr.second);
        target.frameStart = copy(source.frameStart); target.frameEnd = copy(source.frameEnd);
    };
    if (const auto* node = dynamic_cast<const LiteralExpr*>(source)) result = std::make_unique<LiteralExpr>(*node);
    else if (const auto* node = dynamic_cast<const ColumnRefExpr*>(source)) result = std::make_unique<ColumnRefExpr>(*node);
    else if (const auto* node = dynamic_cast<const ParameterExpr*>(source)) result = std::make_unique<ParameterExpr>(*node);
    else if (const auto* node = dynamic_cast<const UnaryOpExpr*>(source)) {
        auto target = std::make_unique<UnaryOpExpr>(); target->op = node->op;
        target->operand = copy(node->operand); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const BinaryOpExpr*>(source)) {
        auto target = std::make_unique<BinaryOpExpr>(); target->op = node->op;
        target->arrayConcat = node->arrayConcat;
        target->comparison = node->comparison;
        target->rowComparison = node->rowComparison;
        target->rowComparisons = node->rowComparisons;
        target->left = copy(node->left); target->right = copy(node->right); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const QuantifiedComparisonExpr*>(source)) {
        auto target = std::make_unique<QuantifiedComparisonExpr>();
        target->op=node->op;target->quantifier=node->quantifier;target->comparison=node->comparison;
        target->left=copy(node->left);target->right=copy(node->right);result=std::move(target);
    } else if (const auto* node = dynamic_cast<const CastExpr*>(source)) {
        auto target = std::make_unique<CastExpr>(); target->typeName = node->typeName;
        target->implicit = node->implicit;
        target->typeMods = node->typeMods; target->operand = copy(node->operand); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const CaseExpr*>(source)) {
        auto target = std::make_unique<CaseExpr>();
        target->simpleComparisonTypes = node->simpleComparisonTypes;
        target->simpleEnumComparisons = node->simpleEnumComparisons;
        target->switchExpr = copy(node->switchExpr); target->elseExpr = copy(node->elseExpr);
        for (const auto& arm : node->whenClauses) target->whenClauses.emplace_back(copy(arm.first),copy(arm.second));
        result = std::move(target);
    } else if (const auto* node = dynamic_cast<const FunctionCallExpr*>(source)) {
        auto target = std::make_unique<FunctionCallExpr>();
        target->schema = node->schema; target->funcName = node->funcName;
        target->sqlValue = node->sqlValue; target->sqlValuePrecision = node->sqlValuePrecision;
        target->setReturning = node->setReturning;
        target->resolvedResultType = node->resolvedResultType;
        target->distinct = node->distinct; target->orderBy = node->orderBy; target->hasOver = node->hasOver;
        for (const auto& key : node->aggregateOrderKeys)
            target->aggregateOrderKeys.push_back({copy(key.expr), key.ascending, key.nullsFirst, key.enumComparison, key.usingOperator});
        for (const auto& arg : node->args) target->args.push_back(copy(arg));
        for (const auto& arg : node->namedArgs) target->namedArgs.push_back({arg.name,copy(arg.value)});
        target->filter = copy(node->filter); window(node->over,target->over); result = std::move(target);
    } else if (const auto* node = dynamic_cast<const ArrayExpr*>(source)) {
        auto target = std::make_unique<ArrayExpr>();
        target->elementType = node->elementType;
        target->nestedElements = node->nestedElements;
        target->typeMetadata = node->typeMetadata;
        for (const auto& arg : node->elements) target->elements.push_back(copy(arg));
        result = std::move(target);
    } else if (const auto* node = dynamic_cast<const RowExpr*>(source)) {
        auto target = std::make_unique<RowExpr>();
        target->constructor = node->constructor;
        target->fieldTypes = node->fieldTypes;
        target->typeMetadata = node->typeMetadata;
        target->resultMetadata = node->resultMetadata;
        for (const auto& arg : node->elements) target->elements.push_back(copy(arg));
        result = std::move(target);
    } else throw SyntaxCloneUnsupported{};
    result->sourceBegin = source->sourceBegin; result->sourceEnd = source->sourceEnd;
    result->type = source->type;
    return result;
}

inline ParseResult copySyntaxResult(const ParseResult& source) {
    ParseResult result;
    result.success = source.success;
    result.error = source.error;
    result.originalSql = source.originalSql;
    result.sqlState = source.sqlState;
    result.rowCountSyntaxError = source.rowCountSyntaxError;
    if (!source.stmt) return result;
    const auto* select = dynamic_cast<const SelectStmt*>(source.stmt.get());
    if (!select || select->command != SqlCommand::Select || select->selectList.size() != 1 ||
        select->fromClause || select->whereClause || !select->groupBy.empty() ||
        !select->groupByElems.empty() || select->having || !select->orderBy.empty() ||
        select->setOp != SetOp::None || select->setOpLhs || select->setOpRhs ||
        !select->valuesRows.empty() || !select->locking.empty() || !select->ctes.empty() ||
        select->distinct || !select->distinctOn.empty() || !select->windowDefs.empty() ||
        select->hasRowCountClause || select->limit || select->offset || select->withTies ||
        select->fetchFirst || select->signedFetchCount || select->signedLimitCount ||
        select->signedOffsetCount || select->limitExpr || select->offsetExpr ||
        select->existsTargetPrunable)
        throw SyntaxCloneUnsupported{};
    auto target = std::make_unique<SelectStmt>();
    target->selectList.push_back({cloneSyntaxExpression(select->selectList.front().expr.get()),
        select->selectList.front().alias, select->selectList.front().sourceExpressionEnd});
    result.stmt = std::move(target);
    return result;
}
} // namespace local_expression_syntax_detail

// Stack-local to one filterRows call. Only pristine parser output is retained.
// Every reached consumer receives a fresh mutable AST and original parser mode.
// This owns no evaluator, function callback, row, provider or parameter frame.
class LocalExpressionSyntax {
    using Key = std::pair<std::string, bool>;
    using KeyView = std::pair<std::string_view, bool>;
    struct KeyLess {
        using is_transparent = void;
        template<class Left, class Right>
        bool operator()(const Left& left, const Right& right) const {
            const std::string_view a(left.first), b(right.first);
            return a != b ? a < b : left.second < right.second;
        }
    };
    std::map<Key, ParseResult, KeyLess> parsed_;
    std::set<Key, KeyLess> unsupported_;
public:
    ParseResult parse(const std::string& source, bool forBinding) {
        const KeyView key{std::string_view(source), forBinding};
        const auto found = parsed_.find(key);
        if (found != parsed_.end())
            return local_expression_syntax_detail::copySyntaxResult(found->second);
        SQLParser parser;
        auto result = forBinding ? parser.parseForBinding(source) : parser.parse(source);
        if (unsupported_.count(key)) return result;
        try {
            auto copy = local_expression_syntax_detail::copySyntaxResult(result);
            parsed_.emplace(Key{source, forBinding}, std::move(result));
            return copy;
        } catch (const local_expression_syntax_detail::SyntaxCloneUnsupported&) {
            // Unsupported syntax keeps the original parser result on this
            // first demand, and the exact original parse path on later rows.
            unsupported_.insert(Key{source, forBinding});
            return result;
        }
    }
};
} // namespace dbms
