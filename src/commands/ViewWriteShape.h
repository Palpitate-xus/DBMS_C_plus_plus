#pragma once

#include "catalog/catalog.h"
#include "common/DbError.h"
#include "expression/aggregate_type.h"
#include "parser/query_binding.h"

struct Session;

namespace dbms::view_write {

// Traverse expressions at this query level. A scalar child has its own
// cardinality and bindings, independently of the view's projection.
template<class Visit>
void visitExpression(const Expr* root, Visit visit) {
    std::function<void(const Expr*)> expression;
    const auto window = [&](const WindowDef& definition) {
        for (const auto& item : definition.partitionBy) expression(item.get());
        for (const auto& item : definition.orderBy) expression(item.first.get());
        expression(definition.frameStart.get());
        expression(definition.frameEnd.get());
    };
    expression = [&](const Expr* node) {
        if (!node || node->preparedSubquery) return;
        visit(*node);
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(node)) {
            expression(unary->operand.get());
        } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node)) {
            expression(binary->left.get()); expression(binary->right.get());
        } else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(node)) {
            for (const auto& argument : call->args) expression(argument.get());
            for (const auto& argument : call->namedArgs) expression(argument.value.get());
            expression(call->filter.get());
            for (const auto& key : call->aggregateOrderKeys) expression(key.expr.get());
            if (call->hasOver) window(call->over);
        } else if (const auto* cast = dynamic_cast<const CastExpr*>(node)) {
            expression(cast->operand.get());
        } else if (const auto* conditional = dynamic_cast<const CaseExpr*>(node)) {
            expression(conditional->switchExpr.get());
            for (const auto& clause : conditional->whenClauses) {
                expression(clause.first.get()); expression(clause.second.get());
            }
            expression(conditional->elseExpr.get());
        } else if (const auto* array = dynamic_cast<const ArrayExpr*>(node)) {
            for (const auto& item : array->elements) expression(item.get());
        } else if (const auto* row = dynamic_cast<const RowExpr*>(node)) {
            for (const auto& item : row->elements) expression(item.get());
        } else if (const auto* comparison = dynamic_cast<const QuantifiedComparisonExpr*>(node)) {
            expression(comparison->left.get()); expression(comparison->right.get());
        } else if (node->type != ExprType::ColumnRef && node->type != ExprType::Literal && node->type != ExprType::Parameter &&
                   node->type != ExprType::A_Star) {
            throw DbError("0A000", "view expression dependency is not implemented");
        }
    };
    expression(root);
}

inline const PreparedQuery::SourceRange* singleSource(const PreparedQuery& view) {
    const auto* select = dynamic_cast<const SelectStmt*>(view.ast.get());
    if (!select || !select->fromClause || select->fromClause->type != FromItem::Type::Table ||
        select->distinct || !select->distinctOn.empty() || !select->groupBy.empty() ||
        !select->groupByElems.empty() || select->having || select->setOp != SetOp::None ||
        select->setOpLhs || select->setOpRhs || !select->ctes.empty() ||
        select->hasRowCountClause || select->limit || select->offset ||
        select->limitExpr || select->offsetExpr)
        return nullptr;

    const auto source = std::find_if(view.sourceRanges.begin(), view.sourceRanges.end(),
        [&](const auto& range) { return range.owner == select && range.source == select->fromClause.get(); });
    if (source == view.sourceRanges.end() || source->cteStatement ||
        source->relationSchema.empty() || source->relationName.empty())
        return nullptr;

    bool changesCardinality = false;
    const auto inspect = [&](const Expr& expr) {
        const auto* call = dynamic_cast<const FunctionCallExpr*>(&expr);
        changesCardinality |= call && (call->hasOver || call->setReturning ||
            !aggregate_type_detail::builtinName(call).empty());
    };
    for (const auto& item : select->selectList) visitExpression(item.expr.get(), inspect);
    for (const auto& item : select->orderBy) visitExpression(item.expr.get(), inspect);
    return changesCardinality ? nullptr : &*source;
}

inline bool supportsSource(const CatalogManager& catalog,
                           const PreparedQuery::SourceRange& source) {
    const auto space = catalog.getNamespaceByName(source.relationSchema);
    const auto relation = space ? catalog.getClassByName(source.relationName, space->oid) : std::nullopt;
    return relation && std::string_view("rfvp").find(relation->relkind) != std::string_view::npos;
}

inline bool directColumn(const PreparedQuery::ProjectionBinding& binding,
                         const PreparedQuery::SourceRange& source) {
    return binding.column && binding.column->scopeDepth == 0 &&
        binding.column->sourceOrdinal == source.ordinal &&
        binding.column->columnOrdinal < source.columns.size() &&
        source.columns[binding.column->columnOrdinal].physicalAttributeNumber >= 0;
}

bool targetsView(Session&, const Stmt*);
void validate(Session&, const PreparedQuery&);

} // namespace dbms::view_write
