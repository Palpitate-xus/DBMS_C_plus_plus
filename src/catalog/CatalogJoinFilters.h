#pragma once

#include "catalog/PgForeignKeyCatalog.h"
#include "catalog/PgIndexCatalog.h"
#include "catalog/TypeCatalogScan.h"

namespace dbms {

using CatalogJoinColumn = std::pair<size_t, int16_t>; // range occurrence, physical attribute
using CatalogJoinFilters = std::map<CatalogJoinColumn, Oid>;

// Derive scan restrictions from a leading WHERE prefix over the owned
// catalogs. Join conditions still execute and retain their original outer
// join semantics. A LEFT join can propagate from its preserved side into
// the right scan, never from the optional row back to the preserved side.
class CatalogJoinFilterPlanner {
public:
    CatalogJoinFilterPlanner(const PreparedQuery& query, const SelectStmt& select)
        : query_(query), select_(select) {}

    CatalogJoinFilters filters() {
        if (!select_.fromClause || select_.fromClause->type != FromItem::Type::Join ||
            !select_.whereClause || !collectSources(*select_.fromClause, false) ||
            !collectJoins(*select_.fromClause))
            return {};
        collectWhere(select_.whereClause.get());
        bool added;
        do {
            added = false;
            for (const auto& [from, into] : equalities_) {
                const auto source = filters_.find(from);
                if (source != filters_.end())
                    added |= filters_.emplace(into, source->second).second;
            }
        } while (added);
        return std::move(filters_);
    }

private:
    struct Source {
        const PreparedQuery::SourceRange* range;
        bool optional;
    };
    const PreparedQuery& query_;
    const SelectStmt& select_;
    std::map<size_t, Source> sources_;
    std::vector<std::pair<CatalogJoinColumn, CatalogJoinColumn>> equalities_;
    CatalogJoinFilters filters_;

    const PreparedQuery::SourceRange* range(const FromItem& item) const {
        const auto found = std::find_if(query_.sourceRanges.begin(), query_.sourceRanges.end(), [&](const auto& value) {
            return value.owner == &select_ && value.source == &item && !value.mergedUsing;
        });
        return found == query_.sourceRanges.end() ? nullptr : &*found;
    }

    bool collectSources(const FromItem& item, bool optional) {
        if (item.type == FromItem::Type::Join) {
            if (!item.usingCols.empty() || (item.joinType != "CROSS" && item.joinType != "INNER" && item.joinType != "LEFT"))
                return false;
            const bool leftJoin = item.joinType == "LEFT";
            if (leftJoin && item.right->type != FromItem::Type::Table) return false;
            return collectSources(*item.left, optional) && collectSources(*item.right, optional || leftJoin);
        }
        if (item.type != FromItem::Type::Table) return false;
        const auto* source = range(item);
        if (!source || source->viewQuery || source->cteStatement || source->relationSchema != "pg_catalog" ||
            (source->relationName != "pg_class" && source->relationName != index_catalog::name &&
             source->relationName != foreign_key_catalog::name))
            return false;
        sources_.emplace(source->ordinal, Source{source, optional});
        return true;
    }

    std::optional<CatalogJoinColumn> column(const Expr* expression, Oid type) const {
        const auto* ref = dynamic_cast<const ColumnRefExpr*>(expression);
        if (!ref || ref->preparedSubquery || ref->generatedRead || ref->generatedBaseDeclaration ||
            !ref->binding || ref->binding->scopeDepth || ref->binding->mergedUsing || ref->binding->typeOid != type)
            return {};
        const auto& binding = *ref->binding;
        const auto source = sources_.find(binding.sourceOrdinal);
        if (source == sources_.end() || binding.columnOrdinal >= source->second.range->columns.size()) return {};
        const auto& field = source->second.range->columns[binding.columnOrdinal];
        return CatalogJoinColumn{binding.sourceOrdinal, field.physicalAttributeNumber};
    }

    bool scanKey(CatalogJoinColumn field) const {
        const auto& relation = sources_.at(field.first).range->relationName;
        if (relation == "pg_class") return field.second == 1;
        if (relation == index_catalog::name)
            return field.second == index_catalog::indexAttribute || field.second == index_catalog::ownerAttribute;
        return field.second == foreign_key_catalog::oidAttribute || field.second == foreign_key_catalog::ownerAttribute ||
            field.second == foreign_key_catalog::referencedAttribute;
    }

    static const BinaryOpExpr* comparison(const Expr* expression) {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
        return binary && !binary->preparedSubquery && !binary->comparison && !binary->predicateCollation &&
            binary->predicateListCollations.empty() && !binary->rowComparison && !binary->arrayConcat ? binary : nullptr;
    }

    static bool conjunction(const BinaryOpExpr& binary) { return binary.op == "AND" || binary.op == "and"; }

    bool collectJoins(const FromItem& item) {
        if (item.type != FromItem::Type::Join) return true;
        const auto* right = item.joinType == "LEFT" ? range(*item.right) : nullptr;
        return collectJoins(*item.left) && collectJoins(*item.right) &&
            (!item.joinCondition || collectOn(item.joinCondition.get(), right));
    }

    bool collectOn(const Expr* expression, const PreparedQuery::SourceRange* optionalRight) {
        const auto* binary = comparison(expression);
        if (!binary) return false;
        if (conjunction(*binary))
            return collectOn(binary->left.get(), optionalRight) && collectOn(binary->right.get(), optionalRight);
        if (binary->op == "in" || binary->op == "IN") {
            // The catalog's one-byte kind codes can be compared to simple
            // character literals without running a cast, routine or child.
            const auto* list = dynamic_cast<const RowExpr*>(binary->right.get());
            return column(binary->left.get(), 18) && list && !list->constructor && !list->preparedSubquery &&
                std::all_of(list->elements.begin(), list->elements.end(), [](const auto& element) {
                    const auto* literal = dynamic_cast<const LiteralExpr*>(element.get());
                    return literal && !literal->preparedSubquery && (literal->typeName.empty() || literal->typeName == "\"char\"") &&
                        literal->value.size() == 3 && literal->value.front() == '\'' && literal->value.back() == '\'' &&
                        literal->value[1] >= 'a' && literal->value[1] <= 'z';
                });
        }
        if (binary->op != "=") return false;
        auto left = column(binary->left.get(), 26), right = column(binary->right.get(), 26);
        if (!left || !right) return false;
        if (!scanKey(*left) || !scanKey(*right)) return true;
        if (optionalRight) {
            if (left->first == optionalRight->ordinal) std::swap(left, right);
            if (right->first == optionalRight->ordinal && !sources_.at(left->first).optional)
                equalities_.emplace_back(*left, *right);
        } else if (!sources_.at(left->first).optional && !sources_.at(right->first).optional) {
            equalities_.emplace_back(*left, *right);
            equalities_.emplace_back(*right, *left);
        }
        return true;
    }

    bool collectWhere(const Expr* expression) {
        const auto* binary = comparison(expression);
        if (!binary) return false;
        // Later terms may observe an error or side effect. Only skip rows for
        // which this leading prefix is provably false, never SQL UNKNOWN.
        if (conjunction(*binary))
            return collectWhere(binary->left.get()) && collectWhere(binary->right.get());
        if (binary->op != "=") return false;
        auto left = column(binary->left.get(), 26), right = column(binary->right.get(), 26);
        const auto safe = [&](CatalogJoinColumn field) { return !sources_.at(field.first).optional && scanKey(field); };
        if (left && right) {
            if (!safe(*left) || !safe(*right)) return false;
            equalities_.emplace_back(*left, *right);
            equalities_.emplace_back(*right, *left);
            return true;
        }
        if (right && !left) {
            const auto value = catalogOidLiteralValue(binary->left.get(), true);
            if (safe(*right) && value) {
                filters_.emplace(*right, *value);
                return true;
            }
        } else if (left) {
            const auto value = catalogOidLiteralValue(binary->right.get(), true);
            if (safe(*left) && value) {
                filters_.emplace(*left, *value);
                return true;
            }
        }
        return false;
    }
};

} // namespace dbms
