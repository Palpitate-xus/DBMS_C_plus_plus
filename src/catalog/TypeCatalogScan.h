#pragma once

#include "catalog/CatalogService.h"
#include "catalog/type_catalog.h"

#include <algorithm>
#include <charconv>
#include <optional>
#include <string_view>

namespace dbms {

inline std::optional<Oid> catalogOidLiteralValue(const Expr* input, bool allowQuoted) {
    if (!input || input->preparedSubquery) return std::nullopt;
    const auto* cast = dynamic_cast<const CastExpr*>(input);
    if (cast) {
        CatalogManager::QualifiedName target;
        if (!cast->typeMods.empty() || !CatalogManager::parseQualifiedName(cast->typeName, target, true) ||
            target.schema != "pg_catalog" || target.name != "oid")
            return std::nullopt;
        input = cast->operand.get();
    }
    const auto* literal = dynamic_cast<const LiteralExpr*>(input);
    if (!literal || literal->preparedSubquery || literal->arraySubquery) return std::nullopt;
    if (!literal->typeName.empty() && (!literal->analyzedPrimitiveConstant ||
        (literal->typeName != "oid" && literal->typeName != "smallint" &&
         literal->typeName != "integer" && literal->typeName != "bigint")))
        return std::nullopt;

    // Never evaluate a literal or cast during source selection.
    std::string_view digits = literal->value;
    if ((cast || literal->analyzedPrimitiveConstant || allowQuoted) && digits.size() >= 2 &&
        digits.front() == '\'' && digits.back() == '\'') {
        digits.remove_prefix(1);
        digits.remove_suffix(1);
    }
    if (digits.empty() || digits.size() > 10) return std::nullopt;
    Oid oid;
    const auto end = digits.data() + digits.size();
    const auto parsed = std::from_chars(digits.data(), end, oid);
    if (parsed.ec == std::errc{} && parsed.ptr == end) return oid;
    return std::nullopt;
}

// Admit only a direct equality on this range's OID column and a decimal
// constant that is lossless as an OID. The original filter still runs.
inline std::optional<Oid> catalogOidLiteralFilter(const PreparedQuery::SourceRange& range,
                                                bool firstConjunct, int16_t attributeNumber = 1) {
    const auto* select = dynamic_cast<const SelectStmt*>(range.owner);
    if (!select || range.source != select->fromClause.get()) return std::nullopt;
    const auto field = std::find_if(range.columns.begin(), range.columns.end(), [&](const auto& column) {
        return column.physicalAttributeNumber == attributeNumber && column.typeOid == 26;
    });
    if (field == range.columns.end())
        return std::nullopt;
    const size_t ordinal = field - range.columns.begin();
    const Expr* predicate = select->whereClause.get();
    if (firstConjunct) {
        // Rows rejected by this leading equality already short-circuit all
        // later terms. Do not skip a term that precedes the equality.
        while (const auto* conjunction = dynamic_cast<const BinaryOpExpr*>(predicate)) {
            if (conjunction->preparedSubquery || (conjunction->op != "and" && conjunction->op != "AND")) break;
            predicate = conjunction->left.get();
        }
    }
    const auto* equality = dynamic_cast<const BinaryOpExpr*>(predicate);
    if (!equality || equality->op != "=" || equality->preparedSubquery ||
        equality->comparison || equality->predicateCollation)
        return std::nullopt;
    const auto oidColumn = [&](const Expr* expression) {
        const auto* column = dynamic_cast<const ColumnRefExpr*>(expression);
        return column && !column->preparedSubquery && column->binding &&
               column->binding->scopeDepth == 0 && !column->binding->mergedUsing &&
               column->binding->sourceOrdinal == range.ordinal &&
               column->binding->columnOrdinal == ordinal && column->binding->typeOid == 26;
    };
    if (oidColumn(equality->left.get())) return catalogOidLiteralValue(equality->right.get(), firstConjunct);
    if (oidColumn(equality->right.get())) return catalogOidLiteralValue(equality->left.get(), firstConjunct);
    return std::nullopt;
}

inline std::optional<Oid> typeCatalogOidFilter(const PreparedQuery::SourceRange& range) {
    return catalogOidLiteralFilter(range, false);
}

inline std::optional<Oid> attributeCatalogRelationFilter(const PreparedQuery::SourceRange& range) {
    return catalogOidLiteralFilter(range, true);
}

class TypeCatalogScan {
public:
    TypeCatalogScan(std::optional<Oid> oid, bool bootstrapFallback)
        : oid_(oid), bootstrapFallback_(bootstrapFallback) {}

    void capture(const CatalogService& service, const std::string& database) {
        if (rows_) return;
        auto snapshot = service.metadataSnapshot(database, false);
        rows_ = bootstrapFallback_ && snapshot.namespaces.empty() && snapshot.types.empty()
            ? CatalogManager::builtinTypeRows() : std::move(snapshot.types);
        if (oid_) {
            rows_->erase(std::remove_if(rows_->begin(), rows_->end(),
                [&](const auto& row) { return row.oid != *oid_; }), rows_->end());
        }
    }

    bool read(const CatalogService& service, const std::string& database, size_t index,
              std::vector<ExprValue>& cells, std::optional<Oid> outerOid = std::nullopt) {
        capture(service, database);
        if (outerOid) {
            for (const auto& row : *rows_) {
                if (row.oid != *outerOid) continue;
                if (index-- != 0) continue;
                cells = ownedTypeCatalogCells(row);
                return true;
            }
            return false;
        }
        if (index >= rows_->size()) return false;
        cells = ownedTypeCatalogCells((*rows_)[index]);
        return true;
    }

private:
    std::optional<Oid> oid_;
    bool bootstrapFallback_;
    std::optional<std::vector<PgTypeRow>> rows_;
};

}  // namespace dbms
