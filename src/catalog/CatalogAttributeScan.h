#pragma once

#include "catalog/SystemCatalogRows.h"
#include "catalog/TypeCatalogScan.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "expression/regclass_input.h"

namespace dbms {

class CatalogAttributeScan {
public:
    CatalogAttributeScan(const PreparedQuery::SourceRange& range, const Stmt* root)
        : literalOid_(attributeCatalogRelationFilter(range)) {
        const auto* select = dynamic_cast<const SelectStmt*>(root);
        // A surrounding query or an evaluated row-count expression may
        // change the namespace before the first demand on this source.
        if (!literalOid_ && select && range.owner == select && select->ctes.empty() &&
            select->setOp == SetOp::None && !select->limitExpr && !select->offsetExpr)
            runtimeInput_ = captureRuntimeInput(catalogOidEqualityInput(range, true, 1, true));
    }

    void capture(const CatalogService& service, const std::string& database) {
        if (rows_ || attributes_) return;
        auto metadata = service.metadataSnapshot(database, false);
        if (runtimeInput_) {
            system_catalog::validateAttributeRead(metadata);
            attributes_ = std::move(metadata.attributes);
        } else {
            rows_ = system_catalog::attributeRows(metadata, literalOid_);
        }
    }

    bool read(StorageEngine& engine, const std::string& database, const std::vector<ExprValue>& parameters,
              size_t index, std::vector<ExprValue>& cells) {
        capture(engine.catalogService(), database);
        if (rows_) {
            if (index >= rows_->size()) return false;
            cells = (*rows_)[index];
            return true;
        }
        if (index == 0) {
            position_ = 0;
            useRuntimeKey_ = true;
            firstOid_.reset();
        }
        // Later filters and outputs can change search_path. If the lookup
        // changes, let the predicate's own evaluator read the remaining rows;
        // it may already own the computed operand from before that change.
        if (position_ < attributes_->size() && useRuntimeKey_) {
            std::optional<Oid> oid;
            try {
                oid = evaluateRuntimeKey(engine, database, parameters);
            } catch (const DbError&) {
                // Report errors where the predicate actually reaches them.
            }
            if (oid && (!firstOid_ || *oid == *firstOid_)) {
                firstOid_ = oid;
                while (position_ < attributes_->size() && (*attributes_)[position_].attrelid != *oid)
                    ++position_;
            } else {
                // NULL equality still allows later AND terms to run.
                useRuntimeKey_ = false;
            }
        }
        if (position_ >= attributes_->size()) return false;
        cells = system_catalog::attributeCells((*attributes_)[position_++]);
        return true;
    }

private:
    struct CastInput {
        std::string spelling;
        const Expr* operand;
    };

    static std::optional<CastInput> castInput(const Expr* expression) {
        if (!expression || expression->preparedSubquery) return {};
        if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
            if (cast->typeMods.empty()) return CastInput{cast->typeName, cast->operand.get()};
        }
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression); binary && binary->op == "::") {
            const auto* target = dynamic_cast<const LiteralExpr*>(binary->right.get());
            if (target) return CastInput{target->value, binary->left.get()};
        }
        return {};
    }

    static Oid builtinName(const std::string& spelling) {
        CatalogManager::QualifiedName name;
        if (!CatalogManager::parseQualifiedName(spelling, name, true) ||
            (!name.schema.empty() && name.schema != "pg_catalog")) return 0;
        if (name.name == "oid") return 26;
        if (name.name == "regclass") return 2205;
        if (name.name == "text") return 25;
        if (name.name == "varchar" || name.name == "character varying") return 1043;
        return 0;
    }

    const Expr* captureRuntimeInput(const Expr* input) {
        auto cast = castInput(input);
        if (cast && builtinName(cast->spelling) == 26) {
            declarations_.emplace_back(cast->spelling, 26);
            cast = castInput(cast->operand);
        }
        if (!cast || builtinName(cast->spelling) != 2205) return nullptr;
        declarations_.emplace_back(cast->spelling, 2205);
        const auto* leaf = cast->operand;
        bool textCast = false;
        while (const auto text = castInput(leaf)) {
            const Oid type = builtinName(text->spelling);
            if (type != 25 && type != 1043) return nullptr;
            declarations_.emplace_back(text->spelling, type);
            textCast = true;
            leaf = text->operand;
        }
        if (!leaf || leaf->preparedSubquery) return nullptr;
        if (const auto* param = dynamic_cast<const ParameterExpr*>(leaf)) {
            const Oid type = builtinName(param->declaredType);
            return param->origin == ParameterOrigin::StatementInput && (type == 25 || type == 1043) ? leaf : nullptr;
        }
        const auto* literal = dynamic_cast<const LiteralExpr*>(leaf);
        if (!literal || literal->arraySubquery || literal->analyzedPrimitiveConstant) return nullptr;
        if (literal->typeName.empty()) {
            const bool stringLiteral = literal->value.size() >= 2 &&
                literal->value.front() == '\'' && literal->value.back() == '\'';
            return textCast && (stringLiteral || SQLParser::toLower(literal->value) == "null") ? leaf : nullptr;
        }
        const Oid type = builtinName(literal->typeName);
        if (type != 25 && type != 1043) return nullptr;
        declarations_.emplace_back(literal->typeName, type);
        return leaf;
    }

    std::optional<Oid> evaluateRuntimeKey(StorageEngine& engine, const std::string& database,
                                           const std::vector<ExprValue>& parameters) const {
        // Check the actual declarations on the currently visible catalog.
        // A same-named user type can supply a domain check with side effects.
        const auto metadata = engine.catalogService().metadataSnapshot(database, false);
        const auto* session = currentSession();
        for (const auto& [spelling, oid] : declarations_) {
            try {
                if (resolveDeclaredTypeName(spelling, &metadata, session).typeOid != oid) return {};
            } catch (const DbError&) {
                return {}; // The ordinary filter owns declaration errors.
            }
        }
        ExprValue text;
        if (const auto* parameter = dynamic_cast<const ParameterExpr*>(runtimeInput_)) {
            if (parameter->slot >= parameters.size()) return {};
            text = parameters[parameter->slot];
            const Oid type = builtinName(text.typeName);
            if (type != 25 && type != 1043) return {};
        } else {
            auto literal = *static_cast<const LiteralExpr*>(runtimeInput_);
            literal.typeName.clear();
            text = ExprEvaluator{}.eval(&literal, RowContext{});
        }
        // Just the OID is needed. Use the same generation that verified the
        // cast declarations and leave display formatting to the predicate.
        if (text.isNull) return {};
        return regclass_detail::textInputOid(text.value, metadata, session, database);
    }

    std::optional<Oid> literalOid_;
    const Expr* runtimeInput_ = nullptr;
    std::vector<std::pair<std::string, Oid>> declarations_;
    std::optional<std::vector<std::vector<ExprValue>>> rows_;
    std::optional<std::vector<PgAttributeRow>> attributes_;
    size_t position_ = 0;
    bool useRuntimeKey_ = true;
    std::optional<Oid> firstOid_;
};

} // namespace dbms
