#pragma once
#include "catalog/catalog.h"
#include "catalog/declared_type.h"
#include "catalog/PgObjectConstant.h"
#include "expression/StoredRegtypeValue.h"
#include "parser/parser.h"
#include <functional>
#include <map>
#include <optional>
#include <set>

namespace dbms::column_default {
using Constant = pg_object_constant::Value;
struct Bound {
    std::string expression;
    std::vector<std::pair<Oid, Oid>> references;
    std::optional<Constant> constant;
    std::string adbin;
    bool nullConstant = false;
};
inline std::optional<std::string> stringLiteral(const Expr* expression) {
    const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
    if (!literal || literal->value.size() < 2 || literal->value.front() != '\'' || literal->value.back() != '\'')
        return std::nullopt;
    // The parser has already converted escape/dollar strings to this common
    // token form while retaining the original byte provenance separately.
    std::string result;
    for (size_t i = 1; i + 1 < literal->value.size(); ++i) {
        const char c = literal->value[i]; result += c;
        if (c == '\'') {
            if (i + 2 >= literal->value.size() || literal->value[i + 1] != '\'')
                throw DbError("XX001", "invalid normalized default literal");
            ++i;
        }
    }
    return result;
}
inline bool nullLiteral(const Expr* expression) {
    const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
    return literal && SQLParser::toLower(literal->value) == "null";
}
inline std::string quoteLiteral(const std::string& text) {
    std::string result = "'";
    for (char c : text) { result += c; if (c == '\'') result += c; }
    return result + '\'';
}

// Bind actual typed object constants without evaluating a routine, row,
// subquery or default. Runtime text -> regclass casts deliberately remain
// runtime casts. All unrelated source bytes retain their original spelling.
inline Bound bind(const std::string& definition, Oid targetType,
    const CatalogManager::MetadataSnapshot& catalog, const Session* session,
    const std::function<bool(const FunctionCallExpr*)>& builtinNextval = {}) {
    constexpr size_t prefix = 7; // "SELECT "
    SQLParser parser;
    auto parsed = parser.parseForBinding("SELECT " + definition);
    const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
    if (!select || select->selectList.size() != 1 || select->fromClause || select->whereClause ||
        !select->groupBy.empty() || select->having || !select->orderBy.empty() || select->setOp != SetOp::None)
        throw DbError("42601", "invalid column default expression");
    const Expr* root = select->selectList[0].expr.get();
    struct Replacement { size_t begin, end; std::string value; };
    std::vector<Replacement> replacements;
    std::set<std::pair<Oid, Oid>> references;
    std::map<const Expr*, Constant> constants;
    Bound result; result.expression = definition;
    const auto bindConstant = [&](const Expr* expression, const Expr* value, Oid type) {
        const auto input = stringLiteral(value);
        if (!input && !nullLiteral(value)) return false;
        if (expression->sourceBegin < prefix || expression->sourceBegin >= expression->sourceEnd ||
            expression->sourceEnd > prefix + definition.size())
            throw DbError("XX001", "column default lost its source provenance");
        Constant constant; constant.type = type;
        if (input) {
            constant.value = stored_regtype_detail::input(*input, pg_object_constant::array(type), catalog, session, pg_object_constant::name(type));
            // PostgreSQL's dependency walker sees scalar Const nodes, not
            // individual objects inside an array-typed Const datum.
            if (!pg_object_constant::array(type)) {
                const Oid oid = stored_regtype_detail::rawOid(*constant.value);
                if (oid && (type == 2205
                    ? std::any_of(catalog.relations.begin(), catalog.relations.end(), [&](const auto& row) { return row.oid == oid; })
                    : std::any_of(catalog.types.begin(), catalog.types.end(), [&](const auto& row) { return row.oid == oid; })))
                    references.emplace(type == 2205 ? PgClassOid_Class : PgClassOid_Type, oid);
            }
        }
        const std::string replacement = "CAST(" + (constant.value ? quoteLiteral(*constant.value) : "NULL") +
            " AS pg_catalog." + pg_object_constant::name(type) + (pg_object_constant::array(type) ? "[]" : "") + ")";
        replacements.push_back({expression->sourceBegin - prefix, expression->sourceEnd - prefix, replacement});
        if (expression == root) result.constant = constant;
        constants.emplace(expression, constant);
        return true;
    };
    std::function<void(const Expr*)> visit = [&](const Expr* expression) {
        if (!expression) return;
        if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
            const auto spelling = SQLParser::toLower(cast->typeName);
            // Resolve only this object-reference family. Other input analysis
            // and the existing ordinary cast execution paths are untouched.
            if (spelling.find("regclass") != std::string::npos || spelling.find("regtype") != std::string::npos) {
                if (!cast->typeMods.empty()) throw DbError("42601", "object identifier types do not accept modifiers");
                const auto declaration = resolveDeclaredTypeName(cast->typeName, &catalog, session);
                if ((declaration.typeOid == 2205 || declaration.typeOid == 2210 || declaration.typeOid == 2206 || declaration.typeOid == 2211) &&
                    bindConstant(expression, cast->operand.get(), declaration.typeOid)) return;
            }
            visit(cast->operand.get());
        } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) {
            visit(unary->operand.get());
        } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
            // The parser retains PostgreSQL's postfix cast as a binary node;
            // CAST(...) has its own CastExpr. Both use the same declaration
            // resolver and constant-input contract.
            if (binary->op == "::") {
                const auto* type = dynamic_cast<const LiteralExpr*>(binary->right.get());
                if (!type) throw DbError("XX001", "column default cast lost its declaration");
                const auto spelling = SQLParser::toLower(type->value);
                if (spelling.find("regclass") != std::string::npos || spelling.find("regtype") != std::string::npos) {
                    const auto declaration = resolveDeclaredTypeName(type->value, &catalog, session);
                    if ((declaration.typeOid == 2205 || declaration.typeOid == 2210 || declaration.typeOid == 2206 || declaration.typeOid == 2211) &&
                        bindConstant(expression, binary->left.get(), declaration.typeOid)) return;
                }
                visit(binary->left.get());
                return;
            }
            visit(binary->left.get()); visit(binary->right.get());
        } else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
            CatalogManager::QualifiedName callee;
            const bool sequenceName = CatalogManager::parseQualifiedName(call->funcName, callee, true) &&
                SQLParser::toLower(callee.name) == "nextval";
            if (sequenceName && call->args.size() == 1 &&
                call->namedArgs.empty() && !call->hasOver && !call->filter && !call->distinct &&
                call->orderBy.empty() && builtinNextval && builtinNextval(call)) {
                const size_t before = replacements.size();
                const auto* argument = call->args[0].get();
                if (!bindConstant(argument, argument, 2205)) visit(argument);
                if (call->sourceBegin < prefix || call->sourceEnd > prefix + definition.size() ||
                    call->sourceBegin >= call->sourceEnd || argument->sourceBegin < call->sourceBegin ||
                    argument->sourceEnd > call->sourceEnd || argument->sourceBegin >= argument->sourceEnd)
                    throw DbError("XX001", "sequence default lost its source provenance");
                std::string sql = definition.substr(argument->sourceBegin - prefix, argument->sourceEnd - argument->sourceBegin);
                std::vector<Replacement> children(replacements.begin() + before, replacements.end());
                std::sort(children.begin(), children.end(), [](const auto& a, const auto& b) { return a.begin > b.begin; });
                for (const auto& child : children) {
                    if (child.begin < argument->sourceBegin - prefix || child.end > argument->sourceEnd - prefix)
                        throw DbError("XX001", "invalid nested sequence default binding");
                    sql.replace(child.begin - (argument->sourceBegin - prefix), child.end - child.begin, child.value);
                }
                const auto typed = constants.find(argument);
                if (typed == constants.end() || typed->second.type != 2205)
                    sql = "CAST(" + sql + " AS pg_catalog.regclass)";
                replacements.resize(before);
                replacements.push_back({call->sourceBegin - prefix, call->sourceEnd - prefix, "pg_catalog.nextval(" + sql + ")"});
                return;
            }
            for (const auto& argument : call->args) visit(argument.get());
            for (const auto& argument : call->namedArgs) visit(argument.value.get());
            visit(call->filter.get());
            for (const auto& order : call->aggregateOrderKeys) visit(order.expr.get());
        } else if (const auto* branch = dynamic_cast<const CaseExpr*>(expression)) {
            visit(branch->switchExpr.get());
            for (const auto& arm : branch->whenClauses) { visit(arm.first.get()); visit(arm.second.get()); }
            visit(branch->elseExpr.get());
        } else if (const auto* array = dynamic_cast<const ArrayExpr*>(expression)) {
            for (const auto& element : array->elements) visit(element.get());
        } else if (const auto* row = dynamic_cast<const RowExpr*>(expression)) {
            for (const auto& element : row->elements) visit(element.get());
        } else if (const auto* comparison = dynamic_cast<const QuantifiedComparisonExpr*>(expression)) {
            visit(comparison->left.get()); visit(comparison->right.get());
        }
    };
    if ((targetType != 2205 && targetType != 2210 && targetType != 2206 && targetType != 2211) || !bindConstant(root, root, targetType)) visit(root);
    if (result.constant) result.adbin = pg_object_constant::node(*result.constant);
    result.nullConstant = nullLiteral(root) || (result.constant && !result.constant->value);
    std::sort(replacements.begin(), replacements.end(), [](const auto& a, const auto& b) { return a.begin > b.begin; });
    size_t next = definition.size();
    for (const auto& replacement : replacements) {
        if (replacement.end > next) throw DbError("XX001", "overlapping column default bindings");
        result.expression.replace(replacement.begin, replacement.end - replacement.begin, replacement.value);
        next = replacement.begin;
    }
    result.references.assign(references.begin(), references.end());
    return result;
}
} // namespace dbms::column_default
