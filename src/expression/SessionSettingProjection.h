#pragma once

#include "catalog/catalog.h"
#include "common/BooleanCodec.h"
#include "common/TrackedSessionSettings.h"
#include "parser/parser.h"
#include <utility>

namespace dbms {

// An explicitly qualified builtin with one projection and only primitive
// inputs can use the session transaction without catalog/table ownership.
// Keep this proof separate from the Bind-time constant evaluator: the
// setting function itself must run only when the query executes.
struct SessionSettingProjection {
    const FunctionCallExpr* call;
    std::string column;
    bool write;
    bool syntaxError;
};

inline constexpr char sessionSettingSyntaxError[] = "invalid syntax for session setting query";

inline bool mayContainSettingProjection(std::string_view sql) {
    // Only a cheap rejection. The AST and its SQL spans own admission.
    if (sql.find('.') == std::string_view::npos) return false;
    constexpr std::string_view names[] = {"current_setting", "set_config"};
    for (const auto name : names) {
        if (std::search(sql.begin(), sql.end(), name.begin(), name.end(), [](char ch, char target) {
                if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch - 'A' + 'a');
                return ch == target;
            }) != sql.end()) return true;
    }
    return false;
}

inline const LiteralExpr* settingProjectionLiteral(const Expr* expression, bool booleanInput) {
    if (!expression || expression->preparedSubquery) return nullptr;
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
        CatalogManager::QualifiedName target;
        if (!cast->typeMods.empty() || !CatalogManager::parseQualifiedName(cast->typeName, target, true) ||
            target.schema != "pg_catalog" || target.name != (booleanInput ? "bool" : "text"))
            return nullptr;
        expression = cast->operand.get();
    }
    const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
    if (!literal || literal->preparedSubquery || !literal->typeName.empty()) return nullptr;
    const auto& token = literal->value;
    if (token.size() >= 2 && token.front() == '\'' && token.back() == '\'') return literal;
    const auto word = SQLParser::toLower(token);
    return word == "null" || (booleanInput && (word == "true" || word == "false")) ? literal : nullptr;
}

std::optional<SessionSettingProjection> findSessionSettingProjection(
    const Stmt* statement, std::string_view sql);

// Require every supplied/inferred parameter to be owned by this call. A
// different declared type, a gap or a conflicting use retains general binding.
inline bool inferSessionSettingProjectionParameters(const SessionSettingProjection& query,
                                                    std::vector<uint32_t>& oids) {
    if (oids.size() > query.call->args.size()) return false;
    std::vector<uint32_t> required(oids.size(), 0);
    for (size_t i = 0; i < query.call->args.size(); ++i) {
        const auto* parameter = dynamic_cast<const ParameterExpr*>(query.call->args[i].get());
        if (!parameter) continue;
        const uint32_t typeOid = sessionSettingBooleanInput(query.write, i) ? 16 : 25;
        if (parameter->slot >= oids.size() ||
            (oids[parameter->slot] && oids[parameter->slot] != typeOid) ||
            (required[parameter->slot] && required[parameter->slot] != typeOid)) return false;
        required[parameter->slot] = typeOid;
    }
    if (std::find(required.begin(), required.end(), 0) != required.end()) return false;
    oids = std::move(required);
    return true;
}

inline std::vector<ExprValue> sessionSettingProjectionArguments(const SessionSettingProjection& query,
                                                                bool allowParameters) {
    ExprEvaluator evaluator;
    RowContext row;
    std::vector<ExprValue> arguments;
    arguments.reserve(query.call->args.size());
    for (size_t i = 0; i < query.call->args.size(); ++i) {
        const auto* input = query.call->args[i].get();
        const bool booleanInput = sessionSettingBooleanInput(query.write, i);
        const auto* literal = settingProjectionLiteral(input, booleanInput);
        if (!literal) {
            if (allowParameters && dynamic_cast<const ParameterExpr*>(input)) continue;
            throw DbError("XX000", "session setting projection lost its literal input");
        }
        auto value = evaluator.eval(literal, row);
        if (booleanInput && !value.isNull) {
            const auto parsed = parsePostgresBoolean(value.value);
            if (!parsed) throw DbError("22P02", "invalid input syntax for type boolean: \"" + value.value + "\"");
            value.value = *parsed ? "t" : "f";
        }
        value.typeName = booleanInput ? "boolean" : "text";
        arguments.push_back(std::move(value));
    }
    return arguments;
}

} // namespace dbms
