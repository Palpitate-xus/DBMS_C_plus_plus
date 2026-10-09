#pragma once

#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"

namespace dbms::aggregate_type_detail {
inline std::string builtinName(const FunctionCallExpr* call) {
    if (!call || call->hasOver || call->setReturning) return {};
    CatalogManager::QualifiedName name;
    const auto spelling = call->schema.empty() ? call->funcName : call->schema + "." + call->funcName;
    if (!CatalogManager::parseQualifiedName(spelling, name, true) ||
        (!name.schema.empty() && name.schema != "pg_catalog")) return {};
    for (const auto* builtin : {"count", "sum", "avg", "min", "max", "bool_and", "bool_or", "every"})
        if (name.name == builtin) return name.name;
    return {};
}

inline std::string resultType(const FunctionCallExpr* call, const std::string& name,
                              const std::string& input) {
    if (!call || call->args.size() != 1 || !call->namedArgs.empty())
        throw DbError("42883", "aggregate function has no matching signature");
    const auto* literal = dynamic_cast<const LiteralExpr*>(call->args.front().get());
    const bool star = literal && literal->value == "*";
    if (star && (name != "count" || call->distinct))
        throw DbError("42883", "aggregate function has no matching star signature");
    const auto type = ExprHelper::canonicalResultTypeName(input);
    if (name == "count") return "bigint";
    if (name == "bool_and" || name == "bool_or" || name == "every") {
        if (type == "boolean") return "boolean";
    } else if (name == "sum" || name == "avg") {
        if (type == "smallint" || type == "integer") return name == "sum" ? "bigint" : "numeric";
        if (type == "bigint" || type == "numeric") return "numeric";
        if (type == "real") return name == "sum" ? "real" : "double precision";
        if (type == "double precision" || type == "interval") return type;
        if (type == "money" && name == "sum") return type;
        if (type == "unknown") throw DbError("42725", "aggregate function is not unique");
    } else if (name == "min" || name == "max") {
        // Resolve actual aggregate signatures, not merely the existence of
        // an ordering operator. JSON, bit strings and UUID have no MIN/MAX
        // overload; VARCHAR/NAME/internal-char select TEXT, CIDR selects INET.
        if (type == "unknown" || type == "character varying" || type == "varchar" ||
            type == "name" || type == "\"char\"") return "text";
        if (type == "cidr") return "inet";
        if (type.size() >= 2 && type.compare(type.size() - 2, 2, "[]") == 0) return type;
        for (const auto* supported : {"smallint", "integer", "bigint", "numeric", "real",
             "double precision", "text", "character", "date", "time", "timetz",
             "timestamp", "timestamptz", "interval", "money", "inet", "oid", "pg_lsn",
             "tid", "xid8", "record"})
            if (type == supported) return type;
        // Preserve separately resolved user enum/composite identities. Their
        // actual ordering implementation remains owned by catalog binding.
        if (!type.empty() && TypeRegistry::instance().normalizeTypeName(type).empty()) return type;
    }
    throw DbError("42883", "aggregate function does not exist for type " + type);
}
} // namespace dbms::aggregate_type_detail
