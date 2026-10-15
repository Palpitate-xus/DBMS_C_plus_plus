#pragma once

#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "table_schema.h"

namespace dbms::aggregate_type_detail {
class BooleanArgument {
public:
    BooleanArgument(const TableSchema& table, StorageEngine* engine,
                    const std::string& database, std::string sql)
        : engine_(engine), database_(database), sql_(std::move(sql)) {
        for (size_t i = 0; i < table.len; ++i) {
            const auto& column = table.cols[i];
            std::vector<std::string> names{column.dataName};
            if (!table.tablename.empty()) names.push_back(table.tablename + "." + column.dataName);
            for (const auto& name : names) {
                types_[name] = column.dataType + (column.isArray ? "[]" : "");
                collations_[name] = column.resolvedCollation.empty()
                    ? column.collation : column.resolvedCollation;
            }
            names_.push_back(std::move(names));
        }
        SQLParser parser;
        auto parsed = parser.parseForBinding("SELECT " + sql_);
        const auto* select = parsed.success
            ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
        if (!select || select->fromClause || select->selectList.size() != 1 || !select->selectList[0].expr)
            throw DbError(parsed.sqlState.empty() ? "42601" : parsed.sqlState,
                          "invalid boolean aggregate argument");
        const auto type = ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedInputType(
            select->selectList[0].expr.get(), types_, database_, engine_));
        if (type == "unknown") sql_ = "CAST((" + sql_ + ") AS BOOLEAN)";
        else if (type != "boolean")
            throw DbError("42883", "boolean aggregate argument must have boolean type");
    }

    template <typename InputRow>
    ExprEvalResult evaluate(const InputRow& row) const {
        if (row.values.size() != names_.size() || row.nulls.size() != names_.size())
            throw DbError("XX000", "boolean aggregate lost its typed source row");
        std::map<std::string, std::string> values;
        std::set<std::string> nulls;
        for (size_t i = 0; i < names_.size(); ++i) {
            for (const auto& name : names_[i]) {
                values[name] = row.values[i];
                if (row.nulls[i]) nulls.insert(name);
            }
        }
        auto result = ExprHelper::evalStringWithNulls(sql_, values, nulls, types_,
            database_, "", engine_, collations_);
        if (!result.ok) {
            if (!result.sqlState.empty()) {
                auto message = result.error;
                const auto suffix = " (SQLSTATE " + result.sqlState + ")";
                if (message.size() >= suffix.size() &&
                    message.compare(message.size() - suffix.size(), suffix.size(), suffix) == 0)
                    message.resize(message.size() - suffix.size());
                throw DbError(result.sqlState, std::move(message));
            }
            throw std::runtime_error(result.error.empty()
                ? "boolean aggregate expression failed" : result.error);
        }
        // A selected NULL branch may be untyped; the admitted expression
        // still owns its resolved BOOLEAN result domain.
        if (result.isNull) result.typeName = "boolean";
        else if (ExprHelper::canonicalResultTypeName(result.typeName) != "boolean")
            throw DbError("XX000", "boolean aggregate expression changed its declared type");
        return result;
    }

private:
    StorageEngine* engine_;
    std::string database_, sql_;
    std::vector<std::vector<std::string>> names_;
    std::map<std::string, std::string> types_, collations_;
};

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
