// ============================================================================
// DML AST Executor — Phase 4 Wave 0.4
// ============================================================================

#include "commands/DmlExecutor.h"

#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "permissions.h"

#include <algorithm>
#include <cctype>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <set>
#include <string>
#include <utility>

extern dbms::StorageEngine g_engine;

namespace dbms {

namespace {
thread_local DmlResult g_lastDmlResult;
}

DmlResult takeLastDmlResult() {
    DmlResult result = std::move(g_lastDmlResult);
    g_lastDmlResult = {};
    return result;
}

void clearLastDmlResult() {
    g_lastDmlResult = {};
}

namespace {

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string identifier(std::string value) {
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        value = value.substr(1, value.size() - 2);
    }
    return lower(value);
}

bool isTempTable(const Session& s, const std::string& name) {
    return s.tempTables.count(name) != 0;
}

bool checkDatabase(const Session& s) {
    if (s.currentDB == "information_schema") return true;
    if (!g_engine.databaseExists(s.currentDB)) {
        std::cout << "Invalid Database name:" << s.currentDB << std::endl;
        return false;
    }
    return true;
}

std::string resolveTable(Session& s, const std::string& name) {
    if (isTempTable(s, name)) return "__tmp_" + name;
    if (g_engine.isMaterializedView(s.currentDB, name)) {
        return StorageEngine::materializedViewPrefix(name);
    }
    const size_t dot = name.find('.');
    if (dot != std::string::npos && dot > 0 && dot + 1 < name.size()) {
        const std::string schema = name.substr(0, dot);
        const std::string table = name.substr(dot + 1);
        if (g_engine.schemaExists(s.currentDB, schema)) return schema + "__" + table;
    }
    return name;
}

bool checkInsertTablePermission(Session& s, const std::string& table) {
    if (sessionIsAdmin(s) || isTempTable(s, table)) return true;
    if (g_engine.hasPermission(s.currentDB, table, effectiveSessionRole(s),
                               StorageEngine::TablePrivilege::Insert)) {
        return true;
    }
    for (const auto& permission : g_engine.getUserPermissions(
             s.currentDB, table, effectiveSessionRole(s))) {
        if (permission == "insert" || permission == "all") return true;
    }
    std::cout << "permission denied on table " << table << std::endl;
    return false;
}

bool isDefaultValue(const ExprPtr& expr) {
    const auto* literal = expr ? dynamic_cast<const LiteralExpr*>(expr.get()) : nullptr;
    return literal && lower(literal->value) == "default";
}

bool supportsConflict(const InsertStmt& stmt) {
    const std::string action = lower(stmt.conflictAction);
    // DO NOTHING may be target-less or explicitly constrained; DO UPDATE is
    // admitted only for a narrow VALUES shape and is constraint-backed by
    // buildConflictUpdatePlan().
    if (action.empty()) return true;
    if (action == "do nothing") {
        if (stmt.conflictTarget.empty()) return !stmt.conflictWhere;
        return !stmt.conflictWhere && !stmt.defaultValues &&
               stmt.selectSource == nullptr && !stmt.values.empty();
    }
    return action == "do update" && !stmt.conflictTarget.empty() &&
           !stmt.defaultValues &&
           stmt.selectSource == nullptr && !stmt.values.empty();
}

bool supportsInsert(const InsertStmt& stmt) {
    // The bridge owns ordinary VALUES inserts, simple single-table SELECT
    // sources, column-projection RETURNING, and narrow conflict actions.
    // Every feature outside this contract remains on the established
    // implementation until its AST semantics are migrated.
    return !stmt.tableName.empty() &&
           supportsConflict(stmt) &&
           stmt.override_.empty() &&
           (stmt.defaultValues || !stmt.values.empty() || stmt.selectSource != nullptr);
}

bool checkInsertColumns(Session& s, const std::string& table,
                        const std::vector<std::string>& columns) {
    if (sessionIsAdmin(s) || isTempTable(s, table)) return true;
    if (!g_engine.hasColumnPermission(s.currentDB, table, effectiveSessionRole(s),
                                      StorageEngine::TablePrivilege::Insert,
                                      columns)) {
        std::cout << "permission denied: INSERT on restricted columns of table "
                  << table << std::endl;
        return false;
    }
    return true;
}

bool evaluateValue(const ExprPtr& expr, const std::string& currentDB,
                   std::string& value);
bool referencesColumn(const Expr* expr);

bool findTableColumn(const TableSchema& table, const std::string& name) {
    const std::string column = identifier(name);
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName == column) return true;
    }
    return false;
}

// Conflict expressions are evaluated once for each conflicting input row.
// SET expressions require the explicit EXCLUDED namespace; WHERE expressions
// additionally opt into the target row through the target table name or an
// unqualified column reference.
bool validateConflictExpression(const Expr* expr, const TableSchema& table,
                                ExprEvaluator& evaluator,
                                std::set<std::string>& sourceColumns,
                                const std::string& targetTable = {}) {
    if (!expr) return false;
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expr)) {
        return lower(literal->value) != "default";
    }
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        if (!ref->schema.empty()) return false;
        const std::string sourceColumn = identifier(ref->column);
        if (!findTableColumn(table, sourceColumn)) return false;
        if (lower(ref->table) == "excluded") {
            sourceColumns.insert(sourceColumn);
            return true;
        }
        if (targetTable.empty() ||
            (!ref->table.empty() && identifier(ref->table) != identifier(targetTable))) {
            return false;
        }
        return true;
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "+", "-", "not", "is null", "is not null",
            "is true", "is not true", "is false", "is not false"
        };
        return supported.count(lower(unary->op)) != 0 &&
               validateConflictExpression(unary->operand.get(), table,
                                           evaluator, sourceColumns, targetTable);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "and", "or", "=", "<>", "!=", "<", ">", "<=", ">=",
            "+", "-", "*", "/", "%", "^", "||", "like", "not like",
            "ilike", "not ilike", "similar to", "not similar to", "in", "::"
        };
        return supported.count(lower(binary->op)) != 0 &&
               validateConflictExpression(binary->left.get(), table,
                                           evaluator, sourceColumns, targetTable) &&
               validateConflictExpression(binary->right.get(), table,
                                           evaluator, sourceColumns, targetTable);
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        if (!evaluator.hasFunction(call->funcName) || call->distinct || call->filter ||
            call->hasOver || !call->namedArgs.empty() || !call->orderBy.empty()) {
            return false;
        }
        for (const auto& arg : call->args) {
            if (!validateConflictExpression(arg.get(), table, evaluator,
                                             sourceColumns, targetTable)) return false;
        }
        return true;
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return validateConflictExpression(cast->operand.get(), table,
                                           evaluator, sourceColumns, targetTable);
    }
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (caseExpr->switchExpr &&
            !validateConflictExpression(caseExpr->switchExpr.get(), table,
                                        evaluator, sourceColumns, targetTable)) {
            return false;
        }
        for (const auto& clause : caseExpr->whenClauses) {
            if (!validateConflictExpression(clause.first.get(), table, evaluator,
                                             sourceColumns, targetTable) ||
                !validateConflictExpression(clause.second.get(), table, evaluator,
                                             sourceColumns, targetTable)) {
                return false;
            }
        }
        return !caseExpr->elseExpr ||
               validateConflictExpression(caseExpr->elseExpr.get(), table,
                                           evaluator, sourceColumns, targetTable);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (!validateConflictExpression(element.get(), table, evaluator,
                                             sourceColumns, targetTable)) return false;
        }
        return true;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (!validateConflictExpression(element.get(), table, evaluator,
                                             sourceColumns, targetTable)) return false;
        }
        return true;
    }
    return false;
}

bool validateUpdateExpression(const Expr* expr, const TableSchema& table,
                              ExprEvaluator& evaluator,
                              const std::string& targetTable) {
    std::set<std::string> excludedColumns;
    // UPDATE expressions may reference the current target row, but EXCLUDED
    // is only defined inside ON CONFLICT DO UPDATE.
    return validateConflictExpression(expr, table, evaluator, excludedColumns,
                                      targetTable) && excludedColumns.empty();
}

bool sameColumnSet(const std::vector<size_t>& left,
                   const std::vector<size_t>& right) {
    if (left.size() != right.size()) return false;
    std::multiset<size_t> lhs(left.begin(), left.end());
    std::multiset<size_t> rhs(right.begin(), right.end());
    return lhs == rhs;
}

bool resolveConflictTarget(const InsertStmt& stmt, const TableSchema& table,
                           std::vector<std::string>& targetColumns) {
    if (stmt.conflictTarget.empty()) return false;

    std::set<std::string> seen;
    std::vector<size_t> targetIndices;
    targetColumns.clear();
    targetColumns.reserve(stmt.conflictTarget.size());
    for (const auto& rawColumn : stmt.conflictTarget) {
        const std::string column = identifier(rawColumn);
        if (column.empty() || !seen.insert(column).second) return false;
        size_t index = table.len;
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == column) {
                index = i;
                break;
            }
        }
        if (index >= table.len) return false;
        targetColumns.push_back(column);
        targetIndices.push_back(index);
    }

    std::vector<size_t> primaryKey;
    if (!table.pkColIndices.empty()) {
        primaryKey = table.pkColIndices;
    } else {
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].isPrimaryKey) primaryKey.push_back(i);
        }
    }
    if (sameColumnSet(targetIndices, primaryKey)) return true;

    for (const auto& uniqueConstraint : table.uniqueConstraints) {
        if (sameColumnSet(targetIndices, uniqueConstraint)) return true;
    }

    // Column-level UNIQUE/PRIMARY KEY constraints are not duplicated in
    // uniqueConstraints, so recognize their single-column form here.
    if (targetIndices.size() == 1) {
        const Column& column = table.cols[targetIndices.front()];
        return column.isUnique || column.isPrimaryKey;
    }
    return false;
}

bool buildConflictUpdatePlan(const InsertStmt& stmt, const TableSchema& table,
                             const std::string& currentDB,
                             std::vector<std::string>& targetColumns,
                             const std::string& targetTable,
                             std::map<std::string, std::string>& updates,
                             std::map<std::string, const Expr*>& expressionUpdates,
                             std::map<std::string, std::set<std::string>>& expressionSources,
                             std::set<std::string>& whereExcludedColumns) {
    if (lower(stmt.conflictAction) != "do update" ||
        stmt.conflictUpdateSet.empty()) {
        return false;
    }

    if (!resolveConflictTarget(stmt, table, targetColumns)) return false;

    std::set<std::string> seen;
    for (const auto& [rawColumn, expr] : stmt.conflictUpdateSet) {
        const std::string column = identifier(rawColumn);
        if (column.empty() || !seen.insert(column).second || isDefaultValue(expr)) {
            return false;
        }
        if (!findTableColumn(table, column)) return false;

        if (referencesColumn(expr.get())) {
            ExprEvaluator evaluator;
            std::set<std::string> sourceColumns;
            if (!validateConflictExpression(expr.get(), table, evaluator,
                                             sourceColumns)) {
                return false;
            }
            expressionUpdates[column] = expr.get();
            expressionSources[column] = std::move(sourceColumns);
            continue;
        }

        std::string value;
        if (!evaluateValue(expr, currentDB, value)) return false;
        updates[column] = std::move(value);
    }
    if (stmt.conflictWhere) {
        ExprEvaluator evaluator;
        if (!validateConflictExpression(stmt.conflictWhere.get(), table, evaluator,
                                         whereExcludedColumns, targetTable)) {
            return false;
        }
    }
    return !updates.empty() || !expressionUpdates.empty();
}

bool checkTablePrivilege(Session& s, const std::string& table,
                         StorageEngine::TablePrivilege privilege) {
    if (sessionIsAdmin(s) || isTempTable(s, table)) return true;
    if (g_engine.hasPermission(s.currentDB, table, effectiveSessionRole(s), privilege)) {
        return true;
    }
    for (const auto& permission : g_engine.getUserPermissions(
             s.currentDB, table, effectiveSessionRole(s))) {
        const bool matches =
            (privilege == StorageEngine::TablePrivilege::Select && permission == "select") ||
            (privilege == StorageEngine::TablePrivilege::Update && permission == "update") ||
            (privilege == StorageEngine::TablePrivilege::Delete && permission == "delete") ||
            permission == "all";
        if (matches) return true;
    }
    std::cout << "permission denied on table " << table << std::endl;
    return false;
}

// Convert the deliberately small predicate subset owned by this executor to
// the StorageEngine condition contract.  AND is safe because the engine
// accepts a conjunction of independent conditions; OR, subqueries, functions
// and column-to-column comparisons remain on the legacy path until they have
// a structured plan representation.
bool appendCondition(const Expr* expr, std::vector<std::string>& conditions) {
    if (!expr) return true;
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        const std::string op = lower(binary->op);
        if (op == "and") {
            return appendCondition(binary->left.get(), conditions) &&
                   appendCondition(binary->right.get(), conditions);
        }
        static const std::set<std::string> supported = {
            "=", "<>", "!=", "<", ">", "<=", ">=", "like"
        };
        if (!supported.count(op)) return false;
        const auto* column = dynamic_cast<const ColumnRefExpr*>(binary->left.get());
        const auto* literal = dynamic_cast<const LiteralExpr*>(binary->right.get());
        if (!column || !literal || !column->schema.empty() || !column->table.empty()) {
            return false;
        }
        if (lower(literal->value) == "null") return false;
        conditions.push_back(op + identifier(column->column) + " " + literal->value);
        return true;
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        const auto* column = dynamic_cast<const ColumnRefExpr*>(unary->operand.get());
        if (!column || !column->schema.empty() || !column->table.empty()) return false;
        const std::string op = lower(unary->op);
        if (op == "is null") {
            conditions.push_back("isnull" + identifier(column->column));
            return true;
        }
        if (op == "is not null") {
            conditions.push_back("isnotnull" + identifier(column->column));
            return true;
        }
    }
    return false;
}

bool buildConditions(const ExprPtr& expr, std::vector<std::string>& conditions) {
    return appendCondition(expr.get(), conditions);
}

struct ReturningProjection {
    const Expr* expression = nullptr;
    std::string column;
    std::string name;
    std::string typeName;
};

std::string tableColumnType(const TableSchema& table, const std::string& name) {
    const std::string column = identifier(name);
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName == column) return table.cols[i].dataType;
    }
    return "text";
}

std::string inferReturningType(const Expr* expr, const TableSchema& table) {
    if (!expr) return "text";
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expr)) {
        if (!literal->typeName.empty()) return literal->typeName;
        if (literal->value.size() >= 2 && literal->value.front() == '\'' &&
            literal->value.back() == '\'') {
            return "text";
        }
        const std::string value = lower(literal->value);
        if (value == "true" || value == "false") return "boolean";
        if (value == "null") return "text";
        if (value.find('.') != std::string::npos) return "double precision";
        bool numeric = !value.empty();
        const size_t start = !value.empty() &&
                             (value.front() == '-' || value.front() == '+') ? 1 : 0;
        for (size_t i = start; i < value.size(); ++i) {
            if (!std::isdigit(static_cast<unsigned char>(value[i]))) {
                numeric = false;
                break;
            }
        }
        return numeric ? "integer" : "text";
    }
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        return tableColumnType(table, ref->column);
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        const std::string op = lower(unary->op);
        if (op == "not" || op.find("is ") == 0) return "boolean";
        return inferReturningType(unary->operand.get(), table);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        const std::string op = lower(binary->op);
        if (op == "and" || op == "or" || op == "=" || op == "<>" || op == "!=" ||
            op == "<" || op == ">" || op == "<=" || op == ">=" || op == "like" ||
            op == "not like" || op == "ilike" || op == "not ilike" || op == "in") {
            return "boolean";
        }
        if (op == "||") return "text";
        if (op == "::") {
            if (const auto* castType = dynamic_cast<const LiteralExpr*>(binary->right.get())) {
                return lower(castType->value);
            }
        }
        const std::string left = lower(inferReturningType(binary->left.get(), table));
        const std::string right = lower(inferReturningType(binary->right.get(), table));
        if (left == "numeric" || right == "numeric" ||
            left == "double precision" || right == "double precision" ||
            left == "real" || right == "real") {
            return left == "numeric" || right == "numeric" ? "numeric" : "double precision";
        }
        return "integer";
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        const std::string name = lower(call->funcName);
        static const std::set<std::string> textFunctions = {
            "lower", "upper", "initcap", "concat", "concat_ws", "substring",
            "substr", "left", "right", "trim", "ltrim", "rtrim", "reverse",
            "replace", "translate", "format", "quote_literal", "quote_nullable"
        };
        static const std::set<std::string> integerFunctions = {
            "length", "char_length", "character_length", "bit_length", "strpos",
            "position", "ascii"
        };
        if (textFunctions.count(name)) return "text";
        if (integerFunctions.count(name)) return "integer";
        if (name == "coalesce" || name == "nullif" || name == "greatest" ||
            name == "least") {
            return call->args.empty() ? "text" : inferReturningType(call->args.front().get(), table);
        }
        if (name == "now" || name == "current_timestamp") return "timestamp";
        if (name == "current_date") return "date";
        return "text";
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) return lower(cast->typeName);
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (!caseExpr->whenClauses.empty()) {
            return inferReturningType(caseExpr->whenClauses.front().second.get(), table);
        }
        return caseExpr->elseExpr ? inferReturningType(caseExpr->elseExpr.get(), table) : "text";
    }
    if (dynamic_cast<const ArrayExpr*>(expr)) return "text";
    if (dynamic_cast<const RowExpr*>(expr)) return "record";
    return "text";
}

bool supportsReturningExpression(const Expr* expr, const TableSchema& table,
                                 ExprEvaluator& evaluator) {
    if (!expr) return false;
    if (dynamic_cast<const LiteralExpr*>(expr)) return true;
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        if (!ref->schema.empty() || !ref->table.empty()) return false;
        const std::string column = identifier(ref->column);
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == column) return true;
        }
        return false;
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "+", "-", "not", "is null", "is not null",
            "is true", "is not true", "is false", "is not false"
        };
        return supported.count(lower(unary->op)) != 0 &&
               supportsReturningExpression(unary->operand.get(), table, evaluator);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "and", "or", "=", "<>", "!=", "<", ">", "<=", ">=",
            "+", "-", "*", "/", "%", "^", "||", "like", "not like",
            "ilike", "not ilike", "similar to", "not similar to", "in", "::"
        };
        return supported.count(lower(binary->op)) != 0 &&
               supportsReturningExpression(binary->left.get(), table, evaluator) &&
               supportsReturningExpression(binary->right.get(), table, evaluator);
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        if (!evaluator.hasFunction(call->funcName) || call->distinct || call->filter ||
            call->hasOver || !call->namedArgs.empty() || !call->orderBy.empty()) {
            return false;
        }
        for (const auto& arg : call->args) {
            if (!supportsReturningExpression(arg.get(), table, evaluator)) return false;
        }
        return true;
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return supportsReturningExpression(cast->operand.get(), table, evaluator);
    }
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (caseExpr->switchExpr &&
            !supportsReturningExpression(caseExpr->switchExpr.get(), table, evaluator)) {
            return false;
        }
        for (const auto& clause : caseExpr->whenClauses) {
            if (!supportsReturningExpression(clause.first.get(), table, evaluator) ||
                !supportsReturningExpression(clause.second.get(), table, evaluator)) {
                return false;
            }
        }
        return !caseExpr->elseExpr ||
               supportsReturningExpression(caseExpr->elseExpr.get(), table, evaluator);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (!supportsReturningExpression(element.get(), table, evaluator)) return false;
        }
        return true;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (!supportsReturningExpression(element.get(), table, evaluator)) return false;
        }
        return true;
    }
    return false;
}

bool buildReturningProjections(const std::vector<SelectItem>& returning,
                               const TableSchema& table,
                               std::vector<ReturningProjection>& projections) {
    projections.clear();
    ExprEvaluator evaluator;
    for (const auto& item : returning) {
        if (!item.expr) return false;
        const auto* star = item.expr ? dynamic_cast<const LiteralExpr*>(item.expr.get()) : nullptr;
        if (star && star->value == "*") {
            if (!item.alias.empty()) return false;
            for (size_t i = 0; i < table.len; ++i) {
                projections.push_back({nullptr, table.cols[i].dataName,
                                       table.cols[i].dataName, table.cols[i].dataType});
            }
            continue;
        }
        const auto* ref = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
        if (ref && identifier(ref->column) == "*") {
            if (!item.alias.empty()) return false;
            for (size_t i = 0; i < table.len; ++i) {
                projections.push_back({nullptr, table.cols[i].dataName,
                                       table.cols[i].dataName, table.cols[i].dataType});
            }
            continue;
        }
        if (!supportsReturningExpression(item.expr.get(), table, evaluator)) return false;
        const std::string name = item.alias.empty()
            ? (ref ? identifier(ref->column) : item.expr->toString())
            : identifier(item.alias);
        projections.push_back({item.expr.get(), {}, name.empty() ? "?column?" : name,
                               inferReturningType(item.expr.get(), table)});
    }
    return !projections.empty();
}

bool collectSourceColumns(const Expr* expr, const TableSchema& table,
                          const std::string& sourceName,
                          const std::string& alias,
                          ExprEvaluator& evaluator,
                          std::set<std::string>& columns) {
    if (!expr) return false;
    if (dynamic_cast<const LiteralExpr*>(expr)) return true;
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        if (!ref->schema.empty()) return false;
        if (!ref->table.empty() &&
            identifier(ref->table) != identifier(sourceName) &&
            (alias.empty() || identifier(ref->table) != identifier(alias))) {
            return false;
        }
        const std::string column = identifier(ref->column);
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == column) {
                columns.insert(column);
                return true;
            }
        }
        return false;
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        return collectSourceColumns(unary->operand.get(), table, sourceName,
                                    alias, evaluator, columns);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        return collectSourceColumns(binary->left.get(), table, sourceName,
                                    alias, evaluator, columns) &&
               collectSourceColumns(binary->right.get(), table, sourceName,
                                    alias, evaluator, columns);
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        if (!evaluator.hasFunction(call->funcName) || call->hasOver ||
            call->filter || !call->namedArgs.empty() || !call->orderBy.empty()) {
            return false;
        }
        for (const auto& arg : call->args) {
            if (!collectSourceColumns(arg.get(), table, sourceName, alias,
                                      evaluator, columns)) return false;
        }
        return true;
    }
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (caseExpr->switchExpr &&
            !collectSourceColumns(caseExpr->switchExpr.get(), table, sourceName,
                                  alias, evaluator, columns)) return false;
        for (const auto& clause : caseExpr->whenClauses) {
            if (!collectSourceColumns(clause.first.get(), table, sourceName,
                                      alias, evaluator, columns) ||
                !collectSourceColumns(clause.second.get(), table, sourceName,
                                      alias, evaluator, columns)) return false;
        }
        return !caseExpr->elseExpr ||
               collectSourceColumns(caseExpr->elseExpr.get(), table, sourceName,
                                    alias, evaluator, columns);
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return collectSourceColumns(cast->operand.get(), table, sourceName,
                                    alias, evaluator, columns);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (!collectSourceColumns(element.get(), table, sourceName, alias,
                                      evaluator, columns)) return false;
        }
        return true;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (!collectSourceColumns(element.get(), table, sourceName, alias,
                                      evaluator, columns)) return false;
        }
        return true;
    }
    return false;
}

bool isStarProjection(const Expr* expr) {
    const auto* literal = expr ? dynamic_cast<const LiteralExpr*>(expr) : nullptr;
    return literal && literal->value == "*";
}

bool evaluateSourceExpression(const Expr* expr, const RowContext& context,
                              ExprEvaluator& evaluator, std::string& value) {
    const ExprValue result = evaluator.eval(expr, context);
    if (result.isUnknown() || result.typeName == "unknown") return false;
    if (result.isNull) {
        value.clear();
        return true;
    }
    value = result.value;
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

enum class InsertSelectBuildResult { Success, Unsupported, Error };

InsertSelectBuildResult buildInsertSelectRows(
    const SelectStmt& select, Session& s,
    const std::vector<std::string>& targetColumns,
    std::vector<std::map<std::string, std::string>>& pendingRows) {
    if (!select.ctes.empty() || !select.groupBy.empty() ||
        !select.groupByElems.empty() || select.having || !select.orderBy.empty() ||
        select.limit || select.offset || select.withTies || select.fetchFirst ||
        select.setOp != SetOp::None || select.setOpLhs || select.setOpRhs ||
        !select.valuesRows.empty() || select.distinct || !select.distinctOn.empty() ||
        !select.locking.empty() || !select.windowDefs.empty() ||
        select.selectList.empty()) {
        return InsertSelectBuildResult::Unsupported;
    }

    std::string sourceName;
    std::string sourceAlias;
    TableSchema sourceTable;
    bool hasSource = select.fromClause != nullptr;
    if (hasSource) {
        if (select.fromClause->type != FromItem::Type::Table ||
            select.fromClause->tableName.empty()) {
            return InsertSelectBuildResult::Unsupported;
        }
        sourceName = identifier(select.fromClause->tableName);
        sourceAlias = identifier(select.fromClause->alias);
        if (select.fromClause->left || select.fromClause->right ||
            select.fromClause->subquery) {
            return InsertSelectBuildResult::Unsupported;
        }
        const std::string resolvedSource = resolveTable(s, sourceName);
        if (g_engine.viewExists(s.currentDB, sourceName) ||
            g_engine.isMaterializedView(s.currentDB, sourceName)) {
            return InsertSelectBuildResult::Unsupported;
        }
        if (!g_engine.tableExists(s.currentDB, resolvedSource)) {
            std::cout << "Table " << sourceName << " not exist" << std::endl;
            return InsertSelectBuildResult::Error;
        }
        if (!checkTablePrivilege(s, sourceName,
                                 StorageEngine::TablePrivilege::Select)) {
            return InsertSelectBuildResult::Error;
        }
        sourceTable = g_engine.getTableSchema(s.currentDB, resolvedSource);
    }

    ExprEvaluator evaluator;
    evaluator.setCurrentDB(s.currentDB);
    std::set<std::string> referencedColumns;
    struct Projection { const Expr* expr = nullptr; size_t sourceIndex = 0; };
    std::vector<Projection> projections;
    for (const auto& item : select.selectList) {
        if (!item.expr || (!item.alias.empty() && isStarProjection(item.expr.get()))) {
            return InsertSelectBuildResult::Unsupported;
        }
        if (isStarProjection(item.expr.get())) {
            if (!hasSource) return InsertSelectBuildResult::Unsupported;
            for (size_t i = 0; i < sourceTable.len; ++i) {
                referencedColumns.insert(sourceTable.cols[i].dataName);
                projections.push_back({nullptr, i});
            }
            continue;
        }
        if (hasSource && !collectSourceColumns(item.expr.get(), sourceTable, sourceName,
                                               sourceAlias, evaluator,
                                               referencedColumns)) {
            return InsertSelectBuildResult::Unsupported;
        }
        if (!hasSource && !collectSourceColumns(item.expr.get(), TableSchema{}, "", "",
                                                evaluator, referencedColumns)) {
            return InsertSelectBuildResult::Unsupported;
        }
        projections.push_back({item.expr.get(), 0});
    }
    if (hasSource && select.whereClause &&
        !collectSourceColumns(select.whereClause.get(), sourceTable, sourceName,
                              sourceAlias, evaluator, referencedColumns)) {
        return InsertSelectBuildResult::Unsupported;
    }
    if (hasSource && !sessionIsAdmin(s) && !isTempTable(s, sourceName) &&
        !g_engine.hasColumnPermission(s.currentDB, sourceName, effectiveSessionRole(s),
                                      StorageEngine::TablePrivilege::Select,
                                      std::vector<std::string>(referencedColumns.begin(),
                                                               referencedColumns.end()))) {
        std::cout << "permission denied: SELECT on restricted columns of table "
                  << sourceName << std::endl;
        return InsertSelectBuildResult::Error;
    }
    if (projections.size() != targetColumns.size()) {
        std::cout << "SQL syntax error: column count mismatch" << std::endl;
        return InsertSelectBuildResult::Error;
    }

    bool evaluationFailed = false;
    auto appendRow = [&](const std::string& row) {
        RowContext context;
        if (hasSource) {
            for (size_t i = 0; i < sourceTable.len; ++i) {
                const auto& column = sourceTable.cols[i];
                const std::string value = g_engine.extractColumnValue(
                    row, sourceTable, i, s.currentDB, true);
                const bool isNull = value.empty() && column.isNull;
                ExprValue expressionValue(column.dataType, value, isNull);
                context.set(column.dataName, expressionValue);
                if (!sourceAlias.empty()) {
                    context.set(sourceAlias + "." + column.dataName,
                                expressionValue);
                }
                context.set(sourceName + "." + column.dataName,
                            expressionValue);
            }
        }
        if (select.whereClause) {
            const ExprValue predicate = evaluator.eval(select.whereClause, context);
            if (predicate.isUnknown() || predicate.typeName == "unknown") {
                evaluationFailed = true;
                return;
            }
            if (predicate.isNull) return;
            if (predicate.typeName != "boolean") {
                evaluationFailed = true;
                return;
            }
            if (!predicate.asBool()) return;
        }

        std::map<std::string, std::string> values;
        for (size_t i = 0; i < projections.size(); ++i) {
            std::string value;
            if (projections[i].expr == nullptr) {
                value = g_engine.extractColumnValue(
                    row, sourceTable, projections[i].sourceIndex, s.currentDB, true);
            } else if (!evaluateSourceExpression(projections[i].expr, context,
                                                 evaluator, value)) {
                evaluationFailed = true;
                return;
            }
            values[targetColumns[i]] = std::move(value);
        }
        pendingRows.push_back(std::move(values));
    };

    if (hasSource) {
        const std::string resolvedSource = resolveTable(s, sourceName);
        const bool scanOk = g_engine.forEachVisibleRow(
            s.currentDB, resolvedSource, "SELECT",
            [&](uint32_t, uint16_t, const char* data, size_t len) {
                if (!evaluationFailed) appendRow(std::string(data, len));
            });
        if (!scanOk) evaluationFailed = true;
    } else {
        appendRow({});
    }
    return evaluationFailed ? InsertSelectBuildResult::Unsupported
                            : InsertSelectBuildResult::Success;
}

RowContext returningContext(const std::map<std::string, std::string>& source,
                            const TableSchema& table) {
    RowContext context;
    for (size_t i = 0; i < table.len; ++i) {
        const Column& column = table.cols[i];
        const auto it = source.find(column.dataName);
        const bool isNull = it == source.end() || it->second.empty();
        context.set(column.dataName,
                    ExprValue(column.dataType, isNull ? "" : it->second, isNull));
    }
    return context;
}

RowContext updateContext(const std::map<std::string, std::string>& source,
                         const TableSchema& table,
                         const std::string& targetTable) {
    RowContext context;
    for (size_t i = 0; i < table.len; ++i) {
        const Column& column = table.cols[i];
        const auto it = source.find(column.dataName);
        const bool isNull = it == source.end() || it->second.empty();
        const ExprValue value(column.dataType,
                              isNull ? std::string{} : it->second, isNull);
        context.set(column.dataName, value);
        if (!targetTable.empty()) context.set(targetTable + "." + column.dataName, value);
    }
    return context;
}

std::string unqualifiedRelationName(const std::string& name) {
    const size_t dot = name.rfind('.');
    return identifier(dot == std::string::npos ? name : name.substr(dot + 1));
}

std::string rowValueKey(const std::map<std::string, std::string>& values) {
    std::string key;
    for (const auto& [column, value] : values) {
        key += std::to_string(column.size());
        key += ':';
        key += column;
        key += std::to_string(value.size());
        key += ':';
        key += value;
        key += ';';
    }
    return key;
}

struct StructuredSourceRelation {
    std::string requestedName;
    std::string resolvedName;
    std::string qualifier;
    TableSchema schema;
    std::vector<std::map<std::string, std::string>> rows;
};

bool validateStructuredExpression(
    const Expr* expr, const TableSchema& targetTable,
    const std::string& targetQualifier,
    const std::vector<StructuredSourceRelation>& sources,
    ExprEvaluator& evaluator) {
    if (!expr) return false;
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expr)) {
        return lower(literal->value) != "default";
    }
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        if (!ref->schema.empty()) return false;
        const std::string column = identifier(ref->column);
        const std::string qualifier = identifier(ref->table);
        if (qualifier.empty()) return findTableColumn(targetTable, column);
        if (qualifier == identifier(targetQualifier) ||
            qualifier == unqualifiedRelationName(targetQualifier)) {
            return findTableColumn(targetTable, column);
        }
        for (const auto& source : sources) {
            if (qualifier == identifier(source.qualifier) ||
                qualifier == unqualifiedRelationName(source.qualifier)) {
                return findTableColumn(source.schema, column);
            }
        }
        return false;
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "+", "-", "not", "is null", "is not null",
            "is true", "is not true", "is false", "is not false"
        };
        return supported.count(lower(unary->op)) != 0 &&
               validateStructuredExpression(unary->operand.get(), targetTable,
                                             targetQualifier, sources, evaluator);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "and", "or", "=", "<>", "!=", "<", ">", "<=", ">=",
            "+", "-", "*", "/", "%", "^", "||", "like", "not like",
            "ilike", "not ilike", "similar to", "not similar to", "in", "::"
        };
        return supported.count(lower(binary->op)) != 0 &&
               validateStructuredExpression(binary->left.get(), targetTable,
                                             targetQualifier, sources, evaluator) &&
               validateStructuredExpression(binary->right.get(), targetTable,
                                             targetQualifier, sources, evaluator);
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        if (!evaluator.hasFunction(call->funcName) || call->distinct || call->filter ||
            call->hasOver || !call->namedArgs.empty() || !call->orderBy.empty()) {
            return false;
        }
        for (const auto& arg : call->args) {
            if (!validateStructuredExpression(arg.get(), targetTable, targetQualifier,
                                               sources, evaluator)) {
                return false;
            }
        }
        return true;
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return validateStructuredExpression(cast->operand.get(), targetTable,
                                            targetQualifier, sources, evaluator);
    }
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (caseExpr->switchExpr &&
            !validateStructuredExpression(caseExpr->switchExpr.get(), targetTable,
                                           targetQualifier, sources, evaluator)) {
            return false;
        }
        for (const auto& clause : caseExpr->whenClauses) {
            if (!validateStructuredExpression(clause.first.get(), targetTable,
                                               targetQualifier, sources, evaluator) ||
                !validateStructuredExpression(clause.second.get(), targetTable,
                                               targetQualifier, sources, evaluator)) {
                return false;
            }
        }
        return !caseExpr->elseExpr ||
               validateStructuredExpression(caseExpr->elseExpr.get(), targetTable,
                                            targetQualifier, sources, evaluator);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (!validateStructuredExpression(element.get(), targetTable,
                                               targetQualifier, sources, evaluator)) {
                return false;
            }
        }
        return true;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (!validateStructuredExpression(element.get(), targetTable,
                                               targetQualifier, sources, evaluator)) {
                return false;
            }
        }
        return true;
    }
    return false;
}

bool validateUpdateFromExpression(const Expr* expr,
                                  const TableSchema& targetTable,
                                  const std::string& targetQualifier,
                                  const TableSchema& sourceTable,
                                  const std::string& sourceQualifier,
                                  ExprEvaluator& evaluator) {
    StructuredSourceRelation source;
    source.qualifier = sourceQualifier;
    source.schema = sourceTable;
    return validateStructuredExpression(expr, targetTable, targetQualifier,
                                         {source}, evaluator);
}

RowContext structuredRelationContext(
    const std::map<std::string, std::string>& targetValues,
    const TableSchema& targetTable, const std::string& targetQualifier,
    const std::vector<StructuredSourceRelation>& sources,
    const std::vector<const std::map<std::string, std::string>*>& sourceRows) {
    RowContext context;
    auto addValues = [&](const std::map<std::string, std::string>& values,
                         const TableSchema& table,
                         const std::vector<std::string>& qualifiers,
                         bool unqualified) {
        for (size_t i = 0; i < table.len; ++i) {
            const std::string& column = table.cols[i].dataName;
            const auto it = values.find(column);
            const bool isNull = it == values.end() || it->second.empty();
            const ExprValue value(table.cols[i].dataType,
                                  isNull ? std::string{} : it->second, isNull);
            if (unqualified) context.set(column, value);
            for (const auto& qualifier : qualifiers) {
                if (!qualifier.empty()) context.set(qualifier + "." + column, value);
            }
        }
    };
    addValues(targetValues, targetTable,
              {identifier(targetQualifier), unqualifiedRelationName(targetQualifier)}, true);
    for (size_t i = 0; i < sources.size() && i < sourceRows.size(); ++i) {
        if (!sourceRows[i]) continue;
        addValues(*sourceRows[i], sources[i].schema,
                  {identifier(sources[i].qualifier),
                   unqualifiedRelationName(sources[i].qualifier)}, false);
    }
    return context;
}

RowContext updateFromContext(const std::map<std::string, std::string>& targetValues,
                             const TableSchema& targetTable,
                             const std::string& targetQualifier,
                             const std::map<std::string, std::string>& sourceValues,
                             const TableSchema& sourceTable,
                             const std::string& sourceQualifier) {
    StructuredSourceRelation source;
    source.qualifier = sourceQualifier;
    source.schema = sourceTable;
    const std::vector<const std::map<std::string, std::string>*> rows = {&sourceValues};
    return structuredRelationContext(targetValues, targetTable, targetQualifier,
                                     {source}, rows);
}

bool collectTableRows(const std::string& currentDB, const std::string& tableName,
                      const TableSchema& table, const std::string& command,
                      std::vector<std::map<std::string, std::string>>& rows) {
    const bool ok = g_engine.forEachVisibleRow(
        currentDB, tableName, command,
        [&](uint32_t, uint16_t, const char* data, size_t len) {
        const std::string row(data, len);
        std::map<std::string, std::string> values;
        for (size_t i = 0; i < table.len; ++i) {
            values[table.cols[i].dataName] =
                g_engine.extractColumnValue(row, table, i, currentDB, true);
        }
        rows.push_back(std::move(values));
    });
    return ok;
}

enum class StructuredRelationResult { Success, Unsupported, Error };

StructuredRelationResult collectStructuredRelations(
    const FromItem* item, Session& s, const std::string& targetQualifier,
    std::vector<StructuredSourceRelation>& sources,
    std::vector<const Expr*>& joinPredicates) {
    if (!item) return StructuredRelationResult::Unsupported;
    if (item->type == FromItem::Type::Table) {
        const std::string requestedName = identifier(item->tableName);
        if (requestedName.empty()) return StructuredRelationResult::Unsupported;
        const std::string resolvedName = resolveTable(s, requestedName);
        if (g_engine.viewExists(s.currentDB, requestedName) ||
            g_engine.isMaterializedView(s.currentDB, requestedName) ||
            !g_engine.tableExists(s.currentDB, resolvedName)) {
            return StructuredRelationResult::Unsupported;
        }
        if (!checkTablePrivilege(s, requestedName,
                                 StorageEngine::TablePrivilege::Select)) {
            return StructuredRelationResult::Error;
        }
        StructuredSourceRelation source;
        source.requestedName = requestedName;
        source.resolvedName = resolvedName;
        source.qualifier = item->alias.empty()
            ? unqualifiedRelationName(requestedName)
            : identifier(item->alias);
        if (source.qualifier.empty() ||
            source.qualifier == identifier(targetQualifier) ||
            std::any_of(sources.begin(), sources.end(), [&](const auto& existing) {
                return existing.qualifier == source.qualifier;
            })) {
            return StructuredRelationResult::Unsupported;
        }
        source.schema = g_engine.getTableSchema(s.currentDB, resolvedName);
        if (!collectTableRows(s.currentDB, resolvedName, source.schema, "SELECT", source.rows)) {
            return StructuredRelationResult::Unsupported;
        }
        sources.push_back(std::move(source));
        return StructuredRelationResult::Success;
    }

    if (item->type != FromItem::Type::Join || !item->left || !item->right) {
        return StructuredRelationResult::Unsupported;
    }
    const std::string joinType = lower(item->joinType);
    // Outer joins require NULL-extended rows and are deliberately kept on the
    // legacy path until the structured relation executor models them exactly.
    if (joinType != "inner" && joinType != "cross") {
        return StructuredRelationResult::Unsupported;
    }
    if (!item->usingCols.empty()) return StructuredRelationResult::Unsupported;
    const size_t sourceCount = sources.size();
    const auto leftResult = collectStructuredRelations(
        item->left.get(), s, targetQualifier, sources, joinPredicates);
    if (leftResult != StructuredRelationResult::Success) return leftResult;
    const auto rightResult = collectStructuredRelations(
        item->right.get(), s, targetQualifier, sources, joinPredicates);
    if (rightResult != StructuredRelationResult::Success) return rightResult;
    if (item->joinCondition) joinPredicates.push_back(item->joinCondition.get());
    if (sources.size() <= sourceCount) return StructuredRelationResult::Unsupported;
    return StructuredRelationResult::Success;
}

bool structuredPredicateMatches(const Expr* expression, const RowContext& context,
                                ExprEvaluator& evaluator, bool& evaluationFailed) {
    if (!expression) return true;
    const ExprValue result = evaluator.eval(expression, context);
    // SQL NULL predicates are valid and simply do not select a row; only an
    // evaluator failure or a non-boolean, non-NULL result is unsupported.
    if (result.isNull) return false;
    if (result.isUnknown() || result.typeName == "unknown" ||
        result.typeName != "boolean") {
        evaluationFailed = true;
        return false;
    }
    return result.asBool();
}

bool findStructuredMatch(
    const std::map<std::string, std::string>& targetValues,
    const TableSchema& targetTable, const std::string& targetQualifier,
    const std::vector<StructuredSourceRelation>& sources,
    const std::vector<const Expr*>& joinPredicates, const Expr* whereClause,
    const std::string& currentDB,
    const std::function<bool(const RowContext&, const std::vector<
                             const std::map<std::string, std::string>*>&)>& consumer,
    bool& evaluationFailed) {
    std::vector<const std::map<std::string, std::string>*> sourceRows;
    std::function<bool(size_t)> visit = [&](size_t index) {
        if (index < sources.size()) {
            for (const auto& row : sources[index].rows) {
                sourceRows.push_back(&row);
                if (visit(index + 1)) return true;
                sourceRows.pop_back();
                if (evaluationFailed) return true;
            }
            return false;
        }

        const RowContext context = structuredRelationContext(
            targetValues, targetTable, targetQualifier, sources, sourceRows);
        ExprEvaluator evaluator;
        evaluator.setCurrentDB(currentDB);
        for (const Expr* predicate : joinPredicates) {
            if (!structuredPredicateMatches(predicate, context, evaluator,
                                            evaluationFailed)) {
                return evaluationFailed;
            }
        }
        if (!structuredPredicateMatches(whereClause, context, evaluator,
                                        evaluationFailed)) {
            return evaluationFailed;
        }
        return consumer(context, sourceRows);
    };
    const bool matched = visit(0);
    return matched && !evaluationFailed;
}

bool evaluateUpdateFromExpression(
    const Expr* expression,
    const std::map<std::string, std::string>& targetValues,
    const TableSchema& targetTable,
    const std::string& targetQualifier,
    const std::map<std::string, std::string>& sourceValues,
    const TableSchema& sourceTable,
    const std::string& sourceQualifier,
    const std::string& currentDB,
    std::string& value) {
    StructuredSourceRelation source;
    source.qualifier = sourceQualifier;
    source.schema = sourceTable;
    const std::vector<const std::map<std::string, std::string>*> rows = {&sourceValues};
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(
        expression, structuredRelationContext(targetValues, targetTable, targetQualifier,
                                              {source}, rows));
    if (result.isUnknown() || result.typeName == "unknown") return false;
    if (result.isNull) {
        value.clear();
        return true;
    }
    value = result.value;
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

bool evaluateStructuredExpression(
    const Expr* expression,
    const std::map<std::string, std::string>& targetValues,
    const TableSchema& targetTable, const std::string& targetQualifier,
    const std::vector<StructuredSourceRelation>& sources,
    const std::vector<const std::map<std::string, std::string>*>& sourceRows,
    const std::string& currentDB, std::string& value) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(
        expression, structuredRelationContext(targetValues, targetTable, targetQualifier,
                                              sources, sourceRows));
    if (result.isUnknown() || result.typeName == "unknown") return false;
    if (result.isNull) {
        value.clear();
        return true;
    }
    value = result.value;
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

bool evaluateUpdateExpression(const Expr* expression,
                              const std::map<std::string, std::string>& source,
                              const TableSchema& table,
                              const std::string& targetTable,
                              const std::string& currentDB,
                              std::string& value) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(
        expression, updateContext(source, table, targetTable));
    if (result.isUnknown() || result.typeName == "unknown") return false;
    if (result.isNull) {
        value.clear();
        return true;
    }
    value = result.value;
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

bool evaluateReturningExpression(const Expr* expression,
                                 const std::map<std::string, std::string>& source,
                                 const TableSchema& table,
                                 const std::string& currentDB,
                                 std::string& value,
                                 std::string& typeName) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(expression, returningContext(source, table));
    if (!result.isNull && (result.isUnknown() || result.typeName == "unknown")) {
        return false;
    }
    if (result.isNull) {
        value = "NULL";
        typeName = result.typeName;
        return true;
    }
    value = result.value;
    typeName = result.typeName;
    return true;
}

bool publishReturning(const std::vector<ReturningProjection>& projections,
                      const TableSchema& table,
                      const std::string& currentDB,
                      const std::vector<std::map<std::string, std::string>>& rows,
                      const std::string& command) {
    g_lastDmlResult.available = true;
    g_lastDmlResult.columns.clear();
    g_lastDmlResult.columnTypes.clear();
    for (const auto& projection : projections) {
        g_lastDmlResult.columns.push_back(projection.name);
        g_lastDmlResult.columnTypes.push_back(projection.typeName);
    }
    g_lastDmlResult.commandTag = command == "INSERT"
        ? "INSERT 0 " + std::to_string(rows.size())
        : command + " " + std::to_string(rows.size());
    g_lastDmlResult.rows.reserve(rows.size());
    for (const auto& source : rows) {
        std::vector<std::string> row;
        row.reserve(projections.size());
        for (size_t i = 0; i < projections.size(); ++i) {
            const auto& projection = projections[i];
            std::string value;
            if (projection.expression == nullptr) {
                const auto it = source.find(projection.column);
                value = it == source.end() || it->second.empty() ? "NULL" : it->second;
            } else {
                std::string typeName;
                if (!evaluateReturningExpression(projection.expression, source, table,
                                                  currentDB, value, typeName)) {
                    std::cout << "RETURNING expression evaluation failed" << std::endl;
                    return false;
                }
                if (!typeName.empty() && typeName != "unknown") {
                    g_lastDmlResult.columnTypes[i] = typeName;
                }
            }
            row.push_back(std::move(value));
        }
        g_lastDmlResult.rows.push_back(std::move(row));
    }
    return true;
}

void printReturningRows(const DmlResult& result) {
    for (const auto& row : result.rows) {
        std::ostringstream line;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i != 0) line << ' ';
            line << row[i];
        }
        std::cout << line.str() << std::endl;
    }
}

bool referencesColumn(const Expr* expr) {
    if (!expr) return false;
    if (dynamic_cast<const ColumnRefExpr*>(expr)) return true;
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        return referencesColumn(unary->operand.get());
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        return referencesColumn(binary->left.get()) ||
               referencesColumn(binary->right.get());
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        for (const auto& arg : call->args) {
            if (referencesColumn(arg.get())) return true;
        }
        return false;
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return referencesColumn(cast->operand.get());
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (referencesColumn(element.get())) return true;
        }
        return false;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (referencesColumn(element.get())) return true;
        }
    }
    return false;
}

bool evaluateValue(const ExprPtr& expr, const std::string& currentDB,
                   std::string& value) {
    if (referencesColumn(expr.get())) return false;
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    RowContext emptyContext;
    ExprValue result = evaluator.eval(expr, emptyContext);
    if (result.isUnknown()) return false;
    if (result.isNull) {
        value.clear();
        return true;
    }
    value = result.value;
    // ExprEvaluator uses PostgreSQL-style t/f internally, while the storage
    // validator accepts the canonical on-disk boolean spellings 1/0.
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

bool evaluateConflictExpression(const Expr* expr, const TableSchema& table,
                                const std::map<std::string, std::string>& values,
                                const std::string& currentDB,
                                std::string& value,
                                const std::map<std::string, std::string>* targetRow = nullptr,
                                const std::string& targetTable = {}) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    RowContext context;
    for (size_t i = 0; i < table.len; ++i) {
        const std::string& column = table.cols[i].dataName;
        const auto it = values.find(column);
        std::string targetValue;
        bool hasTargetValue = false;
        if (targetRow) {
            const auto targetIt = targetRow->find(column);
            if (targetIt != targetRow->end()) {
                targetValue = targetIt->second;
                hasTargetValue = true;
            }
        }
        if (targetRow) {
            context.set(column,
                        ExprValue(table.cols[i].dataType,
                                  targetValue,
                                  !hasTargetValue || targetValue.empty()));
            if (!targetTable.empty()) {
                context.set(targetTable + "." + column,
                            ExprValue(table.cols[i].dataType,
                                      targetValue,
                                      !hasTargetValue || targetValue.empty()));
            }
        }
        context.set("excluded." + column,
                    ExprValue(table.cols[i].dataType,
                              it == values.end() ? std::string{} : it->second,
                              it == values.end() || it->second.empty()));
    }
    const ExprValue result = evaluator.eval(expr, context);
    if (result.isUnknown()) return false;
    if (result.isNull) {
        value.clear();
        return true;
    }
    value = result.value;
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

bool loadConflictTargetRow(const std::string& currentDB,
                           const std::string& tableName,
                           const TableSchema& table,
                           const std::vector<std::string>& targetColumns,
                           const std::map<std::string, std::string>& targetValues,
                           std::map<std::string, std::string>& rowValues) {
    if (targetColumns.empty()) return false;

    std::vector<size_t> targetIndices;
    targetIndices.reserve(targetColumns.size());
    for (const auto& targetColumn : targetColumns) {
        size_t targetIndex = table.len;
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == targetColumn) {
                targetIndex = i;
                break;
            }
        }
        const auto value = targetValues.find(targetColumn);
        if (targetIndex >= table.len || value == targetValues.end() ||
            value->second.empty()) {
            return false;
        }
        targetIndices.push_back(targetIndex);
    }

    auto captureRow = [&](const std::string& rowBuffer) {
        rowValues.clear();
        for (size_t i = 0; i < table.len; ++i) {
            rowValues[table.cols[i].dataName] =
                g_engine.extractColumnValue(rowBuffer, table, i, currentDB, true);
        }
        return true;
    };

    // Preserve the indexed fast path for the common single-column case. The
    // composite path below intentionally verifies every target column from
    // the visible heap row because composite UNIQUE constraints do not yet
    // have a dedicated constraint index.
    if (targetIndices.size() == 1) {
        const std::string& targetColumn = targetColumns.front();
        const std::string& targetValue = targetValues.at(targetColumn);
        BPTree* index = table.cols[targetIndices.front()].isPrimaryKey
            ? g_engine.getPKIndex(currentDB, tableName)
            : g_engine.getSecondaryIndex(currentDB, tableName, targetColumn);
        if (index) {
            int64_t rid = 0;
            if (index->search(targetValue, rid)) {
                std::string rowBuffer;
                PageAllocator* allocator = g_engine.getPageAllocator(currentDB, tableName);
                if (allocator && g_engine.readVisibleRowByRid(allocator, rid, rowBuffer, table)) {
                    return captureRow(rowBuffer);
                }
            }
        }
    }

    bool found = false;
    g_engine.forEachRow(currentDB, tableName,
                        [&](uint32_t, uint16_t, const char* data, size_t len) {
        if (found) return;
        const std::string rowBuffer(data, len);
        for (size_t i = 0; i < targetIndices.size(); ++i) {
            const std::string value = g_engine.extractColumnValue(
                rowBuffer, table, targetIndices[i], currentDB, true);
            const auto expected = targetValues.find(targetColumns[i]);
            if (expected == targetValues.end() || value != expected->second) {
                return;
            }
        }
        {
            found = captureRow(rowBuffer);
        }
    });
    return found;
}

bool executeInsert(const InsertStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (!checkDatabase(s)) return true;

    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);

    // Views and materialized views have separate rewrite/trigger semantics in
    // main.cpp.  Do not bypass those semantics while this bridge is partial.
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }

    if (!checkInsertTablePermission(s, requestedTable)) {
        return true;
    }

    const TableSchema table = g_engine.getTableSchema(s.currentDB, resolvedTable);
    if (table.len == 0) {
        std::cout << "Table has no columns" << std::endl;
        return true;
    }

    std::vector<std::string> columns;
    if (stmt.columns.empty()) {
        columns.reserve(table.len);
        for (size_t i = 0; i < table.len; ++i) {
            columns.push_back(table.cols[i].dataName);
        }
    } else {
        columns.reserve(stmt.columns.size());
        std::set<std::string> seen;
        for (const auto& rawColumn : stmt.columns) {
            const std::string column = identifier(rawColumn);
            if (column.empty() || !seen.insert(column).second) {
                std::cout << "SQL syntax error: duplicate or empty INSERT column"
                          << std::endl;
                return true;
            }
            bool found = false;
            for (size_t i = 0; i < table.len; ++i) {
                if (table.cols[i].dataName == column) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cout << "Column " << column << " does not exist" << std::endl;
                return true;
            }
            columns.push_back(column);
        }
    }

    if (!checkInsertColumns(s, requestedTable, columns)) return true;

    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> insertedRows;
    const bool ignoreDuplicate = lower(stmt.conflictAction) == "do nothing";
    const bool conflictUpdate = lower(stmt.conflictAction) == "do update";
    std::vector<std::string> conflictTarget;
    std::map<std::string, std::string> conflictUpdates;
    std::map<std::string, const Expr*> conflictExpressionUpdates;
    std::map<std::string, std::set<std::string>> conflictExpressionSources;
    std::set<std::string> conflictWhereExcludedColumns;

    if (conflictUpdate || (ignoreDuplicate && !stmt.conflictTarget.empty())) {
        if (!conflictUpdate &&
            !resolveConflictTarget(stmt, table, conflictTarget)) {
            fallback = true;
            return false;
        }
    }

    if (conflictUpdate) {
        if (!checkTablePrivilege(s, requestedTable,
                                 StorageEngine::TablePrivilege::Update)) {
            return true;
        }
        if (!buildConflictUpdatePlan(stmt, table, s.currentDB, conflictTarget,
                                     requestedTable, conflictUpdates,
                                     conflictExpressionUpdates, conflictExpressionSources,
                                     conflictWhereExcludedColumns)) {
            fallback = true;
            return false;
        }
        const std::vector<std::string> updateColumns = [&]() {
            std::vector<std::string> result;
            result.reserve(conflictUpdates.size() + conflictExpressionUpdates.size());
            std::set<std::string> seen;
            for (const auto& [column, value] : conflictUpdates) {
                (void)value;
                if (seen.insert(column).second) result.push_back(column);
            }
            for (const auto& [column, expression] : conflictExpressionUpdates) {
                (void)expression;
                if (seen.insert(column).second) result.push_back(column);
            }
            return result;
        }();
        if (updateColumns.empty()) {
            fallback = true;
            return false;
        }
        if (!sessionIsAdmin(s) && !isTempTable(s, requestedTable) &&
            !g_engine.hasColumnPermission(
                s.currentDB, requestedTable, effectiveSessionRole(s),
                StorageEngine::TablePrivilege::Update, updateColumns)) {
            std::cout << "permission denied: UPDATE on restricted columns of table "
                      << requestedTable << std::endl;
            return true;
        }
    }

    if (stmt.selectSource) {
        const auto* select = dynamic_cast<const SelectStmt*>(stmt.selectSource.get());
        if (!select) {
            fallback = true;
            return false;
        }
        std::vector<std::map<std::string, std::string>> pendingRows;
        const InsertSelectBuildResult buildResult = buildInsertSelectRows(
            *select, s, columns, pendingRows);
        if (buildResult == InsertSelectBuildResult::Unsupported) {
            fallback = true;
            return false;
        }
        if (buildResult == InsertSelectBuildResult::Error) return true;

        int inserted = 0;
        for (const auto& values : pendingRows) {
            const DBStatus status = g_engine.insert(
                s.currentDB, resolvedTable, values,
                stmt.returning.empty() ? nullptr : &insertedRows);
            if (status == DBStatus::DUPLICATE_KEY) {
                if (ignoreDuplicate) continue;
                std::cout << "Duplicate key" << std::endl;
                return true;
            }
            if (status != DBStatus::OK) {
                std::cout << "Invalid data, please check" << std::endl;
                return true;
            }
            ++inserted;
        }
        std::cout << inserted << " row(s) inserted" << std::endl;
        if (!stmt.returning.empty()) {
            if (!publishReturning(returningProjections, table, s.currentDB,
                                  insertedRows, "INSERT")) return true;
            printReturningRows(g_lastDmlResult);
        }
        if (inserted > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
        return false;
    }

    if (stmt.defaultValues) {
        if (!stmt.values.empty()) {
            std::cout << "SQL syntax error: invalid DEFAULT VALUES statement"
                      << std::endl;
            return true;
        }
        const DBStatus status = g_engine.insertDefaultValues(
            s.currentDB, resolvedTable, table,
            stmt.returning.empty() ? nullptr : &insertedRows);
        if (status == DBStatus::DUPLICATE_KEY && ignoreDuplicate) {
            std::cout << "INSERT 0 0 (ON CONFLICT DO NOTHING)" << std::endl;
            if (!stmt.returning.empty()) {
                if (!publishReturning(returningProjections, table, s.currentDB,
                                      insertedRows, "INSERT")) return true;
                printReturningRows(g_lastDmlResult);
            }
            return false;
        }
        if (status != DBStatus::OK) {
            std::cout << "INSERT DEFAULT VALUES failed" << std::endl;
            return true;
        }
        std::cout << "INSERT 0 1 (DEFAULT VALUES)" << std::endl;
        if (!stmt.returning.empty()) {
            if (!publishReturning(returningProjections, table, s.currentDB,
                                  insertedRows, "INSERT")) return true;
            printReturningRows(g_lastDmlResult);
        }
        g_engine.analyzeTable(s.currentDB, resolvedTable);
        return false;
    }

    // Evaluate the complete VALUES list before mutating storage.  If an
    // expression is outside this executor's supported evaluator, the legacy
    // path must receive the untouched statement without a partially inserted
    // prefix.
    std::vector<std::map<std::string, std::string>> pendingRows;
    pendingRows.reserve(stmt.values.size());
    for (const auto& row : stmt.values) {
        if (row.size() != columns.size()) {
            std::cout << "SQL syntax error: column count mismatch" << std::endl;
            return true;
        }

        std::map<std::string, std::string> values;
        for (size_t i = 0; i < row.size(); ++i) {
            if (isDefaultValue(row[i])) continue;
            std::string value;
            if (!evaluateValue(row[i], s.currentDB, value)) {
                // Returning false lets the legacy path retain ownership of
                // expression forms not yet supported by ExprEvaluator.
                fallback = true;
                return false;
            }
            values[columns[i]] = std::move(value);
        }
        pendingRows.push_back(std::move(values));
    }

    if (conflictUpdate || (ignoreDuplicate && !conflictTarget.empty())) {
        // The narrow plan needs the inferred target value before the insert
        // reports a duplicate. DEFAULT/generated target values require a
        // storage-level conflict key API and therefore remain legacy-owned.
        for (const auto& values : pendingRows) {
            for (const auto& targetColumn : conflictTarget) {
                const auto it = values.find(targetColumn);
                if (it == values.end()) {
                    fallback = true;
                    return false;
                }
            }
            for (const auto& [updateColumn, sourceColumns] : conflictExpressionSources) {
                (void)updateColumn;
                for (const auto& sourceColumn : sourceColumns) {
                    const auto source = values.find(sourceColumn);
                    if (source == values.end()) {
                        // DEFAULT/generated incoming values need to be resolved
                        // by the storage insert path before they can become
                        // EXCLUDED.
                        fallback = true;
                        return false;
                    }
                }
            }
            for (const auto& sourceColumn : conflictWhereExcludedColumns) {
                if (values.find(sourceColumn) == values.end()) {
                    // A WHERE expression over EXCLUDED cannot be evaluated
                    // until DEFAULT/generated input values have been materialized.
                    fallback = true;
                    return false;
                }
            }
        }
    }

    int inserted = 0;
    for (const auto& values : pendingRows) {
        const DBStatus status = g_engine.insert(
            s.currentDB, resolvedTable, values,
            stmt.returning.empty() ? nullptr : &insertedRows);
        if (status == DBStatus::DUPLICATE_KEY) {
            if (ignoreDuplicate) {
                if (conflictTarget.empty()) continue;

                std::map<std::string, std::string> targetValues;
                bool targetValueUnavailable = false;
                for (const auto& targetColumn : conflictTarget) {
                    const auto targetValue = values.find(targetColumn);
                    if (targetValue == values.end() || targetValue->second.empty()) {
                        targetValueUnavailable = true;
                        break;
                    }
                    targetValues[targetColumn] = targetValue->second;
                }
                std::map<std::string, std::string> targetRow;
                if (!targetValueUnavailable &&
                    loadConflictTargetRow(s.currentDB, resolvedTable, table,
                                          conflictTarget, targetValues,
                                          targetRow)) {
                    continue;
                }
                // A target-specific DO NOTHING must not hide a duplicate
                // raised by a different unique constraint.
                std::cout << "Duplicate key" << std::endl;
                return true;
            }
            if (conflictUpdate) {
                std::map<std::string, std::string> targetValues;
                std::vector<std::string> conditions;
                conditions.reserve(conflictTarget.size());
                for (const auto& targetColumn : conflictTarget) {
                    const auto targetValue = values.find(targetColumn);
                    if (targetValue == values.end() || targetValue->second.empty()) {
                        std::cout << "ON CONFLICT target value is unavailable" << std::endl;
                        return true;
                    }
                    targetValues[targetColumn] = targetValue->second;
                    conditions.push_back("=" + targetColumn + " " + targetValue->second);
                }
                if (stmt.conflictWhere) {
                    std::map<std::string, std::string> targetRow;
                    if (!loadConflictTargetRow(s.currentDB, resolvedTable, table,
                                                conflictTarget, targetValues,
                                                targetRow)) {
                        std::cout << "ON CONFLICT target row is unavailable" << std::endl;
                        return true;
                    }
                    std::string whereValue;
                    if (!evaluateConflictExpression(stmt.conflictWhere.get(), table,
                                                     values, s.currentDB, whereValue,
                                                     &targetRow, requestedTable)) {
                        std::cout << "ON CONFLICT WHERE evaluation failed" << std::endl;
                        return true;
                    }
                    if (whereValue.empty() || whereValue == "0" ||
                        lower(whereValue) == "false") {
                        // PostgreSQL treats a false/NULL conflict WHERE as
                        // "do not update" for this conflicting input row.
                        continue;
                    }
                }
                std::map<std::string, std::string> rowConflictUpdates = conflictUpdates;
                for (const auto& [updateColumn, expression] : conflictExpressionUpdates) {
                    std::string value;
                    if (!evaluateConflictExpression(expression, table, values,
                                                    s.currentDB, value)) {
                        std::cout << "ON CONFLICT expression evaluation failed" << std::endl;
                        return true;
                    }
                    rowConflictUpdates[updateColumn] = std::move(value);
                }
                std::vector<std::map<std::string, std::string>> updatedRows;
                const DBStatus updateStatus = g_engine.update(
                    s.currentDB, resolvedTable, rowConflictUpdates, conditions,
                    &updatedRows);
                if (updateStatus != DBStatus::OK || updatedRows.size() != 1) {
                    std::cout << "ON CONFLICT DO UPDATE failed" << std::endl;
                    return true;
                }
                if (!stmt.returning.empty()) {
                    insertedRows.insert(insertedRows.end(), updatedRows.begin(),
                                        updatedRows.end());
                }
                ++inserted;
                continue;
            }
            std::cout << "Duplicate key" << std::endl;
            return true;
        }
        if (status != DBStatus::OK) {
            std::cout << "Invalid data, please check" << std::endl;
            return true;
        }
        ++inserted;
    }

    std::cout << inserted << " row(s) inserted" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, table, s.currentDB,
                              insertedRows, "INSERT")) return true;
        printReturningRows(g_lastDmlResult);
    }
    if (inserted > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

bool executeUpdateFromJoin(const UpdateStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (!stmt.fromClause || stmt.fromClause->type != FromItem::Type::Join) {
        fallback = true;
        return false;
    }
    if (!checkDatabase(s)) return true;

    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }
    if (!checkTablePrivilege(s, requestedTable,
                             StorageEngine::TablePrivilege::Update)) return true;

    const TableSchema targetSchema = g_engine.getTableSchema(s.currentDB, resolvedTable);
    const std::string targetQualifier = unqualifiedRelationName(requestedTable);
    std::vector<StructuredSourceRelation> sources;
    std::vector<const Expr*> joinPredicates;
    const StructuredRelationResult relationResult = collectStructuredRelations(
        stmt.fromClause.get(), s, targetQualifier, sources, joinPredicates);
    if (relationResult == StructuredRelationResult::Unsupported) {
        fallback = true;
        return false;
    }
    if (relationResult == StructuredRelationResult::Error) return true;

    std::vector<std::string> columns;
    std::map<std::string, std::string> staticUpdates;
    std::map<std::string, const Expr*> expressionUpdates;
    for (const auto& [rawColumn, expression] : stmt.setClauses) {
        const std::string column = identifier(rawColumn);
        columns.push_back(column);
        if (!findTableColumn(targetSchema, column)) {
            std::cout << "Column " << column << " does not exist" << std::endl;
            return true;
        }
        if (isDefaultValue(expression)) {
            fallback = true;
            return false;
        }
        ExprEvaluator evaluator;
        if (!validateStructuredExpression(expression.get(), targetSchema,
                                          targetQualifier, sources, evaluator)) {
            fallback = true;
            return false;
        }
        if (referencesColumn(expression.get())) {
            expressionUpdates[column] = expression.get();
        } else {
            std::string value;
            if (!evaluateValue(expression, s.currentDB, value)) {
                fallback = true;
                return false;
            }
            staticUpdates[column] = std::move(value);
        }
    }
    if (staticUpdates.empty() && expressionUpdates.empty()) {
        std::cout << "SQL syntax error: empty UPDATE SET clause" << std::endl;
        return true;
    }
    if (!sessionIsAdmin(s) && !isTempTable(s, requestedTable) &&
        !g_engine.hasColumnPermission(
            s.currentDB, requestedTable, effectiveSessionRole(s),
            StorageEngine::TablePrivilege::Update, columns)) {
        std::cout << "permission denied: UPDATE on restricted columns of table "
                  << requestedTable << std::endl;
        return true;
    }
    for (const Expr* predicate : joinPredicates) {
        ExprEvaluator evaluator;
        if (!validateStructuredExpression(predicate, targetSchema, targetQualifier,
                                           sources, evaluator)) {
            fallback = true;
            return false;
        }
    }
    if (stmt.whereClause) {
        ExprEvaluator evaluator;
        if (!validateStructuredExpression(stmt.whereClause.get(), targetSchema,
                                           targetQualifier, sources, evaluator)) {
            fallback = true;
            return false;
        }
    }

    std::vector<std::map<std::string, std::string>> targetRows;
    if (!collectTableRows(s.currentDB, resolvedTable, targetSchema, "UPDATE", targetRows)) {
        fallback = true;
        return false;
    }
    std::map<std::string, std::map<std::string, std::string>> matchedUpdates;
    bool evaluationFailed = false;
    for (const auto& targetValues : targetRows) {
        findStructuredMatch(
            targetValues, targetSchema, targetQualifier, sources, joinPredicates,
            stmt.whereClause.get(), s.currentDB,
            [&](const RowContext&, const std::vector<const std::map<std::string, std::string>*>& sourceRows) {
                std::map<std::string, std::string> effectiveUpdates = staticUpdates;
                for (const auto& [column, expression] : expressionUpdates) {
                    std::string value;
                    if (!evaluateStructuredExpression(
                            expression, targetValues, targetSchema, targetQualifier,
                            sources, sourceRows, s.currentDB, value)) {
                        evaluationFailed = true;
                        return true;
                    }
                    effectiveUpdates[column] = std::move(value);
                }
                matchedUpdates.emplace(rowValueKey(targetValues),
                                      std::move(effectiveUpdates));
                return true;
            }, evaluationFailed);
        if (evaluationFailed) break;
    }
    if (evaluationFailed) {
        std::cout << "UPDATE FROM expression evaluation failed" << std::endl;
        return true;
    }

    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> updatedRows;
    StorageEngine::UpdateResolver updateResolver;
    if (!expressionUpdates.empty()) {
        updateResolver = [matchedUpdates](
                             const std::map<std::string, std::string>& oldValues,
                             std::map<std::string, std::string>& effectiveUpdates) {
            const auto it = matchedUpdates.find(rowValueKey(oldValues));
            if (it == matchedUpdates.end()) return false;
            effectiveUpdates = it->second;
            return true;
        };
    }
    const StorageEngine::UpdateMatcher updateMatcher = [matchedUpdates](
        const std::map<std::string, std::string>& oldValues) {
        return matchedUpdates.find(rowValueKey(oldValues)) != matchedUpdates.end();
    };
    const DBStatus status = g_engine.update(
        s.currentDB, resolvedTable, staticUpdates, {},
        stmt.returning.empty() ? nullptr : &updatedRows,
        updateResolver, updateMatcher);
    if (status != DBStatus::OK) {
        std::cout << "UPDATE FROM failed" << std::endl;
        return true;
    }
    std::cout << "Update done" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, targetSchema, s.currentDB,
                              updatedRows, "UPDATE")) return true;
        printReturningRows(g_lastDmlResult);
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

bool executeUpdateFrom(const UpdateStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (stmt.fromClause && stmt.fromClause->type == FromItem::Type::Join) {
        return executeUpdateFromJoin(stmt, s, fallback);
    }
    if (!stmt.fromClause || stmt.fromClause->type != FromItem::Type::Table) {
        fallback = true;
        return false;
    }
    if (!checkDatabase(s)) return true;

    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }

    const std::string requestedSource = identifier(stmt.fromClause->tableName);
    const std::string resolvedSource = resolveTable(s, requestedSource);
    if (g_engine.viewExists(s.currentDB, requestedSource) ||
        g_engine.isMaterializedView(s.currentDB, requestedSource) ||
        !g_engine.tableExists(s.currentDB, resolvedSource)) {
        fallback = true;
        return false;
    }
    if (!checkTablePrivilege(s, requestedTable,
                             StorageEngine::TablePrivilege::Update) ||
        !checkTablePrivilege(s, requestedSource,
                             StorageEngine::TablePrivilege::Select)) {
        return true;
    }

    const TableSchema targetSchema = g_engine.getTableSchema(s.currentDB, resolvedTable);
    const TableSchema sourceSchema = g_engine.getTableSchema(s.currentDB, resolvedSource);
    const std::string targetQualifier = unqualifiedRelationName(requestedTable);
    const std::string sourceQualifier = stmt.fromClause->alias.empty()
        ? unqualifiedRelationName(requestedSource)
        : identifier(stmt.fromClause->alias);

    std::vector<std::string> columns;
    std::map<std::string, std::string> staticUpdates;
    std::map<std::string, const Expr*> expressionUpdates;
    for (const auto& [rawColumn, expression] : stmt.setClauses) {
        const std::string column = identifier(rawColumn);
        columns.push_back(column);
        if (!findTableColumn(targetSchema, column)) {
            std::cout << "Column " << column << " does not exist" << std::endl;
            return true;
        }
        if (isDefaultValue(expression)) {
            fallback = true;
            return false;
        }
        ExprEvaluator evaluator;
        if (!validateUpdateFromExpression(expression.get(), targetSchema,
                                           targetQualifier, sourceSchema,
                                           sourceQualifier, evaluator)) {
            fallback = true;
            return false;
        }
        if (referencesColumn(expression.get())) {
            expressionUpdates[column] = expression.get();
        } else {
            std::string value;
            if (!evaluateValue(expression, s.currentDB, value)) {
                fallback = true;
                return false;
            }
            staticUpdates[column] = std::move(value);
        }
    }
    if (staticUpdates.empty() && expressionUpdates.empty()) {
        std::cout << "SQL syntax error: empty UPDATE SET clause" << std::endl;
        return true;
    }
    if (!sessionIsAdmin(s) && !isTempTable(s, requestedTable) &&
        !g_engine.hasColumnPermission(
            s.currentDB, requestedTable, effectiveSessionRole(s),
            StorageEngine::TablePrivilege::Update, columns)) {
        std::cout << "permission denied: UPDATE on restricted columns of table "
                  << requestedTable << std::endl;
        return true;
    }
    if (stmt.whereClause) {
        ExprEvaluator evaluator;
        if (!validateUpdateFromExpression(stmt.whereClause.get(), targetSchema,
                                           targetQualifier, sourceSchema,
                                           sourceQualifier, evaluator)) {
            fallback = true;
            return false;
        }
    }

    std::vector<std::map<std::string, std::string>> sourceRows;
    if (!collectTableRows(s.currentDB, resolvedSource, sourceSchema, "SELECT", sourceRows)) {
        fallback = true;
        return false;
    }

    std::map<std::string, std::map<std::string, std::string>> matchedUpdates;
    bool evaluationFailed = false;
    std::vector<std::map<std::string, std::string>> targetRows;
    if (!collectTableRows(s.currentDB, resolvedTable, targetSchema, "UPDATE", targetRows)) {
        fallback = true;
        return false;
    }
    for (const auto& targetValues : targetRows) {
        if (evaluationFailed) break;
        for (const auto& sourceValues : sourceRows) {
            ExprEvaluator evaluator;
            evaluator.setCurrentDB(s.currentDB);
            const RowContext context = updateFromContext(
                targetValues, targetSchema, targetQualifier,
                sourceValues, sourceSchema, sourceQualifier);
            if (stmt.whereClause) {
                const ExprValue predicate = evaluator.eval(stmt.whereClause.get(), context);
                if (predicate.isUnknown() || predicate.isNull || !predicate.asBool()) continue;
            }

            std::map<std::string, std::string> effectiveUpdates = staticUpdates;
            for (const auto& [column, expression] : expressionUpdates) {
                std::string value;
                if (!evaluateUpdateFromExpression(
                        expression, targetValues, targetSchema, targetQualifier,
                        sourceValues, sourceSchema, sourceQualifier,
                        s.currentDB, value)) {
                    evaluationFailed = true;
                    break;
                }
                effectiveUpdates[column] = std::move(value);
            }
            matchedUpdates.emplace(rowValueKey(targetValues), std::move(effectiveUpdates));
            break; // PostgreSQL permits one source row to determine each target row.
        }
    }
    if (evaluationFailed) {
        std::cout << "UPDATE FROM expression evaluation failed" << std::endl;
        return true;
    }

    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> updatedRows;
    StorageEngine::UpdateResolver updateResolver;
    if (!expressionUpdates.empty()) {
        updateResolver = [matchedUpdates](
                             const std::map<std::string, std::string>& oldValues,
                             std::map<std::string, std::string>& effectiveUpdates) {
            const auto it = matchedUpdates.find(rowValueKey(oldValues));
            if (it == matchedUpdates.end()) return false;
            effectiveUpdates = it->second;
            return true;
        };
    }
    const StorageEngine::UpdateMatcher updateMatcher = [matchedUpdates](
        const std::map<std::string, std::string>& oldValues) {
        return matchedUpdates.find(rowValueKey(oldValues)) != matchedUpdates.end();
    };
    const DBStatus status = g_engine.update(
        s.currentDB, resolvedTable, staticUpdates, {},
        stmt.returning.empty() ? nullptr : &updatedRows,
        updateResolver, updateMatcher);
    if (status != DBStatus::OK) {
        std::cout << "UPDATE FROM failed" << std::endl;
        return true;
    }
    std::cout << "Update done" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, targetSchema, s.currentDB,
                              updatedRows, "UPDATE")) return true;
        printReturningRows(g_lastDmlResult);
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

bool executeUpdate(const UpdateStmt& stmt, Session& s, bool& fallback) {
    if (stmt.fromClause) return executeUpdateFrom(stmt, s, fallback);
    fallback = false;
    if (!checkDatabase(s)) return true;
    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }
    if (!checkTablePrivilege(s, requestedTable,
                             StorageEngine::TablePrivilege::Update)) return true;

    const TableSchema table = g_engine.getTableSchema(s.currentDB, resolvedTable);
    const std::string targetQualifier = [&]() {
        const size_t dot = requestedTable.rfind('.');
        return dot == std::string::npos ? requestedTable : requestedTable.substr(dot + 1);
    }();
    std::vector<std::string> columns;
    std::map<std::string, std::string> updates;
    std::map<std::string, const Expr*> expressionUpdates;
    for (const auto& [rawColumn, expr] : stmt.setClauses) {
        const std::string column = identifier(rawColumn);
        columns.push_back(column);
        if (isDefaultValue(expr)) {
            fallback = true;
            return false;
        }
        if (referencesColumn(expr.get())) {
            ExprEvaluator evaluator;
            if (!findTableColumn(table, column) ||
                !validateUpdateExpression(expr.get(), table, evaluator,
                                           targetQualifier)) {
                fallback = true;
                return false;
            }
            expressionUpdates[column] = expr.get();
            continue;
        }
        std::string value;
        if (!evaluateValue(expr, s.currentDB, value)) {
            fallback = true;
            return false;
        }
        updates[column] = std::move(value);
    }
    if (updates.empty() && expressionUpdates.empty()) {
        std::cout << "SQL syntax error: empty UPDATE SET clause" << std::endl;
        return true;
    }
    if (!sessionIsAdmin(s) && !isTempTable(s, requestedTable) &&
        !g_engine.hasColumnPermission(
            s.currentDB, requestedTable, effectiveSessionRole(s),
            StorageEngine::TablePrivilege::Update, columns)) {
        std::cout << "permission denied: UPDATE on restricted columns of table "
                  << requestedTable << std::endl;
        return true;
    }

    std::vector<std::string> conditions;
    if (!buildConditions(stmt.whereClause, conditions)) {
        fallback = true;
        return false;
    }
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> updatedRows;
    StorageEngine::UpdateResolver updateResolver;
    if (!expressionUpdates.empty()) {
        updateResolver = [&, targetQualifier](
                             const std::map<std::string, std::string>& oldValues,
                             std::map<std::string, std::string>& effectiveUpdates) {
            for (const auto& [column, expression] : expressionUpdates) {
                std::string value;
                if (!evaluateUpdateExpression(expression, oldValues, table,
                                              targetQualifier, s.currentDB, value)) {
                    return false;
                }
                effectiveUpdates[column] = std::move(value);
            }
            return true;
        };
    }
    const DBStatus status = g_engine.update(
        s.currentDB, resolvedTable, updates, conditions,
        stmt.returning.empty() ? nullptr : &updatedRows, updateResolver);
    if (status != DBStatus::OK) {
        std::cout << "Update failed" << std::endl;
        return true;
    }
    std::cout << "Update done" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, table, s.currentDB,
                              updatedRows, "UPDATE")) return true;
        printReturningRows(g_lastDmlResult);
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

bool executeDeleteUsingJoin(const DeleteStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (!stmt.usingClause || stmt.usingClause->type != FromItem::Type::Join) {
        fallback = true;
        return false;
    }
    if (!checkDatabase(s)) return true;

    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }
    if (!checkTablePrivilege(s, requestedTable,
                             StorageEngine::TablePrivilege::Delete)) return true;

    const TableSchema targetSchema = g_engine.getTableSchema(s.currentDB, resolvedTable);
    const std::string targetQualifier = unqualifiedRelationName(requestedTable);
    std::vector<StructuredSourceRelation> sources;
    std::vector<const Expr*> joinPredicates;
    const StructuredRelationResult relationResult = collectStructuredRelations(
        stmt.usingClause.get(), s, targetQualifier, sources, joinPredicates);
    if (relationResult == StructuredRelationResult::Unsupported) {
        fallback = true;
        return false;
    }
    if (relationResult == StructuredRelationResult::Error) return true;

    for (const Expr* predicate : joinPredicates) {
        ExprEvaluator evaluator;
        if (!validateStructuredExpression(predicate, targetSchema, targetQualifier,
                                           sources, evaluator)) {
            fallback = true;
            return false;
        }
    }
    if (stmt.whereClause) {
        ExprEvaluator evaluator;
        if (!validateStructuredExpression(stmt.whereClause.get(), targetSchema,
                                           targetQualifier, sources, evaluator)) {
            fallback = true;
            return false;
        }
    }

    std::vector<std::map<std::string, std::string>> targetRows;
    if (!collectTableRows(s.currentDB, resolvedTable, targetSchema, "DELETE", targetRows)) {
        fallback = true;
        return false;
    }
    std::set<std::string> matchedTargets;
    bool evaluationFailed = false;
    for (const auto& targetValues : targetRows) {
        findStructuredMatch(
            targetValues, targetSchema, targetQualifier, sources, joinPredicates,
            stmt.whereClause.get(), s.currentDB,
            [&](const RowContext&, const std::vector<const std::map<std::string, std::string>*>&) {
                matchedTargets.insert(rowValueKey(targetValues));
                return true;
            }, evaluationFailed);
        if (evaluationFailed) break;
    }
    if (evaluationFailed) {
        fallback = true;
        return false;
    }

    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> deletedRows;
    const StorageEngine::DeleteMatcher deleteMatcher = [matchedTargets](
        const std::map<std::string, std::string>& oldValues) {
        return matchedTargets.find(rowValueKey(oldValues)) != matchedTargets.end();
    };
    const DBStatus status = g_engine.remove(
        s.currentDB, resolvedTable, {},
        stmt.returning.empty() ? nullptr : &deletedRows, deleteMatcher);
    if (status != DBStatus::OK) {
        std::cout << "DELETE USING failed" << std::endl;
        return true;
    }
    std::cout << "Delete done" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, targetSchema, s.currentDB,
                              deletedRows, "DELETE")) return true;
        printReturningRows(g_lastDmlResult);
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

bool executeDeleteUsing(const DeleteStmt& stmt, Session& s, bool& fallback) {
    if (stmt.usingClause && stmt.usingClause->type == FromItem::Type::Join) {
        return executeDeleteUsingJoin(stmt, s, fallback);
    }
    fallback = false;
    if (!stmt.usingClause || stmt.usingClause->type != FromItem::Type::Table) {
        fallback = true;
        return false;
    }
    if (!checkDatabase(s)) return true;

    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }

    const std::string requestedSource = identifier(stmt.usingClause->tableName);
    const std::string resolvedSource = resolveTable(s, requestedSource);
    if (g_engine.viewExists(s.currentDB, requestedSource) ||
        g_engine.isMaterializedView(s.currentDB, requestedSource) ||
        !g_engine.tableExists(s.currentDB, resolvedSource)) {
        fallback = true;
        return false;
    }
    if (!checkTablePrivilege(s, requestedTable,
                             StorageEngine::TablePrivilege::Delete) ||
        !checkTablePrivilege(s, requestedSource,
                             StorageEngine::TablePrivilege::Select)) {
        return true;
    }

    const TableSchema targetSchema = g_engine.getTableSchema(s.currentDB, resolvedTable);
    const TableSchema sourceSchema = g_engine.getTableSchema(s.currentDB, resolvedSource);
    const std::string targetQualifier = unqualifiedRelationName(requestedTable);
    const std::string sourceQualifier = stmt.usingClause->alias.empty()
        ? unqualifiedRelationName(requestedSource)
        : identifier(stmt.usingClause->alias);

    if (stmt.whereClause) {
        ExprEvaluator evaluator;
        if (!validateUpdateFromExpression(stmt.whereClause.get(), targetSchema,
                                           targetQualifier, sourceSchema,
                                           sourceQualifier, evaluator)) {
            fallback = true;
            return false;
        }
    }

    std::vector<std::map<std::string, std::string>> sourceRows;
    std::vector<std::map<std::string, std::string>> targetRows;
    if (!collectTableRows(s.currentDB, resolvedSource, sourceSchema, "SELECT", sourceRows) ||
        !collectTableRows(s.currentDB, resolvedTable, targetSchema, "DELETE", targetRows)) {
        fallback = true;
        return false;
    }

    std::set<std::string> matchedTargets;
    bool evaluationFailed = false;
    for (const auto& targetValues : targetRows) {
        for (const auto& sourceValues : sourceRows) {
            if (stmt.whereClause) {
                ExprEvaluator evaluator;
                evaluator.setCurrentDB(s.currentDB);
                const RowContext context = updateFromContext(
                    targetValues, targetSchema, targetQualifier,
                    sourceValues, sourceSchema, sourceQualifier);
                const ExprValue predicate = evaluator.eval(stmt.whereClause.get(), context);
                if (predicate.isUnknown() || predicate.typeName == "unknown") {
                    evaluationFailed = true;
                    break;
                }
                // DELETE ... USING follows normal SQL three-valued logic:
                // NULL predicates do not select a target row.
                if (predicate.isNull) continue;
                if (predicate.typeName != "boolean") {
                    evaluationFailed = true;
                    break;
                }
                if (!predicate.asBool()) continue;
            }
            matchedTargets.insert(rowValueKey(targetValues));
            break; // one matching source row is enough for DELETE.
        }
        if (evaluationFailed) break;
    }
    if (evaluationFailed) {
        fallback = true;
        return false;
    }

    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> deletedRows;
    const StorageEngine::DeleteMatcher deleteMatcher = [matchedTargets](
        const std::map<std::string, std::string>& oldValues) {
        return matchedTargets.find(rowValueKey(oldValues)) != matchedTargets.end();
    };
    const DBStatus status = g_engine.remove(
        s.currentDB, resolvedTable, {},
        stmt.returning.empty() ? nullptr : &deletedRows, deleteMatcher);
    if (status != DBStatus::OK) {
        std::cout << "DELETE USING failed" << std::endl;
        return true;
    }
    std::cout << "Delete done" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, targetSchema, s.currentDB,
                              deletedRows, "DELETE")) return true;
        printReturningRows(g_lastDmlResult);
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

bool executeDelete(const DeleteStmt& stmt, Session& s, bool& fallback) {
    if (stmt.usingClause) return executeDeleteUsing(stmt, s, fallback);
    fallback = false;
    if (!checkDatabase(s)) return true;
    const std::string requestedTable = identifier(stmt.tableName);
    const std::string resolvedTable = resolveTable(s, requestedTable);
    if (g_engine.viewExists(s.currentDB, requestedTable) ||
        g_engine.isMaterializedView(s.currentDB, requestedTable)) {
        fallback = true;
        return false;
    }
    if (!g_engine.tableExists(s.currentDB, resolvedTable)) {
        std::cout << "Table " << requestedTable << " not exist" << std::endl;
        return true;
    }
    if (!checkTablePrivilege(s, requestedTable,
                             StorageEngine::TablePrivilege::Delete)) return true;

    std::vector<std::string> conditions;
    if (!buildConditions(stmt.whereClause, conditions)) {
        fallback = true;
        return false;
    }
    const TableSchema table = g_engine.getTableSchema(s.currentDB, resolvedTable);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> deletedRows;
    const DBStatus status = g_engine.remove(
        s.currentDB, resolvedTable, conditions,
        stmt.returning.empty() ? nullptr : &deletedRows);
    if (status != DBStatus::OK) {
        std::cout << "Delete failed" << std::endl;
        return true;
    }
    std::cout << "Delete done" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(returningProjections, table, s.currentDB,
                              deletedRows, "DELETE")) return true;
        printReturningRows(g_lastDmlResult);
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

} // namespace

bool tryDmlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled, const std::string& rawSql) {
    handled = false;
    if (parsedCmd != SqlCommand::Insert && parsedCmd != SqlCommand::Update &&
        parsedCmd != SqlCommand::Delete) return false;
    clearLastDmlResult();

    SQLParser parser;
    // sql is normalized by the legacy entry point and may have changed the
    // case of string literals.  Parse the original text whenever available so
    // AST execution preserves user data exactly.
    const ParseResult parsed = parser.parse(rawSql.empty() ? sql : rawSql);
    if (!parsed.success || !parsed.stmt) {
        handled = true;
        const char* statementName = parsedCmd == SqlCommand::Update ? "UPDATE" :
                                    parsedCmd == SqlCommand::Delete ? "DELETE" : "INSERT";
        std::cout << "SQL syntax error: "
                  << (parsed.error.empty() ? std::string("invalid ") + statementName + " statement"
                                            : parsed.error)
                  << std::endl;
        return true;
    }

    bool fallback = false;
    bool error = false;
    if (parsedCmd == SqlCommand::Insert) {
        const auto* stmt = dynamic_cast<const InsertStmt*>(parsed.stmt.get());
        if (!stmt || !supportsInsert(*stmt)) return false;
        error = executeInsert(*stmt, s, fallback);
    } else if (parsedCmd == SqlCommand::Update) {
        const auto* stmt = dynamic_cast<const UpdateStmt*>(parsed.stmt.get());
        if (!stmt) return false;
        error = executeUpdate(*stmt, s, fallback);
    } else {
        const auto* stmt = dynamic_cast<const DeleteStmt*>(parsed.stmt.get());
        if (!stmt || stmt->only) return false;
        error = executeDelete(*stmt, s, fallback);
    }
    if (fallback) {
        handled = false;
        return false;
    }
    handled = true;
    return error;
}

} // namespace dbms
