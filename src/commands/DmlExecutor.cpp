// ============================================================================
// DML AST Executor — Phase 4 Wave 0.4
// ============================================================================

#include "commands/DmlExecutor.h"

#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

#include <algorithm>
#include <cctype>
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
    if (s.permission == 1 || isTempTable(s, table)) return true;
    if (g_engine.hasPermission(s.currentDB, table, s.username,
                               StorageEngine::TablePrivilege::Insert)) {
        return true;
    }
    for (const auto& permission : g_engine.getUserPermissions(
             s.currentDB, table, s.username)) {
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
    // Only target-less DO NOTHING and a narrow single-column DO UPDATE shape
    // are admitted here; all other conflict inference remains legacy-owned.
    if (action.empty()) return true;
    if (action == "do nothing") {
        return stmt.conflictTarget.empty() && !stmt.conflictWhere;
    }
    return action == "do update" && stmt.conflictTarget.size() == 1 &&
           !stmt.conflictWhere && !stmt.defaultValues &&
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
    if (s.permission == 1 || isTempTable(s, table)) return true;
    if (!g_engine.hasColumnPermission(s.currentDB, table, s.username,
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

bool buildConflictUpdatePlan(const InsertStmt& stmt, const TableSchema& table,
                             const std::string& currentDB,
                             std::string& targetColumn,
                             std::map<std::string, std::string>& updates) {
    if (lower(stmt.conflictAction) != "do update" ||
        stmt.conflictTarget.size() != 1 || stmt.conflictWhere ||
        stmt.conflictUpdateSet.empty()) {
        return false;
    }

    targetColumn = identifier(stmt.conflictTarget.front());
    size_t targetIndex = table.len;
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName == targetColumn) {
            targetIndex = i;
            break;
        }
    }
    if (targetIndex >= table.len ||
        (!table.cols[targetIndex].isPrimaryKey && !table.cols[targetIndex].isUnique)) {
        return false;
    }

    std::set<std::string> seen;
    for (const auto& [rawColumn, expr] : stmt.conflictUpdateSet) {
        const std::string column = identifier(rawColumn);
        if (column.empty() || !seen.insert(column).second || isDefaultValue(expr)) {
            return false;
        }
        bool found = false;
        for (const auto& tableColumn : table.cols) {
            if (tableColumn.dataName == column) {
                found = true;
                break;
            }
        }
        if (!found) return false;

        std::string value;
        if (!evaluateValue(expr, currentDB, value)) return false;
        updates[column] = std::move(value);
    }
    return !updates.empty();
}

bool checkTablePrivilege(Session& s, const std::string& table,
                         StorageEngine::TablePrivilege privilege) {
    if (s.permission == 1 || isTempTable(s, table)) return true;
    if (g_engine.hasPermission(s.currentDB, table, s.username, privilege)) {
        return true;
    }
    for (const auto& permission : g_engine.getUserPermissions(
             s.currentDB, table, s.username)) {
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
        // forEachRow is intentionally a raw heap scan.  Do not bypass RLS
        // policies until the scan API can apply the SELECT policy itself.
        if (sourceTable.rowLevelSecurity || sourceTable.forceRowLevelSecurity) {
            return InsertSelectBuildResult::Unsupported;
        }
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
    if (hasSource && s.permission != 1 && !isTempTable(s, sourceName) &&
        !g_engine.hasColumnPermission(s.currentDB, sourceName, s.username,
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
        g_engine.forEachRow(s.currentDB, resolvedSource,
                            [&](uint32_t, uint16_t, const char* data, size_t len) {
                                if (!evaluationFailed) appendRow(std::string(data, len));
                            });
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
    std::string conflictTarget;
    std::map<std::string, std::string> conflictUpdates;

    if (conflictUpdate) {
        if (!checkTablePrivilege(s, requestedTable,
                                 StorageEngine::TablePrivilege::Update)) {
            return true;
        }
        if (!buildConflictUpdatePlan(stmt, table, s.currentDB, conflictTarget,
                                     conflictUpdates)) {
            fallback = true;
            return false;
        }
        const std::vector<std::string> updateColumns = [&]() {
            std::vector<std::string> result;
            result.reserve(conflictUpdates.size());
            for (const auto& [column, value] : conflictUpdates) {
                (void)value;
                result.push_back(column);
            }
            return result;
        }();
        if (s.permission != 1 && !isTempTable(s, requestedTable) &&
            !g_engine.hasColumnPermission(
                s.currentDB, requestedTable, s.username,
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

    if (conflictUpdate) {
        // The narrow plan needs the inferred target value before the insert
        // reports a duplicate. DEFAULT/generated target values require a
        // storage-level conflict key API and therefore remain legacy-owned.
        for (const auto& values : pendingRows) {
            const auto it = values.find(conflictTarget);
            if (it == values.end() || it->second.empty()) {
                fallback = true;
                return false;
            }
        }
    }

    int inserted = 0;
    for (const auto& values : pendingRows) {
        const DBStatus status = g_engine.insert(
            s.currentDB, resolvedTable, values,
            stmt.returning.empty() ? nullptr : &insertedRows);
        if (status == DBStatus::DUPLICATE_KEY) {
            if (ignoreDuplicate) continue;
            if (conflictUpdate) {
                const auto targetValue = values.find(conflictTarget);
                if (targetValue == values.end() || targetValue->second.empty()) {
                    std::cout << "ON CONFLICT target value is unavailable" << std::endl;
                    return true;
                }
                std::vector<std::string> conditions = {
                    "=" + conflictTarget + " " + targetValue->second};
                std::vector<std::map<std::string, std::string>> updatedRows;
                const DBStatus updateStatus = g_engine.update(
                    s.currentDB, resolvedTable, conflictUpdates, conditions,
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

bool executeUpdate(const UpdateStmt& stmt, Session& s, bool& fallback) {
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

    std::vector<std::string> columns;
    std::map<std::string, std::string> updates;
    for (const auto& [rawColumn, expr] : stmt.setClauses) {
        const std::string column = identifier(rawColumn);
        columns.push_back(column);
        if (isDefaultValue(expr)) {
            fallback = true;
            return false;
        }
        std::string value;
        if (!evaluateValue(expr, s.currentDB, value)) {
            fallback = true;
            return false;
        }
        updates[column] = std::move(value);
    }
    if (updates.empty()) {
        std::cout << "SQL syntax error: empty UPDATE SET clause" << std::endl;
        return true;
    }
    if (s.permission != 1 && !isTempTable(s, requestedTable) &&
        !g_engine.hasColumnPermission(
            s.currentDB, requestedTable, s.username,
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
    const TableSchema table = g_engine.getTableSchema(s.currentDB, resolvedTable);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table, returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> updatedRows;
    const DBStatus status = g_engine.update(
        s.currentDB, resolvedTable, updates, conditions,
        stmt.returning.empty() ? nullptr : &updatedRows);
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

bool executeDelete(const DeleteStmt& stmt, Session& s, bool& fallback) {
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
        if (!stmt || stmt->fromClause) return false;
        error = executeUpdate(*stmt, s, fallback);
    } else {
        const auto* stmt = dynamic_cast<const DeleteStmt*>(parsed.stmt.get());
        if (!stmt || stmt->usingClause || stmt->only) return false;
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
