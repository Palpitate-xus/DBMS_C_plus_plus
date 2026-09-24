// ============================================================================
// DML AST Executor — Phase 4 Wave 0.4
// ============================================================================

#include "commands/DmlExecutor.h"

#include "access/BPTree.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "permissions.h"

#include <algorithm>
#include <atomic>
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

void publishLastDmlResult(DmlResult result) {
    g_lastDmlResult = std::move(result);
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

std::string canonicalSetType(std::string type) {
    const size_t modifier = type.find('(');
    if (modifier != std::string::npos) type.resize(modifier);
    while (!type.empty() && std::isspace(static_cast<unsigned char>(type.back())))
        type.pop_back();
    size_t start = 0;
    while (start < type.size() &&
           std::isspace(static_cast<unsigned char>(type[start]))) ++start;
    if (start != 0) type.erase(0, start);
    const std::string normalized =
        TypeRegistry::instance().normalizeTypeName(type);
    if (!normalized.empty()) return normalized;
    std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return type;
}

int setNumericRank(const std::string& type) {
    if (type == "smallint") return 0;
    if (type == "integer") return 1;
    if (type == "bigint") return 2;
    if (type == "numeric") return 3;
    if (type == "real") return 4;
    if (type == "double precision") return 5;
    return -1;
}

bool resolveSetType(const std::string& leftType,
                    const std::string& rightType,
                    std::string& result) {
    const std::string left = canonicalSetType(leftType);
    const std::string right = canonicalSetType(rightType);
    if (left.empty() || right.empty()) return false;
    if (left == "unknown") {
        result = right == "unknown" ? "text" : right;
        return true;
    }
    if (right == "unknown" || left == right) {
        result = left;
        return true;
    }

    if (left == "name" || right == "name") {
        const std::string& other = left == "name" ? right : left;
        const auto* otherEntry = TypeRegistry::instance().findType(other);
        if (other == "name" ||
            (otherEntry && otherEntry->category == TypeCategory::String)) {
            result = "text";
            return true;
        }
        return false;
    }

    const auto* leftEntry = TypeRegistry::instance().findType(left);
    const auto* rightEntry = TypeRegistry::instance().findType(right);
    if (!leftEntry || !rightEntry ||
        leftEntry->category != rightEntry->category) return false;

    if (leftEntry->category == TypeCategory::Numeric) {
        const int leftRank = setNumericRank(left);
        const int rightRank = setNumericRank(right);
        if (leftRank < 0 || rightRank < 0) return false;
        result = leftRank >= rightRank ? left : right;
        return true;
    }
    if (leftEntry->category == TypeCategory::String) {
        if (left == "text" || right == "text" || left == "name" || right == "name")
            result = "text";
        else if (left == "character varying" || right == "character varying")
            result = "character varying";
        else
            result = left;
        return true;
    }
    if (leftEntry->category == TypeCategory::DateTime) {
        const bool leftTimestamp = left == "date" || left == "timestamp" ||
                                   left == "timestamptz";
        const bool rightTimestamp = right == "date" || right == "timestamp" ||
                                    right == "timestamptz";
        if (leftTimestamp && rightTimestamp) {
            if (left == "timestamptz" || right == "timestamptz")
                result = "timestamptz";
            else if (left == "timestamp" || right == "timestamp")
                result = "timestamp";
            else
                result = "date";
            return true;
        }
        const bool leftTime = left == "time" || left == "timetz";
        const bool rightTime = right == "time" || right == "timetz";
        if (leftTime && rightTime) {
            result = left == "timetz" || right == "timetz" ? "timetz" : "time";
            return true;
        }
    }
    return false;
}

bool validateStructuredSetInput(const DmlResult& input,
                                StructuredSetError& error) {
    const size_t width = input.columns.size();
    if (!input.available || input.metadataOnly || width == 0 ||
        input.columnTypes.size() != width ||
        input.rows.size() != input.nulls.size()) {
        error = {"0A000", "set operation operand did not publish a complete structured result"};
        return false;
    }
    for (size_t row = 0; row < input.rows.size(); ++row) {
        if (input.rows[row].size() != width || input.nulls[row].size() != width) {
            error = {"0A000", "set operation operand has inconsistent row metadata"};
            return false;
        }
    }
    return true;
}

std::string quoteSetLiteral(const std::string& value) {
    std::string quoted = "'";
    quoted.reserve(value.size() + 2);
    for (char c : value) {
        if (c == '\'') quoted += "''";
        else quoted += c;
    }
    quoted += '\'';
    return quoted;
}

bool coerceSetRows(const DmlResult& input,
                   const std::vector<std::string>& resultTypes,
                   const std::string& database, const std::string& username,
                   std::vector<std::vector<std::string>>& rows,
                   std::vector<std::vector<bool>>& nulls,
                   StructuredSetError& error) {
    rows = input.rows;
    nulls = input.nulls;
    for (size_t row = 0; row < rows.size(); ++row) {
        for (size_t column = 0; column < resultTypes.size(); ++column) {
            if (nulls[row][column]) {
                rows[row][column].clear();
                continue;
            }
            const std::string sourceType = canonicalSetType(input.columnTypes[column]);
            const std::string& targetType = resultTypes[column];
            if (sourceType == targetType ||
                (TypeRegistry::instance().findType(sourceType) &&
                 TypeRegistry::instance().findType(targetType) &&
                 TypeRegistry::instance().findType(sourceType)->category == TypeCategory::String &&
                 TypeRegistry::instance().findType(targetType)->category == TypeCategory::String)) {
                continue;
            }
            const ExprEvalResult evaluated = ExprHelper::evalString(
                "CAST(" + quoteSetLiteral(rows[row][column]) + " AS " +
                    targetType + ")",
                {}, {}, database, username);
            if (!evaluated.ok || evaluated.isNull ||
                canonicalSetType(evaluated.typeName) != targetType) {
                error = {"0A000", "set operation type coercion is not supported"};
                return false;
            }
            rows[row][column] = evaluated.value;
        }
    }
    return true;
}

std::string structuredSetRowKey(const std::vector<std::string>& row,
                                const std::vector<bool>& nulls) {
    std::string key;
    for (size_t column = 0; column < row.size(); ++column) {
        if (nulls[column]) {
            key += "N;";
        } else {
            key += "V" + std::to_string(row[column].size()) + ":" +
                   row[column] + ";";
        }
    }
    return key;
}

} // namespace

bool combineStructuredSetResults(
    const DmlResult& left, const DmlResult& right,
    StructuredSetOperation operation, bool all,
    const std::string& database, const std::string& username,
    DmlResult& output, StructuredSetError& error) {
    output = {};
    error = {};
    if (!validateStructuredSetInput(left, error) ||
        !validateStructuredSetInput(right, error)) return false;
    if (left.columns.size() != right.columns.size()) {
        error = {"42601", "each set operation query must have the same number of columns"};
        return false;
    }

    std::vector<std::string> resultTypes(left.columns.size());
    for (size_t column = 0; column < resultTypes.size(); ++column) {
        if (!resolveSetType(left.columnTypes[column], right.columnTypes[column],
                            resultTypes[column])) {
            error = {"42804", "set operation types " +
                       canonicalSetType(left.columnTypes[column]) + " and " +
                       canonicalSetType(right.columnTypes[column]) +
                       " cannot be matched"};
            return false;
        }
    }

    std::vector<std::vector<std::string>> leftRows;
    std::vector<std::vector<bool>> leftNulls;
    std::vector<std::vector<std::string>> rightRows;
    std::vector<std::vector<bool>> rightNulls;
    if (!coerceSetRows(left, resultTypes, database, username,
                       leftRows, leftNulls, error) ||
        !coerceSetRows(right, resultTypes, database, username,
                       rightRows, rightNulls, error)) return false;

    output.available = true;
    output.columns = left.columns;
    output.columnTypes = std::move(resultTypes);
    auto append = [&](std::vector<std::string> row,
                      std::vector<bool> rowNulls) {
        output.rows.push_back(std::move(row));
        output.nulls.push_back(std::move(rowNulls));
    };

    if (operation == StructuredSetOperation::Union) {
        if (all) {
            for (size_t i = 0; i < leftRows.size(); ++i)
                append(std::move(leftRows[i]), std::move(leftNulls[i]));
            for (size_t i = 0; i < rightRows.size(); ++i)
                append(std::move(rightRows[i]), std::move(rightNulls[i]));
        } else {
            std::set<std::string> seen;
            auto appendDistinct = [&](std::vector<std::vector<std::string>>& rows,
                                      std::vector<std::vector<bool>>& nulls) {
                for (size_t i = 0; i < rows.size(); ++i) {
                    if (seen.insert(structuredSetRowKey(rows[i], nulls[i])).second)
                        append(std::move(rows[i]), std::move(nulls[i]));
                }
            };
            appendDistinct(leftRows, leftNulls);
            appendDistinct(rightRows, rightNulls);
        }
    } else {
        std::map<std::string, size_t> rightCounts;
        for (size_t i = 0; i < rightRows.size(); ++i)
            ++rightCounts[structuredSetRowKey(rightRows[i], rightNulls[i])];
        std::set<std::string> emitted;
        for (size_t i = 0; i < leftRows.size(); ++i) {
            const std::string key = structuredSetRowKey(leftRows[i], leftNulls[i]);
            auto found = rightCounts.find(key);
            const size_t available = found == rightCounts.end() ? 0 : found->second;
            if (operation == StructuredSetOperation::Intersect) {
                if (available == 0) continue;
                if (all) {
                    append(std::move(leftRows[i]), std::move(leftNulls[i]));
                    --found->second;
                } else if (emitted.insert(key).second) {
                    append(std::move(leftRows[i]), std::move(leftNulls[i]));
                }
            } else if (available > 0) {
                if (all) --found->second;
            } else if (all || emitted.insert(key).second) {
                append(std::move(leftRows[i]), std::move(leftNulls[i]));
            }
        }
    }
    output.commandTag = "SELECT " + std::to_string(output.rows.size());
    return true;
}

namespace {

using SqlCell = StorageEngine::SqlCell;
using SqlRow = StorageEngine::SqlRow;

const std::string& structuredNullValue() {
    // Internal-only marker used by structured DML row snapshots. SQL text
    // cannot contain a zero byte, so this distinguishes NULL from both the
    // empty string and the ordinary text value "NULL" without leaking into
    // storage APIs.
    static const std::string marker("\0DBMS_STRUCTURED_NULL", 21);
    return marker;
}

bool isStructuredNull(const std::string& value) {
    return value == structuredNullValue();
}

std::map<std::string, std::string> logicalValues(const SqlRow& row) {
    std::map<std::string, std::string> values;
    for (const auto& [column, value] : row) {
        values[column] = value.value_or(structuredNullValue());
    }
    return values;
}

class DmlStatementScope {
public:
    DmlStatementScope(StorageEngine& engine, const std::string& database)
        : engine_(engine) {
        if (!engine_.inTransaction()) {
            ready_ = engine_.beginTransaction(database) == DBStatus::OK;
            ownsTransaction_ = ready_;
            return;
        }

        static std::atomic<uint64_t> sequence{0};
        savepointName_ = "__dbms_dml_statement_" +
            std::to_string(sequence.fetch_add(1));
        hasSavepoint_ = engine_.savepoint(savepointName_) == DBStatus::OK;
        // Snapshot-backed DDL cannot currently create a savepoint. DML may
        // still proceed, but a later error must roll back the transaction
        // rather than leave a partially applied statement.
        ready_ = true;
        rollbackWholeTransaction_ = !hasSavepoint_;
    }

    DmlStatementScope(const DmlStatementScope&) = delete;
    DmlStatementScope& operator=(const DmlStatementScope&) = delete;

    ~DmlStatementScope() {
        if (!completed_) rollback();
    }

    bool ready() const { return ready_; }

    bool finish() {
        if (!ready_ || completed_) return ready_ && completed_;
        DBStatus status = DBStatus::OK;
        if (ownsTransaction_) {
            status = engine_.commitTransaction();
        } else if (hasSavepoint_) {
            status = engine_.releaseSavepoint(savepointName_);
        }
        if (status == DBStatus::OK) {
            completed_ = true;
            return true;
        }
        rollback();
        completed_ = true;
        return false;
    }

private:
    void rollback() {
        if (!ready_ || !engine_.inTransaction()) return;
        if (ownsTransaction_ || rollbackWholeTransaction_) {
            (void)engine_.rollbackTransaction();
            return;
        }
        const DBStatus rollbackStatus =
            engine_.rollbackToSavepoint(savepointName_);
        if (rollbackStatus == DBStatus::OK) {
            (void)engine_.releaseSavepoint(savepointName_);
        } else if (engine_.inTransaction()) {
            (void)engine_.rollbackTransaction();
        }
    }

    StorageEngine& engine_;
    std::string savepointName_;
    bool ready_ = false;
    bool ownsTransaction_ = false;
    bool hasSavepoint_ = false;
    bool rollbackWholeTransaction_ = false;
    bool completed_ = false;
};

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string identifier(std::string value) {
    std::string decoded;
    decoded.reserve(value.size());
    bool quoted = false;
    for (size_t i = 0; i < value.size(); ++i) {
        const char c = value[i];
        if (c == '"') {
            if (quoted && i + 1 < value.size() && value[i + 1] == '"') {
                decoded += '"';
                ++i;
            } else {
                quoted = !quoted;
            }
            continue;
        }
        decoded += quoted
            ? c
            : static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return quoted ? lower(value) : decoded;
}

bool isTempTable(const Session& s, const std::string& name) {
    return s.tempTables.count(name) != 0 || s.transientTempTables.count(name) != 0;
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
    const size_t dot = name.find('.');
    if (dot != std::string::npos && dot > 0 && dot + 1 < name.size()) {
        const std::string schema = name.substr(0, dot);
        const std::string table = name.substr(dot + 1);
        if ((schema == "pg_temp" || schema.rfind("pg_temp_", 0) == 0) &&
            isTempTable(s, table)) {
            return tempTablePrefix(s, table);
        }
        if (g_engine.schemaExists(s.currentDB, schema)) {
            std::string physical = schema == "public"
                ? table : schema + "__" + table;
            if (const auto materialized = g_engine.resolveMaterializedView(
                    s.currentDB, schema, table)) {
                if (!materialized->populated) {
                    throw DbError(
                        "55000", "materialized view \"" + schema + "." +
                                     table + "\" has not been populated");
                }
                return materialized->backingTable;
            }
            const std::string legacyPublic = "public__" + table;
            if (schema == "public" &&
                !g_engine.tableExists(s.currentDB, physical) &&
                !g_engine.viewExists(s.currentDB, physical) &&
                !g_engine.isMaterializedView(s.currentDB, physical) &&
                (g_engine.tableExists(s.currentDB, legacyPublic) ||
                 g_engine.viewExists(s.currentDB, legacyPublic) ||
                 g_engine.isMaterializedView(s.currentDB, legacyPublic))) {
                physical = legacyPublic;
            }
            if (g_engine.isMaterializedView(s.currentDB, physical)) {
                return StorageEngine::materializedViewPrefix(physical);
            }
            return physical;
        }
        return name;
    }
    if (isTempTable(s, name)) return tempTablePrefix(s, name);

    std::vector<std::string> entries;
    std::string canonical;
    if (!dbms::parseSessionSearchPath(s.searchPath, entries, canonical)) {
        entries = {"public"};
    }
    std::string firstCandidate;
    for (const auto& rawSchema : entries) {
        const std::string schema = dbms::expandSessionSearchPathEntry(
            rawSchema, s.username);
        if (schema == "pg_catalog" || schema == "pg_temp" ||
            schema.rfind("pg_temp_", 0) == 0 ||
            !g_engine.schemaExists(s.currentDB, schema)) {
            continue;
        }
        std::string physical = schema == "public"
            ? name : schema + "__" + name;
        if (const auto materialized = g_engine.resolveMaterializedView(
                s.currentDB, schema, name)) {
            if (!materialized->populated) {
                throw DbError(
                    "55000", "materialized view \"" + schema + "." +
                                 name + "\" has not been populated");
            }
            return materialized->backingTable;
        }
        const std::string legacyPublic = "public__" + name;
        if (schema == "public" &&
            !g_engine.tableExists(s.currentDB, physical) &&
            !g_engine.viewExists(s.currentDB, physical) &&
            !g_engine.isMaterializedView(s.currentDB, physical) &&
            (g_engine.tableExists(s.currentDB, legacyPublic) ||
             g_engine.viewExists(s.currentDB, legacyPublic) ||
             g_engine.isMaterializedView(s.currentDB, legacyPublic))) {
            physical = legacyPublic;
        }
        if (firstCandidate.empty()) firstCandidate = physical;
        if (g_engine.isMaterializedView(s.currentDB, physical)) {
            return StorageEngine::materializedViewPrefix(physical);
        }
        if (g_engine.tableExists(s.currentDB, physical) ||
            g_engine.viewExists(s.currentDB, physical)) {
            return physical;
        }
    }
    return firstCandidate.empty() ? name : firstCandidate;
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
    const bool hasArbiter = !stmt.conflictTarget.empty() ||
        !stmt.conflictConstraint.empty();
    // DO NOTHING may be target-less or explicitly constrained; DO UPDATE is
    // admitted only for a narrow VALUES shape and is constraint-backed by
    // buildConflictUpdatePlan().
    if (action.empty()) return true;
    if (action == "do nothing") {
        if (!hasArbiter) return !stmt.conflictWhere;
        return !stmt.conflictWhere && !stmt.defaultValues &&
               stmt.selectSource == nullptr && !stmt.values.empty();
    }
    return action == "do update" && hasArbiter &&
           !stmt.defaultValues &&
           stmt.selectSource == nullptr && !stmt.values.empty();
}

bool supportsInsert(const InsertStmt& stmt) {
    // The bridge owns ordinary VALUES inserts, simple single-table SELECT
    // sources, column-projection RETURNING, and narrow conflict actions.
    // Every feature outside this contract remains on the established
    // implementation until its AST semantics are migrated.
    const std::string identityOverride = lower(stmt.override_);
    return !stmt.tableName.empty() &&
           supportsConflict(stmt) &&
           (identityOverride.empty() || identityOverride == "system" ||
            identityOverride == "user") &&
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
                   SqlCell& value);
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
                           const std::string& currentDB,
                           const std::string& tableName,
                           std::vector<std::string>& targetColumns,
                           bool* namedConstraintFound = nullptr) {
    if (namedConstraintFound) *namedConstraintFound = false;
    targetColumns.clear();
    if (!stmt.conflictConstraint.empty()) {
        const std::string requested = identifier(stmt.conflictConstraint);
        if (requested.empty()) return false;

        std::vector<size_t> primaryKey = table.pkColIndices;
        if (primaryKey.empty()) {
            for (size_t i = 0; i < table.len; ++i) {
                if (table.cols[i].isPrimaryKey) primaryKey.push_back(i);
            }
        }
        std::string primaryName = table.tablename + "_pkey";
        const auto storedPrimaryName = table.storageParams.find(
            PRIMARY_KEY_CONSTRAINT_NAME_PARAM);
        if (storedPrimaryName != table.storageParams.end() &&
            !storedPrimaryName->second.empty()) {
            primaryName = storedPrimaryName->second;
        }
        if (!primaryKey.empty() && identifier(primaryName) == requested) {
            if (namedConstraintFound) *namedConstraintFound = true;
            for (const size_t index : primaryKey) {
                if (index >= table.len) return false;
                targetColumns.push_back(table.cols[index].dataName);
            }
            return true;
        }

        for (size_t constraintIndex = 0;
             constraintIndex < table.uniqueConstraints.size();
             ++constraintIndex) {
            const auto& columns = table.uniqueConstraints[constraintIndex];
            std::string name;
            if (constraintIndex < table.uniqueConstraintNames.size()) {
                name = table.uniqueConstraintNames[constraintIndex];
            }
            if (name.empty()) {
                name = table.tablename;
                for (const size_t index : columns) {
                    if (index >= table.len) {
                        name.clear();
                        break;
                    }
                    name += "_" + table.cols[index].dataName;
                }
                if (!name.empty()) name += "_key";
            }
            if (!name.empty() && identifier(name) == requested) {
                if (namedConstraintFound) *namedConstraintFound = true;
                for (const size_t index : columns) {
                    if (index >= table.len) return false;
                    targetColumns.push_back(table.cols[index].dataName);
                }
                return !targetColumns.empty();
            }
        }

        for (size_t columnIndex = 0; columnIndex < table.len; ++columnIndex) {
            if (!table.cols[columnIndex].isUnique) continue;
            const std::string defaultName = table.tablename + "_" +
                table.cols[columnIndex].dataName + "_key";
            if (identifier(defaultName) == requested) {
                if (namedConstraintFound) *namedConstraintFound = true;
                targetColumns.push_back(table.cols[columnIndex].dataName);
                return true;
            }
        }

        const auto matches = [&](const std::string& name) {
            return !name.empty() && identifier(name) == requested;
        };
        for (size_t i = 0; i < table.len; ++i) {
            if (matches(table.cols[i].checkConstraintName)) {
                if (namedConstraintFound) *namedConstraintFound = true;
                return false;
            }
        }
        for (const auto& check : table.additionalCheckConstraints) {
            if (matches(check.name)) {
                if (namedConstraintFound) *namedConstraintFound = true;
                return false;
            }
        }
        for (size_t i = 0; i < table.fkLen; ++i) {
            if (matches(table.fks[i].name)) {
                if (namedConstraintFound) *namedConstraintFound = true;
                return false;
            }
        }
        for (const auto& exclusion :
             g_engine.getExclusionConstraints(currentDB, tableName)) {
            if (matches(exclusion.name)) {
                if (namedConstraintFound) *namedConstraintFound = true;
                return false;
            }
        }
        return false;
    }

    if (stmt.conflictTarget.empty()) return false;

    std::set<std::string> seen;
    std::vector<size_t> targetIndices;
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
        if (column.isUnique || column.isPrimaryKey) return true;
    }

    // Standalone CREATE UNIQUE INDEX definitions live in the index sidecar,
    // not TableSchema.  They participate in PostgreSQL-style conflict target
    // inference just like table-level UNIQUE constraints.
    for (const auto& index :
         g_engine.getIndexMetadata(currentDB, tableName)) {
        if (!index.isUnique || index.isExpression ||
            !index.whereCondition.empty()) {
            continue;
        }
        for (size_t columnIndex = 0; columnIndex < table.len;
             ++columnIndex) {
            if (table.cols[columnIndex].dataName == index.name &&
                targetIndices.size() == 1 &&
                targetIndices.front() == columnIndex) {
                return true;
            }
        }
    }
    for (const auto& index :
         g_engine.getCompositeIndexes(currentDB, tableName)) {
        if (!index.isUnique || !index.whereCondition.empty()) continue;
        std::vector<size_t> indexColumns;
        for (const auto& columnName : index.columns) {
            size_t columnIndex = table.len;
            for (size_t candidate = 0; candidate < table.len; ++candidate) {
                if (table.cols[candidate].dataName == columnName) {
                    columnIndex = candidate;
                    break;
                }
            }
            if (columnIndex >= table.len) {
                indexColumns.clear();
                break;
            }
            indexColumns.push_back(columnIndex);
        }
        if (!indexColumns.empty() &&
            sameColumnSet(targetIndices, indexColumns)) {
            return true;
        }
    }
    return false;
}

bool buildConflictUpdatePlan(const InsertStmt& stmt, const TableSchema& table,
                             const std::string& currentDB,
                             const std::string& physicalTable,
                             std::vector<std::string>& targetColumns,
                             const std::string& targetTable,
                             SqlRow& updates,
                             std::map<std::string, const Expr*>& expressionUpdates,
                             std::map<std::string, std::set<std::string>>& expressionSources,
                             std::set<std::string>& whereExcludedColumns) {
    if (lower(stmt.conflictAction) != "do update" ||
        stmt.conflictUpdateSet.empty()) {
        return false;
    }

    if (!resolveConflictTarget(stmt, table, currentDB, physicalTable,
                               targetColumns)) {
        return false;
    }

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

        SqlCell value;
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
    enum class Source { Default, Old, New, Action };
    const Expr* expression = nullptr;
    std::string column;
    std::string name;
    std::string typeName;
    Source source = Source::Default;
};

struct ReturningBinding {
    std::string oldName;
    std::string newName;
    std::set<std::string> defaultQualifiers;
    bool allowMergeAction = false;
};

struct ReturningRowImage {
    SqlRow oldRow;
    SqlRow newRow;
    ReturningProjection::Source defaultSource =
        ReturningProjection::Source::New;
    std::string action;
};

std::string unqualifiedRelationName(const std::string& name);

ReturningBinding returningBinding(const ReturningOptions& options,
                                  const std::string& tableName,
                                  const std::string& tableAlias,
                                  bool allowMergeAction = false) {
    ReturningBinding binding;
    binding.oldName = identifier(
        options.oldAliased ? options.oldAlias : "old");
    binding.newName = identifier(
        options.newAliased ? options.newAlias : "new");
    if (tableAlias.empty()) {
        binding.defaultQualifiers.insert(
            unqualifiedRelationName(identifier(tableName)));
    } else {
        binding.defaultQualifiers.insert(identifier(tableAlias));
    }
    binding.allowMergeAction = allowMergeAction;
    return binding;
}

std::optional<ReturningProjection::Source> returningSourceForQualifier(
    const std::string& rawQualifier, const ReturningBinding& binding) {
    if (rawQualifier.empty()) return ReturningProjection::Source::Default;
    const std::string qualifier = identifier(rawQualifier);
    if (qualifier == binding.oldName) return ReturningProjection::Source::Old;
    if (qualifier == binding.newName) return ReturningProjection::Source::New;
    if (binding.defaultQualifiers.count(qualifier) != 0) {
        return ReturningProjection::Source::Default;
    }
    return std::nullopt;
}

bool returningBindingIsValid(const ReturningBinding& binding) {
    if (binding.oldName.empty() || binding.newName.empty() ||
        binding.oldName == binding.newName) {
        return false;
    }
    return binding.defaultQualifiers.count(binding.oldName) == 0 &&
           binding.defaultQualifiers.count(binding.newName) == 0;
}

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
                                 const ReturningBinding& binding,
                                 ExprEvaluator& evaluator) {
    if (!expr) return false;
    if (dynamic_cast<const LiteralExpr*>(expr)) return true;
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        if (!ref->schema.empty() ||
            !returningSourceForQualifier(ref->table, binding)) return false;
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
               supportsReturningExpression(
                   unary->operand.get(), table, binding, evaluator);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        static const std::set<std::string> supported = {
            "and", "or", "=", "<>", "!=", "<", ">", "<=", ">=",
            "+", "-", "*", "/", "%", "^", "||", "like", "not like",
            "ilike", "not ilike", "similar to", "not similar to", "in", "::"
        };
        return supported.count(lower(binary->op)) != 0 &&
               supportsReturningExpression(
                   binary->left.get(), table, binding, evaluator) &&
               supportsReturningExpression(
                   binary->right.get(), table, binding, evaluator);
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        if (lower(call->funcName) == "merge_action") {
            // Standalone merge_action() is materialized by the RETURNING
            // publisher. Nested use needs evaluator support and therefore
            // remains outside this bounded implementation.
            return false;
        }
        if (!evaluator.hasFunction(call->funcName) || call->distinct || call->filter ||
            call->hasOver || !call->namedArgs.empty() || !call->orderBy.empty()) {
            return false;
        }
        for (const auto& arg : call->args) {
            if (!supportsReturningExpression(arg.get(), table, binding,
                                             evaluator)) return false;
        }
        return true;
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return supportsReturningExpression(
            cast->operand.get(), table, binding, evaluator);
    }
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (caseExpr->switchExpr &&
            !supportsReturningExpression(
                caseExpr->switchExpr.get(), table, binding, evaluator)) {
            return false;
        }
        for (const auto& clause : caseExpr->whenClauses) {
            if (!supportsReturningExpression(
                    clause.first.get(), table, binding, evaluator) ||
                !supportsReturningExpression(
                    clause.second.get(), table, binding, evaluator)) {
                return false;
            }
        }
        return !caseExpr->elseExpr ||
               supportsReturningExpression(
                   caseExpr->elseExpr.get(), table, binding, evaluator);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (!supportsReturningExpression(
                    element.get(), table, binding, evaluator)) return false;
        }
        return true;
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (!supportsReturningExpression(
                    element.get(), table, binding, evaluator)) return false;
        }
        return true;
    }
    return false;
}

bool buildReturningProjections(const std::vector<SelectItem>& returning,
                               const TableSchema& table,
                               const ReturningBinding& binding,
                               std::vector<ReturningProjection>& projections) {
    projections.clear();
    if (!returningBindingIsValid(binding)) return false;
    ExprEvaluator evaluator;
    for (const auto& item : returning) {
        if (!item.expr) return false;
        const auto* star = item.expr ? dynamic_cast<const LiteralExpr*>(item.expr.get()) : nullptr;
        if (star && star->value == "*") {
            if (!item.alias.empty()) return false;
            for (size_t i = 0; i < table.len; ++i) {
                projections.push_back({nullptr, table.cols[i].dataName,
                                       table.cols[i].dataName,
                                       table.cols[i].dataType,
                                       ReturningProjection::Source::Default});
            }
            continue;
        }
        const auto* ref = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
        if (ref && identifier(ref->column) == "*") {
            if (!item.alias.empty()) return false;
            const auto source = returningSourceForQualifier(ref->table, binding);
            if (!ref->schema.empty() || !source) return false;
            for (size_t i = 0; i < table.len; ++i) {
                projections.push_back({nullptr, table.cols[i].dataName,
                                       table.cols[i].dataName,
                                       table.cols[i].dataType, *source});
            }
            continue;
        }
        const std::string name = item.alias.empty()
            ? (ref ? identifier(ref->column) : item.expr->toString())
            : identifier(item.alias);
        const auto* call = dynamic_cast<const FunctionCallExpr*>(item.expr.get());
        const bool mergeAction = call &&
            lower(call->funcName) == "merge_action";
        if (mergeAction) {
            if (!binding.allowMergeAction || !call->args.empty() ||
                call->distinct || call->filter || call->hasOver ||
                !call->namedArgs.empty() || !call->orderBy.empty()) {
                return false;
            }
        } else if (!supportsReturningExpression(
                       item.expr.get(), table, binding, evaluator)) {
            return false;
        }
        projections.push_back({mergeAction ? nullptr : item.expr.get(), {},
                               name.empty() ? "?column?" : name,
                               mergeAction ? "text" :
                                   inferReturningType(item.expr.get(), table),
                               mergeAction
                                   ? ReturningProjection::Source::Action
                                   : ReturningProjection::Source::Default});
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

void addReturningRowToContext(RowContext& context, const SqlRow& source,
                              const TableSchema& table,
                              const std::string& qualifier,
                              bool unqualified) {
    for (size_t i = 0; i < table.len; ++i) {
        const Column& column = table.cols[i];
        const auto it = source.find(column.dataName);
        const bool isNull = it == source.end() || !it->second;
        const ExprValue value(
            column.dataType,
            isNull ? std::string{} : *it->second, isNull);
        if (unqualified) context.set(column.dataName, value);
        if (!qualifier.empty()) {
            context.set(qualifier + "." + column.dataName, value);
        }
    }
}

const SqlRow& returningSourceRow(
    const ReturningRowImage& image, ReturningProjection::Source source) {
    if (source == ReturningProjection::Source::Old) return image.oldRow;
    if (source == ReturningProjection::Source::New) return image.newRow;
    return image.defaultSource == ReturningProjection::Source::Old
        ? image.oldRow : image.newRow;
}

RowContext returningContext(const ReturningRowImage& image,
                            const TableSchema& table,
                            const ReturningBinding& binding) {
    RowContext context;
    const SqlRow& defaultRow = returningSourceRow(
        image, ReturningProjection::Source::Default);
    addReturningRowToContext(context, defaultRow, table, "", true);
    for (const auto& qualifier : binding.defaultQualifiers) {
        addReturningRowToContext(
            context, defaultRow, table, qualifier, false);
    }
    addReturningRowToContext(
        context, image.oldRow, table, binding.oldName, false);
    addReturningRowToContext(
        context, image.newRow, table, binding.newName, false);
    return context;
}

SqlRow returningSqlRow(
    const std::map<std::string, std::string>& legacyRow) {
    SqlRow row;
    for (const auto& [column, value] : legacyRow) {
        // Legacy mutation APIs use the sentinel NULL for SQL NULL. New DML
        // paths use SqlRow and therefore keep the ordinary text "NULL"
        // distinct; this conversion is limited to the remaining legacy
        // insert/update entry points.
        row[column] = value == "NULL" ? SqlCell{} : SqlCell{value};
    }
    return row;
}

std::vector<ReturningRowImage> insertedReturningImages(
    const std::vector<std::map<std::string, std::string>>& rows) {
    std::vector<ReturningRowImage> images;
    images.reserve(rows.size());
    for (const auto& row : rows) {
        images.push_back(
            {{}, returningSqlRow(row), ReturningProjection::Source::New, {}});
    }
    return images;
}

std::vector<ReturningRowImage> updatedReturningImages(
    const std::vector<StorageEngine::UpdateRowImage>& rows) {
    std::vector<ReturningRowImage> images;
    images.reserve(rows.size());
    for (const auto& row : rows) {
        images.push_back(
            {row.oldRow, row.newRow, ReturningProjection::Source::New, {}});
    }
    return images;
}

std::vector<ReturningRowImage> deletedReturningImages(
    const std::vector<SqlRow>& rows) {
    std::vector<ReturningRowImage> images;
    images.reserve(rows.size());
    for (const auto& row : rows) {
        images.push_back({row, {}, ReturningProjection::Source::Old, {}});
    }
    return images;
}

RowContext updateContext(const SqlRow& source,
                         const TableSchema& table,
                         const std::string& targetTable) {
    RowContext context;
    for (size_t i = 0; i < table.len; ++i) {
        const Column& column = table.cols[i];
        const auto it = source.find(column.dataName);
        const bool isNull = it == source.end() || !it->second;
        const ExprValue value(
            column.dataType,
            isNull ? std::string{} : *it->second, isNull);
        context.set(column.dataName, value);
        if (!targetTable.empty()) {
            context.set(targetTable + "." + column.dataName, value);
        }
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
            const bool isNull = it == values.end() ||
                                isStructuredNull(it->second);
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
            bool isNull = false;
            std::string value = g_engine.extractColumnValue(
                row, table, i, currentDB, true, &isNull);
            values[table.cols[i].dataName] = isNull
                ? structuredNullValue() : std::move(value);
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
        value = "NULL";
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
        value = "NULL";
        return true;
    }
    value = result.value;
    if (result.typeName == "boolean") {
        if (value == "t") value = "1";
        else if (value == "f") value = "0";
    }
    return true;
}

bool evaluateStructuredSqlExpression(
    const Expr* expression,
    const std::map<std::string, std::string>& targetValues,
    const TableSchema& targetTable, const std::string& targetQualifier,
    const std::vector<StructuredSourceRelation>& sources,
    const std::vector<const std::map<std::string, std::string>*>& sourceRows,
    const std::string& currentDB, SqlCell& value) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(
        expression, structuredRelationContext(targetValues, targetTable,
                                              targetQualifier, sources,
                                              sourceRows));
    if (result.isUnknown() || result.typeName == "unknown") return false;
    if (result.isNull) {
        value = std::nullopt;
        return true;
    }
    std::string evaluated = result.value;
    if (result.typeName == "boolean") {
        if (evaluated == "t") evaluated = "1";
        else if (evaluated == "f") evaluated = "0";
    }
    value = std::move(evaluated);
    return true;
}

bool evaluateUpdateExpression(const Expr* expression,
                              const SqlRow& source,
                              const TableSchema& table,
                              const std::string& targetTable,
                              const std::string& currentDB,
                              SqlCell& value) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(
        expression, updateContext(source, table, targetTable));
    if (result.isUnknown() || result.typeName == "unknown") return false;
    if (result.isNull) {
        value = std::nullopt;
        return true;
    }
    std::string evaluated = result.value;
    if (result.typeName == "boolean") {
        if (evaluated == "t") evaluated = "1";
        else if (evaluated == "f") evaluated = "0";
    }
    value = std::move(evaluated);
    return true;
}

bool evaluateReturningExpression(const Expr* expression,
                                 const ReturningRowImage& image,
                                 const TableSchema& table,
                                 const ReturningBinding& binding,
                                 const std::string& currentDB,
                                 std::string& value,
                                 bool& isNull,
                                 std::string& typeName) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const ExprValue result = evaluator.eval(
        expression, returningContext(image, table, binding));
    if (!result.isNull &&
        (result.isUnknown() || result.typeName == "unknown")) {
        return false;
    }
    isNull = result.isNull;
    value = isNull ? std::string{} : result.value;
    typeName = result.typeName;
    return true;
}

void publishMutationCount(const std::string& command, size_t affectedRows) {
    g_lastDmlResult = {};
    g_lastDmlResult.available = true;
    g_lastDmlResult.metadataOnly = true;
    g_lastDmlResult.commandTag =
        command + " " + std::to_string(affectedRows);
}


bool publishReturning(const std::vector<ReturningProjection>& projections,
                      const TableSchema& table,
                      const ReturningBinding& binding,
                      const std::string& currentDB,
                      const std::vector<ReturningRowImage>& images,
                      const std::string& command) {
    g_lastDmlResult = {};
    g_lastDmlResult.available = true;
    for (const auto& projection : projections) {
        g_lastDmlResult.columns.push_back(projection.name);
        g_lastDmlResult.columnTypes.push_back(projection.typeName);
    }
    g_lastDmlResult.commandTag = command == "INSERT"
        ? "INSERT 0 " + std::to_string(images.size())
        : command + " " + std::to_string(images.size());
    g_lastDmlResult.rows.reserve(images.size());
    g_lastDmlResult.nulls.reserve(images.size());
    for (const auto& image : images) {
        std::vector<std::string> row;
        std::vector<bool> nulls;
        row.reserve(projections.size());
        nulls.reserve(projections.size());
        for (size_t i = 0; i < projections.size(); ++i) {
            const auto& projection = projections[i];
            std::string value;
            bool isNull = false;
            if (projection.expression == nullptr) {
                if (projection.source == ReturningProjection::Source::Action) {
                    isNull = image.action.empty();
                    value = isNull ? "NULL" : image.action;
                } else {
                    const SqlRow& source = returningSourceRow(
                        image, projection.source);
                    const auto it = source.find(projection.column);
                    isNull = it == source.end() || !it->second;
                    value = isNull ? "NULL" : *it->second;
                }
            } else {
                std::string typeName;
                if (!evaluateReturningExpression(
                        projection.expression, image, table, binding,
                        currentDB, value, isNull, typeName)) {
                    std::cout << "RETURNING expression evaluation failed"
                              << std::endl;
                    g_lastDmlResult = {};
                    return false;
                }
                if (!typeName.empty() && typeName != "unknown") {
                    g_lastDmlResult.columnTypes[i] = typeName;
                }
                if (isNull) value = "NULL";
            }
            row.push_back(std::move(value));
            nulls.push_back(isNull);
        }
        g_lastDmlResult.rows.push_back(std::move(row));
        g_lastDmlResult.nulls.push_back(std::move(nulls));
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

bool usesReturningRowImage(const Expr* expr,
                           const std::set<std::string>& qualifiers) {
    if (!expr) return false;
    if (const auto* ref = dynamic_cast<const ColumnRefExpr*>(expr)) {
        return !ref->table.empty() &&
               qualifiers.count(identifier(ref->table)) != 0;
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
        return usesReturningRowImage(unary->operand.get(), qualifiers);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
        return usesReturningRowImage(binary->left.get(), qualifiers) ||
               usesReturningRowImage(binary->right.get(), qualifiers);
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
        for (const auto& arg : call->args) {
            if (usesReturningRowImage(arg.get(), qualifiers)) return true;
        }
        for (const auto& arg : call->namedArgs) {
            if (usesReturningRowImage(arg.value.get(), qualifiers)) return true;
        }
        if (usesReturningRowImage(call->filter.get(), qualifiers)) return true;
        if (call->hasOver) {
            for (const auto& partition : call->over.partitionBy) {
                if (usesReturningRowImage(
                        partition.get(), qualifiers)) return true;
            }
            for (const auto& order : call->over.orderBy) {
                if (usesReturningRowImage(
                        order.first.get(), qualifiers)) return true;
            }
            if (usesReturningRowImage(
                    call->over.frameStart.get(), qualifiers) ||
                usesReturningRowImage(
                    call->over.frameEnd.get(), qualifiers)) {
                return true;
            }
        }
        return false;
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
        return usesReturningRowImage(cast->operand.get(), qualifiers);
    }
    if (const auto* caseExpr = dynamic_cast<const CaseExpr*>(expr)) {
        if (usesReturningRowImage(
                caseExpr->switchExpr.get(), qualifiers)) return true;
        for (const auto& clause : caseExpr->whenClauses) {
            if (usesReturningRowImage(clause.first.get(), qualifiers) ||
                usesReturningRowImage(clause.second.get(), qualifiers)) {
                return true;
            }
        }
        return usesReturningRowImage(caseExpr->elseExpr.get(), qualifiers);
    }
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
        for (const auto& element : array->elements) {
            if (usesReturningRowImage(element.get(), qualifiers)) return true;
        }
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
        for (const auto& element : row->elements) {
            if (usesReturningRowImage(element.get(), qualifiers)) return true;
        }
    }
    return false;
}

bool usesVersionedReturning(const std::vector<SelectItem>& returning,
                            const ReturningOptions& options) {
    if (returning.empty()) return false;
    if (options.oldAliased || options.newAliased) return true;
    const std::set<std::string> qualifiers = {"old", "new"};
    for (const auto& item : returning) {
        if (usesReturningRowImage(item.expr.get(), qualifiers)) return true;
    }
    return false;
}

bool evaluateValue(const ExprPtr& expr, const std::string& currentDB,
                   SqlCell& value) {
    if (referencesColumn(expr.get())) return false;
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    RowContext emptyContext;
    ExprValue result = evaluator.eval(expr, emptyContext);
    if (result.isUnknown()) return false;
    if (result.isNull) {
        value = std::nullopt;
        return true;
    }
    std::string evaluated = result.value;
    // ExprEvaluator uses PostgreSQL-style t/f internally, while the storage
    // validator accepts the canonical on-disk boolean spellings 1/0.
    if (result.typeName == "boolean") {
        if (evaluated == "t") evaluated = "1";
        else if (evaluated == "f") evaluated = "0";
    }
    value = std::move(evaluated);
    return true;
}

bool evaluateValue(const ExprPtr& expr, const std::string& currentDB,
                   std::string& value) {
    SqlCell typedValue;
    if (!evaluateValue(expr, currentDB, typedValue)) return false;
    value = typedValue ? *typedValue : "NULL";
    return true;
}

bool evaluateConflictExpression(
    const Expr* expr, const TableSchema& table, const SqlRow& values,
    const std::string& currentDB, SqlCell& value,
    const SqlRow* targetRow = nullptr,
    const std::string& targetTable = {}) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    RowContext context;
    for (size_t i = 0; i < table.len; ++i) {
        const std::string& column = table.cols[i].dataName;
        const auto excluded = values.find(column);
        if (targetRow) {
            const auto target = targetRow->find(column);
            const bool targetNull =
                target == targetRow->end() || !target->second;
            const ExprValue targetValue(
                table.cols[i].dataType,
                targetNull ? std::string{} : *target->second, targetNull);
            context.set(column, targetValue);
            if (!targetTable.empty()) {
                context.set(targetTable + "." + column, targetValue);
            }
        }
        const bool excludedNull =
            excluded == values.end() || !excluded->second;
        context.set(
            "excluded." + column,
            ExprValue(table.cols[i].dataType,
                      excludedNull ? std::string{} : *excluded->second,
                      excludedNull));
    }
    const ExprValue result = evaluator.eval(expr, context);
    if (result.isUnknown()) return false;
    if (result.isNull) {
        value = std::nullopt;
        return true;
    }
    std::string evaluated = result.value;
    if (result.typeName == "boolean") {
        if (evaluated == "t") evaluated = "1";
        else if (evaluated == "f") evaluated = "0";
    }
    value = std::move(evaluated);
    return true;
}

bool conflictTargetMatches(const TableSchema& table,
                           const std::vector<std::string>& targetColumns,
                           const SqlRow& targetValues,
                           const SqlRow& candidate) {
    for (const auto& targetColumn : targetColumns) {
        size_t columnIndex = table.len;
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == targetColumn) {
                columnIndex = i;
                break;
            }
        }
        const auto expected = targetValues.find(targetColumn);
        const auto actual = candidate.find(targetColumn);
        if (columnIndex >= table.len || expected == targetValues.end() ||
            actual == candidate.end() || !expected->second ||
            !actual->second ||
            StorageEngine::compareValues(
                table.cols[columnIndex], *actual->second, false,
                *expected->second, false, "=") !=
                StorageEngine::PredicateTruth::True) {
            return false;
        }
    }
    return !targetColumns.empty();
}

bool loadConflictTargetRow(const std::string& currentDB,
                           const std::string& tableName,
                           const TableSchema& table,
                           const std::vector<std::string>& targetColumns,
                           const SqlRow& targetValues,
                           SqlRow& rowValues,
                           bool* scanFailed = nullptr) {
    if (scanFailed) *scanFailed = false;
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
            !value->second) {
            return false;
        }
        targetIndices.push_back(targetIndex);
    }

    auto captureRow = [&](const std::string& rowBuffer, SqlRow& output) {
        output.clear();
        for (size_t i = 0; i < table.len; ++i) {
            bool isNull = false;
            std::string value = g_engine.extractColumnValue(
                rowBuffer, table, i, currentDB, true, &isNull);
            output[table.cols[i].dataName] = isNull
                ? SqlCell{} : SqlCell{std::move(value)};
        }
    };

    // A byte-identical single-column key can still use the physical index.
    // Verify the heap row with SQL equality before accepting it, and fall
    // back to a scan when the B-tree cannot represent the column's collation
    // or canonical numeric equality (for example Alpha/aLPHa or -0/0).
    if (targetIndices.size() == 1) {
        const std::string& targetColumn = targetColumns.front();
        BPTree* index = table.cols[targetIndices.front()].isPrimaryKey
            ? g_engine.getPKIndex(currentDB, tableName)
            : g_engine.getSecondaryIndex(currentDB, tableName, targetColumn);
        if (index) {
            int64_t rid = 0;
            if (index->search(*targetValues.at(targetColumn), rid)) {
                std::string rowBuffer;
                PageAllocator* allocator =
                    g_engine.getPageAllocator(currentDB, tableName);
                if (allocator && g_engine.readVisibleRowByRid(
                        currentDB, allocator, rid, rowBuffer, table)) {
                    SqlRow candidate;
                    captureRow(rowBuffer, candidate);
                    if (conflictTargetMatches(
                            table, targetColumns, targetValues, candidate)) {
                        rowValues = std::move(candidate);
                        return true;
                    }
                }
            }
        }
    }

    bool found = false;
    if (!g_engine.forEachRow(currentDB, tableName,
                        [&](uint32_t, uint16_t, const char* data, size_t len) {
        if (found) return;
        const std::string rowBuffer(data, len);
        SqlRow candidate;
        captureRow(rowBuffer, candidate);
        if (conflictTargetMatches(
                table, targetColumns, targetValues, candidate)) {
            rowValues = std::move(candidate);
            found = true;
        }
    })) {
        if (scanFailed) *scanFailed = true;
        return false;
    }
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

    StorageEngine::IdentityOverride identityOverride =
        StorageEngine::IdentityOverride::None;
    if (lower(stmt.override_) == "system") {
        identityOverride = StorageEngine::IdentityOverride::System;
    } else if (lower(stmt.override_) == "user") {
        identityOverride = StorageEngine::IdentityOverride::User;
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

    std::vector<bool> generatedTargets;
    std::vector<char> identityTargets;
    generatedTargets.reserve(columns.size());
    identityTargets.reserve(columns.size());
    for (const auto& column : columns) {
        bool generated = false;
        char identityKind = 0;
        for (size_t i = 0; i < table.len; ++i) {
            if (table.cols[i].dataName == column) {
                generated = !table.cols[i].generatedExpr.empty();
                identityKind = table.cols[i].identityKind;
                break;
            }
        }
        generatedTargets.push_back(generated);
        identityTargets.push_back(identityKind);
    }
    const auto rejectGeneratedValue = [&](size_t columnIndex) {
        std::cout << "ERROR: cannot insert a non-DEFAULT value into column \""
                  << columns[columnIndex]
                  << "\" (SQLSTATE 428C9)" << std::endl;
        return true;
    };
    const auto rejectAlwaysIdentityValue = [&](size_t columnIndex) {
        std::cout << "ERROR: cannot insert a non-DEFAULT value into identity "
                     "column \"" << columns[columnIndex]
                  << "\" (SQLSTATE 428C9)\n"
                     "HINT: Use OVERRIDING SYSTEM VALUE to override."
                  << std::endl;
        return true;
    };

    const ReturningBinding insertReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, "");
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table,
                                   insertReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> insertedRows;
    const bool ignoreDuplicate = lower(stmt.conflictAction) == "do nothing";
    const bool conflictUpdate = lower(stmt.conflictAction) == "do update";
    const bool hasConflictArbiter = !stmt.conflictTarget.empty() ||
        !stmt.conflictConstraint.empty();
    std::vector<std::string> conflictTarget;
    SqlRow conflictUpdates;
    std::map<std::string, const Expr*> conflictExpressionUpdates;
    std::map<std::string, std::set<std::string>> conflictExpressionSources;
    std::set<std::string> conflictWhereExcludedColumns;

    if (conflictUpdate || (ignoreDuplicate && hasConflictArbiter)) {
        bool namedConstraintFound = false;
        if (!resolveConflictTarget(stmt, table, s.currentDB, resolvedTable,
                                   conflictTarget, &namedConstraintFound)) {
            if (!stmt.conflictConstraint.empty()) {
                const std::string constraint = identifier(
                    stmt.conflictConstraint);
                if (namedConstraintFound) {
                    std::cout << "ERROR: constraint \"" << constraint
                              << "\" has no associated unique index "
                                 "(SQLSTATE 42809)" << std::endl;
                } else {
                    std::cout << "ERROR: constraint \"" << constraint
                              << "\" for table \"" << requestedTable
                              << "\" does not exist (SQLSTATE 42704)"
                              << std::endl;
                }
                return true;
            }
            fallback = true;
            return false;
        }
    }

    if (conflictUpdate) {
        if (!checkTablePrivilege(s, requestedTable,
                                 StorageEngine::TablePrivilege::Update)) {
            return true;
        }
        if (!buildConflictUpdatePlan(stmt, table, s.currentDB, resolvedTable,
                                     conflictTarget, requestedTable,
                                     conflictUpdates,
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
        for (size_t i = 0; i < generatedTargets.size(); ++i) {
            if (generatedTargets[i]) return rejectGeneratedValue(i);
            if (identityTargets[i] == 'a' &&
                identityOverride != StorageEngine::IdentityOverride::System &&
                identityOverride != StorageEngine::IdentityOverride::User) {
                return rejectAlwaysIdentityValue(i);
            }
        }
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
        if (identityOverride == StorageEngine::IdentityOverride::User) {
            for (auto& values : pendingRows) {
                for (size_t i = 0; i < identityTargets.size(); ++i) {
                    if (identityTargets[i] != 0) values.erase(columns[i]);
                }
            }
        }

        DmlStatementScope statementScope(g_engine, s.currentDB);
        if (!statementScope.ready()) {
            std::cout << "Could not start INSERT statement transaction"
                      << std::endl;
            return true;
        }
        int inserted = 0;
        for (const auto& values : pendingRows) {
            const DBStatus status = g_engine.insert(
                s.currentDB, resolvedTable, values,
                stmt.returning.empty() ? nullptr : &insertedRows,
                identityOverride);
            if (status == DBStatus::DUPLICATE_KEY) {
                if (ignoreDuplicate) continue;
                std::cout << "Duplicate key (SQLSTATE 23505)" << std::endl;
                return true;
            }
            if (status != DBStatus::OK) {
                if (status == DBStatus::STRING_DATA_RIGHT_TRUNCATION) {
                    std::cout << "value too long for character column "
                                 "(SQLSTATE 22001)" << std::endl;
                    return true;
                }
                std::cout << "Invalid data, please check" << std::endl;
                return true;
            }
            ++inserted;
        }
        std::cout << inserted << " row(s) inserted" << std::endl;
        if (!stmt.returning.empty()) {
            if (!publishReturning(
                    returningProjections, table, insertReturningBinding,
                    s.currentDB, insertedReturningImages(insertedRows),
                    "INSERT")) return true;
            printReturningRows(g_lastDmlResult);
        }
        if (inserted > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
        if (!statementScope.finish()) {
            std::cout << "Could not finish INSERT statement transaction"
                      << std::endl;
            return true;
        }
        return false;
    }

    if (stmt.defaultValues) {
        if (!stmt.values.empty()) {
            std::cout << "SQL syntax error: invalid DEFAULT VALUES statement"
                      << std::endl;
            return true;
        }
        DmlStatementScope statementScope(g_engine, s.currentDB);
        if (!statementScope.ready()) {
            std::cout << "Could not start INSERT statement transaction"
                      << std::endl;
            return true;
        }
        const DBStatus status = g_engine.insertDefaultValues(
            s.currentDB, resolvedTable, table,
            stmt.returning.empty() ? nullptr : &insertedRows);
        if (status == DBStatus::DUPLICATE_KEY && ignoreDuplicate) {
            std::cout << "INSERT 0 0 (ON CONFLICT DO NOTHING)" << std::endl;
            if (!stmt.returning.empty()) {
                if (!publishReturning(
                        returningProjections, table, insertReturningBinding,
                        s.currentDB, insertedReturningImages(insertedRows),
                        "INSERT")) return true;
                printReturningRows(g_lastDmlResult);
            }
            if (!statementScope.finish()) {
                std::cout << "Could not finish INSERT statement transaction"
                          << std::endl;
                return true;
            }
            return false;
        }
        if (status != DBStatus::OK) {
            std::cout << "INSERT DEFAULT VALUES failed" << std::endl;
            return true;
        }
        std::cout << "INSERT 0 1 (DEFAULT VALUES)" << std::endl;
        if (!stmt.returning.empty()) {
            if (!publishReturning(
                    returningProjections, table, insertReturningBinding,
                    s.currentDB, insertedReturningImages(insertedRows),
                    "INSERT")) return true;
            printReturningRows(g_lastDmlResult);
        }
        g_engine.analyzeTable(s.currentDB, resolvedTable);
        if (!statementScope.finish()) {
            std::cout << "Could not finish INSERT statement transaction"
                      << std::endl;
            return true;
        }
        return false;
    }

    // Evaluate the complete VALUES list before mutating storage.  If an
    // expression is outside this executor's supported evaluator, the legacy
    // path must receive the untouched statement without a partially inserted
    // prefix.
    std::vector<SqlRow> pendingRows;
    std::vector<SqlRow> sqlInsertedRows;
    pendingRows.reserve(stmt.values.size());
    for (const auto& row : stmt.values) {
        if (row.size() != columns.size()) {
            std::cout << "SQL syntax error: column count mismatch" << std::endl;
            return true;
        }

        SqlRow values;
        for (size_t i = 0; i < row.size(); ++i) {
            if (isDefaultValue(row[i])) continue;
            if (generatedTargets[i]) return rejectGeneratedValue(i);
            if (identityTargets[i] != 0 &&
                identityOverride == StorageEngine::IdentityOverride::User) {
                continue;
            }
            if (identityTargets[i] == 'a' &&
                identityOverride != StorageEngine::IdentityOverride::System) {
                return rejectAlwaysIdentityValue(i);
            }
            SqlCell value;
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

    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "Could not start INSERT statement transaction"
                  << std::endl;
        return true;
    }
    int inserted = 0;
    std::vector<ReturningRowImage> returningImages;
    for (const auto& values : pendingRows) {
        const DBStatus status = g_engine.insertRow(
            s.currentDB, resolvedTable, values,
            stmt.returning.empty() ? nullptr : &sqlInsertedRows,
            identityOverride);
        if (status == DBStatus::DUPLICATE_KEY) {
            if (ignoreDuplicate) {
                if (conflictTarget.empty()) continue;

                SqlRow targetValues;
                bool targetValueUnavailable = false;
                for (const auto& targetColumn : conflictTarget) {
                    const auto targetValue = values.find(targetColumn);
                    if (targetValue == values.end() || !targetValue->second) {
                        targetValueUnavailable = true;
                        break;
                    }
                    targetValues[targetColumn] = targetValue->second;
                }
                SqlRow targetRow;
                bool targetScanFailed = false;
                if (!targetValueUnavailable &&
                    loadConflictTargetRow(s.currentDB, resolvedTable, table,
                                          conflictTarget, targetValues,
                                          targetRow, &targetScanFailed)) {
                    continue;
                }
                if (targetScanFailed) {
                    std::cout << "ON CONFLICT target scan failed" << std::endl;
                    return true;
                }
                // A target-specific DO NOTHING must not hide a duplicate
                // raised by a different unique constraint.
                std::cout << "Duplicate key (SQLSTATE 23505)" << std::endl;
                return true;
            }
            if (conflictUpdate) {
                SqlRow targetValues;
                for (const auto& targetColumn : conflictTarget) {
                    const auto targetValue = values.find(targetColumn);
                    if (targetValue == values.end() || !targetValue->second) {
                        std::cout << "ON CONFLICT target value is unavailable" << std::endl;
                        return true;
                    }
                    targetValues[targetColumn] = targetValue->second;
                }
                SqlRow targetRow;
                bool targetScanFailed = false;
                if (!loadConflictTargetRow(
                        s.currentDB, resolvedTable, table, conflictTarget,
                        targetValues, targetRow, &targetScanFailed)) {
                    if (targetScanFailed) {
                        std::cout << "ON CONFLICT target scan failed" << std::endl;
                    } else {
                        // The duplicate came from a different unique key;
                        // this conflict target is not allowed to consume it.
                        std::cout << "ON CONFLICT target row is unavailable"
                                  << std::endl;
                    }
                    return true;
                }
                if (stmt.conflictWhere) {
                    SqlCell whereValue;
                    if (!evaluateConflictExpression(stmt.conflictWhere.get(), table,
                                                     values, s.currentDB, whereValue,
                                                     &targetRow, requestedTable)) {
                        std::cout << "ON CONFLICT WHERE evaluation failed" << std::endl;
                        return true;
                    }
                    if (!whereValue || whereValue->empty() ||
                        *whereValue == "0" || lower(*whereValue) == "false") {
                        // PostgreSQL treats a false/NULL conflict WHERE as
                        // "do not update" for this conflicting input row.
                        continue;
                    }
                }
                SqlRow rowConflictUpdates = conflictUpdates;
                for (const auto& [updateColumn, expression] : conflictExpressionUpdates) {
                    SqlCell value;
                    if (!evaluateConflictExpression(expression, table, values,
                                                    s.currentDB, value)) {
                        std::cout << "ON CONFLICT expression evaluation failed" << std::endl;
                        return true;
                    }
                    rowConflictUpdates[updateColumn] = std::move(value);
                }
                std::vector<SqlRow> updatedRows;
                std::vector<StorageEngine::UpdateRowImage> updateImages;
                const auto targetMatcher =
                    [&](const SqlRow& candidate) {
                        return conflictTargetMatches(
                            table, conflictTarget, targetValues, candidate);
                    };
                const DBStatus updateStatus = g_engine.updateRows(
                    s.currentDB, resolvedTable, rowConflictUpdates, {},
                    &updatedRows, {}, targetMatcher, nullptr,
                    stmt.returning.empty() ? nullptr : &updateImages);
                if (updateStatus != DBStatus::OK || updatedRows.size() != 1) {
                    std::cout << "ON CONFLICT DO UPDATE failed" << std::endl;
                    return true;
                }
                if (!stmt.returning.empty()) {
                    const auto images = updatedReturningImages(updateImages);
                    returningImages.insert(returningImages.end(),
                                           images.begin(), images.end());
                }
                ++inserted;
                continue;
            }
            std::cout << "Duplicate key (SQLSTATE 23505)" << std::endl;
            return true;
        }
        if (status != DBStatus::OK) {
            if (status == DBStatus::STRING_DATA_RIGHT_TRUNCATION) {
                std::cout << "value too long for character column "
                             "(SQLSTATE 22001)" << std::endl;
                return true;
            }
            std::cout << "Invalid data, please check" << std::endl;
            return true;
        }
        if (!stmt.returning.empty() && !sqlInsertedRows.empty()) {
            returningImages.push_back(
                {{}, sqlInsertedRows.back(),
                 ReturningProjection::Source::New, {}});
        }
        ++inserted;
    }

    std::cout << inserted << " row(s) inserted" << std::endl;
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, table, insertReturningBinding,
                s.currentDB, returningImages, "INSERT")) return true;
        printReturningRows(g_lastDmlResult);
    }
    if (inserted > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    if (!statementScope.finish()) {
        std::cout << "Could not finish INSERT statement transaction"
                  << std::endl;
        return true;
    }
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
    const std::string targetQualifier = stmt.alias.empty()
        ? unqualifiedRelationName(requestedTable)
        : identifier(stmt.alias);
    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: UPDATE FROM could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
    }
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

    const ReturningBinding updateReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, stmt.alias);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema,
                                   updateReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> updatedRows;
    std::vector<StorageEngine::UpdateRowImage> updateImages;
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
    size_t affectedRows = 0;
    const DBStatus status = g_engine.update(
        s.currentDB, resolvedTable, staticUpdates, {},
        stmt.returning.empty() ? nullptr : &updatedRows,
        updateResolver, updateMatcher, &affectedRows,
        stmt.returning.empty() ? nullptr : &updateImages);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: UPDATE FROM failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, targetSchema, updateReturningBinding,
                s.currentDB, updatedReturningImages(updateImages),
                "UPDATE")) return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: UPDATE FROM transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    if (affectedRows > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    std::cout << "Update done" << std::endl;
    if (!stmt.returning.empty()) printReturningRows(g_lastDmlResult);
    else publishMutationCount("UPDATE", affectedRows);
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
    const std::string targetQualifier = stmt.alias.empty()
        ? unqualifiedRelationName(requestedTable)
        : identifier(stmt.alias);
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

    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: UPDATE FROM could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
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

    const ReturningBinding updateReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, stmt.alias);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema,
                                   updateReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<std::map<std::string, std::string>> updatedRows;
    std::vector<StorageEngine::UpdateRowImage> updateImages;
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
    size_t affectedRows = 0;
    const DBStatus status = g_engine.update(
        s.currentDB, resolvedTable, staticUpdates, {},
        stmt.returning.empty() ? nullptr : &updatedRows,
        updateResolver, updateMatcher, &affectedRows,
        stmt.returning.empty() ? nullptr : &updateImages);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: UPDATE FROM failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, targetSchema, updateReturningBinding,
                s.currentDB, updatedReturningImages(updateImages),
                "UPDATE")) return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: UPDATE FROM transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    if (affectedRows > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    std::cout << "Update done" << std::endl;
    if (!stmt.returning.empty()) printReturningRows(g_lastDmlResult);
    else publishMutationCount("UPDATE", affectedRows);
    return false;
}

bool executeUpdate(const UpdateStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (!stmt.whereCurrentOf.empty()) {
        std::cout << "ERROR: WHERE CURRENT OF is not supported for UPDATE "
                     "(SQLSTATE 0A000)" << std::endl;
        return true;
    }
    if (stmt.fromClause) return executeUpdateFrom(stmt, s, fallback);
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
    const std::string targetQualifier = stmt.alias.empty()
        ? unqualifiedRelationName(requestedTable)
        : identifier(stmt.alias);
    std::vector<std::string> columns;
    SqlRow updates;
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
        SqlCell value;
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
    const ReturningBinding updateReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, stmt.alias);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table,
                                   updateReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<SqlRow> updatedRows;
    std::vector<StorageEngine::UpdateRowImage> updateImages;
    StorageEngine::SqlUpdateResolver updateResolver;
    if (!expressionUpdates.empty()) {
        updateResolver = [&, targetQualifier](
                             const SqlRow& oldValues,
                             SqlRow& effectiveUpdates) {
            for (const auto& [column, expression] : expressionUpdates) {
                SqlCell value;
                if (!evaluateUpdateExpression(expression, oldValues, table,
                                              targetQualifier, s.currentDB, value)) {
                    return false;
                }
                effectiveUpdates[column] = std::move(value);
            }
            return true;
        };
    }
    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: UPDATE could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
    }
    size_t affectedRows = 0;
    const DBStatus status = g_engine.updateRows(
        s.currentDB, resolvedTable, updates, conditions,
        stmt.returning.empty() ? nullptr : &updatedRows, updateResolver,
        StorageEngine::SqlUpdateMatcher{}, &affectedRows,
        stmt.returning.empty() ? nullptr : &updateImages);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: Update failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, table, updateReturningBinding,
                s.currentDB, updatedReturningImages(updateImages),
                "UPDATE")) return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: UPDATE transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    std::cout << "Update done" << std::endl;
    if (!stmt.returning.empty()) printReturningRows(g_lastDmlResult);
    else publishMutationCount("UPDATE", affectedRows);
    return false;
}

bool executeMerge(const MergeStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (!checkDatabase(s)) return true;

    // MERGE is deliberately owned by the typed executor. Unsupported shapes
    // must fail before the statement scope below is opened; returning them to
    // the legacy string slicer can execute only a prefix of the command.
    auto unsupported = [](const std::string& detail) {
        std::cout << "ERROR: feature not supported: MERGE " << detail
                  << " (SQLSTATE 0A000)" << std::endl;
        return true;
    };
    auto mutationFailure = [](const std::string& action, DBStatus status) {
        std::cout << "ERROR: MERGE " << action << " failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    };
    if (!stmt.source || !stmt.joinCondition) {
        return unsupported("requires a source relation and an ON predicate");
    }

    const std::string requestedTarget = identifier(stmt.targetTable);
    const std::string targetTable = resolveTable(s, requestedTarget);
    if (requestedTarget.empty()) return unsupported("target table is required");
    if (g_engine.viewExists(s.currentDB, requestedTarget) ||
        g_engine.isMaterializedView(s.currentDB, requestedTarget)) {
        return unsupported("views are not writable through MERGE");
    }
    if (!g_engine.tableExists(s.currentDB, targetTable)) {
        std::cout << "Table " << requestedTarget << " not exist" << std::endl;
        return true;
    }
    if (!checkTablePrivilege(s, requestedTarget,
                             StorageEngine::TablePrivilege::Select)) {
        return true;
    }

    const TableSchema targetSchema = g_engine.getTableSchema(s.currentDB, targetTable);
    const std::string targetQualifier = stmt.targetAlias.empty()
        ? unqualifiedRelationName(requestedTarget)
        : identifier(stmt.targetAlias);
    if (targetQualifier.empty()) return unsupported("target alias is invalid");

    // Establish the command snapshot before reading either relation. Keeping
    // source classification, target matching, and all physical actions in the
    // same transaction prevents a concurrent insert/update from falling into
    // the gap between MERGE planning and mutation. Early capability failures
    // below still unwind this scope without writing anything.
    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: MERGE could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
    }

    std::vector<StructuredSourceRelation> sources;
    std::vector<const Expr*> sourceJoinPredicates;
    const StructuredRelationResult relationResult = collectStructuredRelations(
        stmt.source.get(), s, targetQualifier, sources, sourceJoinPredicates);
    if (relationResult == StructuredRelationResult::Error) return true;
    if (relationResult != StructuredRelationResult::Success || sources.empty()) {
        return unsupported(
            "source must be a table or an INNER/CROSS table join");
    }

    // A JOIN inside USING is a source expression, not a lateral reference to
    // the target. Validate and materialize its source tuples independently.
    const TableSchema noTargetSchema;
    for (const Expr* predicate : sourceJoinPredicates) {
        ExprEvaluator evaluator;
        if (!validateStructuredExpression(predicate, noTargetSchema, "",
                                           sources, evaluator)) {
            return unsupported(
                "source JOIN predicate is outside the supported relation subset");
        }
    }
    std::vector<std::vector<const std::map<std::string, std::string>*>> sourceRows;
    bool evaluationFailed = false;
    std::vector<const std::map<std::string, std::string>*> sourceTuple;
    std::function<void(size_t)> visitSource = [&](size_t index) {
        if (evaluationFailed) return;
        if (index < sources.size()) {
            for (const auto& row : sources[index].rows) {
                sourceTuple.push_back(&row);
                visitSource(index + 1);
                sourceTuple.pop_back();
                if (evaluationFailed) return;
            }
            return;
        }
        const std::map<std::string, std::string> emptyTarget;
        const RowContext context = structuredRelationContext(
            emptyTarget, noTargetSchema, "", sources, sourceTuple);
        ExprEvaluator evaluator;
        evaluator.setCurrentDB(s.currentDB);
        for (const Expr* predicate : sourceJoinPredicates) {
            if (!structuredPredicateMatches(predicate, context, evaluator,
                                            evaluationFailed)) {
                return;
            }
        }
        sourceRows.push_back(sourceTuple);
    };
    visitSource(0);
    if (evaluationFailed) {
        return unsupported("source JOIN predicate evaluation failed");
    }

    ExprEvaluator expressionEvaluator;
    if (!validateStructuredExpression(stmt.joinCondition.get(), targetSchema,
                                       targetQualifier, sources,
                                       expressionEvaluator)) {
        return unsupported("ON expression is outside the supported relation subset");
    }

    enum class BranchKind { Matched, NotMatchedByTarget, NotMatchedBySource };
    auto branchKind = [](const MergeStmt::WhenClause& clause) {
        if (clause.matched) return BranchKind::Matched;
        return lower(clause.bySource) == "source"
            ? BranchKind::NotMatchedBySource
            : BranchKind::NotMatchedByTarget;
    };
    std::vector<const MergeStmt::WhenClause*> matchedClauses;
    std::vector<const MergeStmt::WhenClause*> byTargetClauses;
    std::vector<const MergeStmt::WhenClause*> bySourceClauses;
    std::set<std::string> updateColumns;
    std::set<std::string> insertColumns;
    bool needsDelete = false;
    for (const auto& clause : stmt.whenClauses) {
        const std::string action = lower(clause.action);
        const BranchKind kind = branchKind(clause);
        if (action != "update" && action != "insert" && action != "delete" &&
            action != "do nothing") {
            return unsupported("WHEN action is not implemented");
        }
        if ((kind == BranchKind::Matched && action == "insert") ||
            (kind == BranchKind::NotMatchedByTarget && action != "insert" &&
             action != "do nothing") ||
            (kind == BranchKind::NotMatchedBySource && action == "insert")) {
            return unsupported("WHEN action is invalid for its match kind");
        }
        auto& clauses = kind == BranchKind::Matched
            ? matchedClauses
            : (kind == BranchKind::NotMatchedBySource
                   ? bySourceClauses : byTargetClauses);
        clauses.push_back(&clause);

        const TableSchema& availableTarget = kind == BranchKind::NotMatchedByTarget
            ? noTargetSchema : targetSchema;
        const std::string availableTargetQualifier =
            kind == BranchKind::NotMatchedByTarget ? std::string{} : targetQualifier;
        const std::vector<StructuredSourceRelation> availableSources =
            kind == BranchKind::NotMatchedBySource
                ? std::vector<StructuredSourceRelation>{} : sources;
        if (clause.condition) {
            ExprEvaluator evaluator;
            if (!validateStructuredExpression(
                    clause.condition.get(), availableTarget,
                    availableTargetQualifier, availableSources, evaluator)) {
                return unsupported(
                    "WHEN condition references an unavailable relation or expression");
            }
        }

        if (action == "update") {
            if (clause.updateSet.empty()) {
                return unsupported("UPDATE action has an empty SET list");
            }
            std::set<std::string> clauseColumns;
            for (const auto& [rawColumn, expression] : clause.updateSet) {
                const std::string column = identifier(rawColumn);
                if (column.empty() || !clauseColumns.insert(column).second ||
                    !findTableColumn(targetSchema, column) ||
                    isDefaultValue(expression)) {
                    return unsupported(
                        "UPDATE action contains a duplicate/invalid target column or DEFAULT");
                }
                ExprEvaluator evaluator;
                if (!validateStructuredExpression(
                        expression.get(), targetSchema, targetQualifier,
                        availableSources, evaluator)) {
                    return unsupported(
                        "UPDATE expression references an unavailable relation or expression");
                }
                updateColumns.insert(column);
            }
        } else if (action == "insert") {
            if (clause.insertCols.empty()) {
                return unsupported(
                    "INSERT action requires an explicit column/value list");
            }
            std::set<std::string> clauseColumns;
            for (const auto& [rawColumn, expression] : clause.insertCols) {
                const std::string column = identifier(rawColumn);
                if (column.empty() || !clauseColumns.insert(column).second ||
                    !findTableColumn(targetSchema, column) || !expression ||
                    isDefaultValue(expression)) {
                    return unsupported(
                        "INSERT action contains duplicate/invalid columns or DEFAULT");
                }
                ExprEvaluator evaluator;
                if (!validateStructuredExpression(
                        expression.get(), noTargetSchema, "", sources,
                        evaluator)) {
                    return unsupported(
                        "INSERT expression references an unavailable relation or expression");
                }
                insertColumns.insert(column);
            }
        } else if (action == "delete") {
            needsDelete = true;
        }
    }
    if (stmt.whenClauses.empty()) {
        return unsupported("requires at least one WHEN branch");
    }

    if (!updateColumns.empty()) {
        if (!checkTablePrivilege(s, requestedTarget,
                                 StorageEngine::TablePrivilege::Update)) {
            return true;
        }
        const std::vector<std::string> columns(updateColumns.begin(),
                                                updateColumns.end());
        if (!sessionIsAdmin(s) && !isTempTable(s, requestedTarget) &&
            !g_engine.hasColumnPermission(
                s.currentDB, requestedTarget, effectiveSessionRole(s),
                StorageEngine::TablePrivilege::Update, columns)) {
            std::cout << "permission denied: UPDATE on restricted columns of table "
                      << requestedTarget << std::endl;
            return true;
        }
    }
    if (!insertColumns.empty()) {
        const std::vector<std::string> columns(insertColumns.begin(),
                                                insertColumns.end());
        if (!checkInsertTablePermission(s, requestedTarget) ||
            !checkInsertColumns(s, requestedTarget, columns)) {
            return true;
        }
    }
    if (needsDelete &&
        !checkTablePrivilege(s, requestedTarget,
                             StorageEngine::TablePrivilege::Delete)) {
        return true;
    }

    const ReturningBinding mergeReturningBinding = returningBinding(
        stmt.returningOptions, requestedTarget, stmt.targetAlias, true);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty()) {
        for (const auto& item : stmt.returning) {
            const auto* ref = item.expr
                ? dynamic_cast<const ColumnRefExpr*>(item.expr.get()) : nullptr;
            if (ref && identifier(ref->column) == "*" &&
                (!ref->schema.empty() ||
                 !returningSourceForQualifier(
                     ref->table, mergeReturningBinding))) {
                return unsupported(
                    "RETURNING source-qualified star is not implemented");
            }
        }
        if (!buildReturningProjections(stmt.returning, targetSchema,
                                       mergeReturningBinding,
                                       returningProjections)) {
            return unsupported(
                "RETURNING supports target columns and bounded scalar expressions only");
        }
    }

    std::vector<std::map<std::string, std::string>> targetRows;
    if (!collectTableRows(s.currentDB, targetTable, targetSchema, "SELECT",
                          targetRows)) {
        return unsupported("target relation could not be read");
    }

    std::vector<std::vector<size_t>> sourceMatches(sourceRows.size());
    std::vector<std::vector<size_t>> targetMatches(targetRows.size());
    for (size_t sourceIndex = 0; sourceIndex < sourceRows.size(); ++sourceIndex) {
        for (size_t targetIndex = 0; targetIndex < targetRows.size(); ++targetIndex) {
            const RowContext context = structuredRelationContext(
                targetRows[targetIndex], targetSchema, targetQualifier,
                sources, sourceRows[sourceIndex]);
            ExprEvaluator evaluator;
            evaluator.setCurrentDB(s.currentDB);
            if (structuredPredicateMatches(stmt.joinCondition.get(), context,
                                            evaluator, evaluationFailed)) {
                sourceMatches[sourceIndex].push_back(targetIndex);
                targetMatches[targetIndex].push_back(sourceIndex);
            }
            if (evaluationFailed) break;
        }
        if (evaluationFailed) break;
    }
    if (evaluationFailed) {
        std::cout << "ERROR: MERGE ON expression evaluation failed "
                     "(SQLSTATE 22023)" << std::endl;
        return true;
    }

    auto chooseClause = [&](const std::vector<const MergeStmt::WhenClause*>& clauses,
                            const std::map<std::string, std::string>& targetValues,
                            const std::vector<const std::map<std::string, std::string>*>& rows)
        -> const MergeStmt::WhenClause* {
        for (const auto* clause : clauses) {
            if (!clause->condition) return clause;
            const RowContext context = structuredRelationContext(
                targetValues, targetSchema, targetQualifier, sources, rows);
            ExprEvaluator evaluator;
            evaluator.setCurrentDB(s.currentDB);
            if (structuredPredicateMatches(clause->condition.get(), context,
                                           evaluator, evaluationFailed)) {
                return clause;
            }
            if (evaluationFailed) return nullptr;
        }
        return nullptr;
    };

    std::map<std::string, SqlRow> pendingUpdates;
    std::set<std::string> pendingDeletes;
    std::vector<SqlRow> pendingInserts;
    std::vector<bool> targetModified(targetRows.size(), false);
    size_t expectedUpdates = 0;
    size_t expectedDeletes = 0;

    auto planTargetAction = [&](
        size_t targetIndex, const MergeStmt::WhenClause* clause,
        const std::vector<const std::map<std::string, std::string>*>& rows) {
        if (!clause || lower(clause->action) == "do nothing") return true;
        if (targetModified[targetIndex]) {
            std::cout << "ERROR: MERGE command cannot affect row a second time "
                         "(SQLSTATE 21000)" << std::endl;
            return false;
        }
        targetModified[targetIndex] = true;
        const std::string action = lower(clause->action);
        const std::string key = rowValueKey(targetRows[targetIndex]);
        if (action == "delete") {
            if (pendingUpdates.count(key) != 0) {
                std::cout << "ERROR: MERGE cannot distinguish duplicate target "
                             "rows with different actions (SQLSTATE 0A000)"
                          << std::endl;
                return false;
            }
            pendingDeletes.insert(key);
            ++expectedDeletes;
            return true;
        }
        if (action != "update") return false;
        if (pendingDeletes.count(key) != 0) {
            std::cout << "ERROR: MERGE cannot distinguish duplicate target "
                         "rows with different actions (SQLSTATE 0A000)"
                      << std::endl;
            return false;
        }
        SqlRow updates;
        for (const auto& [rawColumn, expression] : clause->updateSet) {
            SqlCell value;
            if (!evaluateStructuredSqlExpression(
                    expression.get(), targetRows[targetIndex], targetSchema,
                    targetQualifier, sources, rows, s.currentDB, value)) {
                evaluationFailed = true;
                return false;
            }
            updates[identifier(rawColumn)] = std::move(value);
        }
        const auto [it, inserted] = pendingUpdates.emplace(key, updates);
        if (!inserted && it->second != updates) {
            std::cout << "ERROR: MERGE cannot distinguish duplicate target "
                         "rows with different UPDATE values (SQLSTATE 0A000)"
                      << std::endl;
            return false;
        }
        ++expectedUpdates;
        return true;
    };

    // Candidate change rows are classified from the immutable pre-statement
    // snapshots. A target may be changed at most once, but a source row is
    // allowed to match several distinct target rows.
    for (size_t sourceIndex = 0; sourceIndex < sourceRows.size(); ++sourceIndex) {
        if (sourceMatches[sourceIndex].empty()) {
            const std::map<std::string, std::string> emptyTarget;
            const auto* clause = chooseClause(byTargetClauses, emptyTarget,
                                              sourceRows[sourceIndex]);
            if (evaluationFailed) break;
            if (!clause || lower(clause->action) == "do nothing") continue;
            SqlRow values;
            for (const auto& [rawColumn, expression] : clause->insertCols) {
                SqlCell value;
                if (!evaluateStructuredSqlExpression(
                        expression.get(), emptyTarget, noTargetSchema, "",
                        sources, sourceRows[sourceIndex], s.currentDB, value)) {
                    evaluationFailed = true;
                    break;
                }
                values[identifier(rawColumn)] = std::move(value);
            }
            if (evaluationFailed) break;
            pendingInserts.push_back(std::move(values));
            continue;
        }
        for (const size_t targetIndex : sourceMatches[sourceIndex]) {
            const auto* clause = chooseClause(
                matchedClauses, targetRows[targetIndex], sourceRows[sourceIndex]);
            if (evaluationFailed ||
                !planTargetAction(targetIndex, clause, sourceRows[sourceIndex])) {
                if (!evaluationFailed) return true;
                break;
            }
        }
        if (evaluationFailed) break;
    }
    if (!evaluationFailed) {
        const std::vector<const std::map<std::string, std::string>*> noSourceRows;
        for (size_t targetIndex = 0; targetIndex < targetRows.size(); ++targetIndex) {
            if (!targetMatches[targetIndex].empty()) continue;
            const auto* clause = chooseClause(
                bySourceClauses, targetRows[targetIndex], noSourceRows);
            if (evaluationFailed ||
                !planTargetAction(targetIndex, clause, noSourceRows)) {
                if (!evaluationFailed) return true;
                break;
            }
        }
    }
    if (evaluationFailed) {
        std::cout << "ERROR: MERGE WHEN expression evaluation failed "
                     "(SQLSTATE 22023)" << std::endl;
        return true;
    }

    std::vector<SqlRow> deletedRows;
    std::vector<SqlRow> updatedRows;
    std::vector<SqlRow> insertedRows;
    std::vector<StorageEngine::UpdateRowImage> updateImages;
    size_t deleted = 0;
    if (!pendingDeletes.empty()) {
        const StorageEngine::SqlDeleteMatcher matcher = [pendingDeletes](
            const SqlRow& oldValues) {
            return pendingDeletes.count(rowValueKey(logicalValues(oldValues))) != 0;
        };
        const DBStatus status = g_engine.removeRows(
            s.currentDB, targetTable, {}, &deletedRows, matcher, &deleted);
        if (status != DBStatus::OK) return mutationFailure("DELETE", status);
        if (deleted != expectedDeletes) {
            std::cout << "ERROR: MERGE target row changed during execution "
                         "(SQLSTATE 40001)" << std::endl;
            return true;
        }
    }

    size_t updated = 0;
    if (!pendingUpdates.empty()) {
        const StorageEngine::SqlUpdateResolver resolver = [pendingUpdates](
            const SqlRow& oldValues, SqlRow& effectiveUpdates) {
            const auto it = pendingUpdates.find(
                rowValueKey(logicalValues(oldValues)));
            if (it == pendingUpdates.end()) return false;
            effectiveUpdates = it->second;
            return true;
        };
        const StorageEngine::SqlUpdateMatcher matcher = [pendingUpdates](
            const SqlRow& oldValues) {
            return pendingUpdates.count(
                rowValueKey(logicalValues(oldValues))) != 0;
        };
        const DBStatus status = g_engine.updateRows(
            s.currentDB, targetTable, {}, {}, &updatedRows, resolver, matcher,
            &updated, stmt.returning.empty() ? nullptr : &updateImages);
        if (status != DBStatus::OK) return mutationFailure("UPDATE", status);
        if (updated != expectedUpdates) {
            std::cout << "ERROR: MERGE target row changed during execution "
                         "(SQLSTATE 40001)" << std::endl;
            return true;
        }
    }

    for (const auto& values : pendingInserts) {
        const DBStatus status = g_engine.insertRow(
            s.currentDB, targetTable, values, &insertedRows);
        if (status != DBStatus::OK) return mutationFailure("INSERT", status);
    }

    std::vector<ReturningRowImage> returningImages;
    returningImages.reserve(deletedRows.size() + updateImages.size() +
                            insertedRows.size());
    for (const auto& row : deletedRows) {
        returningImages.push_back(
            {row, {}, ReturningProjection::Source::Old, "DELETE"});
    }
    for (const auto& row : updateImages) {
        returningImages.push_back(
            {row.oldRow, row.newRow,
             ReturningProjection::Source::New, "UPDATE"});
    }
    for (const auto& row : insertedRows) {
        returningImages.push_back(
            {{}, row, ReturningProjection::Source::New, "INSERT"});
    }
    if (!stmt.returning.empty() &&
        !publishReturning(returningProjections, targetSchema,
                          mergeReturningBinding, s.currentDB,
                          returningImages, "MERGE")) {
        clearLastDmlResult();
        std::cout << "ERROR: MERGE RETURNING expression evaluation failed "
                     "(SQLSTATE 22023)" << std::endl;
        return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: MERGE transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }

    const size_t inserted = insertedRows.size();
    const size_t affected = deleted + updated + inserted;
    if (affected > 0) g_engine.analyzeTable(s.currentDB, targetTable);
    if (!stmt.returning.empty()) {
        printReturningRows(g_lastDmlResult);
    } else {
        publishMutationCount("MERGE", affected);
    }
    std::cout << "MERGE completed: " << updated << " updated, " << deleted
              << " deleted, " << inserted << " inserted" << std::endl;
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
    const std::string targetQualifier = stmt.alias.empty()
        ? unqualifiedRelationName(requestedTable)
        : identifier(stmt.alias);
    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: DELETE USING could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
    }
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
        std::cout << "ERROR: DELETE USING expression evaluation failed "
                     "(SQLSTATE 22023)" << std::endl;
        return true;
    }

    const ReturningBinding deleteReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, stmt.alias);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema,
                                   deleteReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<SqlRow> deletedRows;
    const StorageEngine::SqlDeleteMatcher deleteMatcher = [matchedTargets](
        const SqlRow& oldValues) {
        return matchedTargets.find(rowValueKey(logicalValues(oldValues))) !=
            matchedTargets.end();
    };
    size_t affectedRows = 0;
    const DBStatus status = g_engine.removeRows(
        s.currentDB, resolvedTable, {},
        stmt.returning.empty() ? nullptr : &deletedRows, deleteMatcher,
        &affectedRows);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: DELETE USING failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, targetSchema, deleteReturningBinding,
                s.currentDB, deletedReturningImages(deletedRows),
                "DELETE")) return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: DELETE USING transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    if (affectedRows > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    std::cout << "Delete done" << std::endl;
    if (!stmt.returning.empty()) printReturningRows(g_lastDmlResult);
    else publishMutationCount("DELETE", affectedRows);
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
    const std::string targetQualifier = stmt.alias.empty()
        ? unqualifiedRelationName(requestedTable)
        : identifier(stmt.alias);
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

    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: DELETE USING could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
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
        std::cout << "ERROR: DELETE USING expression evaluation failed "
                     "(SQLSTATE 22023)" << std::endl;
        return true;
    }

    const ReturningBinding deleteReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, stmt.alias);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, targetSchema,
                                   deleteReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<SqlRow> deletedRows;
    const StorageEngine::SqlDeleteMatcher deleteMatcher = [matchedTargets](
        const SqlRow& oldValues) {
        return matchedTargets.find(rowValueKey(logicalValues(oldValues))) !=
            matchedTargets.end();
    };
    size_t affectedRows = 0;
    const DBStatus status = g_engine.removeRows(
        s.currentDB, resolvedTable, {},
        stmt.returning.empty() ? nullptr : &deletedRows, deleteMatcher,
        &affectedRows);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: DELETE USING failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, targetSchema, deleteReturningBinding,
                s.currentDB, deletedReturningImages(deletedRows),
                "DELETE")) return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: DELETE USING transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    if (affectedRows > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    std::cout << "Delete done" << std::endl;
    if (!stmt.returning.empty()) printReturningRows(g_lastDmlResult);
    else publishMutationCount("DELETE", affectedRows);
    return false;
}

bool executeDelete(const DeleteStmt& stmt, Session& s, bool& fallback) {
    fallback = false;
    if (!stmt.whereCurrentOf.empty()) {
        std::cout << "ERROR: WHERE CURRENT OF is not supported for DELETE "
                     "(SQLSTATE 0A000)" << std::endl;
        return true;
    }
    if (stmt.usingClause) return executeDeleteUsing(stmt, s, fallback);
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
    const ReturningBinding deleteReturningBinding = returningBinding(
        stmt.returningOptions, requestedTable, stmt.alias);
    std::vector<ReturningProjection> returningProjections;
    if (!stmt.returning.empty() &&
        !buildReturningProjections(stmt.returning, table,
                                   deleteReturningBinding,
                                   returningProjections)) {
        fallback = true;
        return false;
    }
    std::vector<SqlRow> deletedRows;
    DmlStatementScope statementScope(g_engine, s.currentDB);
    if (!statementScope.ready()) {
        std::cout << "ERROR: DELETE could not establish an atomic statement "
                     "boundary (SQLSTATE 58030)" << std::endl;
        return true;
    }
    size_t affectedRows = 0;
    const DBStatus status = g_engine.removeRows(
        s.currentDB, resolvedTable, conditions,
        stmt.returning.empty() ? nullptr : &deletedRows,
        StorageEngine::SqlDeleteMatcher{}, &affectedRows);
    if (status != DBStatus::OK) {
        std::cout << "Delete failed" << std::endl;
        return true;
    }
    if (!stmt.returning.empty()) {
        if (!publishReturning(
                returningProjections, table, deleteReturningBinding,
                s.currentDB, deletedReturningImages(deletedRows),
                "DELETE")) return true;
    }
    if (!statementScope.finish()) {
        clearLastDmlResult();
        std::cout << "ERROR: DELETE transaction finish failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    g_engine.analyzeTable(s.currentDB, resolvedTable);
    std::cout << "Delete done" << std::endl;
    if (!stmt.returning.empty()) printReturningRows(g_lastDmlResult);
    else publishMutationCount("DELETE", affectedRows);
    return false;
}

} // namespace

bool tryDmlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled, const std::string& rawSql) {
    handled = false;
    if (parsedCmd != SqlCommand::Insert && parsedCmd != SqlCommand::Update &&
        parsedCmd != SqlCommand::Delete && parsedCmd != SqlCommand::Merge) return false;
    clearLastDmlResult();

    SQLParser parser;
    // sql is normalized by the legacy entry point and may have changed the
    // case of string literals.  Parse the original text whenever available so
    // AST execution preserves user data exactly.  Typed literals
    // (DATE 'x', TIMESTAMP 'x', TIME 'x') are normalized away first: the
    // prefix is storage-redundant and the VALUES tokenizer counts the
    // inner space as a value separator.
    std::string parseInput = rawSql.empty() ? sql : rawSql;
    {
        static const char* tlKw[] = { "timestamp", "date", "time" };
        for (const char* kw : tlKw) {
            const size_t kl = std::strlen(kw);
            std::string low;
            for (char c : parseInput)
                low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            size_t pos = 0;
            while (true) {
                size_t hit = std::string::npos;
                for (size_t i = pos; i + kl + 1 < parseInput.size(); ++i) {
                    if (low.compare(i, kl, kw) != 0) continue;
                    if (i > 0 && (std::isalnum(static_cast<unsigned char>(parseInput[i - 1])) ||
                                  parseInput[i - 1] == '_')) continue;
                    const size_t ae = i + kl;
                    if (ae < parseInput.size() &&
                        (std::isalnum(static_cast<unsigned char>(parseInput[ae])) ||
                         parseInput[ae] == '_')) continue;
                    size_t q = ae;
                    while (q < parseInput.size() &&
                           std::isspace(static_cast<unsigned char>(parseInput[q]))) ++q;
                    if (q < parseInput.size() && parseInput[q] == 39) { hit = i; break; }
                }
                if (hit == std::string::npos) break;
                size_t q2 = hit + kl;
                while (q2 < parseInput.size() &&
                       std::isspace(static_cast<unsigned char>(parseInput[q2]))) ++q2;
                parseInput = parseInput.substr(0, hit) + parseInput.substr(q2);
                pos = hit + 1;
                low.clear();
                for (char c : parseInput)
                    low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
        }
    }
    const ParseResult parsed = parser.parse(parseInput);
    if (!parsed.success || !parsed.stmt) {
        handled = true;
        const char* statementName = parsedCmd == SqlCommand::Update ? "UPDATE" :
                                    parsedCmd == SqlCommand::Delete ? "DELETE" :
                                    parsedCmd == SqlCommand::Merge ? "MERGE" : "INSERT";
        std::cout << "SQL syntax error: "
                  << (parsed.error.empty() ? std::string("invalid ") + statementName + " statement"
                                            : parsed.error)
                  << std::endl;
        return true;
    }

    // Read-only transactions may mutate session-local temporary relations,
    // but reject durable writes before triggers, sequences, or heap changes.
    // Keeping this at the typed dispatch edge covers every supported DML
    // shape and provides PostgreSQL's dedicated SQLSTATE.
    if (g_engine.isReadOnly()) {
        std::string target;
        if (const auto* stmt = dynamic_cast<const InsertStmt*>(parsed.stmt.get())) {
            target = identifier(stmt->tableName);
        } else if (const auto* stmt = dynamic_cast<const UpdateStmt*>(parsed.stmt.get())) {
            target = identifier(stmt->tableName);
        } else if (const auto* stmt = dynamic_cast<const DeleteStmt*>(parsed.stmt.get())) {
            target = identifier(stmt->tableName);
        } else if (const auto* stmt = dynamic_cast<const MergeStmt*>(parsed.stmt.get())) {
            target = identifier(stmt->targetTable);
        }
        if (!target.empty() && !isTempTable(s, target)) {
            handled = true;
            std::cout << "ERROR: cannot execute " << parsed.stmt->toString()
                      << " in a read-only transaction (SQLSTATE 25006)"
                      << std::endl;
            return true;
        }
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
    } else if (parsedCmd == SqlCommand::Delete) {
        const auto* stmt = dynamic_cast<const DeleteStmt*>(parsed.stmt.get());
        if (!stmt || stmt->only) return false;
        error = executeDelete(*stmt, s, fallback);
    } else {
        const auto* stmt = dynamic_cast<const MergeStmt*>(parsed.stmt.get());
        if (!stmt) {
            std::cout << "SQL syntax error: invalid MERGE statement" << std::endl;
            handled = true;
            return true;
        }
        error = executeMerge(*stmt, s, fallback);
    }
    if (fallback) {
        bool versionedReturning = false;
        bool aliasedTargetReturning = false;
        if (const auto* insert = dynamic_cast<const InsertStmt*>(
                parsed.stmt.get())) {
            versionedReturning = usesVersionedReturning(
                insert->returning, insert->returningOptions);
        } else if (const auto* update = dynamic_cast<const UpdateStmt*>(
                       parsed.stmt.get())) {
            versionedReturning = usesVersionedReturning(
                update->returning, update->returningOptions);
            aliasedTargetReturning = !update->alias.empty() &&
                !update->returning.empty();
        } else if (const auto* deleteStmt = dynamic_cast<const DeleteStmt*>(
                       parsed.stmt.get())) {
            versionedReturning = usesVersionedReturning(
                deleteStmt->returning, deleteStmt->returningOptions);
            aliasedTargetReturning = !deleteStmt->alias.empty() &&
                !deleteStmt->returning.empty();
        } else if (const auto* merge = dynamic_cast<const MergeStmt*>(
                       parsed.stmt.get())) {
            versionedReturning = usesVersionedReturning(
                merge->returning, merge->returningOptions);
        }
        if (versionedReturning || aliasedTargetReturning) {
            handled = true;
            std::cout << "ERROR: unsupported versioned or aliased RETURNING expression "
                         "(SQLSTATE 0A000)" << std::endl;
            return true;
        }
        if (parsedCmd == SqlCommand::Update) {
            const auto* update = dynamic_cast<const UpdateStmt*>(parsed.stmt.get());
            if (update && update->fromClause) {
                handled = true;
                std::cout << "ERROR: unsupported UPDATE ... FROM shape "
                             "(SQLSTATE 0A000)" << std::endl;
                return true;
            }
        }
        if (parsedCmd == SqlCommand::Delete) {
            const auto* deleteStmt = dynamic_cast<const DeleteStmt*>(parsed.stmt.get());
            if (deleteStmt && deleteStmt->usingClause) {
                handled = true;
                std::cout << "ERROR: unsupported DELETE ... USING shape "
                             "(SQLSTATE 0A000)" << std::endl;
                return true;
            }
        }
        if (parsedCmd == SqlCommand::Insert) {
            const auto* insert = dynamic_cast<const InsertStmt*>(parsed.stmt.get());
            if (insert && !insert->override_.empty()) {
                handled = true;
                std::cout << "ERROR: this INSERT shape is not supported with "
                             "OVERRIDING (SQLSTATE 0A000)" << std::endl;
                return true;
            }
        }
        handled = false;
        return false;
    }
    handled = true;
    return error;
}

} // namespace dbms
