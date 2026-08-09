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
#include <set>
#include <string>

extern dbms::StorageEngine g_engine;

namespace dbms {

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

bool supportsInsert(const InsertStmt& stmt) {
    // The bridge owns only ordinary VALUES inserts.  Every feature outside
    // this contract must remain on the established implementation until its
    // AST semantics and RETURNING behavior are migrated together.
    return !stmt.tableName.empty() && stmt.selectSource == nullptr &&
           stmt.conflictAction.empty() && stmt.returning.empty() &&
           stmt.override_.empty() &&
           (stmt.defaultValues || !stmt.values.empty());
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
                   std::string& value) {
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

    if (stmt.defaultValues) {
        if (!stmt.values.empty()) {
            std::cout << "SQL syntax error: invalid DEFAULT VALUES statement"
                      << std::endl;
            return true;
        }
        const DBStatus status = g_engine.insertDefaultValues(
            s.currentDB, resolvedTable, table);
        if (status != DBStatus::OK) {
            std::cout << "INSERT DEFAULT VALUES failed" << std::endl;
            return true;
        }
        std::cout << "INSERT 0 1 (DEFAULT VALUES)" << std::endl;
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

    int inserted = 0;
    for (const auto& values : pendingRows) {
        const DBStatus status = g_engine.insert(s.currentDB, resolvedTable, values);
        if (status == DBStatus::DUPLICATE_KEY) {
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
    if (inserted > 0) g_engine.analyzeTable(s.currentDB, resolvedTable);
    return false;
}

} // namespace

bool tryDmlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled, const std::string& rawSql) {
    handled = false;
    if (parsedCmd != SqlCommand::Insert) return false;

    SQLParser parser;
    // sql is normalized by the legacy entry point and may have changed the
    // case of string literals.  Parse the original text whenever available so
    // AST execution preserves user data exactly.
    const ParseResult parsed = parser.parse(rawSql.empty() ? sql : rawSql);
    const auto* stmt = parsed.stmt
        ? dynamic_cast<const InsertStmt*>(parsed.stmt.get()) : nullptr;
    if (!parsed.success || !stmt) {
        handled = true;
        std::cout << "SQL syntax error: "
                  << (parsed.error.empty() ? "invalid INSERT statement" : parsed.error)
                  << std::endl;
        return true;
    }
    if (!supportsInsert(*stmt)) return false;

    bool fallback = false;
    const bool error = executeInsert(*stmt, s, fallback);
    if (fallback) {
        handled = false;
        return false;
    }
    handled = true;
    return error;
}

} // namespace dbms
