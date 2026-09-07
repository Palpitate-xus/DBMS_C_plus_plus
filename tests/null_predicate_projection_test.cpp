#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectPredicate(const std::string& database,
                             const std::string& function,
                             const std::string& argument) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {argument};

    const auto rows = g_engine.queryExpr(
        database, "null_predicate_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectNullState(const std::string& database,
                     const std::string& argument,
                     bool isNull) {
    assert(projectPredicate(database, "is_null", argument) ==
           (isNull ? "t" : "f"));
    assert(projectPredicate(database, "is_not_null", argument) ==
           (isNull ? "f" : "t"));
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "null_predicate_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "null_predicate_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("literal_null", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["empty_text"] = "";
    row["literal_null"] = "NULL";
    row["null_text"] = std::nullopt;
    assert(g_engine.insertRow(database, "null_predicate_values", row) ==
           dbms::DBStatus::OK);

    expectNullState(database, "empty_text", false);
    expectNullState(database, "literal_null", false);
    expectNullState(database, "null_text", true);
    expectNullState(database, "''", false);
    expectNullState(database, "'NULL'", false);
    expectNullState(database, "NULL", true);
    expectNullState(database, "upper('null')", false);
    expectNullState(database, "upper(null_text)", true);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[NULL PREDICATE PROJECTION] all passed" << std::endl;
    return 0;
}
