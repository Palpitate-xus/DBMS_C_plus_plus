#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "query_expr_error_unlock";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "projection_source";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "projection_source", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    auto before = g_engine.getLockManager().lockedTables();
    std::sort(before.begin(), before.end());

    dbms::StorageEngine::SelectExpr invalid;
    invalid.displayName = "invalid";
    invalid.isScalar = true;
    invalid.funcName = "function_that_does_not_exist";
    invalid.funcArgs = {"id"};
    bool rejected = false;
    try {
        (void)g_engine.queryExpr(
            database, "projection_source", {}, {invalid});
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find("SQLSTATE 42883") !=
                   std::string::npos;
    }
    assert(rejected);

    auto after = g_engine.getLockManager().lockedTables();
    std::sort(after.begin(), after.end());
    assert(after == before);
    assert(g_engine.query(database, "projection_source", {}, {"id"}).size() ==
           1);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[QUERY EXPR ERROR UNLOCK] all passed" << std::endl;
    return 0;
}
