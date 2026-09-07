#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "scalar_subquery_missing_relation";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "present_rows";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, schema.tablename, {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(std::filesystem::exists(
        std::filesystem::path(database) / "present_rows.dt"));
    const auto missingHeap =
        std::filesystem::path(database) / "missing_rows.dt";

    for (const std::string sql : {
             "select id from missing_rows where true",
             "select m.id from missing_rows m"}) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "value";
        expression.isScalar = true;
        expression.funcName = "subquery";
        expression.funcArgs = {sql};
        assert(!g_engine.tableExists(database, "missing_rows"));
        assert(!std::filesystem::exists(missingHeap));
        bool rejected = false;
        try {
            (void)g_engine.queryExpr(database, schema.tablename, {}, {expression});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 42P01") !=
                std::string::npos;
        }
        // A read of a missing relation must not create an orphan heap file.
        assert(!std::filesystem::exists(missingHeap));
        assert(rejected);
    }

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SCALAR SUBQUERY MISSING RELATION] passed\n";
}
