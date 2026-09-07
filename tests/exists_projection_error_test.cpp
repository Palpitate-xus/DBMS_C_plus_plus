#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    namespace fs = std::filesystem;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "exists_projection_error";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    {
        dbms::StorageEngine writer;
        assert(writer.createDatabase(database, "utf8") == dbms::DBStatus::OK);
        for (const std::string name : {"present_rows", "broken_rows", "empty_rows"}) {
            dbms::TableSchema schema;
            schema.tablename = name;
            schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
            schema.append(dbms::makeIntColumn("id", false, 4, true));
            assert(writer.createTable(database, schema) == dbms::DBStatus::OK);
            if (name != "empty_rows") {
                assert(writer.insert(database, name, {{"id", "1"}}) ==
                       dbms::DBStatus::OK);
            }
        }
    }

    auto project = [&](const std::string& sql, bool negate) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "exists_value";
        expression.isScalar = true;
        expression.funcName = "exists_sub";
        expression.funcArgs = {sql};
        if (negate) expression.funcArgs.push_back("not");
        return g_engine.queryExpr(database, "present_rows", {}, {expression});
    };
    auto expectError = [&](const std::string& sql, bool negate,
                           const std::string& state) {
        bool rejected = false;
        try {
            (void)project(sql, negate);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE " + state) !=
                std::string::npos;
        }
        assert(rejected);
    };

    const auto missingHeap = fs::path(database) / "missing_rows.dt";
    for (bool negate : {false, true}) {
        expectError("select 1 from missing_rows where true", negate, "42P01");
        assert(!fs::exists(missingHeap));
    }

    // A directory at the heap path makes the scan fail independently of
    // filesystem permissions, including when the test runs as root.
    const auto brokenHeap = fs::path(database) / "broken_rows.dt";
    fs::rename(brokenHeap, fs::path(database) / "saved_heap.dt");
    fs::create_directory(brokenHeap);
    for (bool negate : {false, true}) {
        expectError("select 1 from broken_rows", negate, "58030");
    }

    assert((project("select 1 from empty_rows", false) ==
            std::vector<std::string>{"f "}));
    assert((project("select 1 from empty_rows", true) ==
            std::vector<std::string>{"t "}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[EXISTS PROJECTION ERRORS] passed\n";
}
