#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "exists_projection_short_circuit";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    for (const std::string name : {"outer_rows", "inner_rows"}) {
        dbms::TableSchema schema;
        schema.tablename = name;
        schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        schema.append(dbms::makeIntColumn("id", false, 4, true));
        assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, name, {{"id", "1"}}) == dbms::DBStatus::OK);
    }
    assert(g_engine.insert(database, "inner_rows", {{"id", "2"}}) == dbms::DBStatus::OK);

    auto project = [&](const std::string& sql, bool negate) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "exists_value";
        expression.isScalar = true;
        expression.funcName = "exists_sub";
        expression.funcArgs = {sql};
        if (negate) expression.funcArgs.push_back("not");
        return g_engine.queryExpr(database, "outer_rows", {}, {expression});
    };

    const std::string matchedFirst =
        "select 1 from inner_rows where case when id = 1 then true "
        "else 1 / (id - 2) = 0 end";
    for (bool negate : {false, true}) {
        // The first visible row determines the answer. The row-dependent
        // division in the next row must never be evaluated after a match.
        assert((project(matchedFirst, negate) ==
                std::vector<std::string>{negate ? "f " : "t "}));
        assert((project("select 1 from inner_rows where false", negate) ==
                std::vector<std::string>{negate ? "t " : "f "}));
        // Errors encountered before any match still propagate.
        bool rejected = false;
        try {
            (void)project(
                "select 1 from inner_rows where case when id = 1 then false "
                "else 1 / (id - 2) = 0 end", negate);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22012") !=
                std::string::npos;
        }
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[EXISTS PROJECTION SHORT CIRCUIT] passed\n";
}
