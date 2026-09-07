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
    const std::string testName = "scalar_subquery_projection_shape";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "shape_rows";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, schema.tablename, {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    auto project = [&](const std::string& target) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "value";
        expression.isScalar = true;
        expression.funcName = "subquery";
        expression.funcArgs = {"select " + target + " from shape_rows s"};
        return g_engine.queryExpr(database, schema.tablename, {}, {expression});
    };
    assert((project("'a,b'") == std::vector<std::string>{"a,b "}));
    assert((project("'a''b,c'") == std::vector<std::string>{"a'b,c "}));
    assert((project("'('") == std::vector<std::string>{"( "}));
    assert((project("coalesce(id,0)") == std::vector<std::string>{"1 "}));

    for (const std::string target : {"'(',id", "'a,b',id", "id,id"}) {
        bool rejected = false;
        try {
            (void)project(target);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 42601") !=
                std::string::npos;
        }
        assert(rejected);
    }

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SCALAR SUBQUERY PROJECTION SHAPE] passed\n";
}
