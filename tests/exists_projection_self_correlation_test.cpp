#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "exists_projection_self_correlation";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "self_rows";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("marker", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(database, schema.tablename,
                              {{"id", "1"}, {"marker", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(database, schema.tablename,
                              {{"id", "2"}, {"marker", "second"}}) == dbms::DBStatus::OK);

    auto project = [&](const std::string& sql, bool negate = false) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "exists_value";
        expression.isScalar = true;
        expression.funcName = "exists_sub";
        expression.funcArgs = {sql};
        if (negate) expression.funcArgs.push_back("not");
        dbms::StorageEngine::OrderBySpec order;
        order.colName = "id";
        return g_engine.queryExpr(database, schema.tablename, {}, {expression}, {order});
    };

    const std::string sameId =
        "select 1 from self_rows i where i.id = self_rows.id and i.id = 1";
    assert((project(sameId) == std::vector<std::string>{"t ", "f "}));
    assert((project(sameId, true) == std::vector<std::string>{"f ", "t "}));
    assert((project("select 1 from self_rows AS i where i.marker = self_rows.marker") ==
            std::vector<std::string>{"f ", "t "}));
    assert((project("select 1 from self_rows i where i.id = self_rows.id "
                    "and i.marker is null") == std::vector<std::string>{"t ", "f "}));
    // Bare names still bind to the inner relation; no alias means the inner
    // physical name shadows the outer one as well.
    assert((project("select 1 from self_rows i where id = 1") ==
            std::vector<std::string>{"t ", "t "}));
    assert((project("select 1 from self_rows where self_rows.id = 1") ==
            std::vector<std::string>{"t ", "t "}));
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[EXISTS PROJECTION SELF CORRELATION] passed\n";
}
