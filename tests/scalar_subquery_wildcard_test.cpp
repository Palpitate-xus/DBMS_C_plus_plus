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
    const std::string testName = "scalar_subquery_wildcard";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema outer;
    outer.tablename = "outer_rows";
    outer.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    outer.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, outer) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, outer.tablename, {{"id", "1"}}) == dbms::DBStatus::OK);
    for (const std::string name : {"single_rows", "empty_rows", "wide_rows"}) {
        dbms::TableSchema inner;
        inner.tablename = name;
        inner.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        inner.append(dbms::makeIntColumn("value", true, 4));
        if (name == "wide_rows") inner.append(dbms::makeIntColumn("other", true, 4));
        assert(g_engine.createTable(database, inner) == dbms::DBStatus::OK);
    }
    assert(g_engine.insert(database, "single_rows", {{"value", "7"}}) == dbms::DBStatus::OK);

    auto project = [&](const std::string& sql) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "value";
        expression.isScalar = true;
        expression.funcName = "subquery";
        expression.funcArgs = {sql};
        return g_engine.queryExpr(database, outer.tablename, {}, {expression});
    };
    for (const std::string sql : {
             "select * from single_rows",
             "select * from single_rows where true",
             "select single_rows.* from single_rows",
             "select s.* from single_rows AS s"}) {
        assert((project(sql) == std::vector<std::string>{"7 "}));
    }
    assert((project("select outer_rows.* from single_rows s") ==
            std::vector<std::string>{"1 "}));
    assert((project("select outer_rows.* from single_rows outer_rows") ==
            std::vector<std::string>{"7 "}));
    assert((project("select '*' from single_rows") == std::vector<std::string>{"* "}));
    assert((project("select * from empty_rows") == std::vector<std::string>{"NULL "}));

    auto expectError = [&](const std::string& sql, const std::string& state) {
        bool rejected = false;
        try {
            (void)project(sql);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE " + state) != std::string::npos;
        }
        assert(rejected);
    };
    expectError("select * from wide_rows", "42601");
    expectError("select w.* from wide_rows w where false", "42601");
    expectError("select *,1 from single_rows", "42601");
    expectError("select missing.* from single_rows", "42P01");
    expectError("select single_rows.* from single_rows s", "42P01");

    assert(g_engine.updateRows(database, "single_rows", {{"value", std::nullopt}}, {}) ==
           dbms::DBStatus::OK);
    assert((project("select * from single_rows") == std::vector<std::string>{"NULL "}));
    assert(g_engine.insert(database, "single_rows", {{"value", "8"}}) == dbms::DBStatus::OK);
    expectError("select * from single_rows", "21000");
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SCALAR SUBQUERY WILDCARD] passed\n";
}
