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
    const std::string name = "projection_subquery_syntax";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    for (const std::string tableName : {"outer_rows", "inner_rows", "quoted rows"}) {
        dbms::TableSchema table;
        table.tablename = tableName;
        table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        table.append(dbms::makeIntColumn("id", false, 4, true));
        table.append(dbms::makeTextColumn("payload", false));
        assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
        assert(g_engine.insert(db, tableName,
            {{"id", "7"}, {"payload", " from missing where false order by bad limit 0 "}}) ==
            dbms::DBStatus::OK);
    }
    auto project = [&](const std::string& sql, const std::string& function, bool negate = false) {
        dbms::StorageEngine::SelectExpr expr;
        expr.displayName = "value";
        expr.isScalar = true;
        expr.funcName = function;
        expr.funcArgs = {sql};
        if (negate) expr.funcArgs.push_back("not");
        return g_engine.queryExpr(db, "outer_rows", {}, {expr});
    };
    for (const std::string sql : {
        "SELECT 1\nFROM inner_rows\nWHERE id = 7",
        "SELECT ' from missing ' FROM inner_rows WHERE id = 7",
        "select 1 from inner_rows where payload = ' from missing where false order by bad limit 0 '",
        "select 1 from \"quoted rows\" AS q where q.id = 7",
        "SELECT 1\tFROM\tinner_rows\r\nWHERE\t(id + 1) * 2 = 16;"}) {
        assert((project(sql, "exists_sub") == std::vector<std::string>{"t "}));
        assert((project(sql, "exists_sub", true) == std::vector<std::string>{"f "}));
    }
    assert((project("SELECT (id + 1) * 2\nFROM inner_rows\tWHERE id = 7", "subquery") ==
            std::vector<std::string>{"16 "}));
    assert((project("select q.id from \"quoted rows\" AS q;", "subquery") ==
            std::vector<std::string>{"7 "}));
    assert((project("select ' from missing ' from inner_rows", "subquery") ==
            std::vector<std::string>{" from missing  "}));
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[PROJECTION SUBQUERY SYNTAX] passed\n";
}
