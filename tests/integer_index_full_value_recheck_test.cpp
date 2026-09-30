#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("integer_index_full_value_recheck");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "integers";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "integers", {{"id", "0"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "integers", {{"id", "1"}}) == dbms::DBStatus::OK);
    const auto lookup = [&](const std::string& name, const std::string& key, size_t expected) {
        auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<dbms::IndexScanOp>(&g_engine, db, name, "id", key));
        assert(result.ok && result.rows.size() == expected);
        dbms::PlanContext ctx;
        ctx.dbname = db;
        ctx.tablename = name;
        ctx.selectCols = {"id"};
        ctx.conds = {{"=", "id", key}};
        result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
        assert(result.ok && result.rows.size() == expected);
    };
    lookup("integers", "1.0", 1);
    lookup("integers", "1e0", 1);
    lookup("integers", "+0001", 1);
    lookup("integers", "-0.000", 1);
    lookup("integers", "1.5", 0);
    lookup("integers", "1.000000000000000000001", 0);
    table.tablename = "bigints";
    table.cols[0] = dbms::makeIntColumn("id", false, 3, true);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "bigints", {{"id", "9007199254740992"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "bigints", {{"id", "9007199254740993"}}) == dbms::DBStatus::OK);
    lookup("bigints", "9007199254740992.0", 1);
    lookup("bigints", "9007199254740993.000", 1);
    lookup("bigints", "9007199254740993.5", 0);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[INTEGER INDEX FULL VALUE RECHECK] passed\n";
}
