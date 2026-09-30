#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

static void expectRows(const std::string& database, const std::string& table,
                       std::vector<dbms::StorageEngine::Condition> conditions,
                       size_t expected, const char* label) {
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = table;
    context.selectCols = {"*"};
    context.conds = std::move(conditions);
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    if (!result.ok || result.rows.size() != expected) {
        std::cerr << label << ": expected " << expected << " rows, got "
                  << result.rows.size() << std::endl;
    }
    assert(result.ok && result.rows.size() == expected);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "pk_plan_encoding";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema single;
    single.tablename = "explicit_single";
    single.append(dbms::makeIntColumn("id", false, 4, true));
    single.append(dbms::makeIntColumn("value", false, 4));
    single.pkColIndices = {0};
    assert(g_engine.createTable(database, single) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, single.tablename, {{"id", "1"}, {"value", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, single.tablename, "value") == dbms::DBStatus::OK);
    expectRows(database, single.tablename, {{"=", "id", "1"}}, 1, "explicit PK");
    expectRows(database, single.tablename,
               {{"=", "id", "1"}, {"=", "value", "7"}}, 1, "bitmap PK");

    dbms::TableSchema empty;
    empty.tablename = "empty_inline";
    empty.append(dbms::makeVarCharColumn("id", false, 20, true));
    assert(g_engine.createTable(database, empty) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, empty.tablename, {{"id", ""}}) == dbms::DBStatus::OK);
    expectRows(database, empty.tablename, {{"=", "id", ""}}, 1, "empty inline PK");

    for (const bool explicitColumns : {false, true}) {
        dbms::TableSchema composite;
        composite.tablename = explicitColumns ? "explicit_pair" : "inline_pair";
        composite.append(dbms::makeIntColumn("a", false, 4, true));
        composite.append(dbms::makeIntColumn("b", false, 4, true));
        if (explicitColumns) composite.pkColIndices = {0, 1};
        assert(g_engine.createTable(database, composite) == dbms::DBStatus::OK);
        for (const std::string b : {"2", "3"}) {
            assert(g_engine.insert(database, composite.tablename, {{"a", "1"}, {"b", b}}) ==
                   dbms::DBStatus::OK);
        }
        expectRows(database, composite.tablename, {{"=", "a", "1"}}, 2, "PK prefix");
        expectRows(database, composite.tablename,
                   {{"=", "a", "1"}, {"=", "b", "2"}}, 1, "composite equality");
        assert(g_engine.createIndex(database, composite.tablename, "a") == dbms::DBStatus::OK);
        expectRows(database, composite.tablename, {{"=", "a", "1"}}, 2, "secondary on PK part");
    }
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[PRIMARY KEY PLAN ENCODING] passed" << std::endl;
}
