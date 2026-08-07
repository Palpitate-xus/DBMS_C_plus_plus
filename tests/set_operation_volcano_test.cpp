// Verify structured UNION/INTERSECT/EXCEPT execution over Volcano plans.

#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

static std::string normalized(const std::string& row) {
    return row.substr(0, row.find_last_not_of(' ') + 1);
}

static dbms::OpPtr planFor(const std::string& db, const std::string& table) {
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = table;
    ctx.selectCols = {"id"};
    return dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
}

static std::vector<std::string> executeSet(const std::string& db,
                                           dbms::SetOperationType type,
                                           bool all) {
    auto plan = dbms::QueryPlanner::buildSetOperationPlan(
        planFor(db, "left_values"), planFor(db, "right_values"), type, all);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    for (auto& row : rows) row = normalized(row);
    return rows;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupTestDb("set_operation_volcano");
    const std::string db = testDbPath("set_operation_volcano");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE left_values (id INT)", session));
    assert(!ddl.executeSql("CREATE TABLE right_values (id INT)", session));
    for (const auto& value : {"1", "1", "2", "3"})
        assert(g_engine.insert(db, "left_values", {{"id", value}}) == dbms::DBStatus::OK);
    for (const auto& value : {"1", "2", "2", "4"})
        assert(g_engine.insert(db, "right_values", {{"id", value}}) == dbms::DBStatus::OK);

    assert(executeSet(db, dbms::SetOperationType::Union, false) ==
           std::vector<std::string>({"1", "2", "3", "4"}));
    assert(executeSet(db, dbms::SetOperationType::Union, true) ==
           std::vector<std::string>({"1", "1", "2", "3", "1", "2", "2", "4"}));
    assert(executeSet(db, dbms::SetOperationType::Intersect, false) ==
           std::vector<std::string>({"1", "2"}));
    assert(executeSet(db, dbms::SetOperationType::Intersect, true) ==
           std::vector<std::string>({"1", "2"}));
    assert(executeSet(db, dbms::SetOperationType::Except, false) ==
           std::vector<std::string>({"3"}));
    assert(executeSet(db, dbms::SetOperationType::Except, true) ==
           std::vector<std::string>({"1", "3"}));

    assert(!ddl.executeSql("CREATE TABLE distinct_values (id INT, grp VARCHAR(8))", session));
    assert(g_engine.insert(db, "distinct_values", {{"id", "1"}, {"grp", "same"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "distinct_values", {{"id", "2"}, {"grp", "same"}}) == dbms::DBStatus::OK);
    dbms::PlanContext distinctCtx;
    distinctCtx.dbname = db;
    distinctCtx.tablename = "distinct_values";
    distinctCtx.selectCols = {"grp"};
    distinctCtx.distinct = true;
    auto distinctRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, distinctCtx));
    assert(distinctRows.size() == 1);

    cleanupTestDb("set_operation_volcano");
    std::cout << "[SETOP-VOLCANO] UNION/INTERSECT/EXCEPT ALL semantics OK" << std::endl;
    return 0;
}
