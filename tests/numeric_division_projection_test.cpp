#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <set>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string testName = "numeric_division_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (n NUMERIC)", session));
    assert(g_engine.insert(database, "t", {{"n", "0.00"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"n", "100000000"}}) == dbms::DBStatus::OK);
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "q";
    expression.isScalar = true;
    expression.funcName = "arith";
    expression.funcArgs = {"n", "/", "7::numeric"};
    const auto rows = g_engine.queryExpr(database, "t", {}, {expression});
    const std::multiset<std::string> actual(rows.begin(), rows.end());
    const std::multiset<std::string> expected{
        "0.00000000000000000000 ", "14285714.285714285714 "};
    assert(actual == expected);
    cleanupTestDb(testName);
    std::cout << "[NUMERIC DIVISION PROJECTION] passed" << std::endl;
}
