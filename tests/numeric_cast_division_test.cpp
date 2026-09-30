#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string testName = "numeric_cast_division";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT,n NUMERIC)", session));
    assert(g_engine.insert(database, "t", {{"id", "1"}, {"n", "100000000"}}) == dbms::DBStatus::OK);
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "q";
    expression.isScalar = true;
    expression.funcName = "arith";
    // The SQL frontend lowers postfix operand casts to CAST(...) calls.
    expression.funcArgs = {"cast(id as numeric)", "/", "7"};
    assert(g_engine.queryExpr(database, "t", {}, {expression}) ==
        std::vector<std::string>{"0.14285714285714285714 "});
    expression.funcArgs = {"cast(n as numeric)", "/", "7"};
    assert(g_engine.queryExpr(database, "t", {}, {expression}) ==
        std::vector<std::string>{"14285714.285714285714 "});
    expression.funcArgs = {"cast(id as bigint)", "/", "7"};
    assert(g_engine.queryExpr(database, "t", {}, {expression}) ==
        std::vector<std::string>{"0 "});
    cleanupTestDb(testName);
    std::cout << "[NUMERIC CAST DIVISION] passed" << std::endl;
}
