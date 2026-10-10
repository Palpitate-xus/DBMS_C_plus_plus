#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    SQLParser parser;
    for(const auto* sql:{"DROP FUNCTION public.","DROP FUNCTION public..f",
        "DROP PROCEDURE .p","DROP ROUTINE ()"})assert(!parser.parseForBinding(sql).success);
    const auto database=testDbPath("drop_function_namespace");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA \"S.X\"",session));
    assert(!ddl.executeSql("CREATE FUNCTION public.f() RETURNS INT LANGUAGE SQL AS $$SELECT 7$$",session));
    assert(!ddl.executeSql("CREATE FUNCTION \"S.X\".f() RETURNS INT LANGUAGE SQL AS $$SELECT 8$$",session));
    assert(g_engine.udfExists(database,"f","S.X") && g_engine.udfExists(database,"f","public"));
    assert(!ddl.executeSql("DROP FUNCTION \"S.X\" . f ()",session));
    assert(!g_engine.udfExists(database,"f","S.X") && g_engine.udfExists(database,"f","public"));
    assert(!ddl.executeSql("CREATE FUNCTION \"S.X\".f() RETURNS INT LANGUAGE SQL AS $$SELECT 8$$",session));
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    assert(!ddl.executeSql("DROP ROUTINE \"S.X\".f()",session));
    assert(!g_engine.udfExists(database,"f","S.X"));
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(g_engine.udfExists(database,"f","S.X") && g_engine.udfExists(database,"f","public"));
    session.searchPath="\"S.X\",public";
    assert(!ddl.executeSql("DROP FUNCTION f",session));
    assert(!g_engine.udfExists(database,"f","S.X") && g_engine.udfExists(database,"f","public"));
    assert(!ddl.executeSql("DROP ROUTINE public.f()",session));
    assert(!g_engine.udfExists(database,"f","public"));
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[DROP FUNCTION NAMESPACE] exact identity, malformed names, path lookup and rollback passed\n";
}
