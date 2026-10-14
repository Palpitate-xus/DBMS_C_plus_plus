#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="abs_integer_bounds",database=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE empty_rows(i INT)",session));
    assert(!ddl.executeSql("CREATE SCHEMA abs_user",session));
    assert(!ddl.executeSql("CREATE SEQUENCE abs_effects",session));
    assert(!ddl.executeSql("CREATE FUNCTION abs_user.effect() RETURNS BIGINT LANGUAGE SQL VOLATILE AS $$SELECT nextval('public.abs_effects')$$",session));
    assert(!ddl.executeSql("CREATE FUNCTION abs_user.abs(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i$$",session));
    size_t checks=0,failures=0,children=0;
    const auto plan=[&](const std::string& sql,const std::string& state) {
        std::string actual;
        try {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
            PreparedQueryExecution execution(query,&g_engine,database);
            execution.setQueryExecutor([&](const Stmt*,const RowContext&,size_t)->PreparedQueryRows {
                ++children;throw DbError("XX000","constant planning opened a child");
            });
            execution.setChildCursorFactory([&](const Stmt*,const RowContext&)->std::unique_ptr<PreparedQueryCursor> {
                ++children;throw DbError("XX000","constant planning constructed a child cursor");
            });
            execution.planStatementConstants(query->ast.get());
        }catch(const DbError& error){actual=error.sqlState();}
        ++checks;failures+=actual!=state;
        std::cout<<"ABS_NATIVE_PLAN "<<sql<<" expected="<<state<<" actual="<<actual<<'\n';
    };
    for(const auto* argument:{"'-32768'::smallint","-2147483648","'-9223372036854775808'::bigint"}) {
        const std::string call=std::string("abs(")+argument+")";
        for(const auto* suffix:{""," WHERE false"," LIMIT 0"," FROM empty_rows"," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})
            plan("SELECT "+call+suffix,"22003");
        plan("SELECT CASE WHEN i>0 THEN "+call+" ELSE 1 END FROM empty_rows","22003");
        plan("SELECT CASE WHEN false THEN "+call+" ELSE 1 END","");
        plan("SELECT COALESCE(1,"+call+")","");
    }
    for(const auto* expression:{"abs('-32767'::smallint)","abs(-2147483647)","abs('-9223372036854775807'::bigint)",
            "abs(NULL::smallint)","abs(NULL::integer)","abs(NULL::bigint)","abs(-1.25::numeric)","abs(abs(-7))",
            "abs('7')","abs(NULL)"})
        plan(std::string("SELECT ")+expression+" FROM empty_rows LIMIT 0","");
    plan("SELECT abs(abs_user.effect()) FROM empty_rows LIMIT 0","");
    plan("SELECT abs((SELECT nextval('abs_effects'))) LIMIT 0","");
    session.searchPath="abs_user,pg_catalog,public";
    plan("SELECT abs(-2147483648)","");
    plan("SELECT abs(-2147483648) FROM empty_rows LIMIT 0","");
    plan("SELECT pg_catalog.abs(-2147483648)","22003");
    assert(children==0 && g_engine.nextval(database,"abs_effects")==1);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"ABS_INTEGER_BOUNDS_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return checks==42 && !failures?0:1;
}
