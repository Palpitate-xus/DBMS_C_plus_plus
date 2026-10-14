#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <tuple>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="window_routine_identity",database=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;
    session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore { Session* value;~Restore(){setCurrentSession(value);} } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE source_rows(v INT,t TEXT)",session));
    assert(!ddl.executeSql("CREATE SCHEMA routine_first",session));
    assert(!ddl.executeSql("CREATE SEQUENCE routine_effects",session));
    for(const auto& [routine,arguments,body]:std::vector<std::tuple<std::string,std::string,std::string>>{
            {"scalar_value","i INT","i"},{"array_agg","i INT","i"},
            {"sum","i INT","i"},{"row_number","","7"},{"lag","i INT","i"}})
        assert(!ddl.executeSql("CREATE FUNCTION routine_first."+routine+"("+arguments+
            ") RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT "+body+"$$",session));
    assert(!ddl.executeSql("CREATE FUNCTION routine_first.effect() RETURNS BIGINT LANGUAGE plpgsql "
        "VOLATILE AS $$BEGIN RETURN nextval('public.routine_effects'); END;$$",session));
    size_t controls=0;
    const auto reject=[&](const std::string& expression,const std::string& suffix,const std::string& state) {
        std::string actual;
        try {(void)g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM public.source_rows"+suffix);}
        catch(const DbError& error){actual=error.sqlState();}
        if(actual!=state)std::cerr<<"WINDOW_ROUTINE_NATIVE_FAILURE "<<expression<<suffix
            <<" expected "<<state<<" actual "<<actual<<'\n';
        assert(actual==state);++controls;
    };
    for(const auto* expression:{"upper(t)","pg_catalog.upper(t)","abs(v)","length(t)",
                                "routine_first.scalar_value(v)","routine_first.effect()"})
        for(const auto* suffix:{""," WHERE false"," LIMIT 0"})
            reject(std::string(expression)+" OVER()",suffix,"42809");
    reject("missing_window(v) OVER()","","42883");
    reject("upper(missing) OVER()"," WHERE false","42703");
    for(const auto* suffix:{""," WHERE false"," LIMIT 0"}) {
        reject("CASE WHEN false THEN upper(t) OVER() ELSE 'kept' END",suffix,"42809");
        reject("CASE WHEN true THEN 1 ELSE length(t) OVER() END",suffix,"42809");
    }
    session.searchPath="routine_first, pg_catalog, public";
    for(const auto* expression:{"array_agg(v)","sum(v)","row_number()","lag(v)","scalar_value(v)","effect()"})
        for(const auto* suffix:{""," WHERE false"," LIMIT 0"})
            reject(std::string(expression)+" OVER(ORDER BY v)",suffix,"42809");
    for(const auto& [expression,type,oid]:std::vector<std::tuple<std::string,std::string,uint32_t>>{
            {"pg_catalog.array_agg(v)","integer[]",1007},
            {"\"pg_catalog\".\"array_agg\"(v)","integer[]",1007},
            {"pg_catalog.sum(v)","bigint",20},{"pg_catalog.row_number()","bigint",20},
            {"pg_catalog.lag(v)","integer",23}})
        for(const auto* suffix:{""," WHERE false"," LIMIT 0"}) {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+expression+
                " OVER(ORDER BY v) FROM public.source_rows"+suffix);
            assert(query.output.size()==1 && query.output.front().type==type && query.output.front().typeOid==oid);
            ++controls;
        }
    assert(g_engine.nextval(database,"routine_effects")==1);
    assert(controls==59);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"WINDOW_ROUTINE_IDENTITY_NATIVE_COMPLETE "<<controls
        <<" pure role/namespace/empty-source controls passed\n";
}
