#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/QueryHostProvider.h"
#include "expression/expr_helper.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="function_namespace_provider",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session backend;backend.username="admin";backend.permission=1;backend.currentDB=db;backend.pid=24719;
    auto* previous=currentSession();setCurrentSession(&backend);
    struct Restore{Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA path_first",backend));
    assert(!ddl.executeSql("CREATE SCHEMA \"Path.Second\"",backend));
    assert(!ddl.executeSql("CREATE SEQUENCE path_effects",backend));
    assert(!ddl.executeSql("CREATE FUNCTION public.pg_listening_channels() RETURNS BIGINT LANGUAGE plpgsql AS $$BEGIN RETURN nextval('path_effects'); END;$$",backend));
    assert(!ddl.executeSql("CREATE FUNCTION path_first.pg_listening_channels() RETURNS INT LANGUAGE SQL AS $$SELECT 11$$",backend));
    assert(!ddl.executeSql("CREATE FUNCTION \"Path.Second\".pg_listening_channels() RETURNS INT LANGUAGE SQL AS $$SELECT 22$$",backend));
    assert(!ddl.executeSql("CREATE FUNCTION \"Path.Second.pg_listening_channels\"() RETURNS INT LANGUAGE SQL AS $$SELECT 33$$",backend));
    assert(!ddl.executeSql("CREATE FUNCTION public.unnest(p INT[]) RETURNS INT LANGUAGE SQL AS $$SELECT -9$$",backend));
    assert(!ddl.executeSql("CREATE FUNCTION \"Path.Second\".unnest(p TEXT[]) RETURNS TEXT LANGUAGE SQL AS $$SELECT 'second scalar'::TEXT$$",backend));
    assert(g_engine.getUDF(db,"pg_listening_channels","path_first").returnType=="integer");
    assert(g_engine.getUDF(db,"Path.Second.pg_listening_channels").expression!=
        g_engine.getUDF(db,"pg_listening_channels","Path.Second").expression);
    const auto binding=[&](const std::string& sql,const std::string& type,bool set) {
        auto query=g_engine.prepareBoundQuery(db,sql);
        assert(query.output.size()==1 && query.output[0].type==type);
        const auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());assert(select);
        const auto* call=dynamic_cast<const FunctionCallExpr*>(select->selectList[0].expr.get());assert(call);
        assert(call->setReturning.has_value()==set);
        return query;
    };
    backend.searchPath="public";
    binding("SELECT pg_listening_channels() LIMIT 0","text",true);
    backend.searchPath="public, pg_catalog";
    binding("SELECT pg_listening_channels() WHERE false","bigint",false);
    binding("SELECT pg_catalog.pg_listening_channels()","text",true);
    backend.searchPath="pg_catalog, public";
    binding("SELECT pg_listening_channels()","text",true);
    binding("SELECT public.pg_listening_channels() LIMIT 0","bigint",false);
    binding("SELECT unnest(ARRAY[1,2])","integer",false);
    binding("SELECT unnest(ARRAY['a','b'])","text",true);
    binding("SELECT pg_catalog.unnest(ARRAY[1,2])","integer",true);
    try{(void)g_engine.prepareBoundQuery(db,"SELECT unnest(NULL) LIMIT 0");assert(false);}
    catch(const DbError& error){assert(error.sqlState()=="42725");}
    backend.searchPath="path_missing, \"Path.Second\", path_first, public, pg_catalog";
    binding("SELECT pg_listening_channels()","integer",false);
    binding("SELECT unnest(ARRAY['a','b'])","text",false);
    // Every operation above is static metadata, not an invocation used to
    // discover the result type or an attempt to open the backend provider.
    assert(g_engine.nextval(db,"path_effects")==1);
    auto result=ExprHelper::evalStringWithNulls("pg_listening_channels()",{}, {}, {},db,"admin",&g_engine);
    assert(result.ok && result.value=="22" && result.typeName=="integer");
    result=ExprHelper::evalStringWithNulls("\"Path.Second.pg_listening_channels\"()",{}, {}, {},db,"admin",&g_engine);
    assert(result.ok && result.value=="33");
    auto& notifications=notificationManager();notifications.listen(backend.pid,db,"native_channel");
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT pg_catalog.pg_listening_channels()"));
    auto plan=QueryPlanner::buildPreparedSetReturningPlan(&g_engine,db,query,
        static_cast<SelectStmt*>(query->ast.get()),{}, {}, {},true);
    assert(plan);auto executed=QueryPlanner::executePlanChecked(std::move(plan));executed.throwIfFailed();
    assert((executed.structuredRows==std::vector<std::vector<std::string>>{{"native_channel"}}));
    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(!ddl.executeSql("CREATE FUNCTION path_first.rollback_function() RETURNS INT LANGUAGE SQL AS $$SELECT 44$$",backend));
    assert(g_engine.udfExists(db,"rollback_function","path_first"));
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(!g_engine.udfExists(db,"rollback_function","path_first"));
    notifications.disconnect(backend.pid);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[FUNCTION NAMESPACE PROVIDER] pure canonical metadata, real owner and namespace undo passed\n";
}
