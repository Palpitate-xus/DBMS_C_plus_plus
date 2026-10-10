#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    auto& registry=TypeRegistry::instance();
    const auto* entry=registry.findType("NAME");
    assert(entry && entry->canonicalName=="name" && entry->typlen==64 &&
        entry->typalign=='c' && entry->typstorage=='p' &&
        entry->category==TypeCategory::String && !entry->hasTypeMod);
    assert(registry.normalizeTypeName("NaMe")=="name");
    assert(mapBuiltinTypeNameToOid("name")==19 && mapBuiltinTypeNameToOid("name[]")==1003);
    Column scalar,array;
    assert(registry.resolveColumnType(scalar,"NAME",{},false).empty());
    assert(scalar.dataType=="name" && scalar.dsize==64 && scalar.typeMod==-1 &&
        !scalar.isVariableLength && !scalar.isArray);
    assert(registry.resolveColumnType(array,"name",{},true).empty());
    assert(array.dataType=="name[]" && array.typeMod==-1 && array.isArray);
    assert(!registry.applyTypeMods("name",{"2"}).ok());
    assert(registry.normalizeTypeName("char")=="character");
    assert(registry.findType("\"char\"")->typlen==1);
    const auto database=testDbPath("name_declaration");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE FUNCTION declared_name_value() RETURNS NAME LANGUAGE sql AS $$ SELECT 'kept'::NAME $$",session));
    assert(!ddl.executeSql("CREATE FUNCTION declared_name_identity(x NAME) RETURNS NAME LANGUAGE sql AS $$ SELECT x $$",session));
    assert(!ddl.executeSql("CREATE FUNCTION declared_name_null() RETURNS NAME LANGUAGE sql AS $$ SELECT NULL::NAME $$",session));
    const auto function=g_engine.getUDF(database,"declared_name_identity");
    assert(function.returnType=="name" && function.paramTypes==std::vector<std::string>{"name"});
    const auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,
        "SELECT declared_name_value(),declared_name_identity('input'::NAME),declared_name_null()"));
    assert(query->output.size()==3);
    for(const auto& field:query->output)assert(field.type=="name" && field.typeOid==19);
    const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
        &g_engine,database,query,query->ast.get()));
    result.throwIfFailed();
    assert(result.structuredRowsAvailable && result.structuredRows.size()==1);
    assert(result.structuredRows[0].size()==3 && result.structuredRows[0][0]=="kept" &&
        result.structuredRows[0][1]=="input" && result.structuredNulls.size()==1 &&
        result.structuredNulls[0]==std::vector<bool>({false,false,true}));
    const auto builtins=g_engine.prepareBoundQuery(database,
        "SELECT lower('ABC'),length('a'),abs(2),unnest(ARRAY[1,2])");
    assert(builtins.output.size()==4);
    const std::vector<uint32_t> builtinOids={25,23,23,23};
    for(size_t i=0;i<builtinOids.size();++i)assert(builtins.output[i].typeOid==builtinOids[i]);
    std::cout<<"[NAME DECLARATION] registry, routine parameter/result, OID and NULL controls passed\n";
}
