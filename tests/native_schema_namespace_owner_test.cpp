#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    Session session;session.username="schema_owner";session.permission=1;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;int failed=0;
    const auto check=[&](bool good,const std::string& label) {
        std::cout<<"[NATIVE SCHEMA OWNER] "<<label<<' '<<(good?"PASS":"FAIL")<<std::endl;
        if(!good)++failed;
    };
    for(int which=0;which<2;++which) {
        const auto name="native_schema_loaded_"+std::to_string(which);
        const auto database=testDbPath(name);cleanupTestDb(name);
        assert(g_engine.createDatabase(database)==DBStatus::OK);session.currentDB=database;
        const std::string schema=which==0?"public":"owner_schema";
        if(which==1)assert(!ddl.executeSql("CREATE SCHEMA owner_schema",session));
        auto& catalog=g_engine.catalogService().get(database);
        assert(catalog.findNamespaceByName(schema));assert(catalog.persistAll());
        check(g_engine.dropSchema(database,schema,false)==DBStatus::OK,"actual empty native DROP "+schema);
        check(!g_engine.schemaExists(database,schema),"native lookup forgets dropped "+schema);
        const auto snapshot=g_engine.catalogService().metadataSnapshot(database);
        bool remains=false;for(const auto& row:snapshot.namespaces)remains=remains || row.nspname==schema;
        check(!remains,"actual loaded catalog forgets dropped "+schema);
        session.searchPath=schema;
        std::string state;
        try{(void)ddl.executeSql("CREATE DOMAIN rejected_domain AS INT",session);}
        catch(const DbError& error){state=error.sqlState();}
        check(state=="3F000","actual DOMAIN rejected after loaded native DROP state="+state);
        check(!std::filesystem::exists(g_engine.dbPath(database)/".domains"),"no domain side effect after DROP");
        state.clear();
        try{(void)ddl.executeSql("CREATE FUNCTION rejected_function() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$",session);}
        catch(const DbError& error){state=error.sqlState();}
        check(state=="3F000","actual function rejected after loaded native DROP state="+state);
        check(g_engine.getUDFNames(database).empty(),"no routine side effect after DROP");
        check(!g_engine.udfExists(database,"rejected_function",schema),"actual explicit-schema routine absent after DROP");
        check(!g_engine.inTransaction(),"no leaked declaration owner after DROP");
        assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    }
    {
        const auto name="schema_sql_drop_cold_bootstrap";
        const auto database=testDbPath(name);cleanupTestDb(name);
        assert(g_engine.createDatabase(database)==DBStatus::OK);session.currentDB=database;
        session.searchPath="public";
        assert(!ddl.executeSql("DROP SCHEMA public",session));
        auto snapshot=g_engine.catalogService().metadataSnapshot(database);
        bool remains=false;for(const auto& row:snapshot.namespaces)remains=remains || row.nspname=="public";
        check(!remains,"actual SQL DROP removes loaded catalog namespace");
        check(!std::filesystem::exists(g_engine.dbPath(database)/".schema_public"),"actual SQL DROP removes physical marker");
        g_engine.catalogService().evict(database);
        auto& cold=g_engine.catalogService().get(database);
        check(!cold.findNamespaceByName("public"),"cold catalog bootstrap must not resurrect dropped public");
        check(!g_engine.schemaExists(database,"public"),"cold native lookup respects committed SQL DROP");
        std::string state;
        try{(void)ddl.executeSql("CREATE DOMAIN cold_rejected_domain AS INT",session);}
        catch(const DbError& error){state=error.sqlState();}
        check(state=="3F000","DOMAIN remains rejected after cold bootstrap state="+state);
        check(!std::filesystem::exists(g_engine.dbPath(database)/".domains"),"cold bootstrap causes no domain artifact");
        check(!g_engine.inTransaction(),"cold rejection leaks no owner");
        assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    }
    for(bool cascade:{false,true}) {
        const auto name=cascade?"native_public_cascade":"native_public_restrict";
        const auto database=testDbPath(name);cleanupTestDb(name);
        assert(g_engine.createDatabase(database)==DBStatus::OK);session.currentDB=database;session.searchPath="public";
        assert(!ddl.executeSql("CREATE TABLE schema_items(id INT PRIMARY KEY)",session));
        assert(g_engine.insertRow(database,"schema_items",{{"id","1"}})==DBStatus::OK);
        const auto status=g_engine.dropSchema(database,"public",cascade);
        check(status==(cascade?DBStatus::OK:DBStatus::INVALID_VALUE),cascade?"native CASCADE succeeds":"native RESTRICT rejects real public relation");
        check(g_engine.tableExists(database,"schema_items")==!cascade,cascade?"CASCADE removes actual public heap":"RESTRICT preserves actual public heap");
        check(g_engine.schemaExists(database,"public")==!cascade,cascade?"CASCADE removes actual namespace":"RESTRICT preserves actual namespace");
        const auto snapshot=g_engine.catalogService().metadataSnapshot(database);
        bool hasNamespace=false,hasRelation=false;
        for(const auto& row:snapshot.namespaces)hasNamespace=hasNamespace || row.nspname=="public";
        for(const auto& row:snapshot.relations)hasRelation=hasRelation || row.relname=="schema_items";
        check(hasNamespace==!cascade,"actual catalog namespace after dependency decision");
        check(hasRelation==!cascade,"actual catalog relation after dependency decision");
        if(!cascade && g_engine.tableExists(database,"schema_items"))
            check(g_engine.query(database,"schema_items",{},{"id"}).size()==1,"RESTRICT preserves actual rows");
        check(!g_engine.inTransaction(),"native DROP leaks no transaction owner");
        assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    }
    std::cout<<"[NATIVE SCHEMA OWNER] failed="<<failed<<std::endl;
    return failed?1:0;
}
