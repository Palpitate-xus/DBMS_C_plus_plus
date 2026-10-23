#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "commands/SchemaNamespace.h"
#include "common/DbError.h"
#include "access/IndexFileUtil.h"
#include "permissions.h"
#include <filesystem>
#include <iostream>
#include <map>

extern dbms::StorageEngine g_engine;
using namespace dbms;

int main() {
    TypeRegistry::instance().bootstrap();
    // Both engine startup/recovery phases precede all fixture writes.
    StorageEngine local;
    size_t checked=0,failures=0;
    const auto require=[&](bool good,const std::string& label) {
        ++checked;failures+=!good;
        std::cout<<"SCHEMA_OWNER_OID "<<label<<" PASS="<<good<<std::endl;
    };
    auto& roles=authCatalog();
    const auto role=[&](const std::string& name) {
        PgAuthIdRow row;row.rolname=name;row.rolsuper=true;row.rolcanlogin=true;
        const auto oid=roles.createAuthId(row);require(oid!=INVALID_OID,"create actual role "+name);return oid;
    };
    const auto login=role("schema_owner_login"),selected=role("Selected Mixed Role"),other=role("schema_owner_trap");
    require(login!=selected && selected!=other && login!=other,"roles have distinct actual OIDs");
    require(roles.persistAll(),"persist actual cluster roles");
    const std::string database="schema_owner_sql",localDatabase="schema_owner_explicit";
    require(g_engine.createDatabase(database)==DBStatus::OK,"create SQL database");
    require(local.createDatabase(localDatabase)==DBStatus::OK,"create explicit engine database");
    Session session;session.username="schema_owner_login";session.currentDB=database;session.permission=1;
    Session trap;trap.username="schema_owner_trap";trap.currentRole="schema_owner_trap";
    trap.currentDB=database;trap.permission=1;
    auto* previous=currentSession();setCurrentSession(&trap);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    const auto owner=[&](StorageEngine& engine,const std::string& db,const std::string& name,Oid expected) {
        bool found=false;Oid actual=INVALID_OID;
        for(const auto& row:engine.catalogService().metadataSnapshot(db).namespaces)
            if(row.nspname==name){found=true;actual=row.nspowner;}
        require(found && actual==expected,"actual owner "+db+"."+name+" ="+std::to_string(actual));
    };
    DdlExecutor ddl;
    require(!ddl.executeSql("CREATE SCHEMA default_owner",session),"actual SQL CREATE SCHEMA");
    owner(g_engine,database,"default_owner",login);
    session.currentRole="Selected Mixed Role";
    require(!ddl.executeSql("CREATE SCHEMA \"Selected Schema 界\"",session),"actual SQL explicit existing effective role");
    owner(g_engine,database,"Selected Schema 界",selected);
    require(!ddl.executeSql("CREATE SCHEMA IF NOT EXISTS default_owner",session),"IF NOT EXISTS succeeds");
    owner(g_engine,database,"default_owner",login);
    auto explicitSession=session;explicitSession.currentDB=localDatabase;
    const auto create=[&](const std::string& name,Session* actualSession) {
        const auto result=createSchemaNamespace(local,localDatabase,name,actualSession);
        std::cout<<"SCHEMA_OWNER_DIAGNOSTIC "<<name<<" status="<<static_cast<int>(result.status)
                 <<" error="<<result.error<<std::endl;
        return result.status;
    };
    require(create("explicit_owner",&explicitSession)==DBStatus::OK,
            "explicit native engine with actual SQL session owner");
    owner(local,localDatabase,"explicit_owner",selected);
    require(!g_engine.schemaExists(database,"explicit_owner"),"no schema redirected to global engine");
    require(create("native_unknown",nullptr)==DBStatus::OK,
            "session-free native API retains unknown owner");
    owner(local,localDatabase,"native_unknown",INVALID_OID);
    require(currentSession()==&trap,"ambient session identity unchanged");
    {
        DdlTransaction outer(local,localDatabase,&explicitSession);
        require(outer.enableSnapshotRollback() && outer.begin(),"real outer DDL transaction begins");
        require(create("rolled_back",&explicitSession)==DBStatus::OK,
                "create namespace inside real outer owner");
        owner(local,localDatabase,"rolled_back",selected);
        outer.rollback();
    }
    require(!local.schemaExists(localDatabase,"rolled_back"),"rollback removes actual schema marker");
    bool rolledBackPresent=false;
    for(const auto& row:local.catalogService().metadataSnapshot(localDatabase).namespaces)
        rolledBackPresent=rolledBackPresent || row.nspname=="rolled_back";
    require(!rolledBackPresent,"rollback removes owned namespace catalog row");
    index_file::failNextDirectorySyncForTesting();
    require(create("failed_owner",&explicitSession)==DBStatus::IO_ERROR,
            "one-shot directory publication failure is not success");
    require(!local.schemaExists(localDatabase,"failed_owner"),"failed create leaves no schema marker");
    bool failedPresent=false;
    for(const auto& row:local.catalogService().metadataSnapshot(localDatabase).namespaces)
        failedPresent=failedPresent || row.nspname=="failed_owner";
    require(!failedPresent,"failed create leaves no owner row");
    require(g_engine.catalogService().persistAll() && local.catalogService().persistAll(),"persist both real engine catalogs");
    g_engine.catalogService().evict(database);local.catalogService().evict(localDatabase);
    owner(g_engine,database,"default_owner",login);
    owner(g_engine,database,"Selected Schema 界",selected);
    owner(local,localDatabase,"explicit_owner",selected);
    owner(local,localDatabase,"native_unknown",INVALID_OID);
    require(!g_engine.inTransaction() && !local.inTransaction(),"no transaction owner leaked");
    require(currentSession()==&trap,"final ambient session unchanged");
    std::cout<<"SCHEMA_OWNER_OID_COMPLETE "<<checked<<" FAILURES "<<failures<<std::endl;
    return checked==34 && !failures?0:1;
}
