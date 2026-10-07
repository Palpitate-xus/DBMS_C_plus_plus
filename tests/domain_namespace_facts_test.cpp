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
    Session session;session.username="Domain.Creator";session.permission=1;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{previous};
    DdlExecutor ddl;int failed=0;
    const auto check=[&](bool value,const std::string& label) {
        std::cout<<"[DOMAIN NAMESPACE FACTS] "<<label<<" "<<(value?"PASS":"FAIL")<<"\n";
        if(!value)++failed;
    };
    for(int which=0;which<4;++which) {
        const auto name="domain_namespace_reject_"+std::to_string(which);
        const auto db=testDbPath(name);cleanupTestDb(name);
        assert(g_engine.createDatabase(db)==DBStatus::OK);session.currentDB=db;
        session.searchPath="public";
        std::string declaration="domain_rejected";
        if(which<2) {
            assert(g_engine.dropSchema(db,"public",false)==DBStatus::OK);
            if(which==1)declaration="public.domain_rejected";
        } else if(which==2) {
            assert(std::filesystem::create_directory(g_engine.dbPath(db)/".schema_directory"));
            declaration="directory.domain_rejected";
        } else session.searchPath="public,,public";
        assert(!g_engine.catalogService().has(db));
        std::string state;
        try {(void)ddl.executeSql("CREATE DOMAIN "+declaration+" AS INT DEFAULT 7",session);}
        catch(const DbError& error){state=error.sqlState();}
        check(state==(which==3?"22023":"3F000"),"actual rejected namespace "+std::to_string(which)+" state="+state);
        check(!g_engine.inTransaction(),"no leaked statement owner "+std::to_string(which));
        check(!std::filesystem::exists(g_engine.dbPath(db)/".domains"),"no domain record "+std::to_string(which));
        check(!g_engine.catalogService().has(db),"no validation bootstrap "+std::to_string(which));
        if(which<2)check(!std::filesystem::exists(g_engine.dbPath(db)/".schema_public"),"dropped public not recreated");
        assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    }
    const auto name="domain_namespace_actual";const auto db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);session.currentDB=db;
    session.searchPath="public";assert(!g_engine.catalogService().has(db));
    check(!ddl.executeSql("CREATE DOMAIN cold_domain AS INT DEFAULT 7",session),"real cold public remains valid");
    check(g_engine.getDomain(db,"public.cold_domain").defaultValue=="7","actual cold domain metadata");
    assert(!ddl.executeSql("CREATE SCHEMA \"Domain.Path\"",session));
    assert(!ddl.executeSql("CREATE SCHEMA \"Domain.Creator\"",session));
    session.searchPath="missing_domain_namespace,\"Domain.Path\",public";
    check(!ddl.executeSql("CREATE DOMAIN chosen_domain AS INT DEFAULT 8",session),"first real decoded namespace");
    check(g_engine.getDomain(db,"\"Domain.Path\".chosen_domain").defaultValue=="8","actual quoted namespace metadata");
    session.searchPath="\"$user\",public";
    check(!ddl.executeSql("CREATE DOMAIN user_domain AS INT DEFAULT 9",session),"username namespace");
    check(g_engine.getDomain(db,"\"Domain.Creator\".user_domain").defaultValue=="9","actual username namespace metadata");
    session.searchPath="";
    check(!ddl.executeSql("CREATE DOMAIN public.explicit_domain AS INT DEFAULT 10",session),"explicit namespace overrides empty path");
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[DOMAIN NAMESPACE FACTS] failed="<<failed<<"\n";
    return failed?1:0;
}
