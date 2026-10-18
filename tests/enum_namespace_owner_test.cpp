#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;

namespace {
size_t checks=0,failures=0;
void check(bool pass,const std::string& label) {
    ++checks;
    if(!pass){++failures;std::cerr<<"ENUM_NAMESPACE_OWNER_FAIL "<<checks<<' '<<label<<'\n';}
}
std::string ident(const std::string& value) {
    std::string out="\"";for(char ch:value){if(ch=='"')out+='"';out+=ch;}return out+'"';
}
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string database=testDbPath("enum_namespace_owner");
    check(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    Session session;session.username="enum_namespace_owner";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{previous};
    DdlExecutor ddl;
    const auto sql=[&](const std::string& statement) {
        bool pass=false;try{pass=!ddl.executeSql(statement,session);}catch(const std::exception& error){std::cerr<<error.what()<<'\n';}
        check(pass,statement);
    };
    const std::string first="AuxOwnerA",other="aux_owner_b";
    const std::vector<std::string> names={"plain","Quote\"Rank","a.b","name with space","Rank[]","汉"};
    for(const auto& schema:{first,other})sql("CREATE SCHEMA "+ident(schema));
    for(const auto& schema:{first,other})for(const auto& name:names)
        sql("CREATE TYPE "+ident(schema)+"."+ident(name)+" AS ENUM('z','a')");
    const std::string outside=first+".public_survivor";
    sql("CREATE TYPE public."+ident(outside)+" AS ENUM('z','a')");
    StorageEngine::EnumType legacy;legacy.name=other+".legacy_plain";legacy.labels={"z","a"};
    check(g_engine.createEnumType(database,legacy)==DBStatus::OK,"legacy native enum without catalog registration");
    const auto owned=[&](const std::string& schema,const std::string& name) {
        const auto snapshot=g_engine.catalogService().metadataSnapshot(database);
        Oid ns=INVALID_OID;for(const auto& row:snapshot.namespaces)if(row.nspname==schema)ns=row.oid;
        return std::any_of(snapshot.types.begin(),snapshot.types.end(),[&](const auto& row) {
            return row.typtype=='e' && row.typnamespace==ns && row.typname==name;
        });
    };
    const auto sidecar=[&](const std::string& schema,const std::string& name) {
        const auto values=g_engine.getEnumTypeNames(database);
        const auto physical=schema=="public"?name:schema+"."+name;
        return std::find(values.begin(),values.end(),physical)!=values.end();
    };
    const auto pairs=[&](const std::string& schema,bool wanted) {
        for(const auto& name:names) {
            check(owned(schema,name)==wanted,"catalog exact namespace "+schema+"."+name);
            check(sidecar(schema,name)==wanted,"sidecar exact identity "+schema+"."+name);
        }
    };
    pairs(first,true);pairs(other,true);
    check(owned("public",outside) && sidecar("public",outside),"public dotted name is outside target namespace");
    Oid ambiguous=INVALID_OID;
    {
        auto& catalog=g_engine.catalogService().get(database);
        const auto space=catalog.getNamespaceByName(first);
        PgTypeRow shadow;shadow.typname="public_survivor";shadow.typnamespace=space->oid;
        shadow.typtype='e';shadow.typcategory='E';shadow.typlen=4;shadow.typbyval=true;
        ambiguous=catalog.createType(shadow);
        check(ambiguous!=INVALID_OID && catalog.persistAll(),"seed competing canonical auxiliary identity");
    }
    bool ambiguousRejected=false;
    try {(void)g_engine.dropSchema(database,first,true);}
    catch(const DbError& error) {
        ambiguousRejected=error.sqlState()=="XX001" && error.message()=="ambiguous enum auxiliary namespace identity";
    }
    catch(const std::exception&) {}
    check(ambiguousRejected,"ambiguous catalog ownership fails closed before any mutation");
    check(g_engine.schemaExists(database,first) && owned(first,names.front()) &&
          owned("public",outside) && sidecar("public",outside),"ambiguity leaves real owners and storage untouched");
    {
        auto& catalog=g_engine.catalogService().get(database);
        check(catalog.dropType(ambiguous) && catalog.persistAll(),"remove only deliberately seeded competing row");
    }
    const auto drop=[&](const std::string& schema,bool cascade,DBStatus expected) {
        bool pass=false;
        try{pass=g_engine.dropSchema(database,schema,cascade)==expected;}
        catch(const std::exception& error){std::cerr<<error.what()<<'\n';}
        check(pass,std::string(cascade?"CASCADE ":"RESTRICT ")+schema);
        check(!g_engine.inTransaction(),"drop releases statement owner");
    };
    drop(first,false,DBStatus::INVALID_VALUE);
    pairs(first,true);pairs(other,true);
    drop(first,true,DBStatus::OK);
    check(!g_engine.schemaExists(database,first),"native schema lookup removed");
    pairs(first,false);pairs(other,true);
    check(owned("public",outside) && sidecar("public",outside),"public dotted enum survives other schema drop");
    check(g_engine.catalogService().persistAll(),"persist namespace result");
    g_engine.catalogService().evict(database);
    pairs(first,false);pairs(other,true);
    check(owned("public",outside) && sidecar("public",outside),"cold metadata preserves unrelated public enum");
    drop(other,true,DBStatus::OK);
    pairs(other,false);
    check(owned("public",outside) && sidecar("public",outside),"second drop preserves actual public owner");
    check(!sidecar(other,"legacy_plain"),"legacy unregistered enum cleanup retained");
    const size_t expected=1+2+2*names.size()+1+4*names.size()+1+2+4*names.size()+2+1+
        4*names.size()+1+1+4*names.size()+1+2+2*names.size()+1+6;
    std::cout<<"ENUM_NAMESPACE_OWNER_NATIVE_COMPLETE "<<checks<<" EXPECTED "<<expected
             <<" FAILURES "<<failures<<'\n';
    return checks==expected && !failures?0:1;
}
