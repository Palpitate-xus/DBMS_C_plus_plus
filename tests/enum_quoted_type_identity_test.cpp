#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("enum_quoted_type_identity");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    auto* previous=currentSession();
    struct Restore {Session* session;~Restore(){setCurrentSession(session);}} restore{previous};
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    setCurrentSession(&session);DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TYPE \"MixedRank\" AS ENUM ('zeta','','alpha')",session));
    auto& catalog=g_engine.catalogService().get(db);
    const auto* ns=catalog.findNamespaceByName("public");assert(ns);
    const auto nsOid=ns->oid;
    const auto* type=catalog.findTypeByName("MixedRank",nsOid);
    std::cout<<"QUOTED_ENUM_CANONICAL_CATALOG_NAME_PRESENT="<<bool(type)<<std::endl;
    assert(type && type->typtype=='e' && type->typname=="MixedRank");
    const auto typeOid=type->oid;
    assert(!catalog.findTypeByName("\"MixedRank\"",nsOid));
    assert(g_engine.getEnumType(db,"MixedRank").labels==std::vector<std::string>({"zeta","","alpha"}));
    assert(g_engine.getEnumType(db,"\"MixedRank\"").name.empty());
    assert(!ddl.executeSql("CREATE SCHEMA \"ScopeRank\"",session));
    assert(!ddl.executeSql("CREATE TYPE \"ScopeRank\".\"MixedRank\" AS ENUM ('alpha','','zeta')",session));
    const auto* scoped=catalog.findNamespaceByName("ScopeRank");assert(scoped);
    const auto scopeOid=scoped->oid;
    const auto* other=catalog.findTypeByName("MixedRank",scopeOid);assert(other && other->oid!=typeOid);
    assert(g_engine.getEnumType(db,"ScopeRank.MixedRank").labels==std::vector<std::string>({"alpha","","zeta"}));
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SELECT 'zeta'::\"MixedRank\" < 'alpha'","t"},
        {"SELECT CAST('zeta' AS \"MixedRank\") < 'alpha'","t"},
        {"SELECT CASE ''::\"MixedRank\" WHEN '' THEN 1 ELSE 2 END","1"},
        {"SELECT (CASE WHEN true THEN 'zeta'::\"MixedRank\" ELSE 'alpha' END) < 'alpha'","t"},
        {"SELECT 'zeta'::\"ScopeRank\".\"MixedRank\" < 'alpha'","f"}}) {
        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,control.first));
        auto* statement=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(statement);
        PreparedQueryExecution execution(prepared,&g_engine,db);
        execution.prepareExpression(statement->selectList.front().expr.get());
        execution.planStatementConstants(statement);
        const auto value=execution.evaluate(statement->selectList.front().expr.get(),execution.context());
        assert(!value.isNull && value.value==control.second);
    }
    assert(!ddl.executeSql("CREATE TYPE UPPER_RANK AS ENUM ('zeta','alpha')",session));
    assert(catalog.findTypeByName("upper_rank",nsOid) && !catalog.findTypeByName("UPPER_RANK",nsOid));
    {
        StorageEngine cold;
        const auto snapshot=cold.catalogService().metadataSnapshot(db);
        const auto found=std::find_if(snapshot.types.begin(),snapshot.types.end(),[&](const auto& row){return row.oid==typeOid;});
        assert(found!=snapshot.types.end() && found->typname=="MixedRank");
        assert(cold.getEnumType(db,"MixedRank").labels==std::vector<std::string>({"zeta","","alpha"}));
    }
    assert(!ddl.executeSql("DROP TYPE \"MixedRank\"",session));
    assert(!catalog.findTypeByName("MixedRank",nsOid) && g_engine.getEnumType(db,"MixedRank").name.empty());
    assert(catalog.findTypeByName("MixedRank",scopeOid));
    assert(!ddl.executeSql("DROP TYPE UPPER_RANK",session));
    assert(!catalog.findTypeByName("upper_rank",nsOid));
    assert(g_engine.getEnumType(db,"upper_rank").name.empty());
    std::cout<<"[ENUM QUOTED TYPE IDENTITY] canonical SQL identifiers, actual catalog/sidecar identities, distinct scopes, casts/CASE, unquoted folding, cold storage and exact DROP target passed\n";
}
