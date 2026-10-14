#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/DbError.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <vector>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const std::string name="default_value_role",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore { Session* value;~Restore(){setCurrentSession(value);} } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE default_roles(\"default\" INT DEFAULT 7)",session));
    size_t controls=0;
    const auto reject=[&](const std::string& sql,const std::string& expected) {
        std::string actual;
        try {(void)g_engine.prepareBoundQuery(db,sql);}
        catch(const DbError& error){actual=error.sqlState();}
        if(actual!=expected)std::cerr<<"DEFAULT_ROLE_NATIVE_FAILURE "<<sql<<" expected "<<expected<<" actual "<<actual<<'\n';
        assert(actual==expected);++controls;
    };
    for(const auto* expression:{"DEFAULT","CAST(DEFAULT AS INT)","DEFAULT+1",
            "CASE WHEN false THEN DEFAULT ELSE 7 END"})
        for(const auto* suffix:{""," FROM default_roles WHERE false"," FROM default_roles LIMIT 0"})
            reject(std::string("SELECT ")+expression+suffix,"42601");
    for(const auto* sql:{"VALUES(DEFAULT)","INSERT INTO default_roles SELECT DEFAULT",
            "WITH c AS(SELECT 1 AS id) INSERT INTO default_roles SELECT DEFAULT FROM c"})reject(sql,"42601");
    reject("SELECT \"DEFAULT\" FROM default_roles","42703");
    reject("SELECT DEFAULT FROM missing_default_role_relation","42P01");
    reject("SELECT DEFAULT,missing FROM default_roles","42601");
    reject("SELECT missing,DEFAULT FROM default_roles","42703");
    for(const auto* sql:{"SELECT \"default\" FROM default_roles","SELECT t.default FROM default_roles AS t"}) {
        const auto query=g_engine.prepareBoundQuery(db,sql);
        assert(query.output.size()==1 && query.output.front().type=="integer" && query.output.front().typeOid==23);
        ++controls;
    }
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "INSERT INTO default_roles VALUES(DEFAULT) RETURNING \"default\""));
    const auto result=executeAtomicDmlUnit(session,[&]{
        return executeBoundDml(query->ast.get(),session,query,{}, {},{},true);
    });
    assert(result.rows==std::vector<std::vector<std::string>>{{"7"}} &&
        result.nulls==std::vector<std::vector<bool>>{{false}});++controls;
    assert(!g_engine.inTransaction() && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(controls==22);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[DEFAULT VALUE ROLE] "<<controls<<" preparation/empty source/quoted/assignment controls passed\n";
}
