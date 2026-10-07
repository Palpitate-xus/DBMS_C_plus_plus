#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/QueryHostProvider.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="backend_session_srf_provider",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session backend;backend.username="admin";backend.permission=1;backend.currentDB=db;backend.pid=19871;
    auto* previous=currentSession();setCurrentSession(&backend);
    struct Restore {Session* session;~Restore(){setCurrentSession(session);}} restore{previous};
    auto& notifications=notificationManager();
    notifications.listen(backend.pid,db,"alpha");notifications.listen(backend.pid,db,"zeta");
    notifications.listen(backend.pid+1,db,"other_backend");notifications.listen(backend.pid,"other_database","other_database");
    size_t reads=0;
    const PreparedSetReturningReader reader=[&](const QuerySetReturningBinding& binding,const std::vector<ExprValue>& arguments) {
        ++reads;
        return queryHostSetReturningProvider(binding).read(arguments,currentSession(),db);
    };
    const auto plan=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
        auto actual=QueryPlanner::buildPreparedSetReturningPlan(&g_engine,db,query,select,{}, {}, {},true,reader);
        assert(actual);return actual;
    };
    for(const auto* sql:{"SELECT pg_listening_channels() LIMIT 0",
        "SELECT pg_catalog.pg_listening_channels(),7 WHERE false",
        "SELECT pg_listening_channels() WHERE NULL::BOOL"}) {
        reads=0;auto actual=plan(sql);assert(reads==0);
        assert(actual->open());assert(reads==0);std::string row;
        assert(!actual->next(row));assert(reads==0);actual->close();assert(reads==0);
    }
    reads=0;auto actual=plan("SELECT pg_catalog.pg_listening_channels(),7,NULL,'NULL',''");
    assert(reads==0);auto result=QueryPlanner::executePlanChecked(std::move(actual));result.throwIfFailed();
    assert(reads==1 && result.structuredRowsAvailable);
    assert((result.structuredRows==std::vector<std::vector<std::string>>{
        {"alpha","7","","NULL",""},{"zeta","7","","NULL",""}}));
    assert((result.structuredNulls==std::vector<std::vector<bool>>{
        {false,false,true,false,false},{false,false,true,false,false}}));
    notifications.beginTransaction(backend.pid);
    notifications.unlisten(backend.pid,db,"alpha");notifications.listen(backend.pid,db,"staged");
    reads=0;result=QueryPlanner::executePlanChecked(plan("SELECT pg_listening_channels()"));result.throwIfFailed();
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"alpha"},{"zeta"}}));
    assert(reads==1);notifications.rollbackTransaction(backend.pid);
    reads=0;actual=plan("SELECT unnest(ARRAY[1,2,3]),pg_listening_channels()");
    result=QueryPlanner::executePlanChecked(std::move(actual));result.throwIfFailed();assert(reads==2);
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"1","alpha"},{"2","zeta"},{"3",""}}));
    assert(result.structuredNulls.back().back());
    // A plan reopen reads the real current committed subscription snapshot,
    // not metadata-time/session-role guesses or a prior row-value cache.
    reads=0;actual=plan("SELECT pg_listening_channels()");std::string row;
    assert(actual->open() && actual->next(row));actual->close();assert(reads==1);
    notifications.unlistenAll(backend.pid);
    assert(actual->open() && !actual->next(row));actual->close();assert(reads==2);
    auto metadata=g_engine.prepareBoundQuery(db,"SELECT pg_listening_channels() LIMIT 0");
    assert(metadata.output.size()==1 && metadata.output[0].name=="pg_listening_channels" && metadata.output[0].type=="text");
    notifications.disconnect(backend.pid);notifications.disconnect(backend.pid+1);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[BACKEND SESSION SRF PROVIDER] actual registry, pure descriptors/demand, pid/db, staged actions, zip NULLs passed\n";
}
