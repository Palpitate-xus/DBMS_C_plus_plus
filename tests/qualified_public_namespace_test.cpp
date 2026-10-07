#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const std::string name="qualified_public_namespace",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* session;~Restore(){setCurrentSession(session);}} restore{previous};
    DdlExecutor ddl;
    std::string state;
    try{(void)g_engine.prepareBoundQuery(db,"SELECT public.missing()");}
    catch(const DbError& error){state=error.sqlState();}
    assert(state=="42883");
    assert(!ddl.executeSql("DROP SCHEMA public",session));
    state.clear();
    try{(void)g_engine.prepareBoundQuery(db,"SELECT public.missing() LIMIT 0");}
    catch(const DbError& error){state=error.sqlState();}
    std::cout<<"DROPPED PUBLIC ROUTINE actual="<<state<<" expected=3F000"<<std::endl;
    assert(state=="3F000");
    auto builtin=g_engine.prepareBoundQuery(db,"SELECT pg_catalog.upper('x')");
    assert(builtin.output.size()==1 && builtin.output[0].type=="text");
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
}
