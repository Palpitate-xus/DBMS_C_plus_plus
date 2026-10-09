#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("prepared_exists");
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session; session.username="testuser";session.permission=1;session.currentDB=db;
    const auto previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE exists_rows(id INT,k INT)",session));
    assert(g_engine.insertRow(db,"exists_rows",{{"id","1"},{"k","1"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"exists_rows",{{"id","2"},{"k","1"}})==DBStatus::OK);
    size_t failed=0,checked=0;
    const auto check=[&](const std::string& sql,const std::vector<std::vector<std::string>>& expected) {
        ++checked;bool pass=false;
        try {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
            auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()));
            result.throwIfFailed();pass=result.structuredRowsAvailable && result.structuredRows==expected;
        } catch(const DbError& error) {std::cout<<"PREPARED_EXISTS_ERROR "<<error.sqlState()<<' '<<error.what()<<'\n';}
        failed+=!pass;std::cout<<"PREPARED_EXISTS "<<sql<<" pass="<<pass<<'\n';
    };
    for (const auto& item : std::vector<std::pair<std::string,std::string>>{
        {"SELECT EXISTS(SELECT id FROM exists_rows ORDER BY k FETCH FIRST 1 ROW WITH TIES)","t"},
        {"SELECT EXISTS(SELECT id FROM exists_rows ORDER BY k FETCH FIRST 0 ROWS WITH TIES)","f"},
        {"SELECT EXISTS(SELECT id,k FROM exists_rows)","t"},
        {"SELECT EXISTS(SELECT id,k FROM exists_rows WHERE false)","f"},
        {"SELECT EXISTS(SELECT 1)","t"},
        {"SELECT EXISTS(SELECT NULL)","t"},
        {"SELECT EXISTS(SELECT NULL,'text')","t"},
        {"SELECT EXISTS(SELECT 1/0 FROM exists_rows)","t"},
        {"SELECT EXISTS(SELECT id FROM exists_rows ORDER BY 1/0)","t"},
        {"SELECT EXISTS(SELECT DISTINCT id FROM exists_rows)","t"},
        {"SELECT EXISTS(SELECT id,k FROM exists_rows OFFSET 1)","t"},
        {"SELECT EXISTS(SELECT id,k FROM exists_rows OFFSET 2)","f"},
        {"SELECT NOT EXISTS(SELECT id FROM exists_rows WHERE false)","t"},
        {"SELECT CASE WHEN false THEN EXISTS(SELECT 1/0 FROM exists_rows) ELSE false END","f"}})
        check(item.first,{{item.second}});
    check("SELECT o.id,EXISTS(SELECT i.id,i.k FROM exists_rows i WHERE i.id=o.id) FROM exists_rows o ORDER BY o.id",{{"1","t"},{"2","t"}});
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"SELECT EXISTS(SELECT missing FROM exists_rows)","42703"},
        {"SELECT EXISTS(SELECT id FROM missing_relation)","42P01"},
        {"SELECT (SELECT id,k FROM exists_rows)","42601"},
        {"SELECT (SELECT id FROM exists_rows)","21000"}}) {
        ++checked;std::string state;
        try {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,item.first));
            auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()));result.throwIfFailed();
        } catch(const DbError& error){state=error.sqlState();}
        const bool pass=state==item.second;failed+=!pass;
        std::cout<<"PREPARED_EXISTS_STATE "<<item.first<<" actual="<<state<<" expected="<<item.second<<" pass="<<pass<<'\n';
    }
    std::cout<<"PREPARED_EXISTS_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==19 && !failed?0:1;
}
