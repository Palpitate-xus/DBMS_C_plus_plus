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
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("fetch_signed_count");assert(g_engine.createDatabase(db)==DBStatus::OK);
    auto* prior=currentSession();struct Restore{Session* prior;~Restore(){setCurrentSession(prior);}}restore{prior};
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE fetch_rows(id INT)",session));
    assert(!ddl.executeSql("CREATE TABLE empty_outer(id INT)",session));
    assert(!ddl.executeSql("CREATE SEQUENCE fetch_effects",session));
    size_t controls=0;
    const auto run=[&](const std::string& sql) {
        ++controls;std::cout<<"SIGNED_FETCH_NATIVE "<<sql<<std::endl;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        assert(query->output.size()==1);
        auto rows=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()));
        rows.throwIfFailed();return rows;
    };
    const auto error=[&](const std::string& sql,const std::string& expected) {
        std::string state;try{(void)run(sql);}catch(const DbError& exception){state=exception.sqlState();}
        std::cout<<"SIGNED_FETCH_NATIVE_ERROR actual="<<state<<" expected="<<expected<<std::endl;assert(state==expected);
    };
    for(bool populated:{false,true}) {
        if(populated)assert(g_engine.insertRow(db,"fetch_rows",{{"id","1"}})==DBStatus::OK);
        for(const auto& tail:{"FETCH FIRST -1 ROW ONLY","FETCH FIRST -1 ROW WITH TIES",
            "FETCH NEXT -9223372036854775808 ROWS WITH TIES"}) {
            const std::string body="SELECT id FROM fetch_rows ORDER BY id "+std::string(tail);
            error("SELECT("+body+")","2201W");error(body,"2201W");
            assert(run("SELECT("+body+") WHERE FALSE").structuredRows.empty());
            assert(run("SELECT("+body+") LIMIT 0").structuredRows.empty());
            assert(run("SELECT CASE WHEN FALSE THEN("+body+") ELSE 1 END").structuredRows==std::vector<std::vector<std::string>>{{"1"}});
        }
        for(const auto& count:{"+1","0","-0","+0","9223372036854775807"}) {
            auto query=g_engine.prepareBoundQuery(db,"SELECT id FROM fetch_rows ORDER BY id FETCH FIRST "+std::string(count)+" ROWS WITH TIES");
            assert(query.output[0].type=="integer");
        }
        error("SELECT(SELECT id FROM fetch_rows FETCH FIRST -1 ROW WITH TIES)","42601");
        error("SELECT(SELECT missing FROM fetch_rows ORDER BY id FETCH FIRST -1 ROW WITH TIES)","42703");
        error("SELECT(SELECT id FROM missing_rows ORDER BY id FETCH FIRST -1 ROW WITH TIES)","42P01");
        error("SELECT(SELECT CAST('bad' AS INTEGER) FROM fetch_rows ORDER BY id FETCH FIRST -1 ROW WITH TIES)","22P02");
    }
    auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "SELECT(SELECT nextval('fetch_effects') FROM fetch_rows ORDER BY id FETCH FIRST -1 ROW WITH TIES)"));
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,prepared,prepared->ast.get());
    assert(g_engine.nextval(db,"fetch_effects")==1);
    auto failed=QueryPlanner::executePlanChecked(std::move(plan));assert(!failed.ok && failed.errorSqlState=="2201W");
    assert(g_engine.nextval(db,"fetch_effects")==2);
    StorageEngine::SelectExpr legacy;legacy.displayName="value";legacy.isScalar=true;legacy.funcName="subquery";
    legacy.funcArgs={"SELECT id FROM fetch_rows ORDER BY id FETCH FIRST -1 ROW WITH TIES"};
    std::string state;try{(void)g_engine.queryExpr(db,"fetch_rows",{}, {legacy});}catch(const DbError& exception){state=exception.sqlState();}
    assert(state=="2201W");assert(g_engine.queryExpr(db,"empty_outer",{}, {legacy}).empty());
    const std::string provider="SELECT unnest(ARRAY[1,2]) FETCH FIRST -1 ROW ONLY";
    error(provider,"2201W");error("SELECT("+provider+")","2201W");
    assert(run("SELECT("+provider+") WHERE FALSE").structuredRows.empty());
    assert(run("SELECT CASE WHEN FALSE THEN("+provider+") ELSE 1 END").structuredRows==std::vector<std::vector<std::string>>{{"1"}});
    error("SELECT unnest(ARRAY[nextval('fetch_effects')]) FETCH FIRST -1 ROW ONLY","2201W");
    assert(g_engine.nextval(db,"fetch_effects")==3);
    std::cout<<"[SIGNED FETCH COUNT] complete "<<controls<<" actual signed grammar/runtime demand/no-effects/analysis priority and legacy/public graph controls passed\n";
}
