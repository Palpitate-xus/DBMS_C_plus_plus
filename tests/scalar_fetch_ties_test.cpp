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
    const auto db=testDbPath("scalar_fetch_ties");assert(g_engine.createDatabase(db)==DBStatus::OK);
    auto* prior=currentSession();struct Restore{Session* prior;~Restore(){setCurrentSession(prior);}}restore{prior};
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE fetch_rows(id INT,k INT,p TEXT,b BIGINT)",session));
    assert(g_engine.insertRow(db,"fetch_rows",{{"id","1"},{"k","1"},{"p",""},{"b","9007199254740993"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"fetch_rows",{{"id","2"},{"k","1"},{"p","NULL"},{"b","9007199254740992"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"fetch_rows",{{"id","3"},{"k","2"},{"p",std::nullopt},{"b",std::nullopt}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"fetch_rows",{{"id","4"},{"k",std::nullopt},{"p","spaces here"},{"b","0"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"fetch_rows",{{"id","5"},{"k",std::nullopt},{"p","last"},{"b","1"}})==DBStatus::OK);
    size_t controls=0;
    const auto execute=[&](const std::string& sql) {
        ++controls;std::cout<<"SCALAR_FETCH_NATIVE "<<sql<<std::endl;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get()));
        result.throwIfFailed();assert(result.structuredRowsAvailable);return result;
    };
    const auto check=[&](const std::string& sql,const std::vector<std::vector<std::string>>& rows) {
        const auto actual=execute(sql);assert(actual.structuredRows==rows);
    };
    const auto error=[&](const std::string& sql,const std::string& expected,bool bindOnly=false) {
        std::string state;try{
            if(bindOnly){++controls;(void)g_engine.prepareBoundQuery(db,sql);}
            else (void)execute(sql);
        }catch(const DbError& exception){state=exception.sqlState();}
        std::cout<<"SCALAR_FETCH_NATIVE_ERROR "<<sql<<" actual="<<state<<" expected="<<expected<<std::endl;
        assert(state==expected);
    };
    check("SELECT(SELECT id FROM fetch_rows ORDER BY id DESC FETCH FIRST 1 ROW WITH TIES)",{{"5"}});
    error("SELECT(SELECT id FROM fetch_rows ORDER BY k FETCH FIRST 1 ROW WITH TIES)","21000");
    check("SELECT(SELECT id FROM fetch_rows ORDER BY k,id FETCH FIRST 1 ROW WITH TIES)",{{"1"}});
    check("SELECT(SELECT id FROM fetch_rows ORDER BY k OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES)",{{"2"}});
    error("SELECT(SELECT id FROM fetch_rows ORDER BY k NULLS FIRST FETCH FIRST 1 ROW WITH TIES)","21000");
    check("SELECT(SELECT id FROM fetch_rows ORDER BY k NULLS FIRST OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES)",{{"5"}});
    check("SELECT(SELECT id FROM fetch_rows ORDER BY id FETCH FIRST 0 ROWS WITH TIES)",{{""}});
    check("SELECT(SELECT p FROM fetch_rows WHERE id=1 ORDER BY id FETCH FIRST 1 ROW WITH TIES)",{{""}});
    check("SELECT(SELECT p FROM fetch_rows WHERE id=2 ORDER BY id FETCH FIRST 1 ROW WITH TIES)",{{"NULL"}});
    check("SELECT(SELECT p FROM fetch_rows WHERE id=3 ORDER BY id FETCH FIRST 1 ROW WITH TIES)",{{""}});
    check("SELECT(SELECT b FROM fetch_rows ORDER BY b DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES)",{{"9007199254740993"}});
    check("SELECT(SELECT id FROM fetch_rows WHERE FALSE ORDER BY id FETCH FIRST 1 ROW WITH TIES)",{{""}});
    for(const auto& sql:{"SELECT(SELECT id FROM fetch_rows FETCH FIRST 1 ROW WITH TIES)",
        "SELECT(SELECT id FROM fetch_rows FETCH FIRST 0 ROWS WITH TIES) WHERE FALSE LIMIT 0"})error(sql,"42601",true);
    error("SELECT(SELECT missing FROM fetch_rows FETCH FIRST 1 ROW WITH TIES)","42601",true);
    error("SELECT(SELECT id FROM fetch_missing FETCH FIRST 1 ROW WITH TIES)","42601",true);
    error("SELECT(SELECT missing FROM fetch_rows ORDER BY id FETCH FIRST 1 ROW WITH TIES)","42703",true);
    error("SELECT(SELECT id FROM fetch_missing ORDER BY id FETCH FIRST 1 ROW WITH TIES)","42P01",true);
    error("SELECT(SELECT id FROM fetch_rows FETCH FIRST 1 ROW WITH TIES) FROM fetch_missing","42601",true);
    auto typed=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "SELECT(SELECT p FROM fetch_rows WHERE id=3 ORDER BY id FETCH FIRST 1 ROW WITH TIES) AS value"));
    assert(typed->output.size()==1 && typed->output[0].type=="text");
    auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,typed,typed->ast.get()),typed->output);
    std::vector<ExprValue> values;assert(cursor->next(values));assert(values.size()==1 && values[0].typeName=="text" && values[0].isNull);
    assert(!cursor->next(values));cursor->close();
    check("SELECT id FROM fetch_rows ORDER BY k FETCH FIRST 1 ROW WITH TIES",{{"1"},{"2"}});
    check("SELECT id FROM fetch_rows ORDER BY k NULLS FIRST FETCH FIRST 1 ROW WITH TIES",{{"4"},{"5"}});
    check("SELECT id FROM fetch_rows ORDER BY k,id FETCH FIRST 1 ROW WITH TIES",{{"1"}});
    check("SELECT id FROM fetch_rows ORDER BY k OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES",{{"2"}});
    std::cout<<"[SCALAR FETCH TIES] complete "<<controls<<" actual preparation/public graph/scalar cardinality/typed NULL/empty/exact BIGINT/main ties controls passed\n";
}
