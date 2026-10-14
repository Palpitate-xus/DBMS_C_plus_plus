#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include "utils/Session.h"
#include <cassert>
#include <future>
#include <iostream>
#include <thread>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("database_independent_binding");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    TableSchema table;table.tablename="rows";table.append(makeIntColumn("id",false,2,true));
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    g_engine.getLockManager().setLockTimeout(50);
    Session session;session.currentDB=database;session.username="testuser";
    auto* previous=currentSession();setCurrentSession(&session);
    std::promise<void> locked,release;auto released=release.get_future();
    std::thread writer([&] {
        assert(g_engine.beginTransaction(database,true)==DBStatus::OK);
        locked.set_value();released.wait();
        assert(g_engine.rollbackTransaction()==DBStatus::OK);g_engine.endBackendSession();
    });
    locked.get_future().wait();
    size_t checks=0,failures=0;
    {
        StorageEngine::DatabaseIndependentBeginScope logical(g_engine);
        assert(g_engine.beginTransaction(database)==DBStatus::OK);
        for(const auto* sql:{"SELECT 1","SELECT 1+2*3","SELECT 'a'||'b'","SELECT 1=1 AND 2<3",
                "SELECT 1 WHERE false","SELECT 1 LIMIT 0","SELECT DISTINCT 1 ORDER BY 1 OFFSET 1",
                "SELECT NULL","SELECT NULL+1","SELECT 0.1+0.2","SELECT NOT false","SELECT B'101'",
                "SELECT 1+'2'","SELECT 1='1'","SELECT NULL=1","SELECT 1 IN('1',2)",
                "SELECT 1.5+NULL","SELECT NULL||'x'","SELECT B'101'=B'101'","SELECT 'foo'||NULL"}) {
            std::string state;
            std::string stage="bind";
            try {
                auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
                PreparedQueryExecution execution(query,&g_engine,database);
                stage="plan";
                execution.planStatementConstants(query->ast.get());
                auto* select=static_cast<SelectStmt*>(query->ast.get());
                stage="prepare";
                execution.prepareExpression(select->selectList.front().expr.get());
                stage="evaluate";
                (void)execution.evaluate(select->selectList.front().expr.get(),execution.context());
            }catch(const DbError& error){state=error.sqlState();}
            ++checks;failures+=!state.empty();
            std::cout<<"DATABASE_INDEPENDENT_BINDING "<<sql<<" state="<<state<<" stage="<<stage<<std::endl;
            assert(g_engine.databaseTransactionOwnershipDeferred());
        }
        for(const auto* sql:{"SELECT id FROM rows","SELECT abs(1)","SELECT 1::INT"}) {
            std::string state;
            try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){state=error.sqlState();}
            ++checks;failures+=state!="55P03";
            std::cout<<"DATABASE_INDEPENDENT_PHYSICAL "<<sql<<" state="<<state<<std::endl;
        }
        for(const auto& [sql,expected]:std::vector<std::pair<std::string,std::string>>{
                {"SELECT 1='bad'","22P02"},{"SELECT 1='2147483648'","22003"}}) {
            std::string state;
            try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){state=error.sqlState();}
            ++checks;failures+=state!=expected;
            std::cout<<"DATABASE_INDEPENDENT_INPUT_ERROR "<<sql<<" state="<<state<<std::endl;
            assert(g_engine.databaseTransactionOwnershipDeferred());
        }
        assert(g_engine.rollbackTransaction()==DBStatus::OK);
    }
    release.set_value();writer.join();setCurrentSession(previous);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout<<"DATABASE_INDEPENDENT_BINDING_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return checks==25 && !failures?0:1;
}
