#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("insert_default_planning");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SEQUENCE insert_default_calls",session));
    assert(g_engine.createUDF(db,"insert_default_value",{},{},
        "SELECT nextval('insert_default_calls')",'v',"sql","integer")==DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE insert_default_rows(id INT,v INT DEFAULT public.insert_default_value())",session));
    assert(!ddl.executeSql("CREATE TABLE insert_default_bad(id INT DEFAULT 1/0,v INT)",session));
    const auto query=[&](const std::string& sql) {
        return std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
    };
    const auto calls=[&](int64_t expected) {
        auto* previous=currentSession();setCurrentSession(&session);
        if(expected)assert(g_engine.currval(db,"insert_default_calls")==expected);
        else {bool absent=false;try{(void)g_engine.currval(db,"insert_default_calls");}
            catch(const DbError& error){absent=error.sqlState()=="55000";}assert(absent);}
        setCurrentSession(previous);
    };
    for(const std::string sql:{"INSERT INTO insert_default_bad DEFAULT VALUES",
        "INSERT INTO insert_default_bad(v) VALUES(7)",
        "INSERT INTO insert_default_bad(id,v) VALUES(DEFAULT,7)",
        "INSERT INTO insert_default_bad(v) SELECT 7 WHERE false"}) {
        auto prepared=query(sql);
        auto* insert=dynamic_cast<InsertStmt*>(prepared->ast.get());
        assert(insert && insert->preparedDefaults.size()==1 && insert->preparedDefaults[0].first==0);
        calls(0);bool division=false;
        try{(void)buildBoundDmlPlan(insert,session,prepared,{});}
        catch(const DbError& error){division=error.sqlState()=="22012";}
        assert(division && !g_engine.inTransaction());calls(0);
    }
    auto prepared=query("INSERT INTO insert_default_bad(id,v) VALUES(3,7)");
    assert(dynamic_cast<InsertStmt*>(prepared->ast.get())->preparedDefaults.empty());
    auto plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,{});plan->close();calls(0);
    prepared=query("INSERT INTO insert_default_rows(id,v) VALUES(3,DEFAULT),(4,DEFAULT) RETURNING id,v");
    auto* insert=dynamic_cast<InsertStmt*>(prepared->ast.get());
    assert(insert && insert->preparedDefaults.size()==1 && insert->preparedDefaults.front().first==1);
    const auto* site=insert->preparedDefaults.front().second.get();
    PreparedQueryExecution probe(prepared,&g_engine,db);probe.planStatementConstants(insert);
    probe.prepareExpression(insert->preparedDefaults.front().second.get());calls(0);
    assert(insert->preparedDefaults.front().second.get()==site);
    // The passed session/actual engine, not the ambient native caller, owns
    // resolution and execution. Prepare, open and close never call DEFAULT.
    Session ambient;ambient.currentDB="not_the_insert_default_database";setCurrentSession(&ambient);
    plan=buildBoundDmlPlan(insert,session,prepared,{});
    assert(currentSession()==&ambient && plan->open());calls(0);
    std::string display;std::vector<ExprValue> row;
    assert(plan->next(display) && plan->lastStructuredValues(row));
    assert(row[0].value=="3" && row[1].value=="1" && !row[1].isNull);
    assert(plan->next(display) && plan->lastStructuredValues(row));
    assert(row[0].value=="4" && row[1].value=="2");
    assert(!plan->next(display) && plan->runtimeRows()==2 && currentSession()==&ambient);
    plan->close();calls(2);setCurrentSession(&session);
    assert(insert->preparedDefaults.front().second.get()==site);
    // A zero-row source does not evaluate either the source or DEFAULT.
    prepared=query("INSERT INTO insert_default_rows(id) SELECT 7 WHERE false RETURNING v");
    size_t reads=0;
    PreparedChildExecutor empty=[&](const Stmt* stmt,const RowContext&,size_t limit) {
        assert(stmt==dynamic_cast<InsertStmt*>(prepared->ast.get())->selectSource.get() && limit==0);
        ++reads;return PreparedQueryRows{};
    };
    plan=buildBoundDmlPlan(prepared->ast.get(),session,prepared,empty);calls(2);
    assert(plan->open() && !plan->next(display) && reads==1 && plan->runtimeRows()==0);
    plan->close();calls(2);
    // Real stored default definition is compiled once; repeated prepared
    // executions must not mutate it to private function callback keys.
    StorageEngine cold;
    auto reopened=std::make_shared<PreparedQuery>(cold.prepareBoundQuery(db,
        "INSERT INTO insert_default_rows DEFAULT VALUES RETURNING v"));
    auto* coldInsert=dynamic_cast<InsertStmt*>(reopened->ast.get());
    assert(coldInsert && coldInsert->preparedDefaults.size()==1);
    PreparedQueryExecution coldExecution(reopened,&cold,db);
    coldExecution.planStatementConstants(coldInsert);
    coldExecution.prepareExpression(coldInsert->preparedDefaults.front().second.get());calls(2);
    assert(coldExecution.evaluate(coldInsert->preparedDefaults.front().second.get(),coldExecution.context()).value=="3");
    assert(cold.currval(db,"insert_default_calls")==3);
    assert(!g_engine.inTransaction() && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb("insert_default_planning");
    setCurrentSession(nullptr);
    std::cout<<"[INSERT DEFAULT PLANNING] passed\n";
}
