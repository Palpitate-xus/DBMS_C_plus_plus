#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("update_domain_default");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE DOMAIN update_domain AS INT DEFAULT 1",session));
    assert(!ddl.executeSql("CREATE TABLE default_rows(id INT,v update_domain,c update_domain DEFAULT 9,n update_domain DEFAULT NULL)",session));
    assert(g_engine.insertRow(db,"default_rows",{{"id","1"},{"v","10"},{"c","10"},{"n","10"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"default_rows",{{"id","2"},{"v","20"},{"c","20"},{"n","20"}})==DBStatus::OK);
    auto domain=g_engine.getDomain(db,"update_domain");domain.defaultValue="2";
    assert(g_engine.alterDomain(db,"update_domain",domain)==DBStatus::OK);
    const auto run=[&](const std::string& sql) {
        bool handled=false;assert(!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql));assert(handled);
        return takeLastDmlResult();
    };
    auto result=run("UPDATE default_rows SET v=DEFAULT,c=DEFAULT,n=DEFAULT WHERE id=1 RETURNING v,c,n");
    assert(result.commandTag=="UPDATE 1" && result.rows.size()==1 && result.rows[0][0]=="2" && result.rows[0][1]=="9" && result.nulls[0][2]);
    assert(!ddl.executeSql("CREATE SEQUENCE default_calls",session));
    assert(g_engine.createUDF(db,"default_writer",{},{},"SELECT nextval('default_calls')",'v',"sql","integer")==DBStatus::OK);
    assert(g_engine.alterTableSetDefault(db,"default_rows","v","public.default_writer()")==DBStatus::OK);
    const auto noCalls=[&] {
        bool absent=false;try {(void)g_engine.currval(db,"default_calls");}
        catch(const DbError& error){absent=error.sqlState()=="55000";}assert(absent);
    };
    auto pure=g_engine.prepareBoundQuery(db,"EXPLAIN UPDATE default_rows SET v=DEFAULT RETURNING v");
    assert(dynamic_cast<ExplainStmt*>(pure.ast.get()));noCalls();
    result=run("UPDATE default_rows SET v=DEFAULT WHERE false RETURNING v");
    assert(result.commandTag=="UPDATE 0" && result.rows.empty() && result.columnTypes==std::vector<std::string>{"integer"});noCalls();
    result=run("UPDATE default_rows SET v=DEFAULT WHERE id=99 RETURNING v");assert(result.rows.empty());noCalls();
    result=run("UPDATE default_rows SET v=DEFAULT WHERE true RETURNING v");
    assert(result.commandTag=="UPDATE 2" && result.rows.size()==2 && result.rows[0][0]=="1" && result.rows[1][0]=="2");
    assert(g_engine.currval(db,"default_calls")==2);
    bool precise=false;
    try {(void)run("UPDATE default_rows SET v=DEFAULT RETURNING missing_default_function(v)");}
    catch(const DbError& error){precise=error.sqlState()=="42883";}
    assert(precise && g_engine.currval(db,"default_calls")==2);
    // A fresh owner loads persisted source metadata, then compiles its own
    // exact default under that actual engine, rather than borrowing g_engine.
    StorageEngine cold;
    auto reopened=std::make_shared<PreparedQuery>(cold.prepareBoundQuery(db,"UPDATE default_rows SET v=DEFAULT WHERE false RETURNING v"));
    auto* update=dynamic_cast<UpdateStmt*>(reopened->ast.get());assert(update);
    PreparedQueryExecution execution(reopened,&cold,db);execution.prepareExpression(update->setClauses[0].second.get());
    assert(g_engine.currval(db,"default_calls")==2);
    assert(execution.evaluate(update->setClauses[0].second.get(),execution.context()).value=="3");
    assert(cold.currval(db,"default_calls")==3);
    assert(g_engine.createUDF(db,"default_argument",{"input"},{"integer"},
        "SELECT nextval('default_calls')",'v',"sql","integer")==DBStatus::OK);
    const std::string qualifiedDefault="public.default_argument(1/0)";
    assert(!ddl.executeSql("ALTER TABLE default_rows ALTER COLUMN v SET DEFAULT "+qualifiedDefault,session));
    assert(g_engine.getTableSchema(db,"default_rows").cols[1].defaultValue==qualifiedDefault);
    precise=false;
    try {(void)run("UPDATE default_rows SET v=DEFAULT WHERE false RETURNING v");}
    catch(const DbError& error){precise=error.sqlState()=="22012";}
    assert(precise && g_engine.currval(db,"default_calls")==3);
    const std::string deadDefault="CASE WHEN false THEN public.default_argument(1/0) ELSE 1 END";
    assert(!ddl.executeSql("ALTER TABLE default_rows ALTER COLUMN v SET DEFAULT "+deadDefault,session));
    assert(g_engine.getTableSchema(db,"default_rows").cols[1].defaultValue==deadDefault);
    result=run("UPDATE default_rows SET v=DEFAULT WHERE id=1 RETURNING v");
    assert(result.rows.size()==1 && result.rows[0][0]=="1" && g_engine.currval(db,"default_calls")==3);
    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW default_mv AS SELECT id FROM default_rows",session));
    precise=false;
    try {(void)run("UPDATE default_mv SET id=DEFAULT WHERE false RETURNING id");}
    catch(const DbError& error){precise=error.sqlState()=="42809";}
    assert(precise && g_engine.currval(db,"default_calls")==3);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);setCurrentSession(nullptr);
    cleanupTestDb("update_domain_default");
    std::cout<<"[UPDATE DOMAIN DEFAULT] actual carrier/demand/current source/owner passed\n";
}
