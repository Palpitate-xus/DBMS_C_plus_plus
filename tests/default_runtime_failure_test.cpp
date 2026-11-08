#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool value, const std::string& reason) {
    ++checks; if (!value) throw std::runtime_error(reason);
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000,60000);
        const auto database = testDbPath("default_runtime_failure");
        cleanupTestDb("default_runtime_failure");
        need(g_engine.createDatabase(database,"utf8") == DBStatus::OK,"actual database");
        Session session; session.username="admin";session.permission=1;session.currentDB=database;
        struct Scope {Session* previous=currentSession();Scope(Session& s){setCurrentSession(&s);}
            ~Scope(){setCurrentSession(previous);}} scope(session);
        DdlExecutor ddl;
        const auto create=[&](const std::string& sql){need(!ddl.executeSql(sql,session),"actual DDL: "+sql);};
        create("CREATE TABLE prefix_rows(id integer PRIMARY KEY)");
        create("CREATE TABLE fail_text(v text DEFAULT CAST('bad'||'' AS integer))");
        create("CREATE TABLE fail_regex(v text DEFAULT regexp_replace('abc','[','z'))");
        create("CREATE TABLE fail_number(v integer DEFAULT 1/0)");
        create("CREATE TABLE fail_substring(v text DEFAULT substring('abc',1,-1))");
        const auto fail=[&](const std::string& table,const std::string& state) {
            std::vector<StorageEngine::SqlRow> returned{{{"v","sentinel"}}};
            bool rejected=false;
            try {(void)g_engine.insertRow(database,table,{},&returned);}
            catch(const DbError& error){rejected=error.sqlState()==state && !error.message().empty();}
            need(rejected,"original typed DEFAULT failure: "+table);
            need(returned==std::vector<StorageEngine::SqlRow>{{{"v","sentinel"}}},"failed DEFAULT publishes no RETURNING suffix");
            need(g_engine.query(database,table,{}, {"v"}).empty(),"failed DEFAULT stores no SQL source text or row");
        };
        fail("fail_text","22P02");
        fail("fail_regex","2201B");
        fail("fail_substring","22011");
        need(!g_engine.inTransaction(),"implicit failed DEFAULT transaction is closed");
        need(g_engine.getLockManager().lockedTables().empty(),"failed DEFAULT releases table locks");
        fail("fail_number","22012");
        need(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(),"division failure also releases transaction and locks");
        need(g_engine.insert(database,"fail_number",{{"v","42"}})==DBStatus::OK,"explicit value skips the failing DEFAULT");
        need(g_engine.insertRow(database,"fail_number",{{"v",std::nullopt}})==DBStatus::OK,"explicit SQL NULL skips the failing DEFAULT");

        need(g_engine.beginTransaction(database)==DBStatus::OK,"actual surrounding transaction");
        need(g_engine.insert(database,"prefix_rows",{{"id","1"}})==DBStatus::OK,"actual caller prefix");
        need(g_engine.savepoint("before_default")==DBStatus::OK,"actual caller savepoint");
        fail("fail_text","22P02");
        need(g_engine.inTransaction(),"native statement failure retains caller transaction");
        need(g_engine.query(database,"prefix_rows",{}, {"id"})==std::vector<std::string>{"1 "},"caller prefix survives failed DEFAULT statement");
        need(g_engine.rollbackToSavepoint("before_default")==DBStatus::OK,"caller savepoint remains usable");
        need(g_engine.insert(database,"prefix_rows",{{"id","2"}})==DBStatus::OK,"subsequent write can acquire its locks");
        need(g_engine.commitTransaction()==DBStatus::OK,"caller transaction commits its surviving work");
        need(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(),"final transaction/lock owner is closed");

        create("CREATE SEQUENCE runtime_late_sequence START 7");
        create("CREATE TABLE fail_sequence(v bigint DEFAULT nextval('runtime_late_sequence'::text))");
        create("DROP SEQUENCE runtime_late_sequence");
        fail("fail_sequence","42P01");
        need(!g_engine.inTransaction() && g_engine.getLockManager().lockedTables().empty(),"late object failure releases its ordinary native owners");
        create("CREATE SEQUENCE runtime_late_sequence START 20");
        need(g_engine.insert(database,"fail_sequence",{})==DBStatus::OK,"late binding can recover after actual sequence recreation");
        need(g_engine.query(database,"fail_sequence",{}, {"v"})==std::vector<std::string>{"20 "},"recreated sequence produces actual allocation");
        need(g_engine.checkpoint(database),"actual ordinary checkpoint");
        std::cout<<"DEFAULT_RUNTIME_FAILURE_CHECKED="<<checks<<" FAILED=0\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr<<"DEFAULT_RUNTIME_FAILURE_CHECKED="<<checks<<" FAIL="<<error.what()<<'\n';return 1;
    }
}
