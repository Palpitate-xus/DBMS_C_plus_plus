#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    const auto database=testDbPath("procedure_namespace");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA \"S.X\"",session));
    assert(!ddl.executeSql("CREATE PROCEDURE public.p() LANGUAGE SQL AS $$SELECT 7$$",session));
    assert(!ddl.executeSql("CREATE PROCEDURE \"S.X\".p() LANGUAGE SQL AS $$SELECT 8$$",session));
    assert(std::filesystem::exists(std::filesystem::path(database)/".procs"/"p.proc"));
    assert(std::filesystem::exists(std::filesystem::path(database)/".procs"/".namespaces"/"S.X"/"p.proc"));
    assert((g_engine.getProcedureStatements(database,"p","public")==std::vector<std::string>{"SELECT 7"}));
    assert((g_engine.getProcedureStatements(database,"p","S.X")==std::vector<std::string>{"SELECT 8"}));
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    assert(!ddl.executeSql("CREATE PROCEDURE \"S.X\".q() LANGUAGE SQL AS $$SELECT 1$$",session));
    assert(g_engine.procedureExists(database,"q","S.X"));
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(!g_engine.procedureExists(database,"q","S.X"));
    assert(g_engine.procedureExists(database,"p","public"));
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    assert(!ddl.executeSql("DROP PROCEDURE \"S.X\".p()",session));
    assert(!g_engine.procedureExists(database,"p","S.X"));
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(g_engine.procedureExists(database,"p","S.X"));
    assert(!ddl.executeSql("CREATE OR REPLACE PROCEDURE \"S.X\".p() LANGUAGE SQL AS $$SELECT 9$$",session));
    assert((g_engine.getProcedureStatements(database,"p","S.X")==std::vector<std::string>{"SELECT 9"}));
    assert((g_engine.getProcedureStatements(database,"p","public")==std::vector<std::string>{"SELECT 7"}));
    // A literal dotted name in public does not acquire a different schema.
    assert(!ddl.executeSql("CREATE PROCEDURE public.\"S.X.p\"() LANGUAGE SQL AS $$SELECT 11$$",session));
    assert(ddl.executeSql("DROP SCHEMA \"S.X\" RESTRICT",session));
    assert(g_engine.procedureExists(database,"p","S.X"));
    assert(!ddl.executeSql("DROP SCHEMA \"S.X\" CASCADE",session));
    assert(!g_engine.procedureExists(database,"p","S.X"));
    assert(g_engine.procedureExists(database,"S.X.p","public"));
    assert(g_engine.procedureExists(database,"p","public"));
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[PROCEDURE NAMESPACE] legacy files, isolated bodies, replacement, rollback, schema restrict/cascade passed\n";
}
