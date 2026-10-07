#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();SQLParser parser;
    for(const std::string definition:{"1 /* actual source */ / 0","1 + 2","-(1+2)","coalesce(NULL,7) + 1"}) {
        const std::string sql="ALTER TABLE span_rows ALTER COLUMN v SET DEFAULT "+definition;
        auto parsed=parser.parseForBinding(sql);
        auto* alter=parsed.success?dynamic_cast<AlterTableStmt*>(parsed.stmt.get()):nullptr;
        assert(alter && alter->subCommands.size()==1);
        const auto* value=alter->subCommands.front().defaultValue.get();
        assert(value && value->sourceBegin!=std::string::npos && value->sourceEnd<=sql.size());
        assert(sql.substr(value->sourceBegin,value->sourceEnd-value->sourceBegin)==definition);
    }
    const auto db=testDbPath("alter_default_expression_span");assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE span_rows(v INT DEFAULT 7)",session));
    const std::string definition="1 /* actual source */ / 0";
    assert(!ddl.executeSql("ALTER TABLE span_rows ALTER COLUMN v SET DEFAULT "+definition,session));
    assert(g_engine.getTableSchema(db,"span_rows").cols[0].defaultValue==definition);
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"INSERT INTO span_rows DEFAULT VALUES"));
    bool division=false;try{auto plan=buildBoundDmlPlan(query->ast.get(),session,query,{});plan->close();}
    catch(const DbError& error){division=error.sqlState()=="22012";}assert(division);
    assert(!g_engine.inTransaction() && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb("alter_default_expression_span");setCurrentSession(nullptr);
    std::cout<<"[ALTER DEFAULT EXPRESSION SPAN] actual compound provenance/late pure planning passed\n";
}
