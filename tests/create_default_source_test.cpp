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
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("create_default_source");assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;
    const std::string expression="CASE /* retain original bytes */ WHEN false THEN 1/0 ELSE 7 END";
    const std::string source="CREATE TABLE default_source(v INT DEFAULT "+expression+" NOT NULL)";
    SQLParser parser;auto parsed=parser.parseForBinding(source);
    auto* table=parsed.success?dynamic_cast<CreateTableStmt*>(parsed.stmt.get()):nullptr;
    assert(table && table->columns.size()==1 && !table->columns.front().isNull);
    const auto* defaultExpression=table->columns.front().defaultValue.get();
    assert(defaultExpression && defaultExpression->sourceBegin!=std::string::npos &&
        defaultExpression->sourceEnd<=source.size() && source.substr(defaultExpression->sourceBegin,
        defaultExpression->sourceEnd-defaultExpression->sourceBegin)==expression);
    assert(defaultExpression->toString()==expression);
    bool handled=false;
    // The real dispatcher supplies its compatibility copy and original
    // bytes. Persist the latter's default, not evaluator pseudo-functions.
    assert(!tryDdlBridge("create table default_source(v int default case_when(false,1/0,7) not null)",
        SqlCommand::CreateTable,session,handled,source) && handled);
    assert(g_engine.getTableSchema(db,"default_source").cols[0].defaultValue==expression);
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
        "INSERT INTO default_source DEFAULT VALUES RETURNING v"));
    auto plan=buildBoundDmlPlan(query->ast.get(),session,query,{});
    assert(plan->runtimeRows()==0 && plan->open());std::string row;std::vector<ExprValue> cells;
    assert(plan->next(row) && plan->lastStructuredValues(cells) && cells.size()==1 && cells[0].value=="7");
    assert(!plan->next(row));plan->close();
    assert(!ddl.executeSql("CREATE TABLE default_null(v INT DEFAULT CASE WHEN false THEN 1/0 ELSE NULL END CHECK(v>0))",session));
    query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"INSERT INTO default_null DEFAULT VALUES RETURNING v"));
    plan=buildBoundDmlPlan(query->ast.get(),session,query,{});
    assert(plan->open() && plan->next(row) && plan->lastStructuredValues(cells) && cells[0].isNull);
    assert(!plan->next(row));plan->close();
    assert(!ddl.executeSql("CREATE TABLE default_array(v INT[] DEFAULT ARRAY[1,2])",session));
    query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"INSERT INTO default_array DEFAULT VALUES RETURNING v"));
    plan=buildBoundDmlPlan(query->ast.get(),session,query,{});
    assert(plan->open() && plan->next(row) && plan->lastStructuredValues(cells) && cells[0].value=="{1,2}");
    assert(!plan->next(row));plan->close();
    for(const std::string invalid:{"CREATE TABLE missing_default(v INT DEFAULT)",
        "CREATE TABLE missing_case(v INT DEFAULT CASE WHEN true THEN 7)",
        "CREATE TABLE missing_array(v INT[] DEFAULT ARRAY[1,2)"})
        assert(!parser.parseForBinding(invalid).success);
    assert(!g_engine.inTransaction() && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb("create_default_source");setCurrentSession(nullptr);
    std::cout<<"[CREATE DEFAULT SOURCE] original expression/CASE/NULL/array/negative passed\n";
}
