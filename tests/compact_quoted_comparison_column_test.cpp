#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <tuple>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::vector<std::tuple<std::string,std::string,std::string,std::string>> controls={
        {"=\"i I\" '1'","=","i I","1"},
        {"<>\"i I\" '2'","!=","i I","2"},
        {"<=\"a\"\" b\" 'has space'","<=","a\" b","has space"},
        {"=\"Case Name\" 'it''s data'","=","Case Name","it's data"},
        {"=\"key.name\" 'x'","=","key.name","x"}};
    for(const auto& [sql,op,column,value]:controls) {
        const auto parsed=StorageEngine::parseConditions({sql});
        std::cout<<"COMPACT_QUOTED "<<sql<<" count="<<parsed.size();
        if(!parsed.empty())std::cout<<" col="<<parsed[0].colName<<" value="<<parsed[0].value;
        std::cout<<std::endl;
        assert(parsed.size()==1 && parsed[0].op==op && parsed[0].colName==column && parsed[0].value==value);
    }
    TypeRegistry::instance().bootstrap();const auto db=testDbPath("compact_quoted_comparison");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    auto* previous=currentSession();struct Restore{Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE quoted_values(id INT PRIMARY KEY,\"i I\" INT,\"a\"\" b\" TEXT)",session));
    assert(!ddl.executeSql("CREATE INDEX quoted_i ON quoted_values(\"i I\")",session));
    assert(g_engine.insertRow(db,"quoted_values",{{"id","1"},{"i I","1"},{"a\" b","has space"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"quoted_values",{{"id","2"},{"i I","2"},{"a\" b","other"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"quoted_values",{{"id","3"},{"i I",std::nullopt},{"a\" b",std::nullopt}})==DBStatus::OK);
    PlanContext context;context.dbname=db;context.tablename="quoted_values";context.selectCols={"id"};
    context.conds=StorageEngine::parseConditions({"=\"i I\" '1'"});
    auto plan=QueryPlanner::buildSelectPlan(&g_engine,context);
    const auto explanation=QueryPlanner::explain(plan,&g_engine,db);
    assert(explanation.find("IndexScan")!=std::string::npos);
    auto indexed=QueryPlanner::executePlanChecked(std::move(plan));indexed.throwIfFailed();
    assert(indexed.rows==std::vector<std::string>{"1 "});
    context.conds=StorageEngine::parseConditions({"=\"a\"\" b\" 'has space'"});
    auto scanned=QueryPlanner::executePlanChecked(QueryPlanner::buildSelectPlan(&g_engine,context));scanned.throwIfFailed();
    assert(scanned.rows==std::vector<std::string>{"1 "});
    std::cout<<"[COMPACT QUOTED COMPARISON] full quoted/case/space/embedded quote/data separator and actual index/heap/NULL owner controls passed\n";
}
