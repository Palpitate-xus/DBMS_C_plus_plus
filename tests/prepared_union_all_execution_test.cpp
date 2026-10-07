#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

using namespace dbms;
int main() {
    TypeRegistry::instance().bootstrap();StorageEngine engine;
    const std::string name="prepared_union_all_execution";cleanupTestDb(name);
    const auto db=testDbPath(name);assert(engine.createDatabase(db,"utf8")==DBStatus::OK);
    assert(engine.createSequence(db,"append_calls",1,1)==DBStatus::OK);
    assert(engine.createUDF(db,"append_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('append_calls'); RETURN p; END;",'v',"plpgsql","integer")==DBStatus::OK);
    const auto build=[&](std::shared_ptr<PreparedQuery> query,bool plan=true) {
        const PreparedChildCursorFactory cursor=[&engine,db,query](const Stmt* child,const RowContext& row) {
            return QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&engine,db,query,child,row),
                query->statementOutputs.at(child));
        };
        return QueryPlanner::buildPreparedUnionAllPlan(&engine,db,query,static_cast<SelectStmt*>(query->ast.get()),
            [&engine,db,query](SelectStmt* branch,bool body,QueryPlanner::PreparedExecutionProvider owner) {
                return QueryPlanner::buildPreparedQueryPlan(&engine,db,query,branch,{},std::move(owner),body);
            },{},cursor,true,plan);
    };
    const std::vector<std::pair<std::string,PreparedQueryRows>> cases={
        {"SELECT 1 UNION ALL SELECT 2147483648",{{ExprValue("bigint","1")},{ExprValue("bigint","2147483648")}}},
        {"SELECT NULL UNION ALL SELECT 1",{{ExprValue("integer","",true)},{ExprValue("integer","1")}}},
        {"SELECT NULL::BIGINT UNION ALL SELECT 2",{{ExprValue("bigint","",true)},{ExprValue("bigint","2")}}},
        {"SELECT NULL UNION ALL SELECT 'NULL'",{{ExprValue("text","",true)},{ExprValue("text","NULL")}}},
        {"SELECT '' UNION ALL SELECT 'a b'",{{ExprValue("text","")},{ExprValue("text","a b")}}},
        {"SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2147483648",{{ExprValue("bigint","1")},{ExprValue("bigint","1")},{ExprValue("bigint","2147483648")}}},
        {"SELECT ARRAY[1] UNION ALL SELECT ARRAY[2147483648]",{{ExprValue("bigint[]","{1}")},{ExprValue("bigint[]","{2147483648}")}}},
        {"SELECT 1 WHERE false UNION ALL SELECT 2 WHERE false",{}},
        {"SELECT CASE WHEN false THEN(SELECT 1/0) ELSE 2 END UNION ALL SELECT 3",{{ExprValue("integer","2")},{ExprValue("integer","3")}}},
    };
    for(const auto& control:cases) {
        auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,control.first));
        auto* select=static_cast<SelectStmt*>(query->ast.get());const auto* rhs=select->setOpRhs.get();
        auto graph=build(query);assert(graph->preparedPlanNodeName()=="Append" && graph->preparedPlanChildren().size()==2);
        auto cursor=QueryPlanner::makePreparedCursor(std::move(graph),query->output);
        for(const auto& expected:control.second) {
            std::vector<ExprValue> actual;assert(cursor->next(actual) && actual.size()==expected.size());
            for(size_t i=0;i<actual.size();++i)
                assert(actual[i].typeName==expected[i].typeName && actual[i].isNull==expected[i].isNull &&
                    (actual[i].isNull || actual[i].value==expected[i].value));
        }
        std::vector<ExprValue> exhausted;assert(!cursor->next(exhausted));cursor->close();
        assert(select->setOp==SetOp::Union && select->setOpAll && select->setOpRhs.get()==rhs);
        std::cout<<"TYPED APPEND "<<control.first<<" passed\n";
    }
    auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT 1 UNION ALL SELECT append_writer(2)"));
    auto cursor=QueryPlanner::makePreparedCursor(build(query),query->output);
    std::vector<ExprValue> row;assert(cursor->next(row) && row.front().value=="1");cursor->close();
    assert(engine.nextval(db,"append_calls")==1); // right branch was never opened/evaluated
    assert(cursor->supportsRestart());cursor->restart({});
    assert(cursor->next(row) && row.front().value=="1");
    assert(cursor->next(row) && row.front().value=="2");assert(!cursor->next(row));cursor->close();
    assert(engine.nextval(db,"append_calls")==3); // exactly one new invocation
    auto sites=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT(SELECT append_writer(7)) UNION ALL SELECT(SELECT append_writer(7))"));
    auto distinct=QueryPlanner::makePreparedCursor(build(sites),sites->output);
    assert(distinct->next(row) && row.front().value=="7");
    assert(distinct->next(row) && row.front().value=="7");assert(!distinct->next(row));distinct->close();
    assert(engine.nextval(db,"append_calls")==6); // two genuine sites, not one SQL/value-key memo
    auto typed=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "SELECT $1 UNION ALL SELECT 2147483648",{{"p","p","integer",{},true,"7",1}}));
    auto parameters=QueryPlanner::makePreparedCursor(build(typed),typed->output);
    assert(parameters->next(row) && row.front().typeName=="bigint" && row.front().value=="7");
    parameters->close();
    bool rejected=false;
    try {
        auto bad=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT append_writer(1) UNION ALL SELECT 1/0"));
        (void)build(bad);
    } catch(const DbError& error){rejected=error.sqlState()=="22012";}
    assert(rejected && engine.nextval(db,"append_calls")==7); // pure root planning before left writer
    std::cout<<"[TYPED APPEND] rows/types/NULL/lazy RHS/restart/original sites/pure root passed\n";
    cleanupTestDb(name);
}
