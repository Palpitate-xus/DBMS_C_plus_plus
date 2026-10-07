#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name = "quantified_qualification_planning";
    cleanupTestDb(name);
    const auto db = testDbPath(name);
    assert(engine.createDatabase(db,"utf8") == DBStatus::OK);
    TableSchema schema; schema.len=1;
    schema.cols[0].dataName="id"; schema.cols[0].dataType="int"; schema.cols[0].dsize=4;
    assert(engine.createTable(db,"qq_rows",schema) == DBStatus::OK);
    assert(engine.createSequence(db,"qq_calls",1,1) == DBStatus::OK);
    assert(engine.createUDF(db,"qq_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('qq_calls'); RETURN p; END;",'v',"plpgsql","integer") == DBStatus::OK);
    const std::vector<std::pair<std::string,std::string>> cases = {
        {"DELETE FROM qq_rows WHERE false AND id=ANY(SELECT 1/0)","22012"},
        {"DELETE FROM qq_rows WHERE false AND 1=ANY(SELECT 1/0)",""},
        {"DELETE FROM qq_rows WHERE false AND id=ALL(SELECT 1/0)",""},
        {"DELETE FROM qq_rows WHERE true OR id=ANY(SELECT 1/0)",""},
        {"DELETE FROM qq_rows WHERE false AND(id=ANY(SELECT 1/0) OR true)",""},
        {"DELETE FROM qq_rows WHERE false AND CASE WHEN true THEN id=ANY(SELECT 1/0) ELSE false END",""},
        {"DELETE FROM qq_rows WHERE false AND(id+0)=ANY(SELECT 1/0)","22012"},
        {"DELETE FROM qq_rows WHERE false AND abs(id)=ANY(SELECT 1/0)","22012"},
        {"DELETE FROM qq_rows WHERE false AND qq_writer(id)=ANY(SELECT 1/0)",""},
        {"DELETE FROM qq_rows WHERE false AND(SELECT id)=ANY(SELECT 1/0)","22012"},
        {"DELETE FROM qq_rows WHERE false AND id=ANY(SELECT 1/0 WHERE false)","22012"},
        {"DELETE FROM qq_rows WHERE false AND id=ANY(SELECT qq_writer(1/0))","22012"},
        {"SELECT id FROM qq_rows WHERE false AND id=ANY(SELECT 1/0)","22012"},
        {"SELECT CASE WHEN false THEN id=ANY(SELECT 1/0) ELSE false END FROM qq_rows",""},
        {"WITH hint AS(SELECT 1) DELETE FROM qq_rows WHERE false AND id=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)","22012"},
        {"UPDATE qq_rows SET id=CAST(2147483648 AS INT) WHERE false AND id=ANY(SELECT 1/0)","22003"},
        {"DELETE FROM qq_rows WHERE false AND(SELECT qq_writer(id))=ANY(SELECT 1/0)",""},
        {"DELETE FROM qq_rows WHERE false AND(SELECT q.id FROM qq_rows q LIMIT 1)=ANY(SELECT 1/0)",""},
        {"DELETE FROM qq_rows WHERE false AND(SELECT q.id FROM qq_rows q WHERE q.id=qq_rows.id)=ANY(SELECT 1/0)","22012"},
    };
    size_t childAccesses=0, failures=0;
    for(const auto& control:cases) {
        std::string actual;
        try {
            auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,control.first));
            PreparedQueryExecution execution(query,&engine,db);
            execution.setQueryExecutor([&](const Stmt*,const RowContext&,size_t)->PreparedQueryRows {
                ++childAccesses; throw DbError("XX000","pure qualification planning executed a child");
            });
            execution.setChildCursorFactory([&](const Stmt*,const RowContext&)->std::unique_ptr<PreparedQueryCursor> {
                ++childAccesses; throw DbError("XX000","pure qualification planning constructed a cursor");
            });
            execution.planStatementConstants(query->ast.get());
        } catch(const DbError& error) { actual=error.sqlState(); }
        std::cout<<"PURE ANY QUAL "<<control.first<<" actual="<<actual<<" expected="<<control.second<<std::endl;
        failures+=actual!=control.second;
    }
    assert(childAccesses==0);
    assert(engine.nextval(db,"qq_calls")==1);
    // Planning still uses immutable AST sites, not a parameter's current
    // datum or a similarly named child-local column as a local parent Var.
    auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
        "DELETE FROM qq_rows WHERE false AND $1=ANY(SELECT 1/0)",
        {{"p","p","integer",{},true,"1",1}}));
    auto* root=static_cast<DeleteStmt*>(query->ast.get())->whereClause.get();
    auto* conjunction=dynamic_cast<BinaryOpExpr*>(root);
    assert(conjunction && dynamic_cast<QuantifiedComparisonExpr*>(conjunction->right.get()));
    const auto* originalAny=conjunction->right.get();
    PreparedQueryExecution parameterPlan(query,&engine,db);
    parameterPlan.planStatementConstants(query->ast.get());
    assert(conjunction->right.get()==originalAny);
    assert(failures==0);
    std::cout<<"[ANY QUAL PLANNING] "<<cases.size()<<" controls; zero child/routine effects\n";
    cleanupTestDb(name);
}
