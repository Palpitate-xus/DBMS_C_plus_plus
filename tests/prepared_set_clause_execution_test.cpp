#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

using namespace dbms;
int main() {
    TypeRegistry::instance().bootstrap();StorageEngine engine;
    const std::string name="prepared_set_clause_execution";cleanupTestDb(name);
    const auto db=testDbPath(name);assert(engine.createDatabase(db,"utf8")==DBStatus::OK);
    assert(engine.createSequence(db,"set_clause_calls",1,1)==DBStatus::OK);
    assert(engine.createUDF(db,"set_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('set_clause_calls'); RETURN p; END;",'v',"plpgsql","integer")==DBStatus::OK);
    const auto build=[&](std::shared_ptr<PreparedQuery> query) {
        return QueryPlanner::buildPreparedUnionAllPlan(&engine,db,query,static_cast<SelectStmt*>(query->ast.get()),
            [&engine,db,query](SelectStmt* branch,bool body,QueryPlanner::PreparedExecutionProvider owner) {
                return QueryPlanner::buildPreparedQueryPlan(&engine,db,query,branch,{},std::move(owner),body);
            },{},{},true,true);
    };
    const auto consume=[&](const std::string& sql,const PreparedQueryRows& expected,
                           const std::vector<QueryBindingDatum>& parameters=std::vector<QueryBindingDatum>{}) {
        auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,sql,parameters));
        const auto* original=query->ast.get();
        auto cursor=QueryPlanner::makePreparedCursor(build(query),query->output);
        for(const auto& wanted:expected) {
            std::vector<ExprValue> actual;assert(cursor->next(actual) && actual.size()==wanted.size());
            for(const auto& cell:actual)std::cout<<"SET ACTUAL CELL "<<sql<<" type="<<cell.typeName<<" null="<<cell.isNull<<" value="<<cell.value<<std::endl;
            for(size_t column=0;column<actual.size();++column)
                assert(actual[column].typeName==wanted[column].typeName && actual[column].isNull==wanted[column].isNull &&
                    (actual[column].isNull || actual[column].value==wanted[column].value));
        }
        std::vector<ExprValue> end;assert(!cursor->next(end));cursor->close();
        assert(query->ast.get()==original);
        std::cout<<"SET CLAUSE ACTUAL "<<sql<<" passed\n";
    };
    consume("SELECT 10 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1",{{ExprValue("integer","2")}});
    consume("SELECT NULL::BIGINT AS n UNION ALL SELECT 2 ORDER BY n NULLS LAST",{{ExprValue("bigint","2")},{ExprValue("bigint","",true)}});
    consume("SELECT 1 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n FETCH FIRST 1 ROW WITH TIES",{{ExprValue("integer","1")},{ExprValue("integer","1")}});
    consume("(SELECT 3 AS n LIMIT 0) UNION ALL SELECT 2",{{ExprValue("integer","2")}});
    consume("SELECT 3 AS n UNION ALL (SELECT 2 LIMIT 0) ORDER BY n",{{ExprValue("integer","3")}});
    consume("SELECT $1 AS n UNION ALL SELECT 2147483648 ORDER BY n DESC LIMIT 1",{{ExprValue("bigint","2147483648")}},
        {{"p","p","integer",{},true,"7",1}});
    assert(engine.nextval(db,"set_clause_calls")==1); // graph/metadata have made zero calls
    consume("SELECT set_writer(1) AS n UNION ALL SELECT set_writer(2) LIMIT 1",{{ExprValue("integer","1")}});
    assert(engine.nextval(db,"set_clause_calls")==3);
    consume("SELECT set_writer(2) AS n UNION ALL SELECT set_writer(1) ORDER BY n LIMIT 1",{{ExprValue("integer","1")}});
    assert(engine.nextval(db,"set_clause_calls")==6);
    consume("SELECT set_writer(1) AS n UNION ALL SELECT set_writer(2) LIMIT 0",{});
    assert(engine.nextval(db,"set_clause_calls")==7);
    consume("(SELECT set_writer(1) AS n LIMIT 0) UNION ALL SELECT set_writer(2)",{{ExprValue("integer","2")}});
    assert(engine.nextval(db,"set_clause_calls")==9);
    bool rejected=false;
    try {
        auto bad=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,"SELECT set_writer(1) AS n UNION ALL SELECT 1/0 LIMIT 0"));
        (void)build(bad);
    } catch(const DbError& error){rejected=error.sqlState()=="22012";}
    assert(rejected && engine.nextval(db,"set_clause_calls")==10);
    consume("SELECT(SELECT set_writer(7)) AS n UNION ALL SELECT(SELECT set_writer(7)) ORDER BY n",
        {{ExprValue("integer","7")},{ExprValue("integer","7")}});
    assert(engine.nextval(db,"set_clause_calls")==13); // distinct original sites, not sort re-evaluation
    cleanupTestDb(name);
}
