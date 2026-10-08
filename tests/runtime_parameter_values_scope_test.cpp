#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();const auto database=testDbPath("values_child_execution_scope");
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    TableSchema calls;calls.tablename="calls";calls.append(makeIntColumn("id",false,4));
    if(g_engine.createTable(database,calls)!=DBStatus::OK || g_engine.createUDF(database,"scope_writer",{"p"},{"integer"},"BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","integer")!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder})
      for(const auto sql:{"SELECT(SELECT scope_writer($1))","VALUES((SELECT scope_writer($1)))","SELECT 0=ALL(SELECT scope_writer($1))","VALUES(0=ALL(SELECT scope_writer($1)))"}) {
        QueryBindingDatum datum={"actual-child-execution-input","p","integer",{},true,"1",1};datum.origin=origin;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql,{datum}));PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();
        auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output);const auto* graph=cursor->plan();
        const auto before=g_engine.query(database,"calls",{},{"id"}).size();
        for(size_t invocation=1;invocation<=3;++invocation) {
            if(invocation>1)cursor->restart(caller);std::vector<ExprValue> cells;++controls;
            const bool present=cursor->next(cells);const auto actual=g_engine.query(database,"calls",{},{"id"}).size()-before;
            const auto type=std::string(sql).find("ALL")!=std::string::npos?"boolean":"integer";
            const auto value=std::string(type)=="boolean"?"f":"1";
            const bool valid=present && cells.size()==1 && cells[0].typeName==type && cells[0].value==value && !cells[0].isNull && actual==invocation && cursor->plan()==graph;
            if(!valid)++failures;std::cout<<"VALUES CHILD SCOPE "<<sql<<" origin="<<static_cast<unsigned>(origin)<<" invocation="<<invocation<<" writer="<<actual<<" expected="<<invocation<<" value="<<(cells.empty()?"NO ROW":cells[0].value)<<" valid="<<valid<<'\n';cursor->close();
        }
    }
    ++controls;if(controls!=37)++failures;
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[VALUES CHILD SCOPE] complete controls="<<controls<<" failures="<<failures<<'\n';return failures?1:0;
}

