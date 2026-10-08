#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();const auto database=testDbPath("runtime_parameter_set_branch_context");
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    TableSchema calls;calls.tablename="calls";calls.append(makeIntColumn("id",false,4));
    if(g_engine.createTable(database,calls)!=DBStatus::OK || g_engine.createUDF(database,"scope_writer",{"p"},{"integer"},"BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","integer")!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    const auto require=[&](bool valid,const std::string& label){++controls;if(!valid){++failures;std::cerr<<"[SET BRANCH CONTEXT FAIL] "<<label<<'\n';}};
    const auto count=[&]{return g_engine.query(database,"calls",{},{"id"}).size();};
    for(const int mode:{0,1,2})for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        QueryBindingDatum datum={"actual-set-branch-input","p","integer",{},false,{},1};datum.origin=origin;
        const auto sql=mode==2?"SELECT(SELECT $1 WHERE FALSE UNION ALL VALUES(scope_writer($1)))":mode==0?"SELECT 0=ANY(SELECT $1 WHERE FALSE UNION ALL VALUES(scope_writer($1)))":"SELECT 0=ALL(SELECT $1 WHERE FALSE UNION ALL VALUES(scope_writer($1)))";
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql,{datum}));auto* select=dynamic_cast<SelectStmt*>(query->ast.get());auto* expression=select->selectList[0].expr.get();
        PreparedQueryExecution execution(query,&g_engine,database);
        const PreparedChildCursorFactory factory=[&](const Stmt* statement,const RowContext& caller){return QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,statement,caller),query->statementOutputs.at(statement));};
        execution.setChildCursorFactory(factory,mode!=2);
        if(mode==2)execution.setQueryExecutor([&](const Stmt* statement,const RowContext& caller,size_t demand){
            auto cursor=factory(statement,caller);PreparedQueryRows rows;std::vector<ExprValue> cells;
            try{while(rows.size()<demand && cursor->next(cells))rows.push_back(cells);cursor->close();}
            catch(...){const auto failure=std::current_exception();try{cursor->close();}catch(...){}std::rethrow_exception(failure);}return rows;
        });
        const auto before=count();execution.prepareExpression(expression);execution.prepareChildCursors();require(count()==before,"set branch typed preparation is pure");
        const auto plans=execution.childPlans(expression);auto caller=execution.context();
        for(const auto& value:std::vector<std::optional<std::string>>{{},"0","1"}) {
            if(origin!=ParameterOrigin::StatementInput)caller.setParameters({ExprValue("integer",value.value_or(""),!value)});
            const auto actual=execution.evaluate(expression,caller);const bool null=origin==ParameterOrigin::StatementInput || !value;
            require(actual.typeName==(mode==2?"integer":"boolean") && actual.isNull==null && (null || actual.value==(mode==2?*value:*value=="0"?"t":"f")),std::string(sql)+" origin="+std::to_string(static_cast<unsigned>(origin))+" input="+value.value_or("NULL"));
            if(mode!=2)require(execution.childPlans(expression)==plans && plans.size()==1,"set/VALUES original cursor graph remains same");
        }
        require(count()-before==(origin==ParameterOrigin::StatementInput?1:3),"set branch reader/scalar memo or quantified demand uses actual origin");execution.closeChildCursors();
    }
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        QueryBindingDatum datum={"actual-borrowed-branch-input","p","integer",{},true,"1",1};datum.origin=origin;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT scope_writer($1) UNION ALL VALUES((SELECT scope_writer($1)))",{datum}));
        PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();const auto before=count();
        auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output);const auto* graph=cursor->plan();
        require(count()==before && cursor->supportsRestart(),"borrowed VALUES graph preparation pure/restart contract");
        for(size_t invocation=0;invocation<3;++invocation) {
            if(invocation)cursor->restart(caller);
            const size_t rows=invocation?2:1;
            for(size_t ordinal=0;ordinal<rows;++ordinal) {
                std::vector<ExprValue> cells;require(cursor->next(cells) && cells.size()==1 && cells[0].typeName=="integer" && !cells[0].isNull && cells[0].value=="1","borrowed VALUES actual typed row");
            }
            cursor->close();require(count()-before==1+2*invocation && cursor->plan()==graph,"never-opened VALUES + live provider lifetime + exact effects1/3/5");
        }
    }
    require(controls==90,"all cursor/reader/borrowed provider matrix reached");
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[RUNTIME PARAMETER SET BRANCH CONTEXT] controls="<<controls<<" failures="<<failures<<'\n';return failures?1:0;
}
