#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();const auto database=testDbPath("runtime_parameter_quantified_context");
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    TableSchema calls;calls.tablename="calls";calls.append(makeIntColumn("id",false,4));
    if(g_engine.createTable(database,calls)!=DBStatus::OK ||
       g_engine.createUDF(database,"runtime_writer",{"p"},{"integer"},"BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","integer")!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    const auto require=[&](bool valid,const std::string& label){++controls;if(!valid){++failures;std::cerr<<"[RUNTIME PARAMETER QUANTIFIED FAIL] "<<label<<'\n';}};
    const auto count=[&]{return g_engine.query(database,"calls",{},{"id"}).size();};
    for(const auto quantifier:{"ANY","ALL"})
       for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        const auto sql="SELECT 0="+std::string(quantifier)+"(SELECT runtime_writer($1))";
        QueryBindingDatum datum={"actual-quantified-input","p","integer",{},false,{},1};datum.origin=origin;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql,{datum}));
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());auto* expression=select->selectList[0].expr.get();
        PreparedQueryExecution execution(query,&g_engine,database);
        execution.setChildCursorFactory([&](const Stmt* statement,const RowContext& caller) {
            return QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,statement,caller),query->statementOutputs.at(statement));
        });
        const auto before=count();execution.prepareExpression(expression);execution.prepareChildCursors();
        require(count()==before,"actual graph preparation is pure, no runtime input/volatile evaluation");
        const auto plans=execution.childPlans(expression);require(plans.size()==1,"genuine original prepared child graph exists");
        auto row=execution.context();
        for(const auto& value:std::vector<std::optional<std::string>>{{},"0","1"}) {
            if(origin!=ParameterOrigin::StatementInput)row.setParameters({ExprValue("integer",value.value_or(""),!value)});
            const auto actual=execution.evaluate(expression,row);const bool null=origin==ParameterOrigin::StatementInput || !value;
            require(actual.typeName=="boolean" && actual.isNull==null && (null || actual.value==(*value=="0"?"t":"f")),
                sql+" role="+std::to_string(static_cast<unsigned>(origin))+" input="+value.value_or("NULL"));
            require(execution.childPlans(expression)==plans,"restart retains the genuine operator graph, no replacement provider");
        }
        require(count()-before==(origin==ParameterOrigin::StatementInput?1:3),"actual writer demand; true statement input cache retained");
        execution.closeChildCursors();
       }
    // This is the actual VALUES operator API, not the separately unsupported
    // ANY(VALUES ...) grammar. It must rebind its real frame and retain graph.
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        QueryBindingDatum datum={"actual-values-input","p","integer",{},false,{},1};datum.origin=origin;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"VALUES(runtime_writer($1))",{datum}));
        PreparedQueryExecution execution(query,&g_engine,database);auto row=execution.context();const auto before=count();
        auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),row),query->output);
        const auto* graph=cursor->plan();
        require(count()==before && graph && cursor->supportsRestart() && cursor->descriptor().size()==1 &&
            cursor->descriptor()[0].type=="integer","VALUES genuine typed descriptor and pure restart admission");
        for(const auto& value:std::vector<std::optional<std::string>>{{},"0","1"}) {
            if(origin!=ParameterOrigin::StatementInput)row.setParameters({ExprValue("integer",value.value_or(""),!value)});
            cursor->close();cursor->restart(row);std::vector<ExprValue> cells;const bool present=cursor->next(cells);
            const bool null=origin==ParameterOrigin::StatementInput || !value;
            require(present && cells.size()==1 && cells[0].typeName=="integer" && cells[0].isNull==null &&
                (null || cells[0].value==*value),"VALUES actual origin/NULL/typed caller frame");
            require(!cursor->next(cells),"VALUES one-row cardinality");
            require(cursor->plan()==graph,"VALUES restart retains genuine original operator graph");
        }
        require(count()-before==3,"VALUES executes one writer per actual cursor invocation, no scalar memo contract");cursor->close();
    }
    require(controls==87,"all original24 quantified controls plus actual VALUES, pure admission and graph reuse reached");
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[RUNTIME PARAMETER QUANTIFIED CONTEXT] complete "<<controls<<" controls failures="<<failures<<'\n';return failures?1:0;
}
