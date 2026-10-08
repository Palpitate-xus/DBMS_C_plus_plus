#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();const auto database=testDbPath("runtime_parameter_child_context");
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    TableSchema calls;calls.tablename="calls";calls.append(makeIntColumn("id",false,4));
    if(g_engine.createTable(database,calls)!=DBStatus::OK ||
       g_engine.createUDF(database,"memo_writer",{"p"},{"varbit"},"BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","varbit")!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    const auto require=[&](bool valid,const std::string& label){++controls;if(!valid){++failures;std::cerr<<"[RUNTIME PARAMETER CHILD FAIL] "<<label<<'\n';}};
    const auto count=[&]{return g_engine.query(database,"calls",{},{"id"}).size();};
    for(const bool cursorOwner:{false,true})
        for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
            QueryBindingDatum datum={"actual-owned-runtime-input","p","bit varying",{},false,{},1};datum.origin=origin;
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT(SELECT memo_writer($1)) AS value",{datum}));
            auto* select=dynamic_cast<SelectStmt*>(query->ast.get());auto* child=select->selectList[0].expr.get();
            PreparedQueryExecution execution(query,&g_engine,database);
            if(cursorOwner)execution.setChildCursorFactory([&](const Stmt* statement,const RowContext& caller) {
                return QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,statement,caller),query->statementOutputs.at(statement));
            });
            execution.prepareExpression(child);auto row=execution.context();const auto before=count();
            for(const auto& value:std::vector<std::optional<std::string>>{{},"00","01"}) {
                if(origin!=ParameterOrigin::StatementInput)row.setParameters({ExprValue("bit varying",value.value_or(""),!value)});
                const auto actual=execution.evaluate(child,row);const bool null=origin==ParameterOrigin::StatementInput || !value;
                require(actual.typeName=="bit varying" && actual.isNull==null && (null || actual.value==*value),
                    "original12 value/NULL/frame owner="+std::to_string(cursorOwner)+" role="+std::to_string(static_cast<unsigned>(origin))+" input="+value.value_or("NULL"));
            }
            require(count()-before==(origin==ParameterOrigin::StatementInput?1:3),"original12 actual writer demand and true statement memo");
        }
    require(controls==24,"all original12 plus actual second cursor owner reached");
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        QueryBindingDatum datum={"actual-context-input","p","bit varying",{},false,{},1};datum.origin=origin;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT $1 AS value",{datum}));
        PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();
        caller.setParameters({ExprValue("bit varying","01")});const auto actual=execution.context(caller).parameter(0);
        require(actual.typeName=="bit varying" && actual.isNull==(origin==ParameterOrigin::StatementInput) &&
            (actual.isNull || actual.value=="01"),"explicit frame role, no datum/slot ownership guess");
        auto extra=caller;extra.setParameters({ExprValue("integer","1"),ExprValue("integer","2")});bool frameValid=false;
        try {const auto cell=execution.context(extra).parameter(0);frameValid=origin==ParameterOrigin::StatementInput && cell.isNull && cell.typeName=="bit varying";}
        catch(const DbError& error) {frameValid=origin!=ParameterOrigin::StatementInput && error.sqlState()=="XX000";}
        require(frameValid,"only actual runtime frame validates caller width; true statement input never adopts foreign cells");
        if(origin!=ParameterOrigin::StatementInput) {
            caller.setParameters({ExprValue("integer","1")});bool invalid=false;
            const auto before=count();
            try{(void)execution.context(caller);}catch(const DbError& error){invalid=error.sqlState()=="XX000";}
            require(invalid && count()==before,"runtime/metadata frame declared type guard is pure");
        }
    }
    for(const size_t cap:{0u,1u})
      for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder}) {
        QueryBindingDatum datum={"actual-context-cap-input","p","bit varying",{},false,{},1};datum.origin=origin;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT(SELECT memo_writer($1)) AS value LIMIT "+std::to_string(cap),{datum}));
        PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();
        caller.setParameters({ExprValue("bit varying","01")});const auto before=count();
        auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output);
        require(count()==before && cursor->descriptor().size()==1 && cursor->descriptor()[0].type=="bit varying","cap descriptor/graph preparation is pure");
        std::vector<ExprValue> cells;const auto produced=cursor->next(cells);
        require(produced==(cap==1) && (!produced || (cells.size()==1 && cells[0].typeName=="bit varying" &&
            cells[0].isNull==(origin==ParameterOrigin::StatementInput) && (cells[0].isNull || cells[0].value=="01"))),"cap0/1 runtime frame and true statement NULL result");
        require(count()-before==cap,"cap0 has zero writer effects; cap1 has exactly one");cursor->close();
      }
    require(controls==51,"all original12, actual two owners, three origins, pure guards and cap0/1 reached");
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[RUNTIME PARAMETER CHILD CONTEXT] complete "<<controls<<" controls failures="<<failures<<'\n';return failures?1:0;
}
