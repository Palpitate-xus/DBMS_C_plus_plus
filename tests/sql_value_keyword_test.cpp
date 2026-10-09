#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "expression/sql_value.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("sql_value_keyword");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;session.username="reader";session.originalRole="login";
    session.currentRole="role_a";session.currentDB=database;session.searchPath="public";
    const auto previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    size_t checked=0;
    const std::vector<std::string> names={"current_user","session_user","current_role","current_catalog",
        "current_schema","current_date","current_timestamp","localtimestamp","current_time","localtime"};
    const std::vector<uint32_t> oids={19,19,19,19,19,1082,1184,1114,1266,1083};
    const std::vector<std::string> types={"name","name","name","name","name","date","timestamptz","timestamp","timetz","time"};
    for(size_t i=0;i<names.size();++i) {
        const auto sql="SELECT "+names[i];
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
        assert(query->output.size()==1 && query->output[0].name==names[i] &&
            query->output[0].type==types[i] && query->output[0].typeOid==oids[i]);
        const auto* select=static_cast<const SelectStmt*>(query->ast.get());
        const auto* call=dynamic_cast<const FunctionCallExpr*>(select->selectList[0].expr.get());
        assert(call && call->sqlValue==sql_value_detail::kind(names[i]) && call->toString()==names[i]);
        PreparedQueryExecution execution(query,&g_engine,database);
        execution.planStatementConstants(select);
        execution.prepareExpression(select->selectList[0].expr.get());
        RowContext row;row.set(names[i],ExprValue("text","ordinary column cannot replace a keyword"));
        const auto value=execution.evaluate(call,row);
        assert(value.typeName==types[i] && !value.isNull);
        if(i==0 || i==2)assert(value.value=="role_a");
        if(i==1)assert(value.value=="login");
        if(i==3)assert(value.value==database);
        if(i==4)assert(value.value=="public");
        assert(query->legacySql()==sql);
        ++checked;
    }
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT current_user"));
    const auto* expr=static_cast<const SelectStmt*>(query->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution execution(query,&g_engine,database);
    execution.planStatementConstants(query->ast.get());
    execution.prepareExpression(static_cast<SelectStmt*>(query->ast.get())->selectList[0].expr.get());
    assert(execution.evaluate(expr,{}).value=="role_a");
    session.currentRole="role_b";
    assert(execution.evaluate(expr,{}).value=="role_b");++checked;
    RowContext owned;owned.setSqlValue(FunctionCallExpr::SqlValue::CurrentUser,ExprValue("name","host_user"));
    assert(execution.evaluate(expr,owned).value=="host_user");++checked;
    ExprEvaluator direct;direct.setCurrentDB(database);
    size_t effects=0;
    direct.registerFunction("current_user",[&](const std::vector<ExprValue>&){++effects;return ExprValue("name","fake callee");});
    const auto parsed=SQLParser().parse("SELECT current_user");assert(parsed.success);
    assert(direct.eval(static_cast<const SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get(),{}).value=="role_b");
    assert(effects==0);++checked;
    for(const auto* name:{"current_timestamp","localtimestamp","current_time","localtime"}) {
        const auto sql="SELECT "+std::string(name)+"(3)";
        const auto prepared=g_engine.prepareBoundQuery(database,sql);
        const auto* select=static_cast<const SelectStmt*>(prepared.ast.get());
        const auto* call=dynamic_cast<const FunctionCallExpr*>(select->selectList[0].expr.get());
        assert(call && call->sqlValuePrecision==3 && call->toString()==std::string(name)+"(3)");++checked;
    }
    for(const auto* sql:{"SELECT current_user()","SELECT session_user()","SELECT current_role()",
         "SELECT current_catalog()","SELECT current_date()","SELECT localtime()","SELECT localtimestamp()",
         "SELECT current_timestamp(1+1)","SELECT current_timestamp(-1)"}) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){state=error.sqlState();}
        assert(state=="42601");++checked;
        const auto raw=SQLParser().parse(sql);
        assert(!raw.success && raw.sqlState=="42601" && !raw.stmt && !raw.error.empty());++checked;
        const auto binding=SQLParser().parseForBinding(sql);
        assert(!binding.success && binding.sqlState=="42601" && !binding.stmt && !binding.error.empty());++checked;
    }
    for(const auto* sql:{"SELECT \"current_user\"()","SELECT pg_catalog.\"current_user\"()",
         "SELECT current_schema()","SELECT pg_catalog.current_schema()","SELECT current_database()"}) {
        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
        const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(
            &g_engine,database,prepared,prepared->ast.get()));
        result.throwIfFailed();assert(result.structuredRowsAvailable && result.structuredRows.size()==1);++checked;
    }
    std::cout<<"SQL_VALUE_KEYWORD_CHECKED="<<checked<<" FAILED=0\n";
}
