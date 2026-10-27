#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="gcd_lcm_constant_planning",database=testDbPath(fixture);
    cleanupTestDb(fixture);if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE danger(a integer,b integer)",session)||
       ddl.executeSql("INSERT INTO danger VALUES('-2147483648',0)",session)||
       ddl.executeSql("CREATE SCHEMA private_owner",session))return 2;
    size_t controls=0,failures=0;
    const auto require=[](bool value){if(!value)throw std::runtime_error("value mismatch");};
    const auto check=[&](const std::string& label,const std::function<void()>& action) {
        ++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"GCD_LCM_CONSTANT_PLANNING_FAIL "<<label<<" : "<<error.what()<<'\n';}
    };
    const auto state=[](const std::exception& error) {
        if(const auto* typed=dynamic_cast<const DbError*>(&error))return typed->sqlState();
        const std::string text=error.what();const auto start=text.find("SQLSTATE ");return start==std::string::npos?std::string{}:text.substr(start+9,5);
    };
    const auto planned=[&](const std::string& expression,const std::string& type,const std::string& expected,bool null=false,const std::string& planningState="",const std::string& tail="") {
        check(expression+tail,[&] {
            // Whole-query binding and construction remain pure. The explicit
            // planning API owns this mathematical constant's early error.
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+expression+" AS v"+tail));
            require(query->output.size()==1&&query->output[0].type==type);
            auto* select=dynamic_cast<SelectStmt*>(query->ast.get());require(select&&select->selectList.size()==1);
            PreparedQueryExecution execution(query,&g_engine,database);auto* node=select->selectList.front().expr.get();execution.prepareExpression(node);
            std::string actual;
            try{execution.planStatementConstants(query->ast.get());}catch(const std::exception& error){actual=state(error);}
            require(actual==planningState);
            if(planningState.empty()) {
                const auto value=execution.evaluate(node,execution.context());
                require(value.typeName==type&&value.isNull==null&&(null||value.value==expected));
            }
            execution.closeChildCursors();
        });
    };
    for(const auto* fn:{"gcd","lcm"}) {
        const std::string name=fn,result=name=="gcd"?"2":"12";
        planned(name+"(6,4)","integer",result);
        planned(name+"(6::bigint,4)","bigint",result);
        planned(name+"(6::numeric(9,2),4)","numeric",result+".00");
        planned(name+"('6',4)","integer",result);
        planned(name+"(NULL::integer,4)","integer","",true);
        planned(name+"('NaN'::numeric,4)","numeric","NaN");
    }
    const std::string overflow="gcd('-2147483648'::integer,0::integer)";
    for(const std::string tail:{""," FROM danger WHERE false"," FROM danger LIMIT 0"})planned(overflow,"integer","",false,"22003",tail);
    planned("COALESCE(7,"+overflow+")","integer","7");
    planned("CASE WHEN false THEN "+overflow+" ELSE 7 END","integer","7");
    for(const auto* fn:{"gcd","lcm"}) {
        check(std::string("volatile input ")+fn,[&] {
            const std::string seq=std::string("effects_")+fn;require(!ddl.executeSql("CREATE SEQUENCE "+seq,session));
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,std::string("SELECT ")+fn+"(nextval('"+seq+"'),6::bigint) AS v"));
            auto* select=dynamic_cast<SelectStmt*>(query->ast.get());require(select);
            PreparedQueryExecution execution(query,&g_engine,database);auto* node=select->selectList.front().expr.get();execution.prepareExpression(node);
            execution.planStatementConstants(query->ast.get());require(g_engine.nextval(database,seq)==1);
            const auto result=execution.evaluate(node,execution.context());require(result.typeName=="bigint"&&!result.isNull&&result.value==(std::string(fn)=="gcd"?"2":"6"));
            require(g_engine.nextval(database,seq)==3);execution.closeChildCursors();
        });
        check(std::string("actual immutable stored owner ")+fn,[&] {
            require(!ddl.executeSql(std::string("CREATE FUNCTION private_owner.")+fn+"(integer,integer) RETURNS integer LANGUAGE SQL IMMUTABLE AS 'SELECT 1/0'",session));
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,std::string("SELECT private_owner.")+fn+"(6,4) AS v"));
            auto* select=dynamic_cast<SelectStmt*>(query->ast.get());require(select);
            PreparedQueryExecution execution(query,&g_engine,database);auto* node=select->selectList.front().expr.get();execution.prepareExpression(node);
            // This API deliberately never obtains a user routine as a planning
            // datum, even if its spelling and declared volatility look builtin.
            execution.planStatementConstants(query->ast.get());std::string actual;
            try{(void)execution.evaluate(node,execution.context());}catch(const std::exception& error){actual=state(error);}
            require(actual=="22012");execution.closeChildCursors();
        });
        for(const std::string tail:{""," WHERE false"," LIMIT 0"})check(std::string("variable input ")+fn+tail,[&] {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,std::string("SELECT ")+fn+"(a,b) AS v FROM danger"+tail));
            require(query->output.size()==1&&query->output[0].type=="integer");
            PreparedQueryExecution execution(query,&g_engine,database);execution.planStatementConstants(query->ast.get());execution.closeChildCursors();
        });
    }
    require(controls==27);require(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(fixture);finalCleanupTestData();
    std::cout<<"GCD_LCM_CONSTANT_PLANNING_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;return failures?1:0;
}
