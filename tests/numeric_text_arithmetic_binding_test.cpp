#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    cleanupTestDb("numeric_text_arithmetic");
    const auto database=testDbPath("numeric_text_arithmetic");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,bool good) {
        ++controls;if(!good)++failures;
        std::cout<<"NUMERIC_TEXT_NATIVE "<<label<<" match="<<good<<std::endl;
    };
    const auto rejects=[&](const std::string& sql) {
        std::string state;
        try {(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){state=error.sqlState();}
        check("prepare "+sql+" state="+state,state=="42883");
    };
    for(const auto& op:std::vector<std::string>{"+","-","*","/","%","^"})
        for(const auto& operands:std::vector<std::pair<std::string,std::string>>{
            {"'1e-40'::text","1e-40"},{"1e-40","'1e-40'::text"},
            {"NULL::text","1e-40"},{"'2'::varchar","1::numeric"},
            {"NULL::numeric","'2'::text"}})
            rejects("SELECT "+operands.first+" "+op+" "+operands.second);
    struct Control {std::string expression,type,value;};
    for(const auto& control:std::vector<Control>{
        {"'2'+1::numeric","numeric","3"},{"1::numeric+'2'","numeric","3"},
        {"'2'+1","integer","3"},{"'1e-40'+1e-40","numeric","0."+std::string(39,'0')+"2"},
        {"'2'::text::numeric+1","numeric","3"},{"1::integer+2::numeric","numeric","3"},
        {"1::bigint+2::numeric","numeric","3"},{"1.25::real+2::numeric","double precision","3.25"},
        {"NULL+1::numeric","numeric",""},{"NULL::numeric+1","numeric",""}}) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
        check("metadata "+control.expression,query->output.size()==1 && query->output[0].type==control.type);
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
        PreparedQueryExecution execution(query,&g_engine,database);
        execution.prepareExpression(select->selectList[0].expr.get());
        const auto value=execution.evaluate(select->selectList[0].expr.get(),execution.context());
        check("execution "+control.expression,value.typeName==control.type && value.isNull==control.value.empty() && (value.isNull || value.value==control.value));
        execution.closeChildCursors();
    }
    Session session;session.permission=1;session.username="testuser";session.currentDB=database;
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE text_inputs(t TEXT,v VARCHAR,n NUMERIC)",session));
    for(const auto& sql:std::vector<std::string>{
        "SELECT t+n FROM text_inputs","SELECT n+t FROM text_inputs",
        "SELECT v*n FROM text_inputs WHERE false","SELECT '2'::text+1::numeric LIMIT 0",
        "SELECT CASE WHEN false THEN '2'::text+1::numeric ELSE 0 END",
        "INSERT INTO text_inputs(n) VALUES('2'::text+1::numeric)",
        "UPDATE text_inputs SET n='2'::text+1::numeric WHERE false"}) rejects(sql);
    assert(g_engine.createSequence(database,"text_calls",1,1)==DBStatus::OK);
    rejects("SELECT '2'::text+nextval('text_calls')");
    check("binding never executes nextval",g_engine.nextval(database,"text_calls")==1);
    check("binding never writes rows",g_engine.query(database,"text_inputs",{},{}).empty());
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb("numeric_text_arithmetic");finalCleanupTestData();
    assert(controls==60);
    std::cout<<"NUMERIC_TEXT_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    assert(failures==0);
}
