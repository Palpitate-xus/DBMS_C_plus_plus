#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
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
    const auto database=testDbPath("numeric_gcd_lcm_type");
    cleanupTestDb("numeric_gcd_lcm_type");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    struct Control {std::string a,b,type,gcd,lcm;};
    const std::vector<Control> cases={
        {"6e-40","4e-40","numeric","0."+std::string(39,'0')+"2","0."+std::string(38,'0')+"12"},
        {"6E40","4e+40","numeric","2"+std::string(40,'0'),"12"+std::string(40,'0')},
        {"1.250e-2","2.50e-3","numeric","0.00250","0.01250"},
        {"6","4","integer","2","12"},
        {"6::smallint","4","integer","2","12"},
        {"6::bigint","4","bigint","2","12"},
        {"6","4::bigint","bigint","2","12"},
        {"6::bigint","4e0","numeric","2","12"},
        {"'6e-40'","4e-40","numeric","0."+std::string(39,'0')+"2","0."+std::string(38,'0')+"12"},
        {"NULL::numeric","4","numeric","",""},
    };
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,bool good){
        ++controls;if(!good)++failures;
        std::cout<<"NUMERIC_GCD_LCM_NATIVE "<<label<<" match="<<good<<std::endl;
    };
    for(const auto& control:cases)for(const auto& function:std::vector<std::string>{"gcd","lcm"}) {
        const auto expression=function+"("+control.a+","+control.b+")";
        const auto expected=function=="gcd"?control.gcd:control.lcm;
        check("inference "+expression,ExprHelper::inferResultType(expression,{})==control.type);
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+expression));
        check("metadata "+expression,query->output.size()==1 && query->output[0].type==control.type);
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
        PreparedQueryExecution execution(query,&g_engine,database);
        execution.prepareExpression(select->selectList[0].expr.get());
        const auto value=execution.evaluate(select->selectList[0].expr.get(),execution.context());
        check("execution "+expression,value.typeName==control.type && value.isNull==expected.empty() && (value.isNull || value.value==expected));
        execution.closeChildCursors();
    }
    Session session;session.permission=1;session.username="testuser";session.currentDB=database;
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE gcd_inputs(n NUMERIC,b BIGINT,i INT)",session));
    const auto prepared=g_engine.prepareBoundQuery(database,"SELECT gcd(n,4e-40),lcm(n,4e-40),gcd(b,i),lcm(i,b) FROM gcd_inputs");
    check("empty table metadata",prepared.output.size()==4 && prepared.output[0].type=="numeric" && prepared.output[1].type=="numeric" && prepared.output[2].type=="bigint" && prepared.output[3].type=="bigint");
    assert(g_engine.createSequence(database,"gcd_calls",1,1)==DBStatus::OK);
    const auto pure=g_engine.prepareBoundQuery(database,"SELECT gcd(nextval('gcd_calls'),4e-40),lcm(nextval('gcd_calls'),4::bigint)");
    check("pure metadata",pure.output.size()==2 && pure.output[0].type=="numeric" && pure.output[1].type=="bigint");
    check("metadata never calls nextval",g_engine.nextval(database,"gcd_calls")==1);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb("numeric_gcd_lcm_type");finalCleanupTestData();
    assert(controls==63);
    std::cout<<"NUMERIC_GCD_LCM_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    assert(failures==0);
}
