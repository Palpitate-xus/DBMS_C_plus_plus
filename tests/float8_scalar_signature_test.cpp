#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="float8_scalar_signature",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(i INT,t TEXT)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool valid,const std::string& label) {
        ++controls;
        if(!valid){++failures;std::cerr<<"FLOAT8_SIGNATURE_FAIL "<<label<<'\n';}
    };
    const auto reject=[&](const std::string& sql,const std::string& expected) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){actual=error.sqlState();}
        check(actual==expected,sql+" expected="+expected+" actual="+actual);
    };
    const auto descriptor=[&](const std::string& expression,uint32_t oid=701) {
        const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");
        check(prepared.output.size()==1 && prepared.output.front().typeOid==oid &&
              prepared.output.front().type==(oid==701?"double precision":oid==25?"text":"integer"),"descriptor "+expression);
    };
    const auto evaluate=[&](const std::string& expression,double expected,const RowContext* legacy=nullptr,
                            const std::string& resultType="double precision") {
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
        if(!parsed.success){check(false,"parse "+expression);return;}
        auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);RowContext row;
        if(legacy)row=*legacy;
        try {
            evaluator.bindScalarFunctions(select->selectList.front().expr.get(),&g_engine);
            const auto value=evaluator.eval(select->selectList.front().expr.get(),row);
            check(!value.isNull && value.typeName==resultType &&
                  std::abs(std::stod(value.value)-expected)<1e-12,"runtime "+expression);
        } catch(const std::exception& error) {check(false,"runtime "+expression+" "+error.what());}
    };
    struct Unary {const char* name;int input;double expected;};
    const std::vector<Unary> unary={
        {"sin",0,0},{"cos",0,1},{"tan",0,0},{"asin",0,0},{"acos",1,0},{"atan",0,0},
        {"sinh",0,0},{"cosh",0,1},{"tanh",0,0},{"asinh",0,0},{"acosh",1,0},{"atanh",0,0},
        {"degrees",0,0},{"radians",0,0},{"cot",1,0.6420926159343306},{"cbrt",8,2}};
    const std::vector<std::string> suffixes={""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"};
    for(const auto& function:unary) {
        const std::string callee=function.name,value=std::to_string(function.input);
        for(const auto* invalid:{"","0,0","'0'::text","true","NULL::text"})
            for(const auto& suffix:suffixes)for(const auto* role:{""," OVER()"})
                reject("SELECT "+callee+"("+invalid+")"+role+suffix,"42883");
        for(const auto& suffix:suffixes) {
            reject("SELECT "+callee+"('bad')"+suffix,"22P02");
            reject("SELECT "+callee+"('bad') OVER()"+suffix,"42809");
        }
        for(const auto* suffix:{" WHERE false"," LIMIT 0"})for(const auto* role:{""," OVER()"})
            reject("SELECT "+callee+"(t)"+role+" FROM empty_rows"+suffix,"42883");
        for(const auto& argument:std::vector<std::string>{value+"::smallint",value+"::integer",value+"::bigint",
                value+"::real",value+"::numeric",value+"::double precision","'"+value+"'","NULL","NULL::double precision"})
            descriptor(callee+"("+argument+")");
        evaluate(callee+"("+value+"::numeric)",function.expected);
        RowContext legacy;legacy.set("i",ExprValue("integer",std::to_string(function.input+5)));
        evaluate(callee+"(i-5)",function.expected,&legacy);
    }
    for(const auto* invalid:{"","0","0,1,2","'0'::text,1","0,true","NULL::text,1"})
        for(const auto& suffix:suffixes)for(const auto* role:{""," OVER()"})
            reject(std::string("SELECT atan2(")+invalid+")"+role+suffix,"42883");
    for(const auto& suffix:suffixes) {
        reject("SELECT atan2('bad',1)"+suffix,"22P02");
        reject("SELECT atan2('bad',1) OVER()"+suffix,"42809");
    }
    for(const auto* argument:{"0,1","0::numeric,1::numeric","'0','1'","NULL::real,1"})
        descriptor(std::string("atan2(")+argument+")");
    evaluate("atan2(0,1)",0);
    for(const auto* invalid:{"1","NULL","'bad'"})
        for(const auto& suffix:suffixes)for(const auto* role:{""," OVER()"})
            reject(std::string("SELECT pi(")+invalid+")"+role+suffix,"42883");
    descriptor("pi()");evaluate("pi()",3.141592653589793);
    for(const auto& suffix:suffixes)reject("SELECT pi() OVER()"+suffix,"42809");
    RowContext legacyInteger;legacyInteger.set("i",ExprValue("integer","13"));
    RowContext legacyNumeric;legacyNumeric.set("n",ExprValue("numeric","0"));
    evaluate("abs(cbrt(i-5))",2,&legacyInteger);
    evaluate("abs(sin(n))",0,&legacyNumeric);
    if(ddl.executeSql("CREATE SCHEMA float8_user",session) ||
       ddl.executeSql("CREATE SEQUENCE float8_effects",session) ||
       ddl.executeSql("CREATE FUNCTION float8_user.sin(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$",session) ||
       ddl.executeSql("CREATE FUNCTION float8_user.cos(t TEXT) RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$SELECT t$$",session) ||
       ddl.executeSql("CREATE FUNCTION float8_user.effect() RETURNS DOUBLE PRECISION VOLATILE LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('public.float8_effects'); RETURN 0; END;$$",session))return 2;
    session.searchPath="pg_catalog,float8_user,public";
    descriptor("sin(1)",23);descriptor("sin(0::numeric)");descriptor("float8_user.sin(1)",23);
    reject("SELECT float8_user.sin(1::double precision)","42883");
    reject("SELECT sin(1) OVER()","42809");
    descriptor("sin('0')");descriptor("pg_catalog.sin(0)");
    reject("SELECT pg_catalog.\"Sin\"(0)","42883");
    descriptor("pg_catalog.sin(float8_user.effect())");
    evaluate("abs(float8_user.sin(i-5))",18,&legacyInteger,"integer");
    descriptor("cos('payload')",25);descriptor("cos(0)");descriptor("cos(NULL)",25);
    descriptor("cos(t)",25);
    reject("SELECT pg_catalog.cos('payload')","22P02");
    reject("SELECT cos('payload') OVER()","42809");
    bool untouched=false;
    try{(void)g_engine.currval(database,"float8_effects");}
    catch(const DbError& error){untouched=error.sqlState()=="55000";}
    check(untouched,"preparation must not execute volatile arguments");
    check(controls==905,"complete matrix count");
    std::cout<<"FLOAT8_SIGNATURE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(name);
    return failures?1:0;
}
