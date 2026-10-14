#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <tuple>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="scalar_signature",database=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(database)==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore { Session* value;~Restore(){setCurrentSession(value);} } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE empty_rows(i INT,t TEXT)",session));
    assert(!ddl.executeSql("CREATE SCHEMA signature_user",session));
    assert(!ddl.executeSql("CREATE SEQUENCE signature_effects",session));
    assert(!ddl.executeSql("CREATE FUNCTION signature_user.upper(i INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$",session));
    assert(!ddl.executeSql("CREATE FUNCTION signature_user.effect() RETURNS TEXT LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('public.signature_effects'); RETURN 'x'; END;$$",session));
    size_t controls=0;
    const auto reject=[&](const std::string& sql,const std::string& expected) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){actual=error.sqlState();}
        if(actual!=expected)std::cerr<<sql<<" expected "<<expected<<" actual "<<actual<<'\n';
        assert(actual==expected);++controls;
    };
    for(const auto* expression:{"upper(1)","upper()","upper('x'::text,'y'::text)","abs('x'::text)",
            "abs()","abs(1,2)","length(1)","length()","length('x'::text,'UTF8'::name)","lower(true)",
            "upper(i)","abs(t)","length(i)"})
        for(const auto* suffix:{""," WHERE false"," LIMIT 0"}) {
            reject(std::string("SELECT ")+expression+" FROM empty_rows"+suffix,"42883");
            reject(std::string("SELECT ")+expression+" OVER() FROM empty_rows"+suffix,"42883");
        }
    reject("SELECT CASE WHEN false THEN upper(1) ELSE 'kept' END","42883");
    reject("SELECT COALESCE(1,length(1))","42883");
    reject("SELECT upper(missing) FROM empty_rows WHERE false","42703");
    reject("SELECT abs('bad') FROM empty_rows LIMIT 0","22P02");
    reject("SELECT abs('bad') OVER() FROM empty_rows LIMIT 0","42809");
    for(const auto& [expression,type,oid]:std::vector<std::tuple<std::string,std::string,uint32_t>>{
            {"abs(NULL)","double precision",701},{"abs(1)","integer",23},
            {"abs(1::smallint)","smallint",21},{"abs(1::bigint)","bigint",20},
            {"abs(1.25::numeric)","numeric",1700},{"upper(NULL)","text",25},
            {"length(B'101')","integer",23},{"length('abc'::bytea,'UTF8'::name)","integer",23},
            {"length('[(0,0),(3,4)]'::lseg)","double precision",701},
            {"upper('[1,4)'::int4range)","integer",23},
            {"upper(signature_user.effect())","text",25}}) {
        const auto query=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");
        assert(query.output.size()==1 && query.output.front().type==type && query.output.front().typeOid==oid);++controls;
    }
    session.searchPath="pg_catalog,signature_user,public";
    assert(g_engine.prepareBoundQuery(database,"SELECT upper(1)").output.front().typeOid==23);++controls;
    reject("SELECT upper(1) OVER()","42809");
    reject("SELECT pg_catalog.upper(1)","42883");
    reject("SELECT signature_user.upper('x'::text)","42883");
    assert(g_engine.nextval(database,"signature_effects")==1);++controls;
    // Legacy row-context users have no bound range descriptor. Do not infer
    // their column type as TEXT; use the typed row without executing peers.
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    SQLParser parser;RowContext row;row.set("i",ExprValue("integer","-7"));
    auto parsed=parser.parseForBinding("SELECT abs(i)");assert(parsed.success);
    auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());assert(select);
    auto* expression=select->selectList.front().expr.get();
    evaluator.bindScalarFunctions(expression,&g_engine);
    const auto value=evaluator.eval(expression,row);
    assert(value.typeName=="integer" && value.value=="7");++controls;
    assert(controls==100);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"SCALAR_SIGNATURE_NATIVE_COMPLETE "<<controls<<" checks passed\n";
}
