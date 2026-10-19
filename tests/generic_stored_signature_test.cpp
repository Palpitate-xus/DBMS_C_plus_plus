#include "Session.h"
#include "catalog/collation.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
struct Checks {
    size_t controls=0,failures=0;
    template<class Action> void test(const std::string& label,Action action) {
        ++controls;
        try {action();}
        catch(const std::exception& error) {
            ++failures;std::cerr<<"GENERIC_STORED_SIGNATURE_FAIL "<<controls<<' '<<label<<" : "<<error.what()<<'\n';
        }
        catch(...) {++failures;std::cerr<<"GENERIC_STORED_SIGNATURE_FAIL "<<controls<<' '<<label<<" : nonstandard exception\n";}
    }
};
void require(bool valid,const std::string& message) {if(!valid)throw std::runtime_error(message);}
std::string state(const std::exception& error) {
    if(const auto* typed=dynamic_cast<const DbError*>(&error))return typed->sqlState();
    const std::string message=error.what();const auto at=message.find("SQLSTATE ");
    return at==std::string::npos?std::string{}:message.substr(at+9,5);
}
template<class Action> void rejects(const std::string& expected,Action action) {
    std::string actual;
    try {action();}catch(const std::exception& error) {actual=state(error);if(actual.empty())throw;}
    require(actual==expected,"expected SQLSTATE "+expected+", got "+actual);
}
ExprValue helper(const std::string& sql,const std::string& database,
    const std::map<std::string,std::string>& values={},const std::set<std::string>& nulls={},
    const std::map<std::string,std::string>& hints={}) {
    const auto result=ExprHelper::evalStringWithNulls(sql,values,nulls,hints,database,"admin",&g_engine);
    if(!result.ok) {
        std::string code=result.sqlState;
        if(code.empty()) {const auto at=result.error.find("SQLSTATE ");if(at!=std::string::npos)code=result.error.substr(at+9,5);}
        if(!code.empty())throw DbError(code,result.error);
        throw std::runtime_error(result.error);
    }
    ExprValue value(result.typeName,result.value,result.isNull);value.collation=result.collation;return value;
}
void matches(const ExprValue& actual,const std::string& type,const std::string& value,bool null=false) {
    require(ExprHelper::canonicalResultTypeName(actual.typeName)==type,"wrong result type: "+actual.typeName);
    require(actual.isNull==null,"wrong SQL NULL state");
    if(!null)require(actual.value==value,"expected ["+value+"], got ["+actual.value+"]");
}
std::unique_ptr<SelectStmt> parsed(const std::string& expression) {
    SQLParser parser;auto result=parser.parseForBinding("SELECT "+expression);
    require(result.isValid(),"parse failed: "+result.error);
    auto* select=dynamic_cast<SelectStmt*>(result.stmt.get());require(select,"not a SELECT");
    result.stmt.release();return std::unique_ptr<SelectStmt>(select);
}
ExprValue bound(const std::string& expression,const std::string& database) {
    auto query=g_engine.prepareBoundQuery(database,"SELECT "+expression);
    auto* select=dynamic_cast<SelectStmt*>(query.ast.get());require(select,"missing bound SELECT");
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    evaluator.bindScalarFunctions(select->selectList.front().expr.get(),&g_engine);
    return evaluator.eval(select->selectList.front().expr.get(),RowContext{});
}
ExprValue cells(const std::string& name,const std::vector<ExprValue>& values,const std::string& database) {
    FunctionCallExpr call;call.funcName=name;
    for(size_t i=0;i<values.size();++i) {
        auto parameter=std::make_unique<ParameterExpr>();parameter->slot=i;
        parameter->declaredType=values[i].typeName;parameter->origin=ParameterOrigin::RuntimeCell;
        call.args.push_back(std::move(parameter));
    }
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);evaluator.bindScalarFunctions(&call,&g_engine);
    RowContext row;row.setParameters(values);return evaluator.eval(&call,row);
}
}

int main() {
    using namespace dbms;
    Checks checks;TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("generic_stored_signature");
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    session.searchPath="public, pg_catalog";
    auto* prior=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{prior};
    checks.test("private database",[&]{require(g_engine.createDatabase(database)==DBStatus::OK,"createDatabase failed");});
    DdlExecutor ddl;
    const std::vector<std::string> definitions={
        "CREATE TABLE signature_rows(i INTEGER,t TEXT,a INTEGER[])",
        "CREATE SCHEMA r162_first","CREATE SCHEMA r162_second","CREATE SEQUENCE r162_effects",
        "CREATE FUNCTION r162_mix(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT t||':'||n::text$$",
        "CREATE FUNCTION r162_plmix(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT AS $$BEGIN RETURN t||':'||n::text; END;$$",
        "CREATE FUNCTION r162_int(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n+1$$",
        "CREATE FUNCTION r162_big(n BIGINT) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n::text$$",
        "CREATE FUNCTION r162_text(t TEXT) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT t$$",
        "CREATE FUNCTION r162_array(a BIGINT[]) RETURNS BIGINT[] LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT a$$",
        "CREATE FUNCTION r162_textarray(a TEXT[]) RETURNS TEXT[] LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT a$$",
        "CREATE FUNCTION r162_bool(b BOOLEAN) RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT b$$",
        "CREATE FUNCTION \"R162.Mix\"(t TEXT,n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT t||':'||n::text$$",
        "CREATE FUNCTION r162_first.r162_choose(n BIGINT) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 101$$",
        "CREATE FUNCTION r162_second.r162_choose(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 202$$",
        "CREATE FUNCTION r162_first.r162_same(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 11$$",
        "CREATE FUNCTION r162_second.r162_same(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 22$$",
        "CREATE FUNCTION r162_first.r162_prefer(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 31$$",
        "CREATE FUNCTION r162_second.r162_prefer(n TEXT) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 32$$",
        "CREATE FUNCTION r162_first.r162_ambiguous(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 41$$",
        "CREATE FUNCTION r162_second.r162_ambiguous(n BOOLEAN) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 42$$",
        "CREATE FUNCTION r162_effect(n INTEGER) RETURNS INTEGER LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM nextval('public.r162_effects'); RETURN n; END;$$",
        "CREATE FUNCTION r162_native(n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$SELECT 'stored'::text$$",
        "CREATE FUNCTION public.unnest(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT n+9$$",
        "CREATE FUNCTION r162_first.pg_typeof(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT 209$$",
        "CREATE FUNCTION r162_first.r162_hint(n BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n$$",
        "CREATE FUNCTION r162_second.r162_hint(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n$$",
        "CREATE FUNCTION r162_first.r162_hint_first(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n$$",
        "CREATE FUNCTION r162_second.r162_hint_first(n BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n$$",
        "CREATE TYPE public.r162_color AS ENUM('red','blue')",
        "CREATE FUNCTION r162_enum(p public.r162_color) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT 'enum'::text$$",
        "CREATE FUNCTION r162_first.upper(n BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n$$",
        "CREATE FUNCTION r162_second.upper(n INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT n$$"
    };
    for(const auto& sql:definitions)checks.test(sql,[&]{require(!ddl.executeSql(sql,session),"DDL failed");});

    struct Good {std::string sql,type,value;bool null=false;};
    const std::vector<Good> good={
        {"r162_mix('alpha',2)","text","alpha:2"},{"r162_plmix('alpha',2)","text","alpha:2"},
        {"r162_mix('',0)","text",":0"},{"r162_mix('NULL',-1)","text","NULL:-1"},
        {"r162_mix('O''Brien',2)","text","O'Brien:2"},{"r162_mix('雪',3)","text","雪:3"},
        {"r162_mix(NULL,2)","text","",true},{"r162_int('2')","integer","3"},
        {"r162_int(NULL)","integer","",true},{"r162_big(2)","text","2"},
        {"r162_text('abc'::varchar)","text","abc"},
        {"r162_array(ARRAY[1,NULL,2]::integer[])","bigint[]","{1,NULL,2}"},
        {"r162_textarray(ARRAY['a',NULL,'']::varchar[])","text[]","{a,NULL,\"\"}"},
        {"\"R162.Mix\"('q',1)","text","q:1"},
        {"r162_mix(1::text,'2'::integer)","text","1:2"},
        {"r162_int('2'::text::integer)","integer","3"},
        {"r162_array(ARRAY['1',NULL]::text[]::integer[])","bigint[]","{1,NULL}"},
        {"r162_textarray(ARRAY[1]::integer[]::text[])","text[]","{1}"}
    };
    for(const auto& item:good) {
        checks.test("helper "+item.sql,[&]{matches(helper(item.sql,database),item.type,item.value,item.null);});
        checks.test("bound "+item.sql,[&]{matches(bound(item.sql,database),item.type,item.value,item.null);});
        for(const auto* tail:{""," FROM signature_rows WHERE false"," FROM signature_rows LIMIT 0"})
            checks.test("descriptor "+item.sql+tail,[&]{
                const auto query=g_engine.prepareBoundQuery(database,"SELECT "+item.sql+tail);
                require(query.output.size()==1 && query.output[0].type==item.type,"wrong pure descriptor");
            });
    }
    const std::vector<std::string> invalid={
        "r162_mix(1,'2'::text)","r162_mix(NULL::integer,NULL::text)","r162_mix('a',2::bigint)",
        "r162_int('2'::text)","r162_int(2::bigint)","r162_int(1.2::numeric)",
        "r162_text(2)","r162_text(NULL::integer)","r162_array(ARRAY['1']::text[])",
        "r162_array(NULL::text[])","r162_textarray(ARRAY[1]::integer[])",
        "r162_bool(1)","r162_bool('t'::text)","\"R162.Mix\"(2,'a'::text)",
        "r162_mix()","r162_mix('a')","r162_mix('a',1,2)","r162_plmix(1,'2'::text)"
    };
    const auto negativeContexts=[&](const std::string& expression,const std::string& expected) {
        for(const auto& sql:std::vector<std::string>{
                "SELECT "+expression,"SELECT "+expression+" FROM signature_rows WHERE false",
                "SELECT "+expression+" FROM signature_rows LIMIT 0",
                "SELECT CASE WHEN false THEN "+expression+" ELSE NULL END",
                "SELECT COALESCE(NULL,"+expression+")"})
            checks.test(sql,[&]{rejects(expected,[&]{(void)g_engine.prepareBoundQuery(database,sql);});});
    };
    for(const auto& expression:invalid) {
        negativeContexts(expression,"42883");
        checks.test("helper rejects "+expression,[&]{rejects("42883",[&]{(void)helper(expression,database);});});
    }
    for(const auto* expression:{"r162_int('bad')","r162_mix('a','bad')","r162_plmix('a','bad')"})
        negativeContexts(expression,"22P02");

    session.searchPath="r162_first, r162_second, public, pg_catalog";
    for(const auto& item:std::vector<Good>{{"r162_choose(1)","integer","202"},
            {"r162_choose(1::bigint)","integer","101"},{"r162_same(1)","integer","11"},
            {"r162_prefer('word')","integer","32"},{"r162_prefer(1)","integer","31"}}) {
        checks.test("path helper "+item.sql,[&]{matches(helper(item.sql,database),item.type,item.value);});
        checks.test("path bound "+item.sql,[&]{matches(bound(item.sql,database),item.type,item.value);});
    }
    negativeContexts("r162_ambiguous(NULL)","42725");
    checks.test("qualified restricts candidate path",[&]{matches(bound("r162_first.r162_choose(1)",database),"integer","101");});
    checks.test("typed helper hint integer",[&]{matches(helper("r162_choose(x)",database,{{"x","1"}},{},{{"x","integer"}}),"integer","202");});
    checks.test("typed helper hint bigint",[&]{matches(helper("r162_choose(x)",database,{{"x","1"}},{},{{"x","bigint"}}),"integer","101");});
    checks.test("typed helper does not infer from numeric bytes",[&]{rejects("42883",[&]{(void)helper("r162_choose(x)",database,{{"x","1"}},{},{{"x","text"}});});});
    std::unique_ptr<SelectStmt> legacy;FunctionCallExpr* call=nullptr;
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    checks.test("unbound discovery metadata",[&]{legacy=parsed("r162_choose(x)");call=dynamic_cast<FunctionCallExpr*>(legacy->selectList[0].expr.get());require(call && evaluator.hasScalarFunction(call,&g_engine),"missing provisional stored call");require(evaluator.scalarFunctionResultType(call,&g_engine)=="integer","wrong common declaration");});
    checks.test("unbound binding and repeat ownership",[&]{
        require(call,"missing deferred call");
        evaluator.bindScalarFunctions(call,&g_engine);const auto key=call->funcName;
        const auto identity=evaluator.scalarFunctionIdentity(call,&g_engine);
        evaluator.bindScalarFunctions(call,&g_engine);
        require(call->funcName==key && evaluator.scalarFunctionIdentity(call,&g_engine)==identity,"private callback identity changed");
    });
    for(const auto& input:std::vector<std::pair<ExprValue,std::string>>{
            {ExprValue("integer","1"),"202"},{ExprValue("bigint","1"),"101"},
            {ExprValue("integer","2"),"202"},{ExprValue("integer","",true),"202"}})
        checks.test("deferred actual type "+input.first.typeName+input.first.value,[&]{RowContext row;row.set("x",input.first);matches(evaluator.eval(call,row),"integer",input.second);});
    checks.test("deferred typed TEXT rejected",[&]{RowContext row;row.set("x",ExprValue("text","1"));rejects("42883",[&]{(void)evaluator.eval(call,row);});});
    checks.test("deferred UNKNOWN numeric candidates remain ambiguous",[&]{RowContext row;row.set("x",ExprValue("unknown","7"));rejects("42725",[&]{(void)evaluator.eval(call,row);});});
    checks.test("deferred UNKNOWN prefers actual string candidate",[&]{
        auto tree=parsed("r162_prefer(x)");ExprEvaluator choice;choice.setCurrentDB(database);
        choice.bindScalarFunctions(tree->selectList[0].expr.get(),&g_engine);
        RowContext row;row.set("x",ExprValue("unknown","7"));matches(choice.eval(tree->selectList[0].expr.get(),row),"integer","32");
    });
    checks.test("captured path not caller's later path",[&]{session.searchPath="r162_second, r162_first, public, pg_catalog";RowContext row;row.set("x",ExprValue("bigint","2"));matches(evaluator.eval(call,row),"integer","101");});
    session.searchPath="public, pg_catalog";
    for(const auto& input:std::vector<ExprValue>{ExprValue("smallint","1"),ExprValue("integer","2"),ExprValue("unknown","3"),ExprValue("integer","",true)})
        checks.test("typed runtime cells "+input.typeName,[&]{matches(cells("r162_big",{input},database),"text",input.value,input.isNull);});
    for(const auto& input:std::vector<ExprValue>{ExprValue("text","2"),ExprValue("text","",true),ExprValue("bigint","2")})
        checks.test("typed runtime invalid cells "+input.typeName,[&]{rejects("42883",[&]{(void)cells("r162_int",{input},database);});});
    checks.test("native UNKNOWN codec error precedes stored body",[&]{rejects("22P02",[&]{(void)cells("r162_int",{ExprValue("unknown","bad")},database);});});
    checks.test("native array coercion retains nondefault bounds and NULL",[&]{matches(cells("r162_array",{ExprValue("integer[]","[0:2]={1,NULL,2}")},database),"bigint[]","[0:2]={1,NULL,2}");});
    checks.test("native typed NULL array",[&]{matches(cells("r162_array",{ExprValue("integer[]","",true)},database),"bigint[]","",true);});
    checks.test("native illegal array signature",[&]{rejects("42883",[&]{(void)cells("r162_array",{ExprValue("text[]","{1}")},database);});});
    checks.test("bound arguments acquire implicit casts",[&]{
        auto query=g_engine.prepareBoundQuery(database,"SELECT r162_big(1)");
        auto* root=dynamic_cast<SelectStmt*>(query.ast.get());auto* function=dynamic_cast<FunctionCallExpr*>(root->selectList[0].expr.get());
        ExprEvaluator binder;binder.setCurrentDB(database);binder.bindScalarFunctions(function,&g_engine);
        const auto* cast=dynamic_cast<const CastExpr*>(function->args[0].get());
        require(cast && cast->implicit && ExprHelper::canonicalResultTypeName(cast->typeName)=="bigint","missing legal input coercion");
    });
    checks.test("metadata does not execute body",[&]{
        for(const auto* sql:{"SELECT r162_effect(1) LIMIT 0","SELECT CASE WHEN false THEN r162_effect(1) ELSE 0 END","SELECT COALESCE(0,r162_effect(1))"})
            (void)g_engine.prepareBoundQuery(database,sql);
        require(g_engine.nextval(database,"r162_effects")==1,"preparation executed stored body");
    });
    checks.test("unselected effect remains lazy",[&]{matches(bound("CASE WHEN false THEN r162_effect(1) ELSE 7 END",database),"integer","7");require(g_engine.nextval(database,"r162_effects")==2,"unused stored body executed");});
    checks.test("reached effect executes once",[&]{matches(bound("r162_effect(9)",database),"integer","9");require(g_engine.nextval(database,"r162_effects")==4,"reached call count changed");});
    size_t callbackCalls=0;
    ExprEvaluator opaque;opaque.setCurrentDB(database);
    opaque.registerFunction("r162_native",[&](const std::vector<ExprValue>& args){++callbackCalls;require(args.size()==1,"opaque arity changed");return ExprValue("text","native");});
    std::unique_ptr<SelectStmt> native;FunctionCallExpr* nativeCall=nullptr;
    checks.test("opaque peer discovery is pure",[&]{native=parsed("r162_native(x)");nativeCall=dynamic_cast<FunctionCallExpr*>(native->selectList[0].expr.get());require(nativeCall,"missing opaque peer call");opaque.bindScalarFunctions(nativeCall,&g_engine);require(callbackCalls==0,"opaque callback executed for metadata");});
    checks.test("stored exact beats later opaque peer",[&]{RowContext row;row.set("x",ExprValue("integer","2"));matches(opaque.eval(nativeCall,row),"text","stored");require(callbackCalls==0,"opaque peer stole selected stored call");});
    checks.test("opaque peer retains undeclared input contract",[&]{RowContext row;row.set("x",ExprValue("text","raw"));matches(opaque.eval(nativeCall,row),"text","native");require(callbackCalls==1,"opaque fallback not reached once");});
    checks.test("opaque builtin replacement owns signature",[&]{
        ExprEvaluator replacement;replacement.registerFunction("abs",[](const std::vector<ExprValue>& args){return args.front();});
        auto expression=parsed("abs(x)");replacement.bindScalarFunctions(expression->selectList[0].expr.get(),&g_engine);
        RowContext row;ExprValue value("text","raw");value.collation="C";row.set("x",value);
        const auto result=replacement.eval(expression->selectList[0].expr.get(),row);
        matches(result,"text","raw");
        require(collation::normalizeName(result.collation)==collation::normalizeName(value.collation),
                "native payload collation changed");
    });
    checks.test("real SRF keeps query-host role",[&]{
        const auto query=g_engine.prepareBoundQuery(database,"SELECT pg_catalog.unnest(ARRAY[1,2])");
        const auto* root=dynamic_cast<const SelectStmt*>(query.ast.get());const auto* function=dynamic_cast<const FunctionCallExpr*>(root->selectList[0].expr.get());
        require(function && function->setReturning && query.output[0].type=="integer","real SRF reclassified");
    });
    checks.test("same-named scalar SRF overload remains scalar",[&]{matches(bound("public.unnest(1)",database),"integer","10");});
    checks.test("pg_typeof provider keeps real declaration",[&]{require(g_engine.prepareBoundQuery(database,"SELECT pg_catalog.pg_typeof(1)").output[0].type=="regtype","pg_typeof declaration changed");});
    checks.test("qualified pg_typeof user overload remains scalar",[&]{matches(bound("r162_first.pg_typeof(1)",database),"integer","209");});

    // Additive caller-hint controls: preserve every original292 assertion.
    // Differing return types must not force a real INTEGER input to TEXT,
    // nor may the first arity candidate donate its result declaration.
    session.searchPath="r162_first, r162_second, public, pg_catalog";
    const std::vector<std::tuple<std::string,std::map<std::string,std::string>,std::string>> hinted={
        {"r162_hint(x)",{{"x","integer"}},"integer"},
        {"r162_hint(x)",{{"x","bigint"}},"bigint"},
        {"r162_hint_first(x)",{{"x","integer"}},"integer"},
        {"r162_hint_first(x)",{{"x","bigint"}},"bigint"},
        {"r162_hint(r.x)",{{"r.x","integer"},{"x","text"}},"integer"},
        {"r162_hint(x+1)",{{"x","integer"}},"integer"},
        {"r162_hint(x)::bigint",{{"x","integer"}},"bigint"},
        {"r162_hint(x)+1::bigint",{{"x","integer"}},"bigint"}
    };
    for(const auto& item:hinted)checks.test("hint-only declaration "+std::get<0>(item)+std::get<2>(item),[&]{
        require(ExprHelper::inferResultType(std::get<0>(item),std::get<1>(item),database,&g_engine)==std::get<2>(item),"caller hint lost during generic metadata lookup");
    });
    for(const auto* type:{"integer","bigint"})checks.test(std::string("hinted actual result ")+type,[&]{
        matches(helper("r162_hint(x)",database,{{"x","5"}},{},{{"x",type}}),type,"5");
    });
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
            {"upper(x)","integer"},{"upper(x)","bigint"},{"upper(x+1)","integer"}})
        checks.test("stored builtin-name hint "+item.first+item.second,[&]{
            require(ExprHelper::inferResultType(item.first,{{"x",item.second}},database,&g_engine)==item.second,"registry name hid a real stored declaration");
        });
    for(const auto* type:{"integer","bigint"})checks.test(std::string("stored builtin-name actual result ")+type,[&]{
        matches(helper("upper(x)",database,{{"x","5"}},{},{{"x",type}}),type,"5");
    });
    std::unique_ptr<SelectStmt> differing;FunctionCallExpr* differingCall=nullptr;
    ExprEvaluator differingEvaluator;differingEvaluator.setCurrentDB(database);
    checks.test("unavailable declarations retain UNKNOWN not first result",[&]{
        differing=parsed("r162_hint(x)");differingCall=dynamic_cast<FunctionCallExpr*>(differing->selectList[0].expr.get());
        require(differingCall && differingEvaluator.scalarFunctionResultType(differingCall,&g_engine)=="unknown","unbound result guessed a candidate type");
    });
    checks.test("hint probe leaves original AST intact",[&]{
        require(differingCall,"missing differing-return call");const auto* argument=differingCall->args[0].get();const auto name=differingCall->funcName;
        require(ExprHelper::inferParsedInputType(differingCall,{{"x","integer"}},database,&g_engine,&differingEvaluator)=="integer","AST hint did not select exact later result");
        require(differingCall->args[0].get()==argument && differingCall->funcName==name,"metadata mutated original AST");
        differingEvaluator.bindScalarFunctions(differingCall,&g_engine);
    });
    for(const auto& input:std::vector<ExprValue>{ExprValue("integer","5"),ExprValue("bigint","5"),ExprValue("integer","",true),ExprValue("bigint","",true)})
        checks.test("different-return runtime "+input.typeName+(input.isNull?" NULL":""),[&]{RowContext row;row.set("x",input);matches(differingEvaluator.eval(differingCall,row),input.typeName,input.value,input.isNull);});
    checks.test("hint inspection preserves owned private site",[&]{
        require(differingCall,"missing private site");const auto key=differingCall->funcName;const auto identity=differingEvaluator.scalarFunctionIdentity(differingCall,&g_engine);
        (void)ExprHelper::inferParsedInputType(differingCall,{{"x","integer"}},database,&g_engine,&differingEvaluator);
        differingEvaluator.bindScalarFunctions(differingCall,&g_engine);
        require(differingCall->funcName==key && differingEvaluator.scalarFunctionIdentity(differingCall,&g_engine)==identity,"hint probe rebound a private callback by SQL name");
    });
    checks.test("real SQL routine can share generated private spelling",[&]{
        require(differingCall,"missing private site");
        const auto sql="CREATE FUNCTION r162_first."+differingCall->funcName+"(n INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT 'separate'::text$$";
        require(!ddl.executeSql(sql,session),"same-spelled ordinary DDL failed");
    });
    checks.test("same-spelled SQL routine cannot donate private hint metadata",[&]{
        require(differingCall,"missing private site");const auto identity=differingEvaluator.scalarFunctionIdentity(differingCall,&g_engine);
        require(ExprHelper::inferParsedInputType(differingCall,{{"x","integer"}},database,&g_engine,&differingEvaluator)=="unknown","SQL-name collision replaced deferred private declaration");
        require(differingEvaluator.scalarFunctionIdentity(differingCall,&g_engine)==identity,"private identity changed during metadata lookup");
    });
    checks.test("same-spelled ordinary SQL routine retains own declaration",[&]{
        require(differingCall,"missing private site");const auto sql=differingCall->funcName+"(x)";
        require(ExprHelper::inferResultType(sql,{{"x","integer"}},database,&g_engine)=="text","ordinary same-spelled declaration lost");
        matches(helper(sql,database,{{"x","1"}},{},{{"x","integer"}}),"text","separate");
    });
    checks.test("illegal caller hint rejected without value inference",[&]{rejects("42883",[&]{(void)ExprHelper::inferResultType("r162_hint(x)",{{"x","text"}},database,&g_engine);});});
    checks.test("unrelated hint cannot manufacture an input declaration",[&]{auto tree=parsed("r162_hint(x)");require(ExprHelper::inferParsedInputType(tree->selectList[0].expr.get(),{{"other","integer"}},database,&g_engine)=="unknown","unrelated hint chose a candidate");});
    checks.test("hint metadata does not execute effect body",[&]{require(ExprHelper::inferResultType("r162_effect(x)",{{"x","integer"}},database,&g_engine)=="integer","effect declaration changed");require(g_engine.nextval(database,"r162_effects")==5,"hint resolution executed a stored body");});
    session.searchPath="public, pg_catalog";

    // Ordinary scalar enum DDL only. No enum-array producer/factory or
    // synthetic pg_type/pg_proc row is installed by this fixture.
    checks.test("legal UNKNOWN scalar enum helper",[&]{matches(helper("r162_enum('red')",database),"text","enum");});
    checks.test("legal UNKNOWN scalar enum binding",[&]{matches(bound("r162_enum('red')",database),"text","enum");});
    checks.test("scalar enum NULL binding",[&]{matches(bound("r162_enum(NULL)",database),"text","",true);});
    checks.test("actual scalar enum native parameter",[&]{const auto function=g_engine.getUDF(database,"r162_enum");require(function.paramTypes.size()==1,"missing real enum declaration");matches(cells("r162_enum",{ExprValue(function.paramTypes[0],"red")},database),"text","enum");});
    checks.test("UNKNOWN scalar enum native parameter",[&]{matches(cells("r162_enum",{ExprValue("unknown","red")},database),"text","enum");});
    checks.test("typed NULL scalar enum native parameter",[&]{const auto function=g_engine.getUDF(database,"r162_enum");require(function.paramTypes.size()==1,"missing real enum declaration");matches(cells("r162_enum",{ExprValue(function.paramTypes[0],"",true)},database),"text","",true);});
    checks.test("typed TEXT is not implicit scalar enum input",[&]{rejects("42883",[&]{(void)cells("r162_enum",{ExprValue("text","red")},database);});});

    // A separate real engine/database owns a same-named callable with a
    // different declaration/body. Its public native APIs are sufficient;
    // DdlExecutor has no private-engine injection API, so no catalog is forged.
    std::unique_ptr<StorageEngine> privateEngine;
    const auto privateDatabase=testDbPath("generic_stored_signature_private");
    checks.test("private engine/database",[&]{privateEngine=std::make_unique<StorageEngine>();require(privateEngine->createDatabase(privateDatabase)==DBStatus::OK,"private createDatabase failed");});
    checks.test("private engine real stored declaration",[&]{require(privateEngine!=nullptr,"missing private engine");require(privateEngine->createUDF(privateDatabase,"r162_int",std::vector<std::string>{"n"},std::vector<std::string>{"bigint"},"BEGIN RETURN n+200; END;",'i',"plpgsql","bigint",true)==DBStatus::OK,"private createUDF failed");});
    checks.test("metadata uses actual engine/database owner",[&]{require(privateEngine!=nullptr,"missing private engine");require(ExprHelper::inferResultType("r162_int(x)",{{"x","integer"}},privateDatabase,privateEngine.get())=="bigint","private result declaration lost");require(ExprHelper::inferResultType("r162_int(x)",{{"x","integer"}},database,&g_engine)=="integer","global same-name declaration changed");});
    const auto privateValue=[&](ExprValue input) {
        require(privateEngine!=nullptr,"missing private engine");
        FunctionCallExpr function;function.funcName="r162_int";
        auto parameter=std::make_unique<ParameterExpr>();parameter->slot=0;parameter->declaredType=input.typeName;parameter->origin=ParameterOrigin::RuntimeCell;function.args.push_back(std::move(parameter));
        ExprEvaluator actual;actual.setCurrentDB(privateDatabase);actual.bindScalarFunctions(&function,privateEngine.get());
        RowContext row;row.setParameters({input});return actual.eval(&function,row);
    };
    checks.test("private engine implicit argument conversion",[&]{matches(privateValue(ExprValue("integer","7")),"bigint","207");});
    checks.test("private engine NULL argument conversion",[&]{matches(privateValue(ExprValue("integer","",true)),"bigint","",true);});
    checks.test("global same-name execution remains independent",[&]{matches(cells("r162_int",{ExprValue("integer","1")},database),"integer","2");});
    checks.test("private unbound metadata and actual cells",[&]{
        require(privateEngine!=nullptr,"missing private engine");auto tree=parsed("r162_int(x)");ExprEvaluator actual;actual.setCurrentDB(privateDatabase);
        const auto* function=dynamic_cast<const FunctionCallExpr*>(tree->selectList[0].expr.get());require(function && actual.scalarFunctionResultType(function,privateEngine.get())=="bigint","private metadata guessed another owner");
        actual.bindScalarFunctions(tree->selectList[0].expr.get(),privateEngine.get());RowContext row;row.set("x",ExprValue("integer","7"));matches(actual.eval(tree->selectList[0].expr.get(),row),"bigint","207");
    });
    checks.test("private engine database cleanup",[&]{require(privateEngine!=nullptr && privateEngine->dropDatabase(privateDatabase)==DBStatus::OK,"private dropDatabase failed");privateEngine.reset();});
    checks.test("private database cleanup",[&]{require(g_engine.dropDatabase(database)==DBStatus::OK,"dropDatabase failed");});
    // Setup26 + positives90 + invalid108 + badUNKNOWN15 + path10 + ambiguity5
    // + qualification/hints4 + deferred10 + typedcells11 + casts/effects4
    // + opaque4 + providers4 + cleanup1 = original292. Add DDL6 + hint metadata8
    // + hinted execution2 + differing-return/native-site7 + bad/unrelated/pure3
    // + scalar enum7 + private-engine8 = 333. Add builtin-name DDL2/hints3/
    // execution2 and real private-key collision3 = 343. Never shrink after a failure.
    constexpr size_t expected=343;
    if(checks.controls!=expected) {++checks.failures;std::cerr<<"GENERIC_STORED_SIGNATURE_INVENTORY expected "<<expected<<" got "<<checks.controls<<'\n';}
    std::cout<<"GENERIC_STORED_SIGNATURE_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expected<<" FAILURES "<<checks.failures<<'\n';
    return checks.failures?1:0;
}
