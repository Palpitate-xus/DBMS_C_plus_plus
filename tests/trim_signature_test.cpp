#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
struct Positive { std::string args;std::optional<std::string> both,left,right;std::string type="text"; };
const std::vector<Positive> positives={
    {"'  x  '","x","x  ","  x"}, {"'xxhixx','x'","hi","hixx","xxhi"},
    {"'丰x丰','中估'","丰x丰","丰x丰","丰x丰"}, {"'ééxé','é'","x","xé","ééx"},
    {"''","","",""}, {"NULL",{},{},{}}, {"NULL::text",{},{},{}},
    {"'NULL'","NULL","NULL","NULL"}, {"'abc'::varchar,'b'","abc","abc","abc"},
    {"' x '::char(6)","x","x"," x"}, {"' x '::name","x","x "," x"},
    {"NULL::bytea,'\\x78'::bytea",{},{},{},"bytea"},
    {"'\\x00ff7800'::bytea,'\\x00ff'::bytea","\\x78","\\x7800","\\x00ff78","bytea"},
    {"'\\x61'::bytea,NULL::bytea",{},{},{},"bytea"},
    {"'\\x78'::bytea,''","\\x78","\\x78","\\x78","bytea"},
    {"NULL,NULL",{},{},{}}, {"'x',''","x","x","x"},
    {"'a  '::char(3),'a '::char(2)","","",""},
    {"'\\x7878617878'::bytea,'\\x78'::bytea","\\x61","\\x617878","\\x787861","bytea"},
    {"'x'::name,'x'::name","","",""}
};
const std::vector<std::string> invalid={"","1","NULL::integer","true","1.25::numeric","NULL::uuid","ARRAY[1]","'2024-01-01'::date",
    "1,'x'","'x',1","'x',NULL::uuid","NULL::bytea","'\\x78'::bytea,'x'::text","'x'::text,'\\x78'::bytea","'x','y','z'","missing=>'x'"};
const std::vector<std::string> names={"btrim","ltrim","rtrim"};
void require(bool good,const std::string& label){if(!good)throw std::runtime_error(label);}
std::optional<std::string> wanted(const Positive& item,const std::string& name){return name=="ltrim"?item.left:name=="rtrim"?item.right:item.both;}
Expr* target(ParseResult& parsed){require(parsed.success,parsed.error);auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());require(select && select->selectList.size()==1,"missing target");return select->selectList.front().expr.get();}
void matches(const ExprValue& value,const std::optional<std::string>& expected,const std::string& type){require(value.typeName==type && value.isNull==!expected,"wrong result declaration/NULL bit");if(expected)require(value.value==*expected,"expected "+*expected+", got "+value.value);}
}
int main(){
    using namespace dbms;TypeRegistry::instance().bootstrap();size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action){++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"TRIM_SIGNATURE_NATIVE_FAIL "<<label<<" : "<<error.what()<<'\n';}};
    const std::string fixture="trim_signature",database=testDbPath(fixture);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);struct Restore{Session* value;~Restore(){setCurrentSession(value);}}restore{previous};
    check("private setup",[&]{cleanupTestDb(fixture);require(g_engine.createDatabase(database)==DBStatus::OK,"create database");require(!DdlExecutor().executeSql("CREATE TABLE empty_rows(i integer,t text)",session),"empty table");});
    const auto reject=[&](const std::string& sql,const std::string& state){check(sql,[&]{std::string actual;try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){actual=error.sqlState();}require(actual==state,"expected "+state+", got "+actual);});};
    for(const auto& name:names){
        for(const auto& item:positives){
            const auto expected=wanted(item,name);
            for(const auto& spelling:std::vector<std::string>{name,"pg_catalog."+name,"\""+name+"\""}){
                const auto expression=spelling+"("+item.args+")";
                check(expression+" descriptor",[&]{const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");require(prepared.output.size()==1 && prepared.output.front().type==item.type && prepared.output.front().typeOid==(item.type=="bytea"?17:25),"wrong descriptor");});
                for(bool bind:{false,true})check(expression+(bind?" bound":" raw"),[&]{auto parsed=SQLParser().parseForBinding("SELECT "+expression);auto* node=target(parsed);ExprEvaluator evaluator;evaluator.setCurrentDB(database);if(bind)evaluator.bindScalarFunctions(node,&g_engine);matches(evaluator.eval(node,{}),expected,item.type);});
                check(expression+" helper",[&]{const auto value=ExprHelper::evalString(expression,{},{},database,"",&g_engine);require(value.ok,value.error);matches(ExprValue(value.typeName,value.value,value.isNull),expected,item.type);});
            }
            reject("SELECT "+name+"("+item.args+") OVER()","42809");
        }
        for(const auto& args:invalid){
            for(const auto& spelling:std::vector<std::string>{name,"pg_catalog."+name})for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})for(const auto* role:{""," OVER()"})reject("SELECT "+spelling+"("+args+")"+role+suffix,"42883");
            reject("SELECT CASE WHEN false THEN "+name+"("+args+") ELSE 'kept' END","42883");
            reject("SELECT COALESCE('kept',"+name+"("+args+"))","42883");
        }
        for(const auto* suffix:{" WHERE false"," LIMIT 0"})reject("SELECT "+name+"(i) FROM empty_rows"+suffix,"42883");
        reject("SELECT "+name+"(missing) FROM empty_rows LIMIT 0","42703");
        check(name+" actual builtin volatility",[&]{ExprEvaluator evaluator;require(evaluator.volatility(name)=='i',"TRIM builtin is not immutable");});
        for(const auto* args:{"'\\xq0','\\x00'::bytea","'\\x00'::bytea,'\\xq0'"})for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})for(const auto* role:{""," OVER()"})reject("SELECT "+name+"("+args+")"+role+suffix,std::string(role).empty()?"22023":"42809");
        for(const auto* rawType:{"text","varchar","character","name","integer","bytea","binary","varbinary","uuid"})for(bool bind:{false,true})check(name+" legacy "+rawType,[&]{
            const std::string type=rawType;const bool bytes=type=="bytea" || type=="binary" || type=="varbinary",bad=type=="integer" || type=="uuid";
            const std::string source=type=="bytea"?"\\x00ff6800":bytes?std::string("\0\xffh\0",4):type=="integer"?"7":type=="uuid"?"00000000-0000-0000-0000-000000000000":type=="character"?"xxhixx  ":"xxhixx";
            const std::string characters=type=="bytea"?"\\x00ff":bytes?std::string("\0\xff",2):type=="uuid"?source:"x";
            auto parsed=SQLParser().parseForBinding("SELECT "+name+"(v,w)");auto* node=target(parsed);ExprEvaluator evaluator;evaluator.setCurrentDB(database);if(bind)evaluator.bindScalarFunctions(node,&g_engine);
            RowContext row;row.set("v",ExprValue(type,source));row.set("w",ExprValue(bytes||type=="uuid"?type:"text",characters));
            try{const auto value=evaluator.eval(node,row);require(!bad,"invalid row type accepted");matches(value,bytes?std::optional<std::string>(name=="ltrim"?"\\x6800":name=="rtrim"?"\\x00ff68":"\\x68"):std::optional<std::string>(name=="ltrim"?"hixx":name=="rtrim"?"xxhi":"hi"),bytes?"bytea":"text");}catch(const DbError& error){require(bad && error.sqlState()=="42883",error.what());}
        });
        for(const std::string context:{database,std::string{}})for(const char volatility:{'v','i','d'})for(const int profile:{0,1,2,3,4})for(bool bind:{false,true})for(const auto* returnType:{"text","bytea","integer"})check(name+" opaque "+std::to_string(profile)+" -> "+returnType,[&]{
            const std::vector<std::string> arguments={"","1,2,3","'é  '::char(3)","NULL::bytea","'\\x00ff'::bytea,'\\x00'::bytea"};
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);int calls=0;
            const std::string resultType=returnType,resultValue=resultType=="bytea"?"\\x00ff":resultType=="integer"?"42":"opaque";
            ScalarFunction callback=[&](const std::vector<ExprValue>& args){++calls;const size_t arity=profile==0?0:profile==1?3:profile==4?2:1;require(args.size()==arity,"opaque arity");
                if(profile==1)for(size_t i=0;i<args.size();++i)require(args[i].typeName=="integer" && args[i].value==std::to_string(i+1),"opaque integers");
                if(profile==2)require(args[0].typeName=="character" && args[0].value=="é  ","opaque padding");
                if(profile==3)require(args[0].typeName=="bytea" && args[0].isNull,"opaque NULL");
                if(profile==4)require(args[0].typeName=="bytea" && args[1].typeName=="bytea" && args[0].value=="\\x00ff" && args[1].value=="\\x00","opaque bytes");return ExprValue(resultType,resultValue);};
            if(volatility=='d')evaluator.registerFunction(name,callback);else evaluator.registerFunction(name,callback,volatility);
            auto parsed=SQLParser().parseForBinding("SELECT "+name+"("+arguments[profile]+")");auto* call=dynamic_cast<FunctionCallExpr*>(target(parsed));require(call,"missing call");
            require(evaluator.hasScalarFunction(call,&g_engine) && evaluator.scalarFunctionResultType(call,&g_engine).empty() && evaluator.scalarFunctionVolatility(call,&g_engine)==(volatility=='d'?'v':volatility) && evaluator.scalarFunctionIdentity(call,&g_engine).rfind("native",0)==0 && ExprHelper::inferParsedInputType(call,{},context,&g_engine,&evaluator)=="unknown" && calls==0,"opaque metadata or fingerprint");
            if(bind)evaluator.bindScalarFunctions(call,&g_engine);require(calls==0,"binding executed callback");const auto value=evaluator.eval(call,{});require(calls==1 && value.typeName==resultType && !value.isNull && value.value==resultValue,"opaque result");
        });
        for(bool unused:{false,true})check(name+" argument demand",[&]{
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);std::vector<std::string> effects;
            evaluator.registerFunction("trim_source_effect",[&](const std::vector<ExprValue>&){effects.push_back("source");return ExprValue("text","xxhixx");});
            evaluator.registerFunction("trim_characters_effect",[&](const std::vector<ExprValue>&){effects.push_back("characters");return ExprValue("text","x");});
            const auto expression=name+"(trim_source_effect(),trim_characters_effect())";
            auto parsed=SQLParser().parseForBinding("SELECT "+(unused?"COALESCE('kept',"+expression+")":expression));auto* node=target(parsed);evaluator.bindScalarFunctions(node,&g_engine);require(effects.empty(),"metadata evaluated arguments");
            const auto result=evaluator.eval(node,{});matches(result,unused?std::optional<std::string>("kept"):std::optional<std::string>(name=="ltrim"?"hixx":name=="rtrim"?"xxhi":"hi"),"text");require(effects==(unused?std::vector<std::string>{}:std::vector<std::string>{"source","characters"}),"argument evaluation count/order");
        });
    }
    if(positives.size()!=20 || invalid.size()!=16 || controls!=2101){++failures;std::cerr<<"TRIM_SIGNATURE_NATIVE_INVENTORY "<<controls<<'\n';}
    std::cout<<"TRIM_SIGNATURE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;cleanupTestDb(fixture);return failures?1:0;
}
