#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/query_binding.h"
#include "parser/parser.h"
#include <functional>
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
namespace {
void require(bool value,const std::string& message){if(!value)throw std::runtime_error(message);}
struct Case{std::string sql,type,value;uint32_t oid;bool null;};
}
int main(){
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action){
        ++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"JSONB_CONCAT_NATIVE_FAIL "<<label<<" : "<<error.what()<<'\n';}
    };
    Session session;session.username="native_owner";session.permission=1;
    session.currentDB="jsonb_concat_owned";session.searchPath="pg_catalog, public";
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* value;~Restore(){setCurrentSession(value);}}restore{previous};
    check("owned database",[&]{require(g_engine.createDatabase(session.currentDB)==DBStatus::OK,"database setup");});
    const std::vector<Case> cases={
        {"'{}'::jsonb || '{}'::jsonb","jsonb","{}",3802,false},
        {"'{}'::jsonb || '{\"a\":1}'::jsonb","jsonb","{\"a\": 1}",3802,false},
        {"'{}'::jsonb || '[]'::jsonb","jsonb","[{}]",3802,false},
        {"'{}'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[{}, 1, {\"b\": 2}]",3802,false},
        {"'{}'::jsonb || '\"x\"'::jsonb","jsonb","[{}, \"x\"]",3802,false},
        {"'{}'::jsonb || '2'::jsonb","jsonb","[{}, 2]",3802,false},
        {"'{}'::jsonb || 'true'::jsonb","jsonb","[{}, true]",3802,false},
        {"'{}'::jsonb || 'null'::jsonb","jsonb","[{}, null]",3802,false},
        {"'{\"a\":1}'::jsonb || '{}'::jsonb","jsonb","{\"a\": 1}",3802,false},
        {"'{\"a\":1}'::jsonb || '{\"a\":1}'::jsonb","jsonb","{\"a\": 1}",3802,false},
        {"'{\"a\":1}'::jsonb || '[]'::jsonb","jsonb","[{\"a\": 1}]",3802,false},
        {"'{\"a\":1}'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[{\"a\": 1}, 1, {\"b\": 2}]",3802,false},
        {"'{\"a\":1}'::jsonb || '\"x\"'::jsonb","jsonb","[{\"a\": 1}, \"x\"]",3802,false},
        {"'{\"a\":1}'::jsonb || '2'::jsonb","jsonb","[{\"a\": 1}, 2]",3802,false},
        {"'{\"a\":1}'::jsonb || 'true'::jsonb","jsonb","[{\"a\": 1}, true]",3802,false},
        {"'{\"a\":1}'::jsonb || 'null'::jsonb","jsonb","[{\"a\": 1}, null]",3802,false},
        {"'[]'::jsonb || '{}'::jsonb","jsonb","[{}]",3802,false},
        {"'[]'::jsonb || '{\"a\":1}'::jsonb","jsonb","[{\"a\": 1}]",3802,false},
        {"'[]'::jsonb || '[]'::jsonb","jsonb","[]",3802,false},
        {"'[]'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[1, {\"b\": 2}]",3802,false},
        {"'[]'::jsonb || '\"x\"'::jsonb","jsonb","[\"x\"]",3802,false},
        {"'[]'::jsonb || '2'::jsonb","jsonb","[2]",3802,false},
        {"'[]'::jsonb || 'true'::jsonb","jsonb","[true]",3802,false},
        {"'[]'::jsonb || 'null'::jsonb","jsonb","[null]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || '{}'::jsonb","jsonb","[1, {\"b\": 2}, {}]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || '{\"a\":1}'::jsonb","jsonb","[1, {\"b\": 2}, {\"a\": 1}]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || '[]'::jsonb","jsonb","[1, {\"b\": 2}]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[1, {\"b\": 2}, 1, {\"b\": 2}]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || '\"x\"'::jsonb","jsonb","[1, {\"b\": 2}, \"x\"]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || '2'::jsonb","jsonb","[1, {\"b\": 2}, 2]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || 'true'::jsonb","jsonb","[1, {\"b\": 2}, true]",3802,false},
        {"'[1,{\"b\":2}]'::jsonb || 'null'::jsonb","jsonb","[1, {\"b\": 2}, null]",3802,false},
        {"'\"x\"'::jsonb || '{}'::jsonb","jsonb","[\"x\", {}]",3802,false},
        {"'\"x\"'::jsonb || '{\"a\":1}'::jsonb","jsonb","[\"x\", {\"a\": 1}]",3802,false},
        {"'\"x\"'::jsonb || '[]'::jsonb","jsonb","[\"x\"]",3802,false},
        {"'\"x\"'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[\"x\", 1, {\"b\": 2}]",3802,false},
        {"'\"x\"'::jsonb || '\"x\"'::jsonb","jsonb","[\"x\", \"x\"]",3802,false},
        {"'\"x\"'::jsonb || '2'::jsonb","jsonb","[\"x\", 2]",3802,false},
        {"'\"x\"'::jsonb || 'true'::jsonb","jsonb","[\"x\", true]",3802,false},
        {"'\"x\"'::jsonb || 'null'::jsonb","jsonb","[\"x\", null]",3802,false},
        {"'2'::jsonb || '{}'::jsonb","jsonb","[2, {}]",3802,false},
        {"'2'::jsonb || '{\"a\":1}'::jsonb","jsonb","[2, {\"a\": 1}]",3802,false},
        {"'2'::jsonb || '[]'::jsonb","jsonb","[2]",3802,false},
        {"'2'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[2, 1, {\"b\": 2}]",3802,false},
        {"'2'::jsonb || '\"x\"'::jsonb","jsonb","[2, \"x\"]",3802,false},
        {"'2'::jsonb || '2'::jsonb","jsonb","[2, 2]",3802,false},
        {"'2'::jsonb || 'true'::jsonb","jsonb","[2, true]",3802,false},
        {"'2'::jsonb || 'null'::jsonb","jsonb","[2, null]",3802,false},
        {"'true'::jsonb || '{}'::jsonb","jsonb","[true, {}]",3802,false},
        {"'true'::jsonb || '{\"a\":1}'::jsonb","jsonb","[true, {\"a\": 1}]",3802,false},
        {"'true'::jsonb || '[]'::jsonb","jsonb","[true]",3802,false},
        {"'true'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[true, 1, {\"b\": 2}]",3802,false},
        {"'true'::jsonb || '\"x\"'::jsonb","jsonb","[true, \"x\"]",3802,false},
        {"'true'::jsonb || '2'::jsonb","jsonb","[true, 2]",3802,false},
        {"'true'::jsonb || 'true'::jsonb","jsonb","[true, true]",3802,false},
        {"'true'::jsonb || 'null'::jsonb","jsonb","[true, null]",3802,false},
        {"'null'::jsonb || '{}'::jsonb","jsonb","[null, {}]",3802,false},
        {"'null'::jsonb || '{\"a\":1}'::jsonb","jsonb","[null, {\"a\": 1}]",3802,false},
        {"'null'::jsonb || '[]'::jsonb","jsonb","[null]",3802,false},
        {"'null'::jsonb || '[1,{\"b\":2}]'::jsonb","jsonb","[null, 1, {\"b\": 2}]",3802,false},
        {"'null'::jsonb || '\"x\"'::jsonb","jsonb","[null, \"x\"]",3802,false},
        {"'null'::jsonb || '2'::jsonb","jsonb","[null, 2]",3802,false},
        {"'null'::jsonb || 'true'::jsonb","jsonb","[null, true]",3802,false},
        {"'null'::jsonb || 'null'::jsonb","jsonb","[null, null]",3802,false},
        {"'{\"b\":2,\"a\":1}'::jsonb || '{\"a\":3,\"c\":4}'::jsonb","jsonb","{\"a\": 3, \"b\": 2, \"c\": 4}",3802,false},
        {"'{\"a\":{\"x\":1},\"b\":[1]}'::jsonb || '{\"a\":{\"y\":2},\"b\":[2]}'::jsonb","jsonb","{\"a\": {\"y\": 2}, \"b\": [2]}",3802,false},
        {"'{\"a\":1,\"a\":2}'::jsonb || '{\"b\":3,\"b\":4}'::jsonb","jsonb","{\"a\": 2, \"b\": 4}",3802,false},
        {"'{\"\\u0061\":1,\"a\":2}'::jsonb || '{\"\\u0061\":3}'::jsonb","jsonb","{\"a\": 3}",3802,false},
        {"'{\"aa\":1,\"z\":2,\"b\":3,\"é\":4}'::jsonb || '{\"界\":5,\"é\":6}'::jsonb","jsonb","{\"b\": 3, \"z\": 2, \"aa\": 1, \"é\": 6, \"界\": 5}",3802,false},
        {"'{\"\":1,\"a:b,c\":2}'::jsonb || '{\"\":3,\"a\\\"b\":4}'::jsonb","jsonb","{\"\": 3, \"a\\\"b\": 4, \"a:b,c\": 2}",3802,false},
        {"'[1,1,null,[2]]'::jsonb || '[null,1,[3,4]]'::jsonb","jsonb","[1, 1, null, [2], null, 1, [3, 4]]",3802,false},
        {"'[[1,2]]'::jsonb || '[[3,4]]'::jsonb","jsonb","[[1, 2], [3, 4]]",3802,false},
        {"'[\"a,b\",\"[x]\",\"{x}\",\"\\\\\"]'::jsonb || '[\"\\\"\",\"\\n\"]'::jsonb","jsonb","[\"a,b\", \"[x]\", \"{x}\", \"\\\\\", \"\\\"\", \"\\n\"]",3802,false},
        {"'\"null\"'::jsonb || 'null'::jsonb","jsonb","[\"null\", null]",3802,false},
        {"'null'::jsonb || '\"NULL\"'::jsonb","jsonb","[null, \"NULL\"]",3802,false},
        {"'false'::jsonb || 'true'::jsonb","jsonb","[false, true]",3802,false},
        {"'-0.00'::jsonb || '1.2300e-2'::jsonb","jsonb","[0.00, 0.012300]",3802,false},
        {"'9007199254740993'::jsonb || '9007199254740994'::jsonb","jsonb","[9007199254740993, 9007199254740994]",3802,false},
        {"'123456789012345678901234567890.1200'::jsonb || '-2.500e2'::jsonb","jsonb","[123456789012345678901234567890.1200, -250.0]",3802,false},
        {"'1e1000'::jsonb || '2'::jsonb","jsonb","[10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, 2]",3802,false},
        {"'1e-1000'::jsonb || '3'::jsonb","jsonb","[0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 3]",3802,false},
        {"'\"\\ud834\\udd1e\"'::jsonb || '\"𝄞\"'::jsonb","jsonb","[\"𝄞\", \"𝄞\"]",3802,false},
        {"'\"a\\/b\"'::jsonb || '\"a/b\"'::jsonb","jsonb","[\"a/b\", \"a/b\"]",3802,false},
        {"'\"\\b\\f\\n\\r\\t\\u001f\"'::jsonb || '\"界😀\"'::jsonb","jsonb","[\"\\b\\f\\n\\r\\t\\u001f\", \"界😀\"]",3802,false},
        {"NULL::jsonb || '{}'::jsonb","jsonb","",3802,true},
        {"'[]'::jsonb || NULL::jsonb","jsonb","",3802,true},
        {"NULL::jsonb || NULL::jsonb","jsonb","",3802,true},
        {"'null'::jsonb || NULL::jsonb","jsonb","",3802,true},
        {"NULL::jsonb || 'null'::jsonb","jsonb","",3802,true},
        {"'{\"b\":2,\"a\":1}' || '{\"a\":3,\"c\":4}'::jsonb","jsonb","{\"a\": 3, \"b\": 2, \"c\": 4}",3802,false},
        {"'{\"b\":2,\"a\":1}'::jsonb || '{\"a\":3,\"c\":4}'","jsonb","{\"a\": 3, \"b\": 2, \"c\": 4}",3802,false},
        {"'{\"a\":{\"x\":1},\"b\":[1]}' || '{\"a\":{\"y\":2},\"b\":[2]}'::jsonb","jsonb","{\"a\": {\"y\": 2}, \"b\": [2]}",3802,false},
        {"'{\"a\":{\"x\":1},\"b\":[1]}'::jsonb || '{\"a\":{\"y\":2},\"b\":[2]}'","jsonb","{\"a\": {\"y\": 2}, \"b\": [2]}",3802,false},
        {"'{\"a\":1,\"a\":2}' || '{\"b\":3,\"b\":4}'::jsonb","jsonb","{\"a\": 2, \"b\": 4}",3802,false},
        {"'{\"a\":1,\"a\":2}'::jsonb || '{\"b\":3,\"b\":4}'","jsonb","{\"a\": 2, \"b\": 4}",3802,false},
        {"'{\"\\u0061\":1,\"a\":2}' || '{\"\\u0061\":3}'::jsonb","jsonb","{\"a\": 3}",3802,false},
        {"'{\"\\u0061\":1,\"a\":2}'::jsonb || '{\"\\u0061\":3}'","jsonb","{\"a\": 3}",3802,false},
        {"'{\"aa\":1,\"z\":2,\"b\":3,\"é\":4}' || '{\"界\":5,\"é\":6}'::jsonb","jsonb","{\"b\": 3, \"z\": 2, \"aa\": 1, \"é\": 6, \"界\": 5}",3802,false},
        {"'{\"aa\":1,\"z\":2,\"b\":3,\"é\":4}'::jsonb || '{\"界\":5,\"é\":6}'","jsonb","{\"b\": 3, \"z\": 2, \"aa\": 1, \"é\": 6, \"界\": 5}",3802,false},
        {"'{\"\":1,\"a:b,c\":2}' || '{\"\":3,\"a\\\"b\":4}'::jsonb","jsonb","{\"\": 3, \"a\\\"b\": 4, \"a:b,c\": 2}",3802,false},
        {"'{\"\":1,\"a:b,c\":2}'::jsonb || '{\"\":3,\"a\\\"b\":4}'","jsonb","{\"\": 3, \"a\\\"b\": 4, \"a:b,c\": 2}",3802,false},
        {"'[1,1,null,[2]]' || '[null,1,[3,4]]'::jsonb","jsonb","[1, 1, null, [2], null, 1, [3, 4]]",3802,false},
        {"'[1,1,null,[2]]'::jsonb || '[null,1,[3,4]]'","jsonb","[1, 1, null, [2], null, 1, [3, 4]]",3802,false},
        {"'[[1,2]]' || '[[3,4]]'::jsonb","jsonb","[[1, 2], [3, 4]]",3802,false},
        {"'[[1,2]]'::jsonb || '[[3,4]]'","jsonb","[[1, 2], [3, 4]]",3802,false},
        {"'[\"a,b\",\"[x]\",\"{x}\",\"\\\\\"]' || '[\"\\\"\",\"\\n\"]'::jsonb","jsonb","[\"a,b\", \"[x]\", \"{x}\", \"\\\\\", \"\\\"\", \"\\n\"]",3802,false},
        {"'[\"a,b\",\"[x]\",\"{x}\",\"\\\\\"]'::jsonb || '[\"\\\"\",\"\\n\"]'","jsonb","[\"a,b\", \"[x]\", \"{x}\", \"\\\\\", \"\\\"\", \"\\n\"]",3802,false},
        {"'\"null\"' || 'null'::jsonb","jsonb","[\"null\", null]",3802,false},
        {"'\"null\"'::jsonb || 'null'","jsonb","[\"null\", null]",3802,false},
        {"NULL || '{}'::jsonb","jsonb","",3802,true},
        {"NULL::jsonb || '{}'","jsonb","",3802,true},
        {"'[]' || NULL::jsonb","jsonb","",3802,true},
        {"'[]'::jsonb || NULL","jsonb","",3802,true},
        {"NULL || NULL::jsonb","jsonb","",3802,true},
        {"NULL::jsonb || NULL","jsonb","",3802,true},
        {"'null' || NULL::jsonb","jsonb","",3802,true},
        {"'null'::jsonb || NULL","jsonb","",3802,true},
        {"NULL || 'null'::jsonb","jsonb","",3802,true},
        {"NULL::jsonb || 'null'","jsonb","",3802,true},
        {"('{}'::jsonb || '{\"b\":2,\"a\":1}'::jsonb)::text","text","{\"a\": 1, \"b\": 2}",25,false},
        {"'x'::text || ('{}'::jsonb || '{\"a\":1}'::jsonb)","text","x{\"a\": 1}",25,false},
        {"('{}'::jsonb || '{\"a\":1}'::jsonb) || 'x'::text","text","{\"a\": 1}x",25,false},
        {"'{}'::jsonb || 'x'::text","text","{}x",25,false},
        {"'x'::text || '{}'::jsonb","text","x{}",25,false},
        {"'null'::jsonb || NULL::text","text","",25,true},
        {"pg_typeof('{}'::jsonb || '{}'::jsonb)::text","text","jsonb",25,false},
        {"pg_typeof('[]'::jsonb || NULL)::text","text","jsonb",25,false},
        {"jsonb_typeof('1'::jsonb || '2'::jsonb)","text","array",25,false},
        {"jsonb_array_length('[1]'::jsonb || '[2,3]'::jsonb)","integer","3",23,false},
        {"('[]'::jsonb || '[1,2]'::jsonb) ->> 1","text","2",25,false},
        {"ARRAY['{}'::jsonb || '{\"a\":1}'::jsonb,'null'::jsonb || '1'::jsonb]","jsonb[]","{\"{\\\"a\\\": 1}\",\"[null, 1]\"}",3807,false},
        {"'{}'::jsonb || ('{}'::jsonb || '{\"a\":1}'::jsonb)","jsonb","{\"a\": 1}",3802,false},
    };
    require(cases.size()==132,"fixed native matrix inventory");
    for(const auto& item:cases){
        const auto prepare=[&]{return std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(session.currentDB,"SELECT "+item.sql+" AS v"));};
        const auto valid=[&](const ExprValue& value){require(value.typeName==item.type && value.isNull==item.null && (item.null || value.value==item.value),"wrong actual type/NULL/value");};
        check("descriptor "+item.sql,[&]{const auto query=prepare();require(query->output.size()==1 && query->output[0].name=="v" && query->output[0].type==item.type && query->output[0].typeOid==item.oid && query->output[0].typeMod==-1,"wrong descriptor");});
        check("live statement ownership "+item.sql,[&]{const auto query=prepare();const auto& output=query->statementOutputs.at(query->ast.get());require(output.size()==1 && output[0].type==item.type && output[0].typeOid==item.oid,"wrong statement declaration");const auto* select=static_cast<const SelectStmt*>(query->ast.get());require(query->projectionBindings.at(select).front().expression==select->selectList.front().expr.get(),"lost projection expression owner");require(ExprHelper::inferParsedResultType(select->selectList.front().expr.get(),{},session.currentDB,&g_engine)==item.type,"AST/descriptor disagreement");});
        check("prepared value "+item.sql,[&]{const auto query=prepare();auto* select=static_cast<SelectStmt*>(query->ast.get());auto* target=select->selectList[0].expr.get();PreparedQueryExecution execution(query,&g_engine,session.currentDB);execution.planStatementConstants(select);execution.prepareExpression(target);valid(execution.evaluate(target,execution.context()));});
        check("raw evaluator "+item.sql,[&]{const auto parsed=SQLParser().parseForBinding("SELECT "+item.sql);require(parsed.success,parsed.error);const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());ExprEvaluator evaluator;evaluator.setCurrentDB(session.currentDB);valid(evaluator.eval(select->selectList[0].expr.get(),{}));});
        check("SQL helper "+item.sql,[&]{const auto value=ExprHelper::evalString(item.sql,{},{},session.currentDB,session.username,&g_engine);require(value.ok,value.error);valid(ExprValue(value.typeName,value.value,value.isNull));});
    }
    size_t calls=0;
    QueryBindingMetadata metadata;
    metadata.declaredType=[](const std::string& spelling){const auto type=ExprHelper::canonicalResultTypeName(spelling);const auto oid=mapBuiltinTypeNameToOid(type);require(oid!=0,"fixture builtin type absent");return QueryOutputColumn{"",type,false,0,oid,-1};};
    metadata.relation=[](const std::string&){return QueryRelationMetadata{"public","source",{{"j","jsonb",false,0,3802,-1},{"k","jsonb",false,0,3802,-1}},{}};};
    metadata.functionType=[](const FunctionCallExpr*){return std::string("jsonb");};
    for(const std::string suffix:{""," WHERE false"," LIMIT 0"}){
        check("physical declaration "+suffix,[&]{const auto query=prepareQuery("SELECT j || k AS v FROM source"+suffix,{},metadata);require(query.output[0].type=="jsonb" && query.output[0].typeOid==3802 && calls==0,"wrong metadata or unexpected execution");});
        check("opaque native declaration "+suffix,[&]{const auto query=prepareQuery("SELECT jsonb_native() || '{}'::jsonb AS v FROM source"+suffix,{},metadata);require(query.output[0].type=="jsonb" && query.output[0].typeOid==3802 && calls==0,"metadata executed native callback or lost result type");});
    }
    auto query=prepareQuery("SELECT jsonb_native() || '{}'::jsonb AS v",{},metadata);
    const auto* target=static_cast<const SelectStmt*>(query.ast.get())->selectList[0].expr.get();
    ExprEvaluator evaluator;
    evaluator.registerFunction("jsonb_native",[&](const std::vector<ExprValue>&){++calls;return ExprValue("jsonb","{\"b\":2,\"a\":1}",false);},'v');
    check("native descriptor without evaluation",[&]{require(calls==0 && query.output[0].type=="jsonb","native executed during metadata");});
    check("one native actual call",[&]{const auto value=evaluator.eval(target,{});require(calls==1 && value.typeName=="jsonb" && !value.isNull && value.value=="{\"a\": 1, \"b\": 2}","wrong native demand/value/type");});
    evaluator.registerFunction("jsonb_native",[&](const std::vector<ExprValue>&){++calls;return ExprValue("jsonb","",true);},'v');
    check("native actual NULL call",[&]{const auto value=evaluator.eval(target,{});require(calls==2 && value.typeName=="jsonb" && value.isNull,"wrong NULL demand/type");});
    evaluator.registerFunction("jsonb_native",[](const std::vector<ExprValue>&){return ExprValue("text","{\"x\":1}",false);},'v');
    check("opaque TEXT is not an UNKNOWN JSON literal",[&]{const auto value=evaluator.eval(target,{});require(value.typeName=="text" && !value.isNull && value.value=="{\"x\":1}{}","native TEXT was coerced from its content");});
    const auto metadataCall=[](){auto call=std::make_unique<FunctionCallExpr>();call->funcName="jsonb_array_length";auto input=std::make_unique<LiteralExpr>();input->typeName="jsonb";input->value="'[]'";call->args.push_back(std::move(input));return call;};
    check("actual builtin JSON array length has an INTEGER declaration",[&]{auto call=metadataCall();ExprEvaluator builtin;require(builtin.scalarFunctionResultType(call.get(),&g_engine)=="integer","builtin return declaration missing");});
    check("same-named native callback does not inherit the builtin declaration",[&]{size_t invocations=0;ExprEvaluator opaque;opaque.setCurrentDB(session.currentDB);opaque.registerFunction("jsonb_array_length",[&](const std::vector<ExprValue>&){++invocations;return ExprValue("text","opaque",false);},'v');auto call=metadataCall();require(opaque.scalarFunctionResultType(call.get(),&g_engine).empty() && ExprHelper::inferParsedInputType(call.get(),{},session.currentDB,&g_engine,&opaque)=="unknown" && ExprHelper::inferParsedResultType(call.get(),{},session.currentDB,&g_engine,&opaque)=="text" && invocations==0,"metadata guessed/executed a replacement callback");});
    BinaryOpExpr binary;binary.op="||";
    auto left=std::make_unique<ParameterExpr>();left->slot=0;left->declaredType="jsonb";left->origin=ParameterOrigin::RuntimeCell;binary.left=std::move(left);
    auto right=std::make_unique<ParameterExpr>();right->slot=1;right->declaredType="jsonb";right->origin=ParameterOrigin::RuntimeCell;binary.right=std::move(right);
    check("deep JSON data uses bounded C++ call stack",[&]{const std::string deep=std::string(512,'[')+"0"+std::string(512,']');RowContext context;context.setParameters({ExprValue("jsonb",deep,false),ExprValue("jsonb","[]",false)});const auto value=evaluator.eval(&binary,context);require(value.typeName=="jsonb" && !value.isNull && value.value==deep,"deep JSON array concat changed contents");});
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{{"{","22P02"},{R"("\u0000")","22P05"}}){
        check("typed runtime input rejects malformed JSON "+item.second,[&]{RowContext context;context.setParameters({ExprValue("jsonb",item.first,false),ExprValue("jsonb","{}",false)});try{(void)evaluator.eval(&binary,context);}catch(const DbError& error){require(error.sqlState()==item.second,"wrong JSON input SQLSTATE");return;}throw std::runtime_error("malformed typed JSON value concatenated");});
    }
    if(controls!=676){++failures;std::cerr<<"JSONB_CONCAT_NATIVE_INVENTORY "<<controls<<'\n';}
    std::cout<<"JSONB_CONCAT_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
