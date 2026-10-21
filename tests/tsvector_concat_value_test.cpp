#include "Session.h"
#include "catalog/type_registry.h"
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
void require(bool good,const std::string& message){if(!good)throw std::runtime_error(message);}
struct Case{std::string sql,type,value;uint32_t oid;bool null;};
}
int main(){
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action){++controls;try{action();}catch(const std::exception& e){++failures;std::cerr<<"TSVECTOR_CONCAT_NATIVE_FAIL "<<label<<" : "<<e.what()<<'\n';}};
    Session session;session.username="native_owner";session.permission=1;session.currentDB="tsvector_concat_owned";session.searchPath="pg_catalog, public";
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* value;~Restore(){setCurrentSession(value);}}restore{previous};
    check("owned database",[&]{require(g_engine.createDatabase(session.currentDB)==DBStatus::OK,"database setup");});
    const std::vector<Case> cases={
        {"''::tsvector || ''::tsvector","tsvector","",3614,false},
        {"''::tsvector || 'a'::tsvector","tsvector","'a'",3614,false},
        {"''::tsvector || 'a:1'::tsvector","tsvector","'a':1",3614,false},
        {"''::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':5A 'b':2B",3614,false},
        {"''::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':1A,2C,3B",3614,false},
        {"''::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'with space':2C 'x':4",3614,false},
        {"''::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'back\\\\slash':2 'quote''d':3A",3614,false},
        {"''::tsvector || 'end:16383A'::tsvector","tsvector","'end':16383A",3614,false},
        {"'a'::tsvector || ''::tsvector","tsvector","'a'",3614,false},
        {"'a'::tsvector || 'a'::tsvector","tsvector","'a'",3614,false},
        {"'a'::tsvector || 'a:1'::tsvector","tsvector","'a':1",3614,false},
        {"'a'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':5A 'b':2B",3614,false},
        {"'a'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':1A,2C,3B",3614,false},
        {"'a'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'a' 'with space':2C 'x':4",3614,false},
        {"'a'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'a' 'back\\\\slash':2 'quote''d':3A",3614,false},
        {"'a'::tsvector || 'end:16383A'::tsvector","tsvector","'a' 'end':16383A",3614,false},
        {"'a:1'::tsvector || ''::tsvector","tsvector","'a':1",3614,false},
        {"'a:1'::tsvector || 'a'::tsvector","tsvector","'a':1",3614,false},
        {"'a:1'::tsvector || 'a:1'::tsvector","tsvector","'a':1,2",3614,false},
        {"'a:1'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':1,6A 'b':3B",3614,false},
        {"'a:1'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':1,2A,3C,4B",3614,false},
        {"'a:1'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'a':1 'with space':3C 'x':5",3614,false},
        {"'a:1'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'a':1 'back\\\\slash':3 'quote''d':4A",3614,false},
        {"'a:1'::tsvector || 'end:16383A'::tsvector","tsvector","'a':1 'end':16383A",3614,false},
        {"'b:2B a:5A'::tsvector || ''::tsvector","tsvector","'a':5A 'b':2B",3614,false},
        {"'b:2B a:5A'::tsvector || 'a'::tsvector","tsvector","'a':5A 'b':2B",3614,false},
        {"'b:2B a:5A'::tsvector || 'a:1'::tsvector","tsvector","'a':5A,6 'b':2B",3614,false},
        {"'b:2B a:5A'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':5A,10A 'b':2B,7B",3614,false},
        {"'b:2B a:5A'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':5A,6A,7C,8B 'b':2B",3614,false},
        {"'b:2B a:5A'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'a':5A 'b':2B 'with space':7C 'x':9",3614,false},
        {"'b:2B a:5A'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'a':5A 'b':2B 'back\\\\slash':7 'quote''d':8A",3614,false},
        {"'b:2B a:5A'::tsvector || 'end:16383A'::tsvector","tsvector","'a':5A 'b':2B 'end':16383A",3614,false},
        {"'a:3B,1A a:2C'::tsvector || ''::tsvector","tsvector","'a':1A,2C,3B",3614,false},
        {"'a:3B,1A a:2C'::tsvector || 'a'::tsvector","tsvector","'a':1A,2C,3B",3614,false},
        {"'a:3B,1A a:2C'::tsvector || 'a:1'::tsvector","tsvector","'a':1A,2C,3B,4",3614,false},
        {"'a:3B,1A a:2C'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':1A,2C,3B,8A 'b':5B",3614,false},
        {"'a:3B,1A a:2C'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':1A,2C,3B,4A,5C,6B",3614,false},
        {"'a:3B,1A a:2C'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'a':1A,2C,3B 'with space':5C 'x':7",3614,false},
        {"'a:3B,1A a:2C'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'a':1A,2C,3B 'back\\\\slash':5 'quote''d':6A",3614,false},
        {"'a:3B,1A a:2C'::tsvector || 'end:16383A'::tsvector","tsvector","'a':1A,2C,3B 'end':16383A",3614,false},
        {"'''with space'':2C x:4'::tsvector || ''::tsvector","tsvector","'with space':2C 'x':4",3614,false},
        {"'''with space'':2C x:4'::tsvector || 'a'::tsvector","tsvector","'a' 'with space':2C 'x':4",3614,false},
        {"'''with space'':2C x:4'::tsvector || 'a:1'::tsvector","tsvector","'a':5 'with space':2C 'x':4",3614,false},
        {"'''with space'':2C x:4'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':9A 'b':6B 'with space':2C 'x':4",3614,false},
        {"'''with space'':2C x:4'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':5A,6C,7B 'with space':2C 'x':4",3614,false},
        {"'''with space'':2C x:4'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'with space':2C,6C 'x':4,8",3614,false},
        {"'''with space'':2C x:4'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'back\\\\slash':6 'quote''d':7A 'with space':2C 'x':4",3614,false},
        {"'''with space'':2C x:4'::tsvector || 'end:16383A'::tsvector","tsvector","'end':16383A 'with space':2C 'x':4",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || ''::tsvector","tsvector","'back\\\\slash':2 'quote''d':3A",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || 'a'::tsvector","tsvector","'a' 'back\\\\slash':2 'quote''d':3A",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || 'a:1'::tsvector","tsvector","'a':4 'back\\\\slash':2 'quote''d':3A",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':8A 'b':5B 'back\\\\slash':2 'quote''d':3A",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':4A,5C,6B 'back\\\\slash':2 'quote''d':3A",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'back\\\\slash':2 'quote''d':3A 'with space':5C 'x':7",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'back\\\\slash':2,5 'quote''d':3A,6A",3614,false},
        {"'''quote''''d'':3A ''back\\\\slash'':2'::tsvector || 'end:16383A'::tsvector","tsvector","'back\\\\slash':2 'end':16383A 'quote''d':3A",3614,false},
        {"'end:16383A'::tsvector || ''::tsvector","tsvector","'end':16383A",3614,false},
        {"'end:16383A'::tsvector || 'a'::tsvector","tsvector","'a' 'end':16383A",3614,false},
        {"'end:16383A'::tsvector || 'a:1'::tsvector","tsvector","'a':16383 'end':16383A",3614,false},
        {"'end:16383A'::tsvector || 'b:2B a:5A'::tsvector","tsvector","'a':16383A 'b':16383B 'end':16383A",3614,false},
        {"'end:16383A'::tsvector || 'a:3B,1A a:2C'::tsvector","tsvector","'a':16383A 'end':16383A",3614,false},
        {"'end:16383A'::tsvector || '''with space'':2C x:4'::tsvector","tsvector","'end':16383A 'with space':16383C 'x':16383",3614,false},
        {"'end:16383A'::tsvector || '''quote''''d'':3A ''back\\\\slash'':2'::tsvector","tsvector","'back\\\\slash':16383 'end':16383A 'quote''d':16383A",3614,false},
        {"'end:16383A'::tsvector || 'end:16383A'::tsvector","tsvector","'end':16383A",3614,false},
        {"'z:2 aa:1'::tsvector || 'bb:1 a:2'::tsvector","tsvector","'a':4 'aa':1 'bb':3 'z':2",3614,false},
        {"'a:2A'::tsvector || 'a:1B'::tsvector","tsvector","'a':2A,3B",3614,false},
        {"'a'::tsvector || 'a:1B'::tsvector","tsvector","'a':1B",3614,false},
        {"'a:1B'::tsvector || 'a'::tsvector","tsvector","'a':1B",3614,false},
        {"'z:16382B'::tsvector || 'a:1A,2B,3C'::tsvector","tsvector","'a':16383A 'z':16382B",3614,false},
        {"'a:16383A'::tsvector || 'a:1B,2C'::tsvector","tsvector","'a':16383A",3614,false},
        {"'a:1A,1B,1C,1D'::tsvector || 'a:1'::tsvector","tsvector","'a':1A,2",3614,false},
        {"'a:2b,1d'::tsvector || 'b:2a'::tsvector","tsvector","'a':1,2B 'b':4A",3614,false},
        {"'界:1 é:2'::tsvector || '😀:1'::tsvector","tsvector","'é':2 '界':1 '😀':3",3614,false},
        {"'a:20000'::tsvector || 'b:30000'::tsvector","tsvector","'a':16383 'b':16383",3614,false},
        {"'a:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256'::tsvector || 'a:1A'::tsvector","tsvector","'a':1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256",3614,false},
        {"'a:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255'::tsvector || 'a:1B,2A'::tsvector","tsvector","'a':1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256B",3614,false},
        {"NULL::tsvector || 'a:1'::tsvector","tsvector","",3614,true},
        {"'a:1'::tsvector || NULL::tsvector","tsvector","",3614,true},
        {"NULL::tsvector || NULL::tsvector","tsvector","",3614,true},
        {"'a:1' || 'b:1'::tsvector","tsvector","'a':1 'b':2",3614,false},
        {"'a:1'::tsvector || 'b:1'","tsvector","'a':1 'b':2",3614,false},
        {"'a' || 'b:1'::tsvector","tsvector","'a' 'b':1",3614,false},
        {"'a'::tsvector || 'b:1'","tsvector","'a' 'b':1",3614,false},
        {"NULL || 'a:1'::tsvector","tsvector","",3614,true},
        {"NULL::tsvector || 'a:1'","tsvector","",3614,true},
        {"('a:1A'::tsvector || 'b:1B'::tsvector)::text","text","'a':1A 'b':2B",25,false},
        {"'a:1'::tsvector || 'x'::text","text","'a':1x",25,false},
        {"'x'::text || 'a:1'::tsvector","text","x'a':1",25,false},
        {"to_tsvector('simple','alpha beta') || to_tsvector('simple','gamma')","tsvector","'alpha':1 'beta':2 'gamma':3",3614,false},
        {"setweight('a:1'::tsvector,'A') || 'b:1B'::tsvector","tsvector","'a':1A 'b':2B",3614,false},
        {"'a:1'::tsvector || setweight('b:1'::tsvector,'B')","tsvector","'a':1 'b':2B",3614,false},
        {"to_tsvector(NULL::text) || 'a:1'::tsvector","tsvector","",3614,true},
        {"NULL::tsvector || to_tsvector('simple','a')","tsvector","",3614,true},
        {"('a:1'::tsvector || 'b:1'::tsvector) @@ 'a <-> b'::tsquery","boolean","t",16,false},
        {"('a:1'::tsvector || 'b:1'::tsvector) @@ 'b <-> a'::tsquery","boolean","f",16,false},
    };
    require(cases.size()==95,"fixed case inventory");
    for(const auto& item:cases){
        const auto prepare=[&]{return std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(session.currentDB,"SELECT "+item.sql+" AS v"));};
        const auto valid=[&](const ExprValue& value){require(value.typeName==item.type && value.isNull==item.null && (item.null || value.value==item.value),"actual type/NULL/value");};
        check("descriptor "+item.sql,[&]{const auto q=prepare();require(q->output.size()==1 && q->output[0].name=="v" && q->output[0].type==item.type && q->output[0].typeOid==item.oid && q->output[0].typeMod==-1,"descriptor");});
        check("live AST declaration "+item.sql,[&]{const auto q=prepare();const auto* select=static_cast<const SelectStmt*>(q->ast.get());const auto& columns=q->statementOutputs.at(q->ast.get());require(columns[0].type==item.type && columns[0].typeOid==item.oid,"statement output");require(q->projectionBindings.at(select)[0].expression==select->selectList[0].expr.get(),"expression owner");require(ExprHelper::inferParsedResultType(select->selectList[0].expr.get(),{},session.currentDB,&g_engine)==item.type,"AST declaration");});
        check("prepared value "+item.sql,[&]{const auto q=prepare();auto* select=static_cast<SelectStmt*>(q->ast.get());auto* target=select->selectList[0].expr.get();PreparedQueryExecution execution(q,&g_engine,session.currentDB);execution.planStatementConstants(select);execution.prepareExpression(target);valid(execution.evaluate(target,execution.context()));});
        check("raw evaluator "+item.sql,[&]{const auto p=SQLParser().parseForBinding("SELECT "+item.sql);require(p.success,p.error);ExprEvaluator evaluator;evaluator.setCurrentDB(session.currentDB);valid(evaluator.eval(static_cast<const SelectStmt*>(p.stmt.get())->selectList[0].expr.get(),{}));});
        check("SQL helper "+item.sql,[&]{const auto value=ExprHelper::evalString(item.sql,{},{},session.currentDB,session.username,&g_engine);require(value.ok,value.error);valid(ExprValue(value.typeName,value.value,value.isNull));});
    }
    for(const std::string name:{"to_tsvector","setweight"})check("actual builtin return identity "+name,[&]{
        auto call=std::make_unique<FunctionCallExpr>();call->funcName=name;
        auto arg=std::make_unique<LiteralExpr>();arg->value="'a'";call->args.push_back(std::move(arg));
        ExprEvaluator builtin;require(builtin.scalarFunctionResultType(call.get(),&g_engine)=="tsvector","actual builtin declaration");
        size_t invocations=0;ExprEvaluator opaque;opaque.setCurrentDB(session.currentDB);
        opaque.registerFunction(name,[&](const std::vector<ExprValue>&){++invocations;return ExprValue("text","opaque",false);},'v');
        require(opaque.scalarFunctionResultType(call.get(),&g_engine).empty() && ExprHelper::inferParsedInputType(call.get(),{},session.currentDB,&g_engine,&opaque)=="unknown" && invocations==0,"opaque callback guessed or executed");
    });
    if(controls!=478){++failures;std::cerr<<"TSVECTOR_CONCAT_NATIVE_INVENTORY "<<controls<<'\n';}
    std::cout<<"TSVECTOR_CONCAT_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
