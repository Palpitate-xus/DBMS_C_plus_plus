#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "commands/DdlExecutor.h"
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
void require(bool value,const std::string& message) {if(!value)throw std::runtime_error(message);}
struct Case {std::string sql,type,value;uint32_t oid;bool null=false;};
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action) {
        ++controls;
        try{action();}catch(const std::exception& error){++failures;std::cerr<<"CONCAT_RESULT_DECLARATION_NATIVE_FAIL "<<label<<" : "<<error.what()<<'\n';}
    };
    Session session;session.username="native_owner";session.permission=1;
    session.currentDB="concat_declaration_owned";session.searchPath="pg_catalog, public";
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    check("owned database",[&]{require(g_engine.createDatabase(session.currentDB)==DBStatus::OK,"database setup");});
    DdlExecutor ddl;
    check("owned integer routine",[&]{require(!ddl.executeSql("CREATE FUNCTION public.r215_fn(p integer) RETURNS integer LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p+1$$",session),"routine setup");});
    check("owned sequence",[&]{require(!ddl.executeSql("CREATE SEQUENCE public.r215_effect_seq",session),"sequence setup");});
    check("owned volatile routine",[&]{require(!ddl.executeSql("CREATE FUNCTION public.r215_effect() RETURNS bigint LANGUAGE SQL VOLATILE AS $$SELECT nextval('public.r215_effect_seq')$$",session),"volatile routine setup");});
    const std::vector<Case> cases={
        {"1 || ''","text","1",25},{"'' || 1","text","1",25},
        {"9007199254740993::bigint || ''","text","9007199254740993",25},
        {"true || ''","text","true",25},{"NULL::boolean || ''","text","",25,true},
        {"'NULL'::name || ''","text","NULL",25},{"NULL::integer || ''","text","",25,true},
        {"'a'::varchar(8) || 'b'::varchar(3)","text","ab",25},
        {"'A'::char(4) || 'x'","text","Ax",25},
        {"'\\x00ff'::bytea || '\\x80'::bytea","bytea","\\x00ff80",17},
        {"'\\x'::bytea || '\\x'::bytea","bytea","\\x",17},
        {"'\\x00ff'::bytea || '\\x80'","bytea","\\x00ff80",17},
        {"'\\x80' || '\\x00ff'::bytea","bytea","\\x8000ff",17},
        {"NULL || '\\x00'::bytea","bytea","",17,true},
        {"B'101' || B'01'","bit varying","10101",1562},
        {"B'10'::varbit || B'1'::varbit","bit varying","101",1562},
        {"B''::varbit || B''::varbit","bit varying","",1562},
        {"NULL::varbit || B'1'::varbit","bit varying","",1562,true},
        {"B'10' || '1'","bit varying","101",1562},{"'1' || B'10'","bit varying","110",1562},
        {"NULL || B'10'","bit varying","",1562,true},
        {"B'10' || '110'","bit varying","10110",1562},{"'110' || B'10'","bit varying","11010",1562},
        {"ARRAY[1,NULL] || ARRAY[2]","integer[]","{1,NULL,2}",1007},
        {"ARRAY[1] || ARRAY[2147483648::bigint]","bigint[]","{1,2147483648}",1016},
        {"NULL::integer[] || ARRAY[1]","integer[]","{1}",1007},
        {"ARRAY[]::integer[] || ARRAY[]::integer[]","integer[]","{}",1007},
        {"ARRAY[1] || 2","integer[]","{1,2}",1007},{"0 || ARRAY[1]","integer[]","{0,1}",1007},
        {"public.r215_fn(-2) || ''","text","-1",25},
        {"pg_catalog.current_schema() || ''","text","pg_catalog",25},
        {"length(1 || 'x')","integer","2",23}};
    require(cases.size()==32,"fixed native matrix inventory");
    for(const auto& item:cases) {
        const auto prepare=[&]{return std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(session.currentDB,"SELECT "+item.sql+" AS v"));};
        const auto valid=[&](const ExprValue& value){require(value.typeName==item.type && value.isNull==item.null && (item.null || value.value==item.value),"wrong value/type/NULL");};
        check("descriptor "+item.sql,[&]{auto query=prepare();require(query->output.size()==1 && query->output[0].name=="v" && query->output[0].type==item.type && query->output[0].typeOid==item.oid && query->output[0].typeMod==-1,"wrong bound descriptor");});
        check("statement owner "+item.sql,[&]{auto query=prepare();const auto& output=query->statementOutputs.at(query->ast.get());require(output.size()==1 && output[0].type==item.type && output[0].typeOid==item.oid,"wrong statement output descriptor");auto* select=dynamic_cast<SelectStmt*>(query->ast.get());require(select && query->projectionBindings.at(select).size()==1 && query->projectionBindings.at(select)[0].expression==select->selectList[0].expr.get(),"lost live projection owner");});
        check("bound AST type "+item.sql,[&]{auto query=prepare();auto* select=static_cast<SelectStmt*>(query->ast.get());require(ExprHelper::inferParsedResultType(select->selectList[0].expr.get(),{},session.currentDB,&g_engine)==item.type,"bound AST/descriptor disagreement");});
        check("prepared value "+item.sql,[&]{auto query=prepare();auto* select=static_cast<SelectStmt*>(query->ast.get());auto* target=select->selectList[0].expr.get();PreparedQueryExecution execution(query,&g_engine,session.currentDB);execution.planStatementConstants(select);execution.prepareExpression(target);valid(execution.evaluate(target,execution.context()));});
        check("SQL helper value "+item.sql,[&]{const auto value=ExprHelper::evalString(item.sql,{},{},session.currentDB,session.username,&g_engine);require(value.ok,value.error);valid(ExprValue(value.typeName,value.value,value.isNull));});
    }
    // Extended primitive Bind uses the raw evaluator, before a caller
    // has installed the helper/binder's implicit literal conversions.
    for(size_t index:{11u,12u,13u,18u,19u,20u,21u,22u}) {
        const auto& item=cases.at(index);
        check("raw UNKNOWN literal "+item.sql,[&]{
            const auto parsed=SQLParser().parseForBinding("SELECT "+item.sql);
            require(parsed.success,parsed.error);
            const auto* select=dynamic_cast<const SelectStmt*>(parsed.stmt.get());
            require(select && select->selectList.size()==1,"raw target missing");
            ExprEvaluator evaluator;evaluator.setCurrentDB(session.currentDB);
            const auto value=evaluator.eval(select->selectList[0].expr.get(),{});
            require(value.typeName==item.type && value.isNull==item.null &&
                    (item.null || value.value==item.value),"wrong raw input codec/type/NULL/value");
        });
    }
    size_t callbackEffects=0;
    QueryBindingMetadata metadata;
    metadata.declaredType=[](const std::string& spelling) {
        const auto type=ExprHelper::canonicalResultTypeName(spelling);
        const auto oid=mapBuiltinTypeNameToOid(type);
        require(oid!=0,"fixture catalog has no builtin declaration");
        return QueryOutputColumn{"",type,false,0,oid,-1};
    };
    metadata.relation=[](const std::string&){return QueryRelationMetadata{"public","r215_source",{{"n","integer",false,0,23,-1},{"t","text",false,0,25,-1}},{}};};
    metadata.functionType=[](const FunctionCallExpr*){return std::string("integer");};
    for(const std::string suffix:{""," WHERE false"," LIMIT 0"}) {
        check("metadata-only relation "+suffix,[&]{const auto query=prepareQuery("SELECT n || t AS v FROM r215_source"+suffix,{},metadata);require(query.output.size()==1 && query.output[0].type=="text" && query.output[0].typeOid==25 && callbackEffects==0,"wrong physical result or unexpected execution");});
        check("metadata-only routine "+suffix,[&]{const auto query=prepareQuery("SELECT r215_native() || '' AS v FROM r215_source"+suffix,{},metadata);require(query.output.size()==1 && query.output[0].type=="text" && query.output[0].typeOid==25 && callbackEffects==0,"wrong routine result or unexpected execution");});
    }
    auto callback=prepareQuery("SELECT r215_native() || '' AS v",{},metadata);
    check("native declaration did not invoke callback",[&]{require(callbackEffects==0 && callback.output[0].type=="text","metadata invoked callback or used operand declaration");});
    ExprEvaluator evaluator;
    evaluator.registerFunction("r215_native",[&](const std::vector<ExprValue>&){++callbackEffects;return ExprValue("integer","9",false);},'v');
    check("native actual callback exactly once",[&]{const auto* select=static_cast<const SelectStmt*>(callback.ast.get());const auto value=evaluator.eval(select->selectList[0].expr.get(),{});require(callbackEffects==1 && value.typeName=="text" && !value.isNull && value.value=="9","wrong actual callback result/demand");});
    evaluator.registerFunction("r215_native",[&](const std::vector<ExprValue>&){++callbackEffects;return ExprValue("integer","",true);},'v');
    check("native typed NULL callback exactly once",[&]{const auto* select=static_cast<const SelectStmt*>(callback.ast.get());const auto value=evaluator.eval(select->selectList[0].expr.get(),{});require(callbackEffects==2 && value.typeName=="text" && value.isNull,"wrong callback NULL/result type/demand");});
    std::shared_ptr<PreparedQuery> effect;
    for(const std::string suffix:{""," WHERE false"," LIMIT 0"})check("volatile declaration "+suffix,[&]{auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(session.currentDB,"SELECT public.r215_effect() || '' AS v"+suffix));if(suffix.empty())effect=query;require(query->output.size()==1 && query->output[0].type=="text" && query->output[0].typeOid==25,"volatile descriptor inherited BIGINT");});
    check("declarations do not consume sequence",[&]{require(g_engine.nextval(session.currentDB,"public.r215_effect_seq")==1,"metadata evaluated routine");});
    PreparedQueryExecution execution(effect,&g_engine,session.currentDB);
    auto* select=static_cast<SelectStmt*>(effect->ast.get());
    check("planning does not consume sequence",[&]{execution.planStatementConstants(select);execution.prepareExpression(select->selectList[0].expr.get());require(g_engine.nextval(session.currentDB,"public.r215_effect_seq")==2,"planning evaluated volatile routine");});
    check("one actual volatile call",[&]{const auto value=execution.evaluate(select->selectList[0].expr.get(),execution.context());require(value.typeName=="text" && !value.isNull && value.value=="3","wrong volatile value/type/demand");});
    check("no duplicate actual volatile call",[&]{require(g_engine.nextval(session.currentDB,"public.r215_effect_seq")==4,"routine evaluated more than once");});
    if(controls!=188){++failures;std::cerr<<"CONCAT_RESULT_DECLARATION_NATIVE_INVENTORY "<<controls<<'\n';}
    std::cout<<"CONCAT_RESULT_DECLARATION_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
