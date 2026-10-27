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
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="numeric_min_scale_function", database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(n numeric,t text)",session))return 2;
    size_t controls=0,failures=0;
    const auto require=[](bool value){if(!value)throw std::runtime_error("value mismatch");};
    const auto check=[&](const std::string& label,const std::function<void()>& action) {
        ++controls;
        try{action();}catch(const std::exception& error){++failures;std::cerr<<"NUMERIC_MIN_SCALE_FAIL "<<label.substr(0,180)<<" : "<<error.what()<<'\n';}
    };
    const auto reject=[&](const std::string& sql,const std::string& state) {
        check(sql,[&]{
            std::string actual;
            try{(void)g_engine.prepareBoundQuery(database,sql);}
            catch(const DbError& error){actual=error.sqlState();}
            require(actual==state);
        });
    };
    const std::vector<std::pair<std::string,std::string>> positive={
        {"1.2300::numeric","2"},{"0.00::numeric","0"},{"-0.000::numeric","0"},
        {"6e-4","4"},{"6e4","0"},{"1::smallint","0"},{"1::integer","0"},{"1::bigint","0"},
        {"'1.2300'","2"},{"NULL","NULL"},{"NULL::numeric","NULL"},
        {"'NaN'::numeric","NULL"},{"'Infinity'::numeric","NULL"},{"'-Infinity'::numeric","NULL"},
        {"1.2300::decimal","2"},{"0.00::numeric(6,4)","0"},
        {"1.2300::numeric+0.00000::numeric","2"},{"'0."+std::string(1000,'0')+"'::numeric","0"},
    };
    for(const auto& entry:positive) {
        const auto expression="min_scale("+entry.first+")";
        check("descriptor "+expression,[&]{
            const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");
            require(prepared.output.size()==1 && prepared.output.front().typeOid==23 && prepared.output.front().type=="integer");
        });
        check("inference "+expression,[&]{require(ExprHelper::inferResultType(expression,{})=="integer");});
        check("execution "+expression,[&]{
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            require(select && select->selectList.size()==1);
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);
            const auto result=evaluator.eval(node,RowContext{});
            require(result.typeName=="integer" && (entry.second=="NULL"?result.isNull:!result.isNull && result.value==entry.second));
        });
    }
    const std::vector<std::string> invalid={"","1,2","x=>1","'1.00'::text","NULL::text","1::real",
        "1::double precision","true","'2000-01-01'::date","ARRAY[1]","'\\x31'::bytea","NULL::uuid"};
    for(const auto& argument:invalid)for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})
        reject("SELECT min_scale("+argument+")"+suffix,"42883");
    for(const auto* expression:{"min_scale(1) OVER ()","min_scale(1) FILTER (WHERE true)","min_scale(DISTINCT 1)","min_scale(1 ORDER BY 1)"})
        reject("SELECT "+std::string(expression),"42809");
    reject("SELECT COALESCE(1,min_scale('bad'))","22P02");
    reject("SELECT CASE WHEN false THEN min_scale('bad') ELSE 0 END","22P02");
    reject("SELECT min_scale(missing) FROM empty_rows LIMIT 0","42703");
    for(const auto* type:{"numeric","decimal","smallint","integer","bigint","real","text","boolean"})for(const bool null:{false,true}) {
        check("native row "+std::string(type)+" "+std::to_string(null),[&]{
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT min_scale(v)");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;require(select);
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);
            const std::string typeName=type;
            const bool exact=typeName=="numeric" || typeName=="decimal";
            const bool rejected=typeName=="real" || typeName=="text" || typeName=="boolean";
            RowContext row;row.set("v",ExprValue(type,exact?"0.000":"1",null));
            try{
                const auto value=evaluator.eval(node,row);
                require(!rejected && value.typeName=="integer" && value.isNull==null &&
                    (null || value.value=="0"));
            }catch(const DbError& error){require(rejected && error.sqlState()=="42883");}
        });
    }
    for(const std::string context:{database,std::string{}})for(const char volatility:{'i','v'})for(const auto* args:{"'raw'","1,2,3"}) {
        check("opaque "+context+volatility+args,[&]{
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);int calls=0;
            const size_t count=std::string(args)=="'raw'"?1:3;
            evaluator.registerFunction("min_scale",[&](const std::vector<ExprValue>& values){
                ++calls;require(values.size()==count);return ExprValue("text","opaque-min_scale");
            },volatility);
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT min_scale("+std::string(args)+")");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;require(select);
            auto* call=dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get());require(call);
            require(evaluator.scalarFunctionResultType(call,&g_engine).empty());
            require(evaluator.scalarFunctionIdentity(call,&g_engine).rfind("native",0)==0);
            require(ExprHelper::inferParsedInputType(call,{},context,&g_engine,&evaluator)=="unknown" && calls==0);
            evaluator.bindScalarFunctions(call,&g_engine);require(calls==0);
            const auto value=evaluator.eval(call,RowContext{});
            require(calls==1 && value.typeName=="text" && value.value=="opaque-min_scale");
        });
    }
    for(const bool unused:{false,true}) {
        check("argument demand "+std::to_string(unused),[&]{
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);int calls=0;
            evaluator.registerFunction("min_scale_input_effect",[&](const std::vector<ExprValue>&){++calls;return ExprValue("numeric","0.000");});
            const std::string inner="min_scale(min_scale_input_effect())";
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+(unused?"COALESCE(7,"+inner+")":inner));
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;require(select);
            auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);require(calls==0);
            const auto value=evaluator.eval(node,RowContext{});
            require(calls==(unused?0:1) && value.typeName=="integer" && !value.isNull && value.value==(unused?"7":"0"));
        });
    }
    check("immutable declaration",[&]{ExprEvaluator evaluator;require(evaluator.volatility("min_scale")=='i');});
    // Exhaust the existing native scale domain without widening it.
    const auto runtimeCell=[&](const std::string& text,int expected) {
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT min_scale(v)");
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;require(select);
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);auto* node=select->selectList.front().expr.get();
        evaluator.bindScalarFunctions(node,&g_engine);RowContext row;row.set("v",ExprValue("numeric",text));
        const auto result=evaluator.eval(node,row);
        require(result.typeName=="integer" && !result.isNull && result.value==std::to_string(expected));
    };
    for(int scale=0;scale<=1000;++scale) {
        const std::string text="0"+(scale?"."+std::string(scale,'0'):"");
        check("zero domain "+std::to_string(scale),[&]{runtimeCell(text,0);});
    }
    for(int first=1;first<=1000;++first)for(int tail:{0,1,2,3,7}) {
        if(first+tail>1000)continue;
        const std::string text="0."+std::string(first-1,'0')+"1"+std::string(tail,'0');
        check("fraction domain "+std::to_string(first)+"/"+std::to_string(tail),[&]{runtimeCell(text,first);});
    }
    if(controls!=6112){++failures;std::cerr<<"NUMERIC_MIN_SCALE_INVENTORY "<<controls<<'\n';}
    std::cout<<"NUMERIC_MIN_SCALE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(fixture);finalCleanupTestData();
    return failures?1:0;
}
