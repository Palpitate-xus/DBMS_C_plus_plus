#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="starts_with_signature", database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(i INTEGER,t TEXT)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;if(!good){++failures;std::cerr<<"STARTS_WITH_NATIVE_FAILURE "<<label<<'\n';}
    };
    const auto reject=[&](const std::string& sql,const std::string& expected) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){state=error.sqlState();}
        catch(const std::exception& error){state=error.what();}
        check(state==expected,sql+" expected="+expected+" actual="+state);
    };
    for(const std::string arguments:{"","'a'","'a','b','c'","1,'a'","'a',1","NULL::integer,'a'",
        "'a',true","'a'::bytea,'a'","'a',ARRAY[1]","missing=>'a'"})
        for(const std::string suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})
            reject("SELECT starts_with("+arguments+")"+suffix,"42883");
    reject("SELECT CASE WHEN false THEN starts_with(1,'a') ELSE false END","42883");
    reject("SELECT COALESCE(true,starts_with('a',1))","42883");
    reject("SELECT starts_with(missing,'a') FROM empty_rows LIMIT 0","42703");
    reject("SELECT starts_with(i,'a') FROM empty_rows LIMIT 0","42883");
    reject("SELECT starts_with('a',i) FROM empty_rows LIMIT 0","42883");
    for(const auto& [arguments,expected]:std::vector<std::pair<std::string,std::string>>{
        {"'alphabet','alpha'","t"},{"'abc','z'","f"},{"'中é','中'","t"},{"'😀a','😀'","t"},
        {"'',''","t"},{"'','a'","f"},{"NULL,'a'","NULL"},{"'a',NULL","NULL"},
        {"NULL,NULL","NULL"},{"'a','a  '::char(3)","t"},{"'a'::name,'a'::varchar","t"}}) {
        const auto expression="starts_with("+arguments+")";
        try {
            const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");
            check(prepared.output.size()==1 && prepared.output.front().typeOid==16 && prepared.output.front().type=="boolean","descriptor "+expression);
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)return 2;
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);
            const auto value=evaluator.eval(node,RowContext{});
            check(value.typeName=="boolean" && (expected=="NULL"?value.isNull:!value.isNull && value.value==expected),"runtime "+expression);
        } catch(const std::exception& error){check(false,expression+" "+error.what());}
    }
    for(const std::string context:{database,std::string{}})for(const auto volatility:{'v','i'})
        for(const std::string arguments:{"","1","1,2","1,2,3","'é  '::character(3)","NULL::text"}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);int calls=0;
            evaluator.registerFunction("starts_with",[&](const std::vector<ExprValue>& args){
                ++calls;
                const auto arity=arguments.empty()?0:arguments=="1,2"?2:arguments=="1,2,3"?3:1;
                bool kept=args.size()==static_cast<size_t>(arity);
                if(kept && arguments=="'é  '::character(3)")kept=args[0].typeName=="character" && args[0].value=="é  ";
                if(kept && arguments=="NULL::text")kept=args[0].isNull && args[0].typeName=="text";
                return ExprValue("text",kept?"callback-value":"bad-input");
            },volatility);
            try {
                SQLParser parser;auto parsed=parser.parseForBinding("SELECT starts_with("+arguments+")");
                auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
                if(!select)return 2;
                auto* node=dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get());
                if(!node)return 2;
                evaluator.bindScalarFunctions(node,&g_engine);
                const auto identity=evaluator.scalarFunctionIdentity(node,&g_engine);
                const auto value=evaluator.eval(node,RowContext{});
                check(!value.isNull && value.typeName=="text" && value.value=="callback-value" && calls==1 && identity.rfind("native",0)==0,"opaque starts_with("+arguments+")");
            }catch(const std::exception& error){check(false,"opaque starts_with "+std::string(error.what()));}
        }
    cleanupTestDb(fixture);finalCleanupTestData();
    std::cout<<"STARTS_WITH_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
