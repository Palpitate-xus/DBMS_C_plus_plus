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
    const std::string fixture="character_code_signature", database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(i INTEGER,t TEXT)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;if(!good){++failures;std::cerr<<"CHARACTER_CODE_NATIVE_FAILURE "<<label<<'\n';}
    };
    const auto reject=[&](const std::string& sql,const std::string& expected) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){state=error.sqlState();}
        catch(const std::exception& error){state=error.what();}
        check(state==expected,sql+" expected="+expected+" actual="+state);
    };
    for(const std::string name:{"ascii","chr"}) {
        const auto oid=name=="ascii"?23:25;
        const auto type=name=="ascii"?"integer":"text";
        const std::vector<std::string> bad=name=="ascii"?
            std::vector<std::string>{"","'a','b'","1","NULL::integer","true","'a'::bytea","ARRAY[1]"}:
            std::vector<std::string>{"","65,66","'65'::text","65::bigint","65.0::numeric","true","NULL::bytea"};
        for(const auto& argument:bad)for(const std::string suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})
            reject("SELECT "+name+"("+argument+")"+suffix,"42883");
        const std::string invalid=name=="ascii"?"1":"true",fallback=name=="ascii"?"0":"'x'";
        reject("SELECT CASE WHEN false THEN "+name+"("+invalid+") ELSE "+fallback+" END","42883");
        reject("SELECT COALESCE("+fallback+","+name+"("+invalid+"))","42883");
        reject("SELECT "+name+"(missing) FROM empty_rows LIMIT 0","42703");
        std::vector<std::pair<std::string,std::string>> good=name=="ascii"?
            std::vector<std::pair<std::string,std::string>>{{"'中'","20013"},{"'😀'","128512"},{"''","0"},{"'   '::char(3)","0"},{"NULL","NULL"}}:
            std::vector<std::pair<std::string,std::string>>{{"65::smallint","A"},{"'65'","A"},{"20013","中"},{"128512","😀"},{"1114111","\xF4\x8F\xBF\xBF"},{"NULL","NULL"}};
        for(const auto& [argument,expected]:good) {
            const auto expression=name+"("+argument+")";
            try {
                const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");
                check(prepared.output.size()==1 && prepared.output.front().typeOid==oid && prepared.output.front().type==type,"descriptor "+expression);
                SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
                auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
                if(!select)return 2;
                ExprEvaluator evaluator;evaluator.setCurrentDB(database);
                auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);
                const auto value=evaluator.eval(node,RowContext{});
                check(value.typeName==type && (expected=="NULL"?value.isNull:!value.isNull && value.value==expected),"runtime "+expression);
            } catch(const std::exception& error){check(false,expression+" "+error.what());}
        }
        for(const std::string context:{database,std::string{}})for(const auto volatility:{'v','i'})
            for(const std::string arguments:{"","1","1,2","'é  '::character(3)","NULL::text"}) {
                ExprEvaluator evaluator;evaluator.setCurrentDB(context);int calls=0;
                evaluator.registerFunction(name,[&](const std::vector<ExprValue>& args){
                    ++calls;
                    const auto arity=arguments.empty()?0:arguments=="1,2"?2:1;
                    bool kept=args.size()==static_cast<size_t>(arity);
                    if(kept && arguments=="'é  '::character(3)")kept=args[0].typeName=="character" && args[0].value=="é  ";
                    if(kept && arguments=="NULL::text")kept=args[0].isNull && args[0].typeName=="text";
                    return ExprValue("text",kept?"callback-value":"bad-input");
                },volatility);
                try {
                    SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+name+"("+arguments+")");
                    auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
                    if(!select)return 2;
                    auto* node=dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get());
                    if(!node)return 2;
                    evaluator.bindScalarFunctions(node,&g_engine);
                    const auto identity=evaluator.scalarFunctionIdentity(node,&g_engine);
                    const auto value=evaluator.eval(node,RowContext{});
                    check(!value.isNull && value.typeName=="text" && value.value=="callback-value" && calls==1 && identity.rfind("native",0)==0,"opaque "+name+"("+arguments+")");
                }catch(const std::exception& error){check(false,"opaque "+name+" "+error.what());}
            }
    }
    reject("SELECT chr('65x')","22P02");
    reject("SELECT chr('2147483648')","22003");
    cleanupTestDb(fixture);finalCleanupTestData();
    std::cout<<"CHARACTER_CODE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
