#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="strpos_signature",database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(i INTEGER,t TEXT)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;
        if(!good){++failures;std::cerr<<"STRPOS_SIGNATURE_NATIVE_FAILURE "<<label<<'\n';}
    };
    const auto reject=[&](const std::string& sql,const std::string& state) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){actual=error.sqlState();}
        catch(const std::exception& error){actual=error.what();}
        check(actual==state,sql+" expected="+state+" actual="+actual);
    };
    const std::vector<std::pair<std::string,std::string>> positive={
        {"strpos('甲乙针','针')","3"}, {"strpos('abc','')","1"},
        {"strpos('','x')","0"}, {"strpos(NULL::text,'')","NULL"},
        {"strpos(NULL,NULL)","NULL"}, {"strpos('a  '::char(3),' ')","0"},
        {"strpos('abc','a '::char(2))","1"}, {"strpos('abc'::varchar,'b')","2"},
        {"strpos('abc'::name,'b'::name)","2"}, {"pg_catalog.strpos('abc','b')","2"}};
    for(const auto& item:positive) {
        try {
            const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+item.first+" FROM empty_rows LIMIT 0");
            check(prepared.output.size()==1 && prepared.output.front().typeOid==23 &&
                  prepared.output.front().type=="integer","descriptor "+item.first);
        }catch(const std::exception& error){check(false,"descriptor "+item.first+" "+error.what());}
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+item.first);
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        if(!select)return 2;
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);
        try {
            auto* node=select->selectList.front().expr.get();
            evaluator.bindScalarFunctions(node,&g_engine);
            const auto value=evaluator.eval(node,RowContext{});
            check(value.typeName=="integer" && (item.second=="NULL"?value.isNull:
                  !value.isNull && value.value==item.second),"runtime "+item.first);
        }catch(const std::exception& error){check(false,"runtime "+item.first+" "+error.what());}
    }
    const std::vector<std::string> invalid={"","'abc'","'abc','b','x'","1,2",
        "NULL::integer,NULL::integer","true,false","'\\x61'::bytea,'\\x61'::bytea",
        "NULL::uuid,''::text","'abc',NULL::uuid","'abc',B'1'","'abc',ARRAY[1]","missing=>'abc'"};
    for(const auto& args:invalid)
        for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"})
            reject("SELECT strpos("+args+")"+suffix,"42883");
    reject("SELECT COALESCE(1,strpos(1,2))","42883");
    reject("SELECT CASE WHEN false THEN strpos(1,2) ELSE 0 END","42883");
    reject("SELECT strpos(missing,'') FROM empty_rows LIMIT 0","42703");

    // A native callback replacement owns its own arity, types and metadata.
    for(const std::string context:{database,std::string{}})for(const auto volatility:{'v','i'})
        for(const auto* arguments:{"'abc'","1,2,3"}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);int calls=0;
            const size_t arity=std::string(arguments)=="'abc'"?1:3;
            evaluator.registerFunction("strpos",[&](const std::vector<ExprValue>& args){
                ++calls;return ExprValue("text",args.size()==arity?"callback-result":"bad-arity");
            },volatility);
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT strpos("+std::string(arguments)+")");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            auto* node=select?dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
            if(!node)return 2;
            try {
                const bool metadata=evaluator.hasScalarFunction(node,&g_engine) &&
                    evaluator.scalarFunctionResultType(node,&g_engine).empty() &&
                    evaluator.scalarFunctionVolatility(node,&g_engine)==volatility &&
                    evaluator.scalarFunctionIdentity(node,&g_engine).rfind("native",0)==0 &&
                    ExprHelper::inferParsedInputType(node,{},context,&g_engine,&evaluator)=="unknown" && calls==0;
                evaluator.bindScalarFunctions(node,&g_engine);
                const auto result=evaluator.eval(node,RowContext{});
                check(metadata && calls==1 && result.typeName=="text" && result.value=="callback-result",
                      "opaque replacement "+std::string(arguments));
            }catch(const std::exception& error){check(false,"opaque replacement "+std::string(error.what()));}
        }
    for(const bool unused:{false,true}) {
        int calls=0;ExprEvaluator evaluator;evaluator.setCurrentDB(database);
        evaluator.registerFunction("strpos_input_effect",[&](const std::vector<ExprValue>&){++calls;return ExprValue("text","甲乙针");});
        const std::string expression="strpos(strpos_input_effect(),'针')";
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+(unused?"COALESCE(7,"+expression+")":expression));
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        if(!select)return 2;
        try {
            auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);
            const bool pure=calls==0;const auto value=evaluator.eval(node,RowContext{});
            check(pure && calls==(unused?0:1) && !value.isNull && value.typeName=="integer" &&
                  value.value==(unused?"7":"3"),"argument demand");
        }catch(const std::exception& error){check(false,"argument demand "+std::string(error.what()));}
    }
    for(const auto* type:{"text","varchar","character","name","integer","bytea","binary"}) {
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT strpos(v,w)");
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        if(!select)return 2;
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);RowContext row;
        const std::string raw=std::string(type)=="integer"?"7":
            std::string(type)=="bytea"?"\\x61":std::string(type)=="binary"?"a":"é  ";
        row.set("v",ExprValue(type,raw));row.set("w",ExprValue("text","é"));
        const bool bad=std::string(type)=="integer" || std::string(type)=="bytea" || std::string(type)=="binary";
        try {
            auto* node=select->selectList.front().expr.get();evaluator.bindScalarFunctions(node,&g_engine);
            const auto value=evaluator.eval(node,row);
            check(!bad && value.typeName=="integer" && !value.isNull && value.value=="1","legacy "+std::string(type));
        }catch(const DbError& error){check(bad && error.sqlState()=="42883","legacy "+std::string(type)+" "+error.what());}
        catch(const std::exception& error){check(false,"legacy "+std::string(type)+" "+error.what());}
    }
    if(controls!=76){++failures;std::cerr<<"STRPOS_SIGNATURE_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"STRPOS_SIGNATURE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(fixture);
    return failures?1:0;
}
