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
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="string_length_signature",database=testDbPath(fixture);
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
        if(!good){++failures;std::cerr<<"STRING_LENGTH_SIGNATURE_NATIVE_FAILURE "<<label<<'\n';}
    };
    const auto reject=[&](const std::string& sql,const std::string& state) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){actual=error.sqlState();}
        catch(const std::exception& error){actual=error.what();}
        check(actual==state,sql+" expected="+state+" actual="+actual);
    };
    const std::vector<std::string> names={"char_length","character_length","octet_length","bit_length"};
    const std::vector<std::string> suffixes={""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"};
    for(const auto& name:names) {
        const bool character=name=="char_length" || name=="character_length";
        std::vector<std::string> invalid={"","'a','b'","1","NULL::integer","true","1.25::numeric",
            "NULL::uuid","ARRAY[1]","'2024-01-01'::date"};
        if(character)for(const auto* argument:{"B'101'","'abc'::bytea","NULL::bit varying"})invalid.emplace_back(argument);
        for(const auto& argument:invalid) {
            const auto expression=name+"("+argument+")";
            for(const auto& suffix:suffixes)for(const auto* role:{""," OVER()"})
                reject("SELECT "+expression+role+suffix,"42883");
            reject("SELECT CASE WHEN false THEN "+expression+" ELSE 0 END","42883");
            reject("SELECT COALESCE(1,"+expression+")","42883");
        }
        for(const auto* suffix:{" WHERE false"," LIMIT 0"})
            reject("SELECT "+name+"(i) FROM empty_rows"+suffix,"42883");
        reject("SELECT pg_catalog."+name+"(1)","42883");
        reject("SELECT "+name+"(missing) FROM empty_rows LIMIT 0","42703");
        const auto amount=[&](int characters,int octets,int bits) {
            return std::to_string(character?characters:name=="octet_length"?octets:bits);
        };
        std::vector<std::pair<std::string,std::string>> positive={
            {"'你好é'",amount(3,8,64)},{"'é  '::text",amount(3,4,32)},
            {"'é  '::varchar",amount(3,4,32)},{"'é  '::character(3)",amount(1,4,16)},
            {"'é  '::name",amount(3,4,32)},{"''","0"},{"NULL","NULL"},
            {"NULL::text","NULL"},{"NULL::character(4)","NULL"}};
        if(!character) {
            positive.emplace_back("B'101010101'",amount(0,2,9));
            positive.emplace_back("B''","0");
            positive.emplace_back("'\\x610062'::bytea",amount(0,3,24));
            positive.emplace_back("NULL::bytea","NULL");
            positive.emplace_back("NULL::bit varying","NULL");
        }
        for(const auto& [argument,expected]:positive) {
            const auto expression=name+"("+argument+")";
            try {
                const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows LIMIT 0");
                check(prepared.output.size()==1 && prepared.output.front().typeOid==23 &&
                    prepared.output.front().type=="integer","descriptor "+expression);
            }catch(const std::exception& error){check(false,"descriptor "+expression+" "+error.what());}
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)return 2;
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            try {
                auto* node=select->selectList.front().expr.get();
                evaluator.bindScalarFunctions(node,&g_engine);
                const auto value=evaluator.eval(node,RowContext{});
                check(value.typeName=="integer" && (expected=="NULL"?value.isNull:!value.isNull && value.value==expected),"runtime "+expression);
            }catch(const std::exception& error){check(false,"runtime "+expression+" "+error.what());}
            reject("SELECT "+expression+" OVER()","42809");
        }
        for(const auto* type:{"varchar","character","integer","binary","varbinary"}) {
            const bool bytes=std::string(type)=="binary" || std::string(type)=="varbinary";
            const bool bad=std::string(type)=="integer" || (character && bytes);
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+name+"(v)");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)return 2;
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);RowContext row;
            row.set("v",ExprValue(type,bytes?std::string("a\0b",3):std::string(type)=="integer"?"7":"é  "));
            try {
                auto* node=select->selectList.front().expr.get();
                evaluator.bindScalarFunctions(node,&g_engine);
                const auto value=evaluator.eval(node,row);
                const auto expected=bytes?amount(0,3,24):std::string(type)=="character"?amount(1,4,16):amount(3,4,32);
                check(!bad && !value.isNull && value.typeName=="integer" && value.value==expected,"legacy "+name+" "+type);
            }catch(const DbError& error){check(bad && error.sqlState()=="42883","legacy "+name+" "+type+" "+error.what());}
            catch(const std::exception& error){check(false,"legacy "+name+" "+type+" "+error.what());}
        }
        for(const std::string context:{database,std::string{}})for(const auto volatility:{'v','i'})
            for(const auto* arguments:{"1::integer","'é  '::character(3)","NULL::text","","1,2"}) {
                ExprEvaluator evaluator;evaluator.setCurrentDB(context);int calls=0;
                const size_t expectedArity=std::string(arguments).empty()?0:std::string(arguments)=="1,2"?2:1;
                bool inputsKept=false;
                evaluator.registerFunction(name,[&](const std::vector<ExprValue>& args){
                    ++calls;inputsKept=args.size()==expectedArity;
                    if(inputsKept && !args.empty()) {
                        if(std::string(arguments)=="'é  '::character(3)")
                            inputsKept=!args[0].isNull && args[0].typeName=="character" && args[0].value=="é  ";
                        else if(std::string(arguments)=="NULL::text")
                            inputsKept=args[0].isNull && args[0].typeName=="text";
                        else for(size_t i=0;i<args.size();++i)
                            inputsKept=inputsKept && !args[i].isNull && args[i].typeName=="integer" && args[i].value==std::to_string(i+1);
                    }
                    return ExprValue("text","callback-result");
                },volatility);
                SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+name+"("+arguments+")");
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
                    const auto value=evaluator.eval(node,RowContext{});
                    check(metadata && calls==1 && inputsKept && value.typeName=="text" && value.value=="callback-result",
                        "opaque replacement "+name+"("+arguments+")");
                }catch(const std::exception& error){check(false,"opaque replacement "+name+"("+arguments+") "+error.what());}
            }
        for(const bool unused:{false,true}) {
            int calls=0;ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            evaluator.registerFunction("length_input_effect",[&](const std::vector<ExprValue>&){++calls;return ExprValue("text","你好é");});
            const auto expression=name+"(length_input_effect())";
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+(unused?"COALESCE(7,"+expression+")":expression));
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)return 2;
            try {
                auto* node=select->selectList.front().expr.get();
                evaluator.bindScalarFunctions(node,&g_engine);
                const bool pure=calls==0;
                const auto value=evaluator.eval(node,RowContext{});
                check(pure && calls==(unused?0:1) && !value.isNull && value.typeName=="integer" &&
                    value.value==(unused?"7":amount(3,8,64)),"argument demand "+name+(unused?" unused":" selected"));
            }catch(const std::exception& error){check(false,"argument demand "+name+" "+error.what());}
        }
    }
    if(ddl.executeSql("CREATE SCHEMA length_user",session))return 2;
    for(const auto& name:names)
        if(ddl.executeSql("CREATE FUNCTION length_user."+name+"(i INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT i+100$$",session))return 2;
    session.searchPath="pg_catalog,length_user,public";
    for(const auto& name:names) {
        try {
            const auto prepared=g_engine.prepareBoundQuery(database,"SELECT "+name+"(1)");
            check(prepared.output.size()==1 && prepared.output.front().typeOid==23,"stored overload "+name);
        }catch(const std::exception& error){check(false,"stored overload "+name+" "+error.what());}
        reject("SELECT pg_catalog."+name+"(1)","42883");
    }
    check(controls==606,"complete matrix");
    std::cout<<"STRING_LENGTH_SIGNATURE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(fixture);
    return failures?1:0;
}
