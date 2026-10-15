#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <exception>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="round_result_type",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(i2 SMALLINT,i4 INTEGER,i8 BIGINT,r4 REAL,"
                      "r8 DOUBLE PRECISION,n NUMERIC,d DECIMAL)",session) ||
       ddl.executeSql("CREATE SCHEMA round_result_user",session))return 2;

    size_t controls=0,failures=0,nestedControls=0;
    const auto check=[&](bool valid,const std::string& label) {
        ++controls;
        if(!valid){++failures;std::cerr<<"ROUND_RESULT_TYPE_FAIL "<<label<<'\n';}
    };
    const auto descriptor=[&](const std::string& expression,const std::string& tail,
                              const std::string& type,uint32_t oid) {
        const std::string sql="SELECT "+expression+tail;
        try {
            const auto query=g_engine.prepareBoundQuery(database,sql);
            check(query.output.size()==1 && query.output.front().type==type &&
                  query.output.front().typeOid==oid,
                  sql+" expected="+type+"/"+std::to_string(oid)+
                  (query.output.size()==1?" actual="+query.output.front().type+"/"+
                      std::to_string(query.output.front().typeOid):" wrong output width"));
        } catch(const DbError& error) {
            check(false,sql+" SQLSTATE="+error.sqlState()+" "+error.what());
        } catch(const std::exception& error) {check(false,sql+" "+error.what());}
    };
    struct Input {const char* declaration;const char* column;bool numeric;};
    const std::vector<Input> inputs={
        {"smallint","i2",false},{"integer","i4",false},{"bigint","i8",false},
        {"real","r4",false},{"double precision","r8",false},
        {"numeric","n",true},{"decimal","d",true}};
    const std::vector<std::string> literalTails={""," WHERE false"," LIMIT 0"};
    const std::vector<std::string> tableTails={" FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"};
    for(const auto& input:inputs) {
        const std::string resultType=input.numeric?"numeric":"double precision";
        const uint32_t resultOid=input.numeric?1700:701;
        for(const auto& tail:literalTails) {
            descriptor(std::string("round(1::")+input.declaration+")",tail,resultType,resultOid);
            descriptor(std::string("round(NULL::")+input.declaration+")",tail,resultType,resultOid);
        }
        for(const auto& tail:tableTails)
            descriptor(std::string("round(")+input.column+")",tail,resultType,resultOid);
    }
    // ROUND has only numeric/float8 one-argument overloads. These UNKNOWN
    // controls are verified for ROUND, not transferable to TRUNC's MAC types.
    for(const auto* unknown:{"'1.25'","NULL"})for(const auto& tail:literalTails)
        descriptor(std::string("round(")+unknown+")",tail,"double precision",701);
    for(const auto& tail:literalTails) {
        descriptor("abs(round(1::integer))",tail,"double precision",701);
        descriptor("round(round(1::smallint))",tail,"double precision",701);
        descriptor("abs(round(1.25::numeric))",tail,"numeric",1700);
        descriptor("round(round(1.25::decimal))",tail,"numeric",1700);
    }
    for(const auto& tail:tableTails) {
        descriptor("abs(round(i4))",tail,"double precision",701);
        descriptor("abs(round(n))",tail,"numeric",1700);
        descriptor("pg_catalog.round(i4)",tail,"double precision",701);
        descriptor("pg_catalog.round(n)",tail,"numeric",1700);
    }
    if(ddl.executeSql("CREATE FUNCTION round_result_user.round(i INTEGER) RETURNS INTEGER "
                      "LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$",session))return 2;
    for(const auto& tail:literalTails)
        descriptor("round_result_user.round(1)",tail,"integer",23);
    for(const auto& tail:tableTails)
        descriptor("round_result_user.round(i4)",tail,"integer",23);

    // Existing one-argument callbacks already produce the required datum
    // domains. This candidate does not alter their rounding implementation.
    for(const bool numeric:{false,true})for(const bool isNull:{false,true}) {
        const std::string expression=std::string("round(")+(isNull?"NULL":"1")+
            (numeric?"::numeric)":"::integer)");
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        if(!select)return 2;
        try {
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            evaluator.bindScalarFunctions(select->selectList.front().expr.get(),&g_engine);
            const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});
            check(value.typeName==(numeric?"numeric":"double precision") && value.isNull==isNull &&
                  (isNull || value.value=="1"),"callback domain "+expression);
        } catch(const std::exception& error) {check(false,"callback domain "+expression+" "+error.what());}
    }

    struct NativeInput {const char* sql;const char* type;const char* value;bool zero;};
    const std::vector<NativeInput> nativeInputs={
        {"1::integer","integer","1",false},
        {"1::numeric","numeric","1",false},
        {"'payload'::text","text","payload",false},
        {"","","",true}};
    for(const auto& context:std::vector<std::string>{"",database}) {
        session.currentDB=context;
        for(const bool immutable:{false,true})for(const auto& input:nativeInputs) {
            size_t calls=0;bool originalArgument=true;
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            const auto callback=[&](const std::vector<ExprValue>& args) {
                ++calls;
                originalArgument=originalArgument && args.size()==(input.zero?0u:1u);
                if(!input.zero && args.size()==1)
                    originalArgument=originalArgument && args.front().typeName==input.type &&
                        !args.front().isNull && args.front().value==input.value;
                return ExprValue("integer","-7");
            };
            if(immutable)evaluator.registerFunction("round",callback,'i');
            else evaluator.registerFunction("round",callback);
            const std::string expression=std::string("round(")+input.sql+")";
            const std::string label=(context.empty()?"empty DB ":"real DB ")+
                expression+(immutable?" immutable":" default");
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            auto* call=select?dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
            if(!call)return 2;
            try {
                const bool pure=evaluator.hasScalarFunction(call,&g_engine) &&
                    evaluator.scalarFunctionResultType(call,&g_engine).empty() &&
                    evaluator.scalarFunctionVolatility(call,&g_engine)==(immutable?'i':'v') && calls==0;
                evaluator.bindScalarFunctions(call,&g_engine);
                const bool stillPure=calls==0 && evaluator.scalarFunctionResultType(call,&g_engine).empty();
                const auto result=evaluator.eval(call,RowContext{});
                check(pure && stillPure && calls==1 && originalArgument && !result.isNull &&
                      result.typeName=="integer" && result.value=="-7","opaque direct "+label);
            } catch(const std::exception& error) {check(false,"opaque direct "+label+" "+error.what());}

            // Exact legacy nested behavior was established on the old Root:
            // integer/numeric/zero-arg success, TEXT rejection before callback.
            calls=0;originalArgument=true;
            SQLParser nestedParser;
            auto nested=nestedParser.parseForBinding("SELECT abs("+expression+")");
            auto* nestedSelect=nested.success?dynamic_cast<SelectStmt*>(nested.stmt.get()):nullptr;
            if(!nestedSelect)return 2;
            std::string phase="binding";
            const bool textRejected=std::string(input.type)=="text";
            try {
                evaluator.bindScalarFunctions(nestedSelect->selectList.front().expr.get(),&g_engine);
                const size_t bindCalls=calls;phase="runtime";
                const auto result=evaluator.eval(nestedSelect->selectList.front().expr.get(),RowContext{});
                check(!textRejected && bindCalls==0 && calls==1 && originalArgument &&
                      !result.isNull && result.value=="7" &&
                      result.typeName==(input.zero?"double precision":input.type),
                      "opaque nested "+label);
            } catch(const DbError& error) {
                check(textRejected && phase=="binding" && error.sqlState()=="42883" &&
                      calls==0 && originalArgument,"opaque nested error "+label);
            } catch(const std::exception& error) {
                check(false,"opaque nested "+label+" "+error.what());
            }
            ++nestedControls;
        }
    }
    session.currentDB=database;
    check(controls==123 && nestedControls==16,"complete matrix and nested compatibility controls");
    std::cout<<"ROUND_RESULT_TYPE_COMPLETE "<<controls<<" FAILURES "<<failures
        <<" NESTED_COMPATIBILITY_CONTROLS "<<nestedControls<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(name);
    return failures?1:0;
}
