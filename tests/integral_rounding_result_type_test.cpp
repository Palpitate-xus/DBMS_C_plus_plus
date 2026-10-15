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
    const std::string name="integral_rounding_result_type",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE empty_rows(i2 SMALLINT,i4 INTEGER,i8 BIGINT,r4 REAL,"
                      "r8 DOUBLE PRECISION,n NUMERIC,d DECIMAL)",session) ||
       ddl.executeSql("CREATE SCHEMA integral_rounding_user",session))return 2;

    size_t controls=0,failures=0;
    const auto check=[&](bool valid,const std::string& label) {
        ++controls;
        if(!valid){++failures;std::cerr<<"INTEGRAL_ROUNDING_TYPE_FAIL "<<label<<'\n';}
    };
    const auto descriptor=[&](const std::string& expression,const std::string& tail,
                              const std::string& type,uint32_t oid) {
        const std::string sql="SELECT "+expression+tail;
        try {
            const auto query=g_engine.prepareBoundQuery(database,sql);
            const bool valid=query.output.size()==1 && query.output.front().type==type &&
                query.output.front().typeOid==oid;
            check(valid,sql+" expected="+type+"/"+std::to_string(oid)+
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
    const std::vector<std::string> functions={"ceil","ceiling","floor"};

    for(const auto& callee:functions) {
        for(const auto& input:inputs) {
            const std::string resultType=input.numeric?"numeric":"double precision";
            const uint32_t resultOid=input.numeric?1700:701;
            for(const auto& tail:literalTails) {
                descriptor(callee+"(1::"+input.declaration+")",tail,resultType,resultOid);
                descriptor(callee+"(NULL::"+input.declaration+")",tail,resultType,resultOid);
            }
            for(const auto& tail:tableTails)
                descriptor(callee+"("+input.column+")",tail,resultType,resultOid);
        }
        for(const auto* unknown:{"'1.25'","NULL"})for(const auto& tail:literalTails)
            descriptor(callee+"("+unknown+")",tail,"double precision",701);
        for(const auto& tail:literalTails) {
            descriptor("abs("+callee+"(1::integer))",tail,"double precision",701);
            descriptor(callee+"("+callee+"(1::smallint))",tail,"double precision",701);
            descriptor("abs("+callee+"(1.25::numeric))",tail,"numeric",1700);
            descriptor(callee+"("+callee+"(1.25::decimal))",tail,"numeric",1700);
        }
        for(const auto& tail:tableTails) {
            descriptor("abs("+callee+"(i4))",tail,"double precision",701);
            descriptor("abs("+callee+"(n))",tail,"numeric",1700);
            descriptor("pg_catalog."+callee+"(i4)",tail,"double precision",701);
            descriptor("pg_catalog."+callee+"(n)",tail,"numeric",1700);
        }
    }

    // The result declaration belongs to the actual qualified stored callee,
    // not to a same-named builtin's rounding rule. Deliberately use INTEGER
    // to distinguish this metadata from the builtin float8 declaration.
    for(const auto& callee:functions) {
        if(ddl.executeSql("CREATE FUNCTION integral_rounding_user."+callee+
            "(i INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$SELECT i+10$$",session))return 2;
        for(const auto& tail:literalTails)
            descriptor("integral_rounding_user."+callee+"(1)",tail,"integer",23);
        for(const auto& tail:tableTails)
            descriptor("integral_rounding_user."+callee+"(i4)",tail,"integer",23);
    }

    // Actual callbacks already keep the numeric and float8 value domains.
    // Metadata repair must agree with those results, including typed NULL.
    for(const auto& callee:functions)for(const bool numeric:{false,true})for(const bool isNull:{false,true}) {
        const std::string expression=callee+"("+(isNull?"NULL":"1")+
            std::string(numeric?"::numeric)":"::integer)");
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
    // Native callback registration has no result declaration. Both overloads
    // (including an explicitly immutable callback) retain their original
    // argument types and the pre-existing nested inference behavior.
    struct NativeInput {const char* sql;const char* type;const char* value;char volatility;};
    const std::vector<NativeInput> nativeInputs={
        {"1::integer","integer","1",'v'},
        {"1::numeric","numeric","1",'i'},
        {"'payload'::text","text","payload",'s'}};
    for(const auto& context:std::vector<std::string>{"",database}) {
        session.currentDB=context;
        for(const auto& callee:functions)for(const auto& input:nativeInputs)for(const bool nested:{false,true}) {
            size_t calls=0;bool originalArgument=true;
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            const auto callback=[&](const std::vector<ExprValue>& args) {
                ++calls;
                originalArgument=originalArgument && args.size()==1 &&
                    args.front().typeName==input.type && !args.front().isNull &&
                    args.front().value==input.value;
                return ExprValue("integer","-7");
            };
            if(input.volatility=='v')evaluator.registerFunction(callee,callback);
            else evaluator.registerFunction(callee,callback,input.volatility);
            const std::string call=callee+"("+input.sql+")";
            const std::string expression=nested?"abs("+call+")":call;
            const std::string label=(context.empty()?"empty DB ":"real DB ")+expression;
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            auto* root=select?dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
            auto* native=root && nested?dynamic_cast<FunctionCallExpr*>(root->args.front().get()):root;
            if(!native)return 2;
            const bool rejectText=nested && std::string(input.type)=="text";
            bool metadataPure=false;
            try {
                metadataPure=evaluator.hasScalarFunction(native,&g_engine) &&
                    evaluator.scalarFunctionResultType(native,&g_engine).empty() &&
                    evaluator.scalarFunctionVolatility(native,&g_engine)==input.volatility && calls==0;
                evaluator.bindScalarFunctions(root,&g_engine);
                metadataPure=metadataPure && evaluator.scalarFunctionResultType(native,&g_engine).empty();
                if(calls!=0){check(false,label+" callback executed during binding");continue;}
                const auto value=evaluator.eval(root,RowContext{});
                check(!rejectText && metadataPure && calls==1 && originalArgument && !value.isNull &&
                    value.typeName==(nested?input.type:"integer") && value.value==(nested?"7":"-7"),label);
            } catch(const DbError& error) {
                check(rejectText && metadataPure && error.sqlState()=="42883" && calls==0,
                    label+" SQLSTATE="+error.sqlState());
            } catch(const std::exception& error) {check(false,label+" "+error.what());}
        }
    }
    session.currentDB=database;
    check(controls==309,"complete metadata and callback matrix");
    std::cout<<"INTEGRAL_ROUNDING_TYPE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(name);
    return failures?1:0;
}
