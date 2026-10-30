#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/common_type.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
struct Control {std::string expression;std::optional<std::string> value;std::string state;};
std::string state(const std::exception& error) {
    if(const auto* typed=dynamic_cast<const dbms::DbError*>(&error))return typed->sqlState();
    const std::string text=error.what();const auto at=text.find("SQLSTATE ");
    return at==std::string::npos?std::string{}:text.substr(at+9,5);
}
dbms::FunctionCallExpr cellCall(const std::vector<dbms::ExprValue>& values) {
    dbms::FunctionCallExpr call;call.funcName="array_positions";
    for(size_t i=0;i<values.size();++i) {
        auto parameter=std::make_unique<dbms::ParameterExpr>();
        parameter->slot=i;parameter->declaredType=values[i].typeName;
        call.args.push_back(std::move(parameter));
    }
    return call;
}
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="array_positions_typed_equality",database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;if(ddl.executeSql("CREATE TABLE empty_rows(i INTEGER)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;if(!good){++failures;std::cerr<<"ARRAY_POSITIONS_TYPED_NATIVE_FAILURE "<<label<<'\n';}
    };
    const std::vector<Control> cases={
        {"array_positions('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1::numeric)","{1,3}",""},
        {"array_positions('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.0::numeric)","{1,3}",""},
        {"array_positions('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.00::numeric)","{1,3}",""},
        {"array_positions('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1e0::numeric)","{1,3}",""},
        {"array_positions('{\"9007199254740992.00\",\"9007199254740993.00\",\"9007199254740993.0\"}'::numeric[],9007199254740993::numeric)","{2,3}",""},
        {"array_positions('{\"NaN\",\"0\",\"NaN\"}'::numeric[],'NaN'::numeric)","{1,3}",""},
        {"array_positions('{\"Infinity\",\"0\",\"Infinity\"}'::numeric[],'Infinity'::numeric)","{1,3}",""},
        {"array_positions('{\"-Infinity\",\"0\",\"-Infinity\"}'::numeric[],'-Infinity'::numeric)","{1,3}",""},
        {"array_positions('{\"-0\",\"0\",\"1\"}'::real[],0::real)","{1,2}",""},
        {"array_positions('{\"NaN\",\"0\",\"NaN\"}'::real[],'NaN'::real)","{1,3}",""},
        {"array_positions('{\"Infinity\",\"0\",\"Infinity\"}'::real[],'Infinity'::real)","{1,3}",""},
        {"array_positions('{\"-0\",\"0\",\"1\"}'::double precision[],0::double precision)","{1,2}",""},
        {"array_positions('{\"NaN\",\"0\",\"NaN\"}'::double precision[],'NaN'::double precision)","{1,3}",""},
        {"array_positions('{\"Infinity\",\"0\",\"Infinity\"}'::double precision[],'Infinity'::double precision)","{1,3}",""},
        {"array_positions('{\"1\",\"2\",\"1\"}'::integer[],1.0::numeric)","{1,3}",""},
        {"array_positions('{\"0.10000000149\",\"0\",\"0.10000000149\"}'::numeric[],0.1::real)","{1,3}",""},
        {"array_positions('{\"a  \",\"b\",\"a\"}'::character(3)[],'a'::character(1))","{1,3}",""},
        {"array_positions('{\"é  \",\"e\",\"é\"}'::character(3)[],'é'::character(1))","{1,3}",""},
        {"array_positions('{\"a  \",\"a\",\"a  \"}'::text[],'a  '::text)","{1,3}",""},
        {"array_positions('{\"01\",\"1\",\"1.0\",\"1\"}'::text[],'1'::text)","{2,4}",""},
        {"array_positions('{\"NULL\",NULL,\"\"}'::text[],'NULL'::text)","{1}",""},
        {"array_positions('{\"NULL\",NULL,\"\"}'::text[],NULL::text)","{2}",""},
        {"array_positions('{\"NULL\",NULL,\"\"}'::text[],''::text)","{3}",""},
        {"array_positions('{\"1 mon\",\"30 days\",\"2 mons\"}'::interval[],'30 days'::interval)","{1,2}",""},
        {"array_positions('{\"12:00:00\",\"13:00:00\",\"12:00\"}'::time[],'12:00'::time)","{1,3}",""},
        {"array_positions('{\"12:00:00+01\",\"11:00:00+00\",\"12:00:00+01\"}'::timetz[],'11:00:00+00'::timetz)","{2}",""},
        {"array_positions('{\"2024-01-01 00:00:00\",\"2024-02-01\",\"2024-01-01\"}'::timestamp[],'2024-01-01'::timestamp)","{1,3}",""},
        {"array_positions('{\"2024-01-01 00:00:00+01\",\"2023-12-31 23:00:00+00\"}'::timestamptz[],'2023-12-31 23:00:00+00'::timestamptz)","{1,2}",""},
        {"array_positions('{\"true\",\"false\",\"true\"}'::boolean[],true)","{1,3}",""},
        {"array_positions('{\"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF\",\"ffffffff-ffff-ffff-ffff-ffffffffffff\"}'::uuid[],'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)","{1,2}",""},
        {"array_positions('{\"\\\\x610062\",\"\\\\x61\",\"\\\\x610062\"}'::bytea[],'\\x610062'::bytea)","{1,3}",""},
        {"array_positions('{\"01\",\"1\",\"1\"}'::bit varying[],B'1')","{2,3}",""},
        {"array_positions('{\"192.0.2.1/32\",\"192.0.2.2\",\"192.0.2.1\"}'::inet[],'192.0.2.1'::inet)","{1,3}",""},
        {"array_positions('{\"192.0.2.0/24\",\"198.51.100.0/24\",\"192.0.2.0/24\"}'::cidr[],'192.0.2.0/24'::inet)","{1,3}",""},
        {"array_positions('{\"08:00:2b:01:02:03\",\"08-00-2b-01-02-03\"}'::macaddr[],'08:00:2b:01:02:03'::macaddr)","{1,2}",""},
        {"array_positions('{\"1.00\",\"2\",\"1\"}'::jsonb[],'1.0'::jsonb)","{1,3}",""},
        {"array_positions('{\"[1,2]\",\"[2,1]\",\"[1.00,2.0]\"}'::jsonb[],'[1,2]'::jsonb)","{1,3}",""},
        {"array_positions('{\"{\\\"a\\\":1,\\\"b\\\":2}\",\"{\\\"a\\\":2}\",\"{\\\"b\\\":2.00,\\\"a\\\":1.0}\"}'::jsonb[],'{\"a\":1,\"b\":2}'::jsonb)","{1,3}",""},
        {"array_positions('{\"true\",\"false\",\"true\"}'::jsonb[],'true'::jsonb)","{1,3}",""},
        {"array_positions('{\"null\",\"\\\"null\\\"\",\"null\"}'::jsonb[],'null'::jsonb)","{1,3}",""},
        {"array_positions('{\"[1.00,2.00)\",\"[2,3)\",\"[1,2)\"}'::numrange[],'[1,2)'::numrange)","{1,3}",""},
        {"array_positions('{\"[1,3]\",\"[1,4)\",\"[5,6)\"}'::int4range[],'[1,4)'::int4range)","{1,2}",""},
        {"array_positions('{\"empty\",\"[1,2)\",\"empty\"}'::numrange[],'empty'::numrange)","{1,3}",""},
        {"array_positions('{\"a:1\",\"a:2\",\"a:1\"}'::tsvector[],'a:1'::tsvector)","{1,3}",""},
        {"array_positions('[-3:1]={\"1.00\",NULL,\"1.0\",\"2\",\"1\"}'::numeric[],1::numeric)","{-3,-1,1}",""},
        {"array_positions('[-3:-1]={\"1.00\",NULL,\"1.0\"}'::numeric[],NULL::numeric)","{-2}",""},
        {"array_positions('[-3:-1]={\"1.00\",NULL,\"1.0\"}'::numeric[],99::numeric)","{}",""},
        {"array_positions('[0:4]={\"1.00\",NULL,\"1.0\",\"2\",\"1\"}'::numeric[],1::numeric)","{0,2,4}",""},
        {"array_positions('[0:2]={\"1.00\",NULL,\"1.0\"}'::numeric[],NULL::numeric)","{1}",""},
        {"array_positions('[0:2]={\"1.00\",NULL,\"1.0\"}'::numeric[],99::numeric)","{}",""},
        {"array_positions('[7:11]={\"1.00\",NULL,\"1.0\",\"2\",\"1\"}'::numeric[],1::numeric)","{7,9,11}",""},
        {"array_positions('[7:9]={\"1.00\",NULL,\"1.0\"}'::numeric[],NULL::numeric)","{8}",""},
        {"array_positions('[7:9]={\"1.00\",NULL,\"1.0\"}'::numeric[],99::numeric)","{}",""},
        {"array_positions(NULL::numeric[],1::numeric)",std::nullopt,""},
        {"array_positions('{}'::numeric[],NULL::numeric)","{}",""},
        {"array_positions('{1,2}'::integer[],NULL)","{}",""},
        {"array_positions('{1,2}'::integer[],99)","{}",""},
        {"array_positions(ARRAY[ARRAY[1,2]],1)",std::nullopt,"0A000"},
        {"array_positions(NULL,NULL)",std::nullopt,""},
        {"array_positions('{}',NULL)","{}",""},
        {"array_positions('{1.00,1.0}'::numeric[],'1')","{1,2}",""},
        {"array_positions('{\"(0,0)\"}'::point[],'(0,0)'::point)",std::nullopt,"42883"},
        {"array_positions('{\"(0,0)\"}'::point[],NULL::point)","{}",""},
        {"array_positions('{NULL}'::point[],NULL::point)",std::nullopt,"42883"},
        {"array_positions('{}'::point[],'(0,0)'::point)","{}",""},
        {"array_positions(NULL::point[],'(0,0)'::point)",std::nullopt,""},
        {"array_positions('{\"{1,2,3}\"}'::line[],'{1,2,3}'::line)",std::nullopt,"42883"},
        {"array_positions('{\"{1,2,3}\"}'::line[],NULL::line)","{}",""},
        {"array_positions('{NULL}'::line[],NULL::line)",std::nullopt,"42883"},
        {"array_positions('{}'::line[],'{1,2,3}'::line)","{}",""},
        {"array_positions(NULL::line[],'{1,2,3}'::line)",std::nullopt,""},
        {"array_positions('{\"[(0,0),(1,1)]\"}'::lseg[],'[(0,0),(1,1)]'::lseg)",std::nullopt,"42883"},
        {"array_positions('{\"[(0,0),(1,1)]\"}'::lseg[],NULL::lseg)","{}",""},
        {"array_positions('{NULL}'::lseg[],NULL::lseg)",std::nullopt,"42883"},
        {"array_positions('{}'::lseg[],'[(0,0),(1,1)]'::lseg)","{}",""},
        {"array_positions(NULL::lseg[],'[(0,0),(1,1)]'::lseg)",std::nullopt,""},
        {"array_positions('{\"(0,0),(1,1)\"}'::box[],'(0,0),(1,1)'::box)",std::nullopt,"42883"},
        {"array_positions('{\"(0,0),(1,1)\"}'::box[],NULL::box)","{}",""},
        {"array_positions('{NULL}'::box[],NULL::box)",std::nullopt,"42883"},
        {"array_positions('{}'::box[],'(0,0),(1,1)'::box)","{}",""},
        {"array_positions(NULL::box[],'(0,0),(1,1)'::box)",std::nullopt,""},
        {"array_positions('{\"[(0,0),(1,1)]\"}'::path[],'[(0,0),(1,1)]'::path)",std::nullopt,"42883"},
        {"array_positions('{\"[(0,0),(1,1)]\"}'::path[],NULL::path)","{}",""},
        {"array_positions('{NULL}'::path[],NULL::path)",std::nullopt,"42883"},
        {"array_positions('{}'::path[],'[(0,0),(1,1)]'::path)","{}",""},
        {"array_positions(NULL::path[],'[(0,0),(1,1)]'::path)",std::nullopt,""},
        {"array_positions('{\"((0,0),(1,1),(0,2))\"}'::polygon[],'((0,0),(1,1),(0,2))'::polygon)",std::nullopt,"42883"},
        {"array_positions('{\"((0,0),(1,1),(0,2))\"}'::polygon[],NULL::polygon)","{}",""},
        {"array_positions('{NULL}'::polygon[],NULL::polygon)",std::nullopt,"42883"},
        {"array_positions('{}'::polygon[],'((0,0),(1,1),(0,2))'::polygon)","{}",""},
        {"array_positions(NULL::polygon[],'((0,0),(1,1),(0,2))'::polygon)",std::nullopt,""},
        {"array_positions('{\"<(0,0),1>\"}'::circle[],'<(0,0),1>'::circle)",std::nullopt,"42883"},
        {"array_positions('{\"<(0,0),1>\"}'::circle[],NULL::circle)","{}",""},
        {"array_positions('{NULL}'::circle[],NULL::circle)",std::nullopt,"42883"},
        {"array_positions('{}'::circle[],'<(0,0),1>'::circle)","{}",""},
        {"array_positions(NULL::circle[],'<(0,0),1>'::circle)",std::nullopt,""},
        {"array_positions('{\"1\"}'::json[],'1'::json)",std::nullopt,"42883"},
        {"array_positions('{\"1\"}'::json[],NULL::json)","{}",""},
        {"array_positions('{NULL}'::json[],NULL::json)",std::nullopt,"42883"},
        {"array_positions('{}'::json[],'1'::json)","{}",""},
        {"array_positions(NULL::json[],'1'::json)",std::nullopt,""},
    };
    for(const auto& control:cases)for(const int mode:{0,1,2}) {
        const std::string label="mode"+std::to_string(mode)+" "+control.expression;
        try {
            ExprValue result;bool descriptor=true;
            if(mode) {
                auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
                descriptor=query->output.size()==1 && query->output[0].typeOid==1007 && query->output[0].type=="integer[]";
                auto* select=dynamic_cast<SelectStmt*>(query->ast.get());if(!select)return 2;
                auto* node=select->selectList.front().expr.get();
                PreparedQueryExecution execution(query,&g_engine,database);
                if(mode==2)execution.planExpressionConstants(node);
                execution.prepareExpression(node);
                result=execution.evaluate(node,execution.context());
            } else {
                SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+control.expression);
                auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;if(!select)return 2;
                auto* node=select->selectList.front().expr.get();
                ExprHelper::prepareArrayTypes(node,{},database,&g_engine);
                ExprEvaluator evaluator;evaluator.setCurrentDB(database);
                evaluator.bindScalarFunctions(node,&g_engine);
                result=evaluator.eval(node,RowContext{});
            }
            check(control.state.empty() && descriptor && result.typeName=="integer[]" &&
                result.isNull==!control.value && (!control.value || result.value==*control.value),
                label+" datum="+result.typeName+" value="+result.value+" null="+std::to_string(result.isNull));
        }catch(const std::exception& error){check(!control.state.empty() && state(error)==control.state,label+" "+error.what());}
    }
    const std::vector<std::string> invalid={
        "array_positions()",
        "array_positions(ARRAY[1])",
        "array_positions(ARRAY[1],1,1)",
        "array_positions(1,1)",
        "array_positions('{}'::text,1)",
        "array_positions('{1}'::integer[],'1'::text)",
        "array_positions('{true}'::boolean[],'true'::text)",
        "array_positions('{a}'::text[],1)",
        "array_positions(ARRAY[1],ARRAY[1])",
    };
    for(const auto& expression:invalid)for(const auto* suffix:{" WHERE false"," LIMIT 0"}) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows"+suffix);}
        catch(const std::exception& error){actual=state(error);}
        check(actual=="42883","empty signature "+expression+suffix+" "+actual);
    }
    for(const auto* expression:{"array_positions(ARRAY[1],1)","array_positions(NULL::integer[],NULL)",
            "array_positions('{1}'::integer[],'bad')",
            "array_positions('{\"(0,0)\"}'::point[],'(0,0)'::point)"})
        for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"}) {
            std::string actual;
            try{(void)g_engine.prepareBoundQuery(database,std::string("SELECT ")+expression+" OVER()"+suffix);}
            catch(const std::exception& error){actual=state(error);}
            check(actual=="42809","scalar OVER priority "+std::string(expression)+suffix+" "+actual);
        }
    const std::vector<std::vector<ExprValue>> cells={
        {{"numeric[]","{1.00,2,1.0}"},{"numeric","1"}},
        {{"numeric[]","[-3:-1]={1.00,NULL,1.0}"},{"numeric","1"}},
        {{"numeric[]","[-3:-1]={1.00,NULL,1.0}"},{"numeric","",true}},
        {{"real[]","{-0,0,1}"},{"real","0"}},
        {{"integer[]","{1,2,1}"},{"numeric","1.0"}},
        {{"numeric[]","{}"},{"numeric","1"}},
        {{"numeric[]","",true},{"numeric","1"}},
    };
    const std::vector<std::optional<std::string>> expected={"{1,3}","{-3,-1}","{-2}","{1,2}","{1,3}","{}",std::nullopt};
    for(size_t i=0;i<cells.size();++i) {
        auto call=cellCall(cells[i]);RowContext row;row.setParameters(cells[i]);
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);
        try {
            const auto result=evaluator.eval(&call,row);
            check(result.typeName=="integer[]" && result.isNull==!expected[i] &&
                (!expected[i] || result.value==*expected[i]),"native cells "+std::to_string(i));
        }catch(const std::exception& error){check(false,std::string("native cells ")+error.what());}
    }
    for(const std::string& context:{database,std::string{}})for(const char volatility:{'v','i'})
        for(const auto& input:std::vector<std::vector<ExprValue>>{cells[0],cells[5],cells[6],{},
                {{"text","{a,b}"},{"text","b"}}}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            size_t calls=0;bool same=true;
            evaluator.registerFunction("array_positions",[&](const std::vector<ExprValue>& actual) {
                ++calls;same=actual.size()==input.size();
                for(size_t i=0;i<actual.size() && i<input.size();++i)
                    same=same && actual[i].typeName==input[i].typeName &&
                        actual[i].value==input[i].value && actual[i].isNull==input[i].isNull &&
                        actual[i].objectOid==input[i].objectOid && actual[i].elementOids==input[i].elementOids;
                return ExprValue("text","owned");
            },volatility);
            auto call=cellCall(input);RowContext row;row.setParameters(input);
            try {
                const bool pure=evaluator.hasScalarFunction(&call,&g_engine) &&
                    evaluator.scalarFunctionResultType(&call,&g_engine).empty() &&
                    evaluator.scalarFunctionVolatility(&call,&g_engine)==volatility &&
                    ExprHelper::inferParsedInputType(&call,{},context,&g_engine,&evaluator)=="unknown" && calls==0;
                evaluator.bindScalarFunctions(&call,&g_engine);
                const auto result=evaluator.eval(&call,row);
                check(pure && calls==1 && same && result.typeName=="text" && result.value=="owned","opaque ownership");
            }catch(const std::exception& error){check(false,std::string("opaque ownership ")+error.what());}
        }
    for(const bool unused:{false,true}) {
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);size_t calls=0;
        evaluator.registerFunction("positions_needle",[&](const std::vector<ExprValue>&){++calls;return ExprValue("numeric","1");});
        const std::string expression="array_positions('{1.00,2,1.0}'::numeric[],positions_needle())";
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+(unused?"COALESCE('{7}'::integer[],"+expression+")":expression));
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;if(!select)return 2;
        auto* node=select->selectList.front().expr.get();
        try {
            evaluator.bindScalarFunctions(node,&g_engine);const bool pure=calls==0;
            const auto result=evaluator.eval(node,RowContext{});
            check(pure && calls==(unused?0:1) && !result.isNull && result.typeName=="integer[]" &&
                result.value==(unused?"{7}":"{1,3}"),"native needle demand");
        }catch(const std::exception& error){check(false,std::string("native needle demand ")+error.what());}
    }
    {
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);auto call=cellCall(cells[0]);
        try{check(evaluator.hasScalarFunction(&call,&g_engine) &&
            evaluator.scalarFunctionResultType(&call,&g_engine)=="integer[]" &&
            evaluator.scalarFunctionVolatility(&call,&g_engine)=='i',"true builtin pure declaration");}
        catch(const std::exception& error){check(false,std::string("builtin declaration ")+error.what());}
    }
    if(ddl.executeSql("CREATE SCHEMA positions_user",session) ||
       ddl.executeSql("CREATE FUNCTION positions_user.array_positions(p INTEGER[],q INTEGER) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$BEGIN RETURN 'user-owned';END;$$",session))return 2;
    session.searchPath="positions_user,pg_catalog,public";
    struct UserControl {std::string expression,value,type;};
    for(const auto& control:std::vector<UserControl>{
            {"array_positions('{1}'::integer[],1)","user-owned","text"},
            {"pg_catalog.array_positions('{1}'::integer[],1)","{1}","integer[]"},
            {"array_positions('{1.00,2,1.0}'::numeric[],1::numeric)","{1,3}","integer[]"},
            {"array_positions('{1}',1)","user-owned","text"},
            {"array_positions(NULL::integer[],1)","user-owned","text"},
            {"COALESCE('ok'::text,array_positions('{1}'::integer[],1))","ok","text"}})
        for(const bool plan:{false,true}) {
            try {
                auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
                auto* select=dynamic_cast<SelectStmt*>(query->ast.get());if(!select)return 2;
                auto* node=select->selectList.front().expr.get();PreparedQueryExecution execution(query,&g_engine,database);
                if(plan)execution.planExpressionConstants(node);
                execution.prepareExpression(node);const auto result=execution.evaluate(node,execution.context());
                check(query->output.size()==1 && query->output[0].type==control.type &&
                    query->output[0].typeOid==(control.type=="text"?25:1007) && !result.isNull &&
                    result.typeName==control.type && result.value==control.value,"user declaration "+control.expression);
            }catch(const std::exception& error){check(false,"user declaration "+control.expression+" "+error.what());}
        }
    for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"}) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,std::string("SELECT array_positions(NULL,NULL)")+suffix);}
        catch(const std::exception& error){actual=state(error);}
        check(actual=="42725","unknown overload ambiguity "+std::string(suffix));
    }
    std::string overState;
    try{(void)g_engine.prepareBoundQuery(database,"SELECT array_positions('{1}'::integer[],1) OVER()");}
    catch(const std::exception& error){overState=state(error);}
    check(overState=="42809","user OVER role");
    for(const auto& control:std::vector<std::vector<std::string>>{
            {"{a,b,a}","a","{1,3}"},{"{01,1,1}","1","{2,3}"},{"{NULL,\"NULL\",NULL}","'NULL'","{2}"}})
        for(const bool bind:{false,true}) {
            FunctionCallExpr call;call.funcName="array_positions";
            for(const auto& value:{control[0],control[1]}) {
                auto literal=std::make_unique<LiteralExpr>();literal->typeName="text";literal->value=value;
                call.args.push_back(std::move(literal));
            }
            ExprEvaluator evaluator;
            try {
                if(bind)evaluator.bindScalarFunctions(&call,&g_engine);
                const auto result=evaluator.eval(&call,RowContext{});
                check(!result.isNull && result.typeName=="integer[]" && result.value==control[2],"native raw TEXT array");
            }catch(const std::exception& error){check(false,std::string("native raw TEXT array ")+error.what());}
        }
    {
        FunctionCallExpr call;call.funcName="array_positions";
        for(const auto& value:{std::string("'{a}'"),std::string("'a'")}) {
            auto literal=std::make_unique<LiteralExpr>();literal->typeName="text";literal->value=value;
            call.args.push_back(std::move(literal));
        }
        ExprEvaluator evaluator;std::string actual;
        try{(void)evaluator.eval(&call,RowContext{});}catch(const std::exception& error){actual=state(error);}
        check(actual=="42883","quoted typed TEXT is not raw native array carrier");
    }
    check(!common_type_detail::implicit("text","text[]"),"no new global TEXT array implicit edge");
    check(controls==387,"complete typed all-match and declaration/ownership matrix");
    std::cout<<"ARRAY_POSITIONS_TYPED_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(fixture);return failures?1:0;
}
