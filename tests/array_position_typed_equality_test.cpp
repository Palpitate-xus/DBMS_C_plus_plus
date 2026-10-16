#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
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
dbms::FunctionCallExpr cellCall(const std::vector<dbms::ExprValue>& cells) {
    dbms::FunctionCallExpr call;call.funcName="array_position";
    for(size_t i=0;i<cells.size();++i) {
        auto parameter=std::make_unique<dbms::ParameterExpr>();
        parameter->slot=i;parameter->declaredType=cells[i].typeName;
        call.args.push_back(std::move(parameter));
    }
    return call;
}
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="array_position_typed_equality",database=testDbPath(fixture);
    cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;if(ddl.executeSql("CREATE TABLE empty_rows(i INTEGER)",session))return 2;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;if(!good){++failures;std::cerr<<"ARRAY_POSITION_TYPED_NATIVE_FAILURE "<<label<<'\n';}
    };
    const std::vector<Control> cases={
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1::numeric)","1",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1::numeric,2)","3",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1::numeric,4)",std::nullopt,""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.0::numeric)","1",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.0::numeric,2)","3",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.0::numeric,4)",std::nullopt,""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.00::numeric)","1",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.00::numeric,2)","3",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1.00::numeric,4)",std::nullopt,""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1e0::numeric)","1",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1e0::numeric,2)","3",""},
        {"array_position('{\"1.00\",\"2.00\",\"1.0\"}'::numeric[],1e0::numeric,4)",std::nullopt,""},
        {"array_position('{\"9007199254740992.00\",\"9007199254740993.00\"}'::numeric[],9007199254740993::numeric)","2",""},
        {"array_position('{\"NaN\",\"Infinity\",\"-Infinity\",\"0\"}'::numeric[],'NaN'::numeric)","1",""},
        {"array_position('{\"NaN\",\"Infinity\",\"-Infinity\",\"0\"}'::numeric[],'Infinity'::numeric)","2",""},
        {"array_position('{\"NaN\",\"Infinity\",\"-Infinity\",\"0\"}'::numeric[],'-Infinity'::numeric)","3",""},
        {"array_position('{\"-0\",\"1\"}'::real[],0::real)","1",""},
        {"array_position('{\"NaN\",\"0\"}'::real[],'NaN'::real)","1",""},
        {"array_position('{\"Infinity\",\"0\"}'::real[],'Infinity'::real)","1",""},
        {"array_position('{\"-0\",\"1\"}'::double precision[],0::double precision)","1",""},
        {"array_position('{\"NaN\",\"0\"}'::double precision[],'NaN'::double precision)","1",""},
        {"array_position('{\"Infinity\",\"0\"}'::double precision[],'Infinity'::double precision)","1",""},
        {"array_position('{\"1\",\"2\"}'::integer[],1.0::numeric)","1",""},
        {"array_position('{\"0.10000000149\"}'::numeric[],0.1::real)","1",""},
        {"array_position('{\"a  \",\"b\"}'::character(3)[],'a'::character(1))","1",""},
        {"array_position('{\"é  \",\"e\"}'::character(3)[],'é'::character(1))","1",""},
        {"array_position('{\"a  \"}'::character(3)[],'a  '::text)","1",""},
        {"array_position('{\"a  \",\"a\"}'::text[],'a'::text)","2",""},
        {"array_position('{\"01\",\"1\",\"1.0\"}'::text[],'1'::text)","2",""},
        {"array_position('{\"NULL\",NULL,\"\"}'::text[],'NULL'::text)","1",""},
        {"array_position('{\"NULL\",NULL,\"\"}'::text[],NULL::text)","2",""},
        {"array_position('{\"NULL\",NULL,\"\"}'::text[],''::text)","3",""},
        {"array_position('{\"1 mon\",\"2 mons\"}'::interval[],'30 days'::interval)","1",""},
        {"array_position('{\"1 day\",\"2 days\"}'::interval[],'24 hours'::interval)","1",""},
        {"array_position('{\"12:00:00\"}'::time[],'12:00'::time)","1",""},
        {"array_position('{\"12:00:00+01\"}'::timetz[],'11:00:00+00'::timetz)",std::nullopt,""},
        {"array_position('{\"2024-01-01 00:00:00\"}'::timestamp[],'2024-01-01'::timestamp)","1",""},
        {"array_position('{\"2024-01-01 00:00:00+01\"}'::timestamptz[],'2023-12-31 23:00:00+00'::timestamptz)","1",""},
        {"array_position('{\"true\",\"false\"}'::boolean[],true)","1",""},
        {"array_position('{\"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF\"}'::uuid[],'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)","1",""},
        {"array_position('{\"\\\\x610062\",\"\\\\x61\"}'::bytea[],'\\x610062'::bytea)","1",""},
        {"array_position('{\"01\",\"1\"}'::bit varying[],B'1')","2",""},
        {"array_position('{\"192.0.2.1/32\"}'::inet[],'192.0.2.1'::inet)","1",""},
        {"array_position('{\"192.0.2.0/24\"}'::cidr[],'192.0.2.0/24'::inet)","1",""},
        {"array_position('{\"08:00:2b:01:02:03\"}'::macaddr[],'08-00-2b-01-02-03'::macaddr)","1",""},
        {"array_position('{\"1.00\",\"2\"}'::jsonb[],'1'::jsonb)","1",""},
        {"array_position('{\"[1,2]\",\"[2,1]\"}'::jsonb[],'[1.0,2.00]'::jsonb)","1",""},
        {"array_position('{\"[1,2]\",\"[2,1]\"}'::jsonb[],'[2,1]'::jsonb)","2",""},
        {"array_position('{\"{\\\"a\\\":1,\\\"b\\\":2}\"}'::jsonb[],'{\"b\":2.00,\"a\":1.0}'::jsonb)","1",""},
        {"array_position('{\"[1.00,2.00)\",\"[2,3)\"}'::numrange[],'[1,2)'::numrange)","1",""},
        {"array_position('{\"[1,3]\",\"[4,8)\"}'::int4range[],'[1,4)'::int4range)","1",""},
        {"array_position('{\"a:1\",\"a:2\"}'::tsvector[],'a:2'::tsvector)","2",""},
        {"array_position('[-3:-1]={\"1.00\",NULL,\"1.0\"}'::numeric[],1::numeric)","-3",""},
        {"array_position('[-3:-1]={\"1.00\",NULL,\"1.0\"}'::numeric[],1::numeric,-2)","-1",""},
        {"array_position('[-3:-1]={\"1.00\",NULL,\"1.0\"}'::numeric[],NULL::numeric)","-2",""},
        {"array_position('[0:2]={\"1.00\",NULL,\"1.0\"}'::numeric[],1::numeric)","0",""},
        {"array_position('[0:2]={\"1.00\",NULL,\"1.0\"}'::numeric[],1::numeric,1)","2",""},
        {"array_position('[0:2]={\"1.00\",NULL,\"1.0\"}'::numeric[],NULL::numeric)","1",""},
        {"array_position('[7:9]={\"1.00\",NULL,\"1.0\"}'::numeric[],1::numeric)","7",""},
        {"array_position('[7:9]={\"1.00\",NULL,\"1.0\"}'::numeric[],1::numeric,8)","9",""},
        {"array_position('[7:9]={\"1.00\",NULL,\"1.0\"}'::numeric[],NULL::numeric)","8",""},
        {"array_position(NULL::numeric[],1::numeric)",std::nullopt,""},
        {"array_position('{}'::numeric[],NULL::numeric)",std::nullopt,""},
        {"array_position('{}'::integer[],1,NULL::integer)",std::nullopt,""},
        {"array_position('{1,2}'::integer[],NULL,NULL::integer)",std::nullopt,""},
        {"array_position('{1,NULL}'::integer[],NULL,NULL::integer)",std::nullopt,"22004"},
        {"array_position(ARRAY[ARRAY[1,2]],1)",std::nullopt,"0A000"},
        {"array_position(NULL,NULL)",std::nullopt,""},
        {"array_position('{}',NULL)",std::nullopt,""},
        {"array_position('{1.00}'::numeric[],'1')","1",""},
        {"array_position('{\"(0,0)\"}'::point[],'(0,0)'::point)",std::nullopt,"42883"},
        {"array_position('{\"(0,0)\"}'::point[],'(0,0)'::point,2)",std::nullopt,"42883"},
        {"array_position('{\"(0,0)\"}'::point[],NULL::point)",std::nullopt,""},
        {"array_position('{\"(0,0)\"}'::point[],'(0,0)'::point,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"(0,0)\"}'::point[],NULL::point,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::point[],NULL::point)",std::nullopt,"42883"},
        {"array_position('{}'::point[],'(0,0)'::point)",std::nullopt,""},
        {"array_position('{\"{1,2,3}\"}'::line[],'{1,2,3}'::line)",std::nullopt,"42883"},
        {"array_position('{\"{1,2,3}\"}'::line[],'{1,2,3}'::line,2)",std::nullopt,"42883"},
        {"array_position('{\"{1,2,3}\"}'::line[],NULL::line)",std::nullopt,""},
        {"array_position('{\"{1,2,3}\"}'::line[],'{1,2,3}'::line,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"{1,2,3}\"}'::line[],NULL::line,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::line[],NULL::line)",std::nullopt,"42883"},
        {"array_position('{}'::line[],'{1,2,3}'::line)",std::nullopt,""},
        {"array_position('{\"[(0,0),(1,1)]\"}'::lseg[],'[(0,0),(1,1)]'::lseg)",std::nullopt,"42883"},
        {"array_position('{\"[(0,0),(1,1)]\"}'::lseg[],'[(0,0),(1,1)]'::lseg,2)",std::nullopt,"42883"},
        {"array_position('{\"[(0,0),(1,1)]\"}'::lseg[],NULL::lseg)",std::nullopt,""},
        {"array_position('{\"[(0,0),(1,1)]\"}'::lseg[],'[(0,0),(1,1)]'::lseg,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"[(0,0),(1,1)]\"}'::lseg[],NULL::lseg,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::lseg[],NULL::lseg)",std::nullopt,"42883"},
        {"array_position('{}'::lseg[],'[(0,0),(1,1)]'::lseg)",std::nullopt,""},
        {"array_position('{\"(0,0),(1,1)\"}'::box[],'(0,0),(1,1)'::box)",std::nullopt,"42883"},
        {"array_position('{\"(0,0),(1,1)\"}'::box[],'(0,0),(1,1)'::box,2)",std::nullopt,"42883"},
        {"array_position('{\"(0,0),(1,1)\"}'::box[],NULL::box)",std::nullopt,""},
        {"array_position('{\"(0,0),(1,1)\"}'::box[],'(0,0),(1,1)'::box,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"(0,0),(1,1)\"}'::box[],NULL::box,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::box[],NULL::box)",std::nullopt,"42883"},
        {"array_position('{}'::box[],'(0,0),(1,1)'::box)",std::nullopt,""},
        {"array_position('{\"[(0,0),(1,1)]\"}'::path[],'[(0,0),(1,1)]'::path)",std::nullopt,"42883"},
        {"array_position('{\"[(0,0),(1,1)]\"}'::path[],'[(0,0),(1,1)]'::path,2)",std::nullopt,"42883"},
        {"array_position('{\"[(0,0),(1,1)]\"}'::path[],NULL::path)",std::nullopt,""},
        {"array_position('{\"[(0,0),(1,1)]\"}'::path[],'[(0,0),(1,1)]'::path,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"[(0,0),(1,1)]\"}'::path[],NULL::path,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::path[],NULL::path)",std::nullopt,"42883"},
        {"array_position('{}'::path[],'[(0,0),(1,1)]'::path)",std::nullopt,""},
        {"array_position('{\"((0,0),(1,1),(0,2))\"}'::polygon[],'((0,0),(1,1),(0,2))'::polygon)",std::nullopt,"42883"},
        {"array_position('{\"((0,0),(1,1),(0,2))\"}'::polygon[],'((0,0),(1,1),(0,2))'::polygon,2)",std::nullopt,"42883"},
        {"array_position('{\"((0,0),(1,1),(0,2))\"}'::polygon[],NULL::polygon)",std::nullopt,""},
        {"array_position('{\"((0,0),(1,1),(0,2))\"}'::polygon[],'((0,0),(1,1),(0,2))'::polygon,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"((0,0),(1,1),(0,2))\"}'::polygon[],NULL::polygon,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::polygon[],NULL::polygon)",std::nullopt,"42883"},
        {"array_position('{}'::polygon[],'((0,0),(1,1),(0,2))'::polygon)",std::nullopt,""},
        {"array_position('{\"<(0,0),1>\"}'::circle[],'<(0,0),1>'::circle)",std::nullopt,"42883"},
        {"array_position('{\"<(0,0),1>\"}'::circle[],'<(0,0),1>'::circle,2)",std::nullopt,"42883"},
        {"array_position('{\"<(0,0),1>\"}'::circle[],NULL::circle)",std::nullopt,""},
        {"array_position('{\"<(0,0),1>\"}'::circle[],'<(0,0),1>'::circle,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"<(0,0),1>\"}'::circle[],NULL::circle,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::circle[],NULL::circle)",std::nullopt,"42883"},
        {"array_position('{}'::circle[],'<(0,0),1>'::circle)",std::nullopt,""},
        {"array_position('{\"1\"}'::json[],'1'::json)",std::nullopt,"42883"},
        {"array_position('{\"1\"}'::json[],'1'::json,2)",std::nullopt,"42883"},
        {"array_position('{\"1\"}'::json[],NULL::json)",std::nullopt,""},
        {"array_position('{\"1\"}'::json[],'1'::json,NULL::integer)",std::nullopt,"22004"},
        {"array_position('{\"1\"}'::json[],NULL::json,NULL::integer)",std::nullopt,""},
        {"array_position('{NULL}'::json[],NULL::json)",std::nullopt,"42883"},
        {"array_position('{}'::json[],'1'::json)",std::nullopt,""},
        {"array_position()",std::nullopt,"42883"},
        {"array_position(ARRAY[1])",std::nullopt,"42883"},
        {"array_position(ARRAY[1],1,1,1)",std::nullopt,"42883"},
        {"array_position(1,1)",std::nullopt,"42883"},
        {"array_position('{}'::text,1)",std::nullopt,"42883"},
        {"array_position('{1}'::integer[],'1'::text)",std::nullopt,"42883"},
        {"array_position('{1}'::integer[],1,'1'::text)",std::nullopt,"42883"},
        {"array_position('{1}'::integer[],1,1::bigint)",std::nullopt,"42883"},
        {"array_position('{1}'::integer[],1,1.0::numeric)",std::nullopt,"42883"},
        {"array_position('{true}'::boolean[],'true'::text)",std::nullopt,"42883"},
    };
    for(const auto& control:cases)for(const bool prepared:{false,true}) {
        const std::string label=std::string(prepared?"prepared ":"parsed ")+control.expression;
        try {
            ExprValue actual;bool descriptor=true;
            if(prepared) {
                auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
                descriptor=query->output.size()==1 && query->output[0].typeOid==23 && query->output[0].type=="integer";
                auto* select=dynamic_cast<SelectStmt*>(query->ast.get());if(!select)return 2;
                PreparedQueryExecution execution(query,&g_engine,database);
                auto* expression=select->selectList.front().expr.get();
                execution.prepareExpression(expression);
                actual=execution.evaluate(expression,execution.context());
            } else {
                SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+control.expression);
                auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
                if(!select)return 2;
                auto* expression=select->selectList.front().expr.get();
                ExprHelper::prepareArrayTypes(expression,{},database,&g_engine);
                ExprEvaluator evaluator;evaluator.setCurrentDB(database);
                evaluator.bindScalarFunctions(expression,&g_engine);
                actual=evaluator.eval(expression,RowContext{});
            }
            check(control.state.empty() && descriptor && actual.typeName=="integer" &&
                actual.isNull==!control.value && (!control.value || actual.value==*control.value),label);
        } catch(const std::exception& error) {
            check(!control.state.empty() && state(error)==control.state,label+" SQLSTATE="+state(error)+" "+error.what());
        }
    }
    const std::vector<std::string> invalidSignatures={
        "array_position()",
        "array_position(ARRAY[1])",
        "array_position(ARRAY[1],1,1,1)",
        "array_position(1,1)",
        "array_position('{}'::text,1)",
        "array_position('{1}'::integer[],'1'::text)",
        "array_position('{1}'::integer[],1,'1'::text)",
        "array_position('{1}'::integer[],1,1::bigint)",
        "array_position('{1}'::integer[],1,1.0::numeric)",
        "array_position('{true}'::boolean[],'true'::text)",
    };
    // Function declaration errors are separate from execution-time missing
    // element equality. Every invalid signature must reject an empty source.
    for(const auto& expression:invalidSignatures)for(const auto* suffix:{" WHERE false"," LIMIT 0"}) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,"SELECT "+expression+" FROM empty_rows"+suffix);}
        catch(const std::exception& error){actual=state(error);}
        check(actual=="42883","empty declaration "+expression+suffix+" SQLSTATE="+actual);
    }
    const std::vector<std::vector<ExprValue>> cells={
        {{"numeric[]","{1.00,2.00,1.0}"},{"numeric","1"}},
        {{"numeric[]","[-3:-1]={1.00,NULL,1.0}"},{"numeric","1"},{"integer","-2"}},
        {{"real[]","{-0,1}"},{"real","0"}},
        {{"integer[]","{1,2}"},{"numeric","1.0"}},
        {{"character[]","{\"a  \",b}"},{"character","a"}},
        {{"interval[]","{\"1 mon\",\"2 mons\"}"},{"interval","30 days"}},
        {{"numeric[]","{}"},{"numeric","1"},{"text","bad"}},
        {{"numeric[]","{}"},{"numeric","1"},{"integer","",true}},
        {{"numeric[]","",true},{"numeric","1"},{"text","bad"}},
    };
    const std::vector<std::optional<std::string>> expected={"1","-1","1","1","1","1",std::nullopt,std::nullopt,std::nullopt};
    for(size_t i=0;i<cells.size();++i) {
        auto call=cellCall(cells[i]);RowContext row;row.setParameters(cells[i]);
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);
        try {
            const auto result=evaluator.eval(&call,row);
            check(i!=6 && result.typeName=="integer" && result.isNull==!expected[i] &&
                (!expected[i] || result.value==*expected[i]),"native cells "+std::to_string(i));
        } catch(const std::exception& error) {
            check(i==6 && state(error)=="22P02","native cells "+std::to_string(i)+" "+error.what());
        }
    }
    for(const std::string& context:{database,std::string{}})for(const char volatility:{'v','i'})
        for(const auto& input:std::vector<std::vector<ExprValue>>{cells[0],cells[6],cells[8],{},
                {{"text","{a,b}"},{"text","b"}}}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            size_t calls=0;bool same=true;
            evaluator.registerFunction("array_position",[&](const std::vector<ExprValue>& actual){
                ++calls;same=actual.size()==input.size();
                for(size_t i=0;i<actual.size() && i<input.size();++i)
                    same=same && actual[i].typeName==input[i].typeName &&
                        actual[i].value==input[i].value && actual[i].isNull==input[i].isNull;
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
                check(pure && calls==1 && same && result.typeName=="text" && result.value=="owned","opaque callback");
            }catch(const std::exception& error){check(false,std::string("opaque callback ")+error.what());}
        }
    for(const bool unused:{false,true}) {
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);size_t calls=0;
        evaluator.registerFunction("array_eq_needle",[&](const std::vector<ExprValue>&){++calls;return ExprValue("numeric","1");});
        const std::string expression="array_position('{1.00}'::numeric[],array_eq_needle())";
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+(unused?"COALESCE(7,"+expression+")":expression));
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;if(!select)return 2;
        auto* node=select->selectList.front().expr.get();
        try {
            evaluator.bindScalarFunctions(node,&g_engine);const bool pure=calls==0;
            const auto result=evaluator.eval(node,RowContext{});
            check(pure && calls==(unused?0:1) && !result.isNull && result.typeName=="integer" &&
                result.value==(unused?"7":"1"),"argument demand");
        }catch(const std::exception& error){check(false,std::string("argument demand ")+error.what());}
    }
    for(const auto& control:std::vector<Control>{
            {"array_position('{}',NULL)",std::nullopt,""},
            {"array_position('{1.00}'::numeric[],'1')","1",""},
            {"array_position(NULL::numeric[],'1')",std::nullopt,""},
            {"array_position('{1}'::integer[],'bad')",std::nullopt,"22P02"},
            {"array_position('{}'::integer[],1,'1')",std::nullopt,""},
            {"array_position('{1}'::integer[],1,'1')","1",""}}) {
        try {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
            auto* select=dynamic_cast<SelectStmt*>(query->ast.get());if(!select)return 2;
            auto* node=select->selectList.front().expr.get();
            PreparedQueryExecution execution(query,&g_engine,database);
            execution.planExpressionConstants(node);
            execution.prepareExpression(node);
            const auto result=execution.evaluate(node,execution.context());
            check(control.state.empty() && result.typeName=="integer" && result.isNull==!control.value &&
                (!control.value || result.value==*control.value),"planning UNKNOWN "+control.expression);
        }catch(const std::exception& error){check(!control.state.empty() && state(error)==control.state,
            "planning UNKNOWN "+control.expression+" "+error.what());}
    }
    if(ddl.executeSql("CREATE SCHEMA array_eq_user",session) ||
       ddl.executeSql("CREATE FUNCTION array_eq_user.array_position(p INTEGER[],q INTEGER) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$BEGIN RETURN 'user-owned';END;$$",session))return 2;
    session.searchPath="array_eq_user,pg_catalog,public";
    for(const auto& control:std::vector<Control>{
            {"array_position('{1}'::integer[],1)","user-owned","text"},
            {"pg_catalog.array_position('{1}'::integer[],1)","1","integer"},
            {"array_position('{1.00}'::numeric[],1::numeric)","1","integer"},
            {"array_position('{1}',1)","user-owned","text"},
            {"array_position(NULL::integer[],1)","user-owned","text"},
            {"COALESCE('ok'::text,array_position('{1}'::integer[],1))","ok","text"}})
        for(const bool plan:{false,true}) {
            try {
                auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
                auto* select=dynamic_cast<SelectStmt*>(query->ast.get());if(!select)return 2;
                auto* node=select->selectList.front().expr.get();
                PreparedQueryExecution execution(query,&g_engine,database);
                if(plan)execution.planExpressionConstants(node);
                execution.prepareExpression(node);
                const auto result=execution.evaluate(node,execution.context());
                check(query->output.size()==1 && query->output[0].type==control.state &&
                    !result.isNull && result.typeName==control.state && result.value==*control.value,
                    "user ownership "+control.expression+(plan?" planned":"")+
                    " descriptor="+(query->output.empty()?"<none>":query->output[0].type)+
                    " datum="+result.typeName+" value="+result.value+" null="+std::to_string(result.isNull));
            }catch(const std::exception& error){check(false,"user ownership "+control.expression+" "+error.what());}
        }
    for(const auto* suffix:{""," FROM empty_rows WHERE false"," FROM empty_rows LIMIT 0"}) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,std::string("SELECT array_position(NULL,NULL)")+suffix);}
        catch(const std::exception& error){actual=state(error);}
        check(actual=="42725","unknown user overload ambiguity "+std::string(suffix));
    }
    std::string overState;
    try{(void)g_engine.prepareBoundQuery(database,"SELECT array_position('{1}'::integer[],1) OVER()");}
    catch(const std::exception& error){overState=state(error);}
    check(overState=="42809","user overload OVER role");
    // evaluate() exposes an expression datum, not the final projection's
    // declared-type conversion. Calibrate this without an array/routine.
    struct DatumControl {std::string expression,descriptor,datum;};
    for(const auto& control:std::vector<DatumControl>{
            {"'ok'","unknown","character varying"},{"'ok'::text","text","text"}}) {
        try {
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.expression));
            auto* select=dynamic_cast<SelectStmt*>(query->ast.get());if(!select)return 2;
            auto* node=select->selectList.front().expr.get();
            PreparedQueryExecution execution(query,&g_engine,database);
            execution.prepareExpression(node);
            const auto result=execution.evaluate(node,execution.context());
            check(query->output.size()==1 && query->output[0].type==control.descriptor &&
                !result.isNull && result.typeName==control.datum && result.value=="ok",
                "raw datum vs query descriptor "+control.expression+
                " descriptor="+(query->output.empty()?"<none>":query->output[0].type)+
                " datum="+result.typeName+" value="+result.value+" null="+std::to_string(result.isNull));
        }catch(const std::exception& error){check(false,std::string("raw datum calibration ")+error.what());}
    }
    for(const auto& control:std::vector<std::vector<std::string>>{
            {"{a,b,c}","c","3"},{"{01,1}","1","2"},{"{NULL,\"NULL\"}","'NULL'","2"}})
        for(const bool bind:{false,true}) {
            FunctionCallExpr call;call.funcName="array_position";
            for(const auto& value:{control[0],control[1]}) {
                auto literal=std::make_unique<LiteralExpr>();literal->typeName="text";literal->value=value;
                call.args.push_back(std::move(literal));
            }
            ExprEvaluator evaluator;
            try {
                if(bind)evaluator.bindScalarFunctions(&call,&g_engine);
                const auto result=evaluator.eval(&call,RowContext{});
                check(!result.isNull && result.typeName=="integer" && result.value==control[2],
                    "legacy native TEXT array "+control[0]+(bind?" bound":""));
            }catch(const std::exception& error){check(false,std::string("legacy native TEXT array ")+error.what());}
        }
    check(controls==353,"complete typed/metadata/native-ownership/planning matrix");
    std::cout<<"ARRAY_POSITION_TYPED_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(fixture);return failures?1:0;
}
