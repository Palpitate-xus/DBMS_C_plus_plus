#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
struct Checks {
    size_t controls=0,failures=0;
    template<class F> void test(const std::string& label,F action) {
        ++controls;
        try {action();}
        catch(const std::exception& error) {
            ++failures;std::cerr<<"ANGLE_CONVERSION_RANGE_FAIL "<<controls<<' '<<label<<" : "<<error.what()<<'\n';
        }
        catch(...) {++failures;std::cerr<<"ANGLE_CONVERSION_RANGE_FAIL "<<controls<<' '<<label<<" : nonstandard exception\n";}
    }
};
void require(bool good,const std::string& message) {if(!good)throw std::runtime_error(message);}
std::string sqlState(const std::string& message) {
    const auto position=message.find("SQLSTATE ");
    return position==std::string::npos?std::string{}:message.substr(position+9,5);
}
std::string sqlState(const std::exception& error) {
    if(const auto* typed=dynamic_cast<const DbError*>(&error))return typed->sqlState();
    return sqlState(std::string(error.what()));
}
double floating(const std::string& text) {
    // strtod may set ERANGE for an accepted, nonzero subnormal. stod throws
    // for those values, so it cannot serve as this fixture's output decoder.
    char* end=nullptr;const double result=std::strtod(text.c_str(),&end);
    require(end && end!=text.c_str() && *end=='\0',"not a complete float8 result: "+text);
    return result;
}
uint64_t bits(double value) {uint64_t result=0;std::memcpy(&result,&value,sizeof(result));return result;}
struct Expected {bool isNull=false;double value=0;std::string state;};
Expected expected(const std::string& function,const std::optional<std::string>& input) {
    if(!input)return {true,0,{}};
    const double value=floating(*input);
    // Independent PG 18.6 float.h constant and float.c operations. Store the
    // one operation as binary64; no long-double or algebraic reassociation.
    constexpr double radiansPerDegree=0.0174532925199432957692;
    volatile double rounded=function=="degrees"?value/radiansPerDegree:value*radiansPerDegree;
    const double result=rounded;
    if(std::isinf(result) && !std::isinf(value))return {false,0,"22003"};
    if(result==0 && value!=0)return {false,0,"22003"};
    return {false,result,{}};
}
void valueMatches(const ExprValue& actual,const Expected& wanted) {
    require(actual.typeName=="double precision","wrong result type: "+actual.typeName);
    require(actual.isNull==wanted.isNull,"wrong SQL NULL state");
    if(wanted.isNull)return;
    const double parsed=floating(actual.value);
    if(std::isnan(wanted.value))require(std::isnan(parsed),"expected NaN, got "+actual.value);
    else require(bits(parsed)==bits(wanted.value),"different binary64 result: "+actual.value+
        " actual-bits="+std::to_string(bits(parsed))+" expected-bits="+std::to_string(bits(wanted.value)));
}
template<class F> void outcome(const Expected& wanted,F evaluate) {
    ExprValue actual;std::string state;
    try {actual=evaluate();}
    catch(const std::exception& error) {state=sqlState(error);if(state.empty())throw;}
    require(state==wanted.state,"expected SQLSTATE "+wanted.state+", got "+state);
    if(state.empty())valueMatches(actual,wanted);
}
ExprValue helperValue(const ExprEvalResult& result) {
    if(!result.ok) {
        const auto state=result.sqlState.empty()?sqlState(result.error):result.sqlState;
        if(state.empty())throw std::runtime_error(result.error);
        throw DbError(state,result.error);
    }
    ExprValue value(result.typeName,result.value,result.isNull);value.collation=result.collation;return value;
}
std::string literal(const std::optional<std::string>& input,const std::string& type="double precision") {
    return input?"'"+*input+"'::"+type:"NULL::"+type;
}
FunctionCallExpr call(const std::string& function,const std::vector<ExprValue>& cells) {
    FunctionCallExpr expression;expression.funcName=function;
    for(size_t i=0;i<cells.size();++i) {
        auto parameter=std::make_unique<ParameterExpr>();parameter->slot=i;parameter->declaredType=cells[i].typeName;
        expression.args.push_back(std::move(parameter));
    }
    return expression;
}
bool same(const std::vector<ExprValue>& actual,const std::vector<ExprValue>& wanted) {
    if(actual.size()!=wanted.size())return false;
    for(size_t i=0;i<actual.size();++i)
        if(actual[i].typeName!=wanted[i].typeName || actual[i].value!=wanted[i].value ||
           actual[i].isNull!=wanted[i].isNull || actual[i].collation!=wanted[i].collation ||
           actual[i].objectOid!=wanted[i].objectOid || actual[i].elementOids!=wanted[i].elementOids ||
           bool(actual[i].recordFields)!=bool(wanted[i].recordFields))return false;
    return true;
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();Checks checks;
    const std::string fixture="angle_conversion_range",database=testDbPath(fixture);
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    checks.test("binary64 oracle platform",[]{
        require(sizeof(double)==8 && std::numeric_limits<double>::is_iec559 &&
            std::numeric_limits<double>::digits==53 && std::numeric_limits<double>::has_denorm==std::denorm_present,
            "fixture requires IEEE binary64 with subnormals");
    });
    checks.test("create private metadata database",[&]{
        cleanupTestDb(fixture);require(g_engine.createDatabase(database)==DBStatus::OK,"createDatabase failed");
    });
    // The inventory is fixed, including errors and representable subnormal
    // results. Every input is exercised through every path; no early abort.
    const std::vector<std::optional<std::string>> inputs={
        "0","-0","1","-1","90","-90","180","-180",
        "3.141592653589793","-3.141592653589793",
        "1e306","-1e306","1e308","-1e308",
        "1.7976931348623157e308","-1.7976931348623157e308",
        "2.2250738585072014e-308","-2.2250738585072014e-308",
        "1e-310","-1e-310","1e-320","-1e-320",
        "4.9406564584124654e-324","-4.9406564584124654e-324",
        "2.4703282292062327e-322","-2.4703282292062327e-322",
        "NaN","Infinity","-Infinity",std::nullopt
    };
    for(const std::string function:{"degrees","radians"})for(const auto& input:inputs) {
        const auto wanted=expected(function,input);
        const auto label=function+"("+input.value_or("NULL")+")";
        const auto expression=function+"("+literal(input)+")";
        const ExprValue cell("double precision",input.value_or(""),!input);
        for(bool bind:{false,true})checks.test(label+(bind?" native bound cell":" native raw cell"),[&]{
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);auto node=call(function,{cell});RowContext row;row.setParameters({cell});
            if(bind) {
                require(ExprHelper::inferParsedInputType(&node,{},database,&g_engine,&evaluator)=="double precision",
                        "native input metadata lost builtin declaration");
                evaluator.bindScalarFunctions(&node,&g_engine);
            }
            outcome(wanted,[&]{return evaluator.eval(&node,row);});
        });
        checks.test(label+" parsed literal",[&]{
            auto parsed=SQLParser().parseForBinding("SELECT "+expression);
            require(parsed.success,"literal parse failed: "+parsed.error);
            auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());require(select && select->selectList.size()==1,"missing parsed target");
            auto* node=select->selectList.front().expr.get();ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            require(ExprHelper::inferParsedResultType(node,{},database,&g_engine,&evaluator)=="double precision",
                    "literal metadata changed float8 declaration");
            evaluator.bindScalarFunctions(node,&g_engine);
            outcome(wanted,[&]{return evaluator.eval(node,RowContext{});});
        });
        checks.test(label+" scalar helper literal",[&]{
            require(ExprHelper::inferResultType(expression,{},database,&g_engine)=="double precision","helper type discovery changed declaration");
            outcome(wanted,[&]{return helperValue(ExprHelper::evalString(expression,{},{},database,"",&g_engine));});
        });
        checks.test(label+" scalar helper row",[&]{
            const std::set<std::string> nulls=input?std::set<std::string>{}:std::set<std::string>{"x"};
            require(ExprHelper::inferResultType(function+"(x)",{{"x","double precision"}},database,&g_engine)=="double precision",
                    "helper row type discovery changed declaration");
            outcome(wanted,[&]{return helperValue(ExprHelper::evalStringWithNulls(function+"(x)",{{"x",input.value_or("")}},
                nulls,{{"x","double precision"}},database,"",&g_engine));});
        });
        checks.test(label+" bound descriptor is pure",[&]{
            QueryBindingDatum datum={"angle-range-input","p","double precision",{},false,input,1};
            // Binding/describing the expression must not evaluate this datum,
            // including genuine overflow/underflow cases. No SQL is executed
            // to discover the result type, and no planner timing is changed.
            const auto prepared=g_engine.prepareBoundQuery(database,"SELECT pg_catalog."+function+"($1) LIMIT 0",{datum});
            require(prepared.output.size()==1 && prepared.output.front().type=="double precision" &&
                    prepared.output.front().typeOid==701 && prepared.parameters.size()==1,
                    "wrong bound float8 descriptor/parameter frame");
        });
    }
    for(const std::string function:{"degrees","radians"})
        for(const std::string type:{"smallint","integer","bigint","real","numeric","double precision"}) {
            const Expected wanted{true,0,{}};
            checks.test(function+" typed NULL cell "+type,[&]{
                ExprValue cell(type,"",true);auto node=call(function,{cell});RowContext row;row.setParameters({cell});
                ExprEvaluator evaluator;evaluator.setCurrentDB(database);evaluator.bindScalarFunctions(&node,&g_engine);
                outcome(wanted,[&]{return evaluator.eval(&node,row);});
            });
            checks.test(function+" typed NULL helper "+type,[&]{
                const auto expression=function+"("+literal(std::nullopt,type)+")";
                require(ExprHelper::inferResultType(expression,{},database,&g_engine)=="double precision","typed NULL result declaration");
                outcome(wanted,[&]{return helperValue(ExprHelper::evalString(expression,{},{},database,"",&g_engine));});
            });
        }

    const std::vector<std::vector<ExprValue>> opaqueInputs={
        {},{{"integer","7"},{"text","payload"}},{{"text","not a float"}},{{"numeric","",true}}
    };
    for(const std::string function:{"degrees","radians"})for(const std::string context:{std::string{},database})
        for(char volatility:{'v','i'})for(const std::string hint:{std::string{},std::string("text")})
            for(const auto& supplied:opaqueInputs) {
                ExprEvaluator evaluator;evaluator.setCurrentDB(context);size_t calls=0;std::vector<ExprValue> received;
                const bool null=!supplied.empty() && supplied.front().isNull;
                const ExprValue returned("text",null?"":"owned angle callback",null);
                const ScalarFunction callback=[&](const std::vector<ExprValue>& actual){++calls;received=actual;return returned;};
                if(volatility=='v')evaluator.registerFunction(function,callback);
                else evaluator.registerFunction(function,callback,volatility);
                auto node=call(function,supplied);node.resolvedResultType=hint;RowContext row;row.setParameters(supplied);
                const auto label=function+" opaque context="+context+" volatility="+volatility+" hint="+hint+
                    " argc="+std::to_string(supplied.size());
                const auto inputType=hint.empty()?"unknown":hint;
                checks.test(label+" metadata ownership",[&]{
                    require(evaluator.hasScalarFunction(&node,&g_engine) && evaluator.scalarFunctionResultType(&node,&g_engine).empty() &&
                        evaluator.scalarFunctionIdentity(&node,&g_engine).rfind("native",0)==0 &&
                        evaluator.scalarFunctionVolatility(&node,&g_engine)==volatility,"replacement inherited builtin signature/identity");
                    require(ExprHelper::inferParsedInputType(&node,{},context,&g_engine,&evaluator)==inputType &&
                        ExprHelper::inferParsedResultType(&node,{},context,&g_engine,&evaluator)=="text" && calls==0,
                        "replacement metadata evaluated callback or acquired float8 result");
                });
                checks.test(label+" binding ownership",[&]{
                    evaluator.bindScalarFunctions(&node,&g_engine);
                    require(node.resolvedResultType==hint && calls==0 &&
                        ExprHelper::inferParsedInputType(&node,{},context,&g_engine,&evaluator)==inputType,
                        "binding coerced opaque arguments or invented return declaration");
                });
                checks.test(label+" actual invocation",[&]{
                    const auto actual=evaluator.eval(&node,row);
                    require(actual.typeName==returned.typeName && actual.value==returned.value && actual.isNull==returned.isNull &&
                        calls==1 && same(received,supplied),"replacement lost argument bytes/types/NULL or invocation ownership");
                });
            }
    for(const std::string function:{"degrees","radians"})
        for(const std::string arguments:{"","1,2","'payload'::text","true","NULL::text","ARRAY[1]"})
            for(const std::string role:{""," OVER()"})checks.test(function+" builtin signature "+arguments+role,[&]{
                std::string observed;
                try {(void)g_engine.prepareBoundQuery(database,"SELECT "+function+"("+arguments+")"+role+" LIMIT 0");}
                catch(const std::exception& error){observed=sqlState(error);}
                require(observed=="42883","genuine builtin no longer owns its float8 signature: "+observed);
            });
    checks.test("drop only private fixture database",[&]{require(g_engine.dropDatabase(database)==DBStatus::OK,"dropDatabase failed");});
    // 1 platform + 1 setup + 2 functions * 30 cells * 6 paths + 2 functions
    // * 6 typed NULLs * 2 paths + 2 functions * 2 contexts * 2 volatilities
    // * 2 hints * 4 opaque argument sets * 3 ownership checks + 2 functions
    // * 6 invalid signatures * 2 roles + 1 cleanup = 603, with no skips.
    constexpr size_t expectedControls=603;
    std::cout<<"ANGLE_CONVERSION_RANGE_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expectedControls
             <<" FAILURES "<<checks.failures<<'\n';
    return checks.controls==expectedControls && checks.failures==0?0:1;
}
