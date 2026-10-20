#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
struct Case {std::optional<std::string> input,value;std::string state;};
const std::vector<Case> cases={
    {"\\xq0",{},"22023"},{"\\x0",{},"22023"},{"\\x0 0",{},"22023"},{"\\x00 g0",{},"22023"},
    {"\\x00 1",{},"22023"},{"\\xGG",{},"22023"},{"\\x0\t0",{},"22023"},{"\\x   0",{},"22023"},
    {"\\",{},"22P02"},{"\\q0",{},"22P02"},{"\\400",{},"22P02"},{"\\08a",{},"22P02"},{"\\00",{},"22P02"},
    {{},{},""},{"","\\x",""},{"abc","\\x616263",""},{"\\x","\\x",""},
    {"\\x00AFff","\\x00afff",""},{"\\x00 AF ff","\\x00afff",""},{"\\x00\tAF\nff\r","\\x00afff",""},
    {"\\000\\377","\\x00ff",""},{"\\\\","\\x5c",""},{"NULL","\\x4e554c4c",""}
};
void require(bool good,const std::string& label){if(!good)throw std::runtime_error(label);}
std::string literal(const std::optional<std::string>& value){if(!value)return "NULL";std::string result="'";for(char c:*value){result+=c;if(c=='\'')result+=c;}return result+"'";}
void outcome(const Case& item,const std::function<ExprValue()>& action){
    ExprValue value;std::string state;try{value=action();}catch(const DbError& error){state=error.sqlState();}
    require(state==item.state,"expected "+item.state+", got "+state);
    if(state.empty()){require(value.typeName=="bytea" && value.isNull==!item.input,"wrong type/NULL bit");if(item.input)require(item.value && value.value==*item.value,"wrong BYTEA bytes");}
}
}
int main(){
    using namespace dbms;TypeRegistry::instance().bootstrap();size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action){++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"BYTEA_HEX_SQLSTATE_NATIVE_FAIL "<<label<<" : "<<error.what()<<'\n';}};
    for(const std::string source:{"unknown","text","varchar","name"})for(const auto& item:cases){
        const auto value=literal(item.input);const auto expression="CAST("+(source=="unknown"?value:value+"::"+source)+" AS bytea)";
        for(bool bind:{false,true})check(expression+(bind?" native bound":" native raw"),[&]{CastExpr cast;cast.typeName="bytea";auto parameter=std::make_unique<ParameterExpr>();parameter->slot=0;parameter->declaredType=source;cast.operand=std::move(parameter);ExprEvaluator evaluator;RowContext row;row.setParameters({ExprValue(source,item.input.value_or(""),!item.input)});if(bind)evaluator.bindScalarFunctions(&cast,&g_engine);outcome(item,[&]{return evaluator.eval(&cast,row);});});
        for(bool bind:{false,true})check(expression+(bind?" parsed bound":" parsed raw"),[&]{auto parsed=SQLParser().parseForBinding("SELECT "+expression);require(parsed.success,parsed.error);auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());require(select && select->selectList.size()==1,"missing target");auto* node=select->selectList.front().expr.get();ExprEvaluator evaluator;if(bind)evaluator.bindScalarFunctions(node,&g_engine);outcome(item,[&]{return evaluator.eval(node,{});});});
        check(expression+" helper",[&]{outcome(item,[&]{const auto result=ExprHelper::evalString(expression,{},{},"","",&g_engine);if(!result.ok)throw DbError(result.sqlState.empty()?"XX000":result.sqlState,result.error);return ExprValue(result.typeName,result.value,result.isNull);});});
    }
    for(const auto& pair:std::vector<std::pair<std::string,size_t>>{{"smallint",2},{"integer",4},{"bigint",8}})for(const std::string input:{"0","-1","1"})for(bool bind:{false,true})check("integer width "+pair.first+" "+input,[&]{
        CastExpr cast;cast.typeName="bytea";auto parameter=std::make_unique<ParameterExpr>();parameter->slot=0;parameter->declaredType=pair.first;cast.operand=std::move(parameter);ExprEvaluator evaluator;if(bind)evaluator.bindScalarFunctions(&cast,&g_engine);RowContext row;row.setParameters({ExprValue(pair.first,input)});const auto value=evaluator.eval(&cast,row);std::string expected="\\x";for(size_t i=0;i<pair.second;++i)expected+=input=="-1"?"ff":i+1==pair.second && input=="1"?"01":"00";require(!value.isNull && value.typeName=="bytea" && value.value==expected,"integer BYTEA conversion changed");
    });
    if(cases.size()!=23 || controls!=478){++failures;std::cerr<<"BYTEA_HEX_SQLSTATE_NATIVE_INVENTORY "<<controls<<'\n';}
    std::cout<<"BYTEA_HEX_SQLSTATE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}
