#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace dbms;
static int checks=0, failures=0;
static void check(bool pass,const std::string& label) {
    ++checks;
    if(!pass) {++failures;std::cerr<<"FAIL "<<label<<'\n';}
}
static ExprPtr column(size_t ordinal,const std::string& type) {
    auto value=std::make_unique<ColumnRefExpr>();value->column="c"+std::to_string(ordinal);
    QueryColumnBinding binding;binding.sourceOrdinal=0;binding.columnOrdinal=ordinal;
    binding.declaredType=type;value->binding=binding;return value;
}
static void expect(ExprEvaluator& evaluator,const std::string& name,
                   const std::vector<ExprValue>& inputs,const std::string& target,
                   const std::string& expected,bool null=false,const std::string& collation={}) {
    FunctionCallExpr call;call.funcName=name;call.resolvedResultType=target;RowContext row;
    for(size_t i=0;i<inputs.size();++i) {
        call.args.push_back(column(i,inputs[i].typeName));row.setBoundColumn(0,i,inputs[i]);
    }
    const std::string label=name+" -> "+target+"/"+expected;
    try {
        const auto actual=evaluator.eval(&call,row);
        check(ExprHelper::canonicalResultTypeName(actual.typeName)==ExprHelper::canonicalResultTypeName(target),label+" type="+actual.typeName);
        check(actual.isNull==null,label+" null");
        check(null || actual.value==expected,label+" value="+actual.value);
        if(!collation.empty())check(actual.collation==collation,label+" collation="+actual.collation);
    } catch(const std::exception& error) {
        check(false,label+" exception="+error.what());check(false,label+" null unavailable");check(false,label+" value unavailable");
        if(!collation.empty())check(false,label+" collation unavailable");
    }
}
int main() {
    TypeRegistry::instance().bootstrap();ExprEvaluator evaluator;
    for(const std::string name:{"coalesce","greatest","least"}) {
        expect(evaluator,name,{{"bigint","",true},{"integer","7"}},"bigint","7");
        expect(evaluator,name,{{"integer","7"},{"bigint","",true}},"bigint","7");
        expect(evaluator,name,{{"bigint","",true},{"integer","",true}},"bigint","",true);
        expect(evaluator,name,{{"smallint","2"},{"integer","",true}},"integer","2");
        expect(evaluator,name,{{"integer","7"},{"numeric","",true}},"numeric","7");
        expect(evaluator,name,{{"integer","16777217"},{"real","",true}},"real","1.6777216e+07");
        expect(evaluator,name,{{"integer","7"},{"double precision","",true}},"double precision","7");
        expect(evaluator,name,{{"date","2020-01-02"},{"timestamp","",true}},"timestamp","2020-01-02 00:00:00");
        expect(evaluator,name,{{"character varying","10"},{"text","",true}},"text","10");
        expect(evaluator,name,{{"integer[]","{1,NULL,3}"},{"bigint[]","",true}},"bigint[]","{1,NULL,3}");
        // An implicit unbounded CHAR or BIT conversion is not CHAR(1)/BIT(1).
        expect(evaluator,name,{{"text","abc"},{"bpchar","",true}},"bpchar","abc");
        expect(evaluator,name,{{"bit varying","1010"},{"bit","",true}},"bit","1010");
        ExprValue collated("character varying","value");collated.collation="c";
        expect(evaluator,name,{collated,{"text","",true}},"text","value",false,"c");
    }
    expect(evaluator,"greatest",{{"integer","16777217"},{"real","1.6777216e+07"}},"real","1.6777216e+07");
    expect(evaluator,"least",{{"integer","16777217"},{"real","1.6777216e+07"}},"real","1.6777216e+07");
    expect(evaluator,"greatest",{{"text","10"},{"text","2"}},"text","2");
    expect(evaluator,"least",{{"text","10"},{"text","2"}},"text","10");
    // Pure declaration must select the actual common character type before
    // the evaluator coerces a chosen cell. Do not infer it from row values.
    const std::vector<std::vector<std::string>> commonTypes={
        {"varchar","text","varchar"},{"bpchar","text","bpchar"},
        {"text","varchar","text"},{"text","bpchar","text"},
        {"varchar","bpchar","varchar"},{"bpchar","varchar","bpchar"},
        {"unknown","unknown","text"},{"integer","bigint","bigint"},
        {"date","timestamp","timestamp"},{"timestamp","date","timestamp"}};
    for(const std::string name:{"coalesce","greatest","least"})
        for(const auto& types:commonTypes) {
            FunctionCallExpr call;call.funcName=name;
            call.args.push_back(column(0,types[0]));call.args.push_back(column(1,types[1]));
            const auto actual=ExprHelper::inferParsedResultType(&call);
            check(ExprHelper::canonicalResultTypeName(actual)==ExprHelper::canonicalResultTypeName(types[2]),
                name+" pure common "+types[0]+"/"+types[1]+" expected="+types[2]+" actual="+actual);
        }
    // The native, unbound scalar API keeps its existing lazy contract.
    // A guessed result type is not a query-owned declaration to coerce to.
    {
        FunctionCallExpr call;call.funcName="coalesce";
        auto first=std::make_unique<LiteralExpr>();first->value="'first'";
        auto divide=std::make_unique<BinaryOpExpr>();divide->op="/";
        auto one=std::make_unique<LiteralExpr>();one->value="1";
        auto zero=std::make_unique<LiteralExpr>();zero->value="0";
        divide->left=std::move(one);divide->right=std::move(zero);
        call.args.push_back(std::move(first));call.args.push_back(std::move(divide));
        try {
            const auto value=evaluator.eval(&call,RowContext{});
            check(value.typeName=="character varying","unbound legacy type");
            check(!value.isNull,"unbound legacy NULL state");
            check(value.value=="first","unbound legacy selected arm");
        } catch(const std::exception& error) {
            check(false,std::string("unbound legacy exception=")+error.what());
            check(false,"unbound legacy NULL unavailable");check(false,"unbound legacy selected arm unavailable");
        }
    }
    for(const std::string name:{"coalesce","greatest","least"}) {
        FunctionCallExpr call;call.funcName=name;
        for(int i=0;i<2;++i) {auto input=std::make_unique<LiteralExpr>();input->value="NULL";call.args.push_back(std::move(input));}
        const auto value=evaluator.eval(&call,RowContext{});
        check(value.typeName=="unknown",name+" unbound NULL retains legacy carrier");
        check(value.isNull,name+" unbound NULL state");check(value.value.empty(),name+" unbound NULL bytes");
    }
    int calls=0;
    evaluator.registerFunction("r321_tick",[&](const auto&) {++calls;return ExprValue("bigint","8");},'v');
    for(const bool firstNull:{false,true}) {
        FunctionCallExpr call;call.funcName="coalesce";call.resolvedResultType="bigint";
        call.args.push_back(column(0,"integer"));
        auto tick=std::make_unique<FunctionCallExpr>();tick->funcName="r321_tick";tick->resolvedResultType="bigint";
        call.args.push_back(std::move(tick));RowContext row;row.setBoundColumn(0,0,{"integer","7",firstNull});
        calls=0;const auto result=evaluator.eval(&call,row);
        check(result.typeName=="bigint","lazy typed result");
        check(result.value==(firstNull?"8":"7"),"lazy selected value");
        check(calls==(firstNull?1:0),"unused argument must not execute");
    }
    for(const std::string name:{"greatest","least"}) {
        FunctionCallExpr call;call.funcName=name;call.resolvedResultType="bigint";
        call.args.push_back(column(0,"integer"));
        auto tick=std::make_unique<FunctionCallExpr>();tick->funcName="r321_tick";tick->resolvedResultType="bigint";
        call.args.push_back(std::move(tick));RowContext row;row.setBoundColumn(0,0,{"integer","7"});calls=0;
        const auto result=evaluator.eval(&call,row);
        check(result.typeName=="bigint","extremum typed result");
        check(result.value==(name=="greatest"?"8":"7"),"extremum selected value");
        check(calls==1,"extrema must evaluate every argument");
    }
    std::cout<<"SCALAR_COMMON_RESULT_TYPE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
