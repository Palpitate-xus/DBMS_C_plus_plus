#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label){
        ++controls;
        if(!good){++failures;std::cerr<<"INTERVAL_JUSTIFY_CALLBACK_FAILURE "<<label<<'\n';}
    };
    const auto expression=[](SQLParser& parser,const std::string& sql){
        auto parsed=parser.parseForBinding("SELECT "+sql);
        if(!parsed.success || !dynamic_cast<SelectStmt*>(parsed.stmt.get()))
            throw std::runtime_error("fixture parse failed: "+sql);
        return parsed;
    };
    for (const auto* function : {"justify_hours","justify_days","justify_interval"})
      for (const auto& spelling : std::vector<std::string>{function, std::string("\"")+function+"\"", std::string("pg_catalog.")+function})
       for (bool null : {false,true}) {
        SQLParser parser;auto parsed=expression(parser,spelling+(null?"(NULL::interval)":"(INTERVAL '1 day -1 hour')"));
        auto* call=dynamic_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList.front().expr.get());if(!call)return 2;
        ExprEvaluator evaluator;
        try {
          const bool metadata=evaluator.scalarFunctionResultType(call)=="interval" && evaluator.scalarFunctionIdentity(call).rfind("builtin",0)==0 && ExprHelper::inferParsedInputType(call,{},"",nullptr,&evaluator)=="interval";
          evaluator.bindScalarFunctions(call);const auto value=evaluator.eval(call,{});
          const std::string want=std::string(function)=="justify_days"?"1 day -01:00:00":"23:00:00";
          check(metadata && value.typeName=="interval" && value.isNull==null && (null || value.value==want),spelling+" builtin");
        }catch(const std::exception& error){check(false,spelling+" "+error.what());}
       }
    for(const auto volatility:{'v','i'})
        for(const auto* function:{"justify_hours","justify_days","justify_interval"})
          for(const auto& name:std::vector<std::string>{function,std::string("\"")+function+"\"",std::string("pg_catalog.")+function})
            for(const auto* args:{"'x'","'x','y'"})
                for(const auto& expected:std::vector<ExprValue>{{"text","opaque"},{"integer","7"},{"bytea","\\x00ff"}}) {
                    SQLParser parser;auto parsed=expression(parser,std::string(name)+"("+args+")");
                    auto* call=dynamic_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList.front().expr.get());
                    if(!call)return 2;
                    ExprEvaluator evaluator;int calls=0;
                    const size_t arity=call->args.size();
                    evaluator.registerFunction(function,[&](const std::vector<ExprValue>& actual){
                        ++calls;if(actual.size()!=arity)throw std::runtime_error("native callback arity changed");return expected;
                    },volatility);
                    try {
                        const bool metadata=evaluator.hasScalarFunction(call) && evaluator.scalarFunctionResultType(call).empty() &&
                            evaluator.scalarFunctionIdentity(call).rfind("native",0)==0 && evaluator.scalarFunctionVolatility(call)==volatility &&
                            ExprHelper::inferParsedInputType(call,{},"",nullptr,&evaluator)=="unknown" && calls==0;
                        evaluator.bindScalarFunctions(call);
                        const bool pure=calls==0;
                        const auto actual=evaluator.eval(call,{});
                        check(metadata && pure && calls==1 && !actual.isNull && actual.typeName==expected.typeName && actual.value==expected.value,
                              std::string(name)+"("+args+") "+expected.typeName);
                    }catch(const std::exception& error){check(false,std::string(name)+" "+expected.typeName+" "+error.what());}
                }
    if(controls!=126){++failures;std::cerr<<"INTERVAL_JUSTIFY_CALLBACK_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"INTERVAL_JUSTIFY_CALLBACK_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
