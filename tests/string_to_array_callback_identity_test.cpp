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
        if(!good){++failures;std::cerr<<"STRING_TO_ARRAY_CALLBACK_FAILURE "<<label<<'\n';}
    };
    const auto expression=[](SQLParser& parser,const std::string& sql){
        auto parsed=parser.parseForBinding("SELECT "+sql);
        if(!parsed.success || !dynamic_cast<SelectStmt*>(parsed.stmt.get()))
            throw std::runtime_error("fixture parse failed: "+sql);
        return parsed;
    };
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"string_to_array('abc',NULL)","{a,b,c}"},
        {"\"string_to_array\"('é界😀',NULL)","{é,界,😀}"},
        {"pg_catalog.string_to_array('abc',NULL)","{a,b,c}"},
        {"string_to_array('',NULL)","{}"},
        {"string_to_array(NULL::text,NULL)","NULL"},
        {"string_to_array('aba',NULL,'a')","{NULL,b,NULL}"}}) {
        SQLParser parser;auto parsed=expression(parser,item.first);
        auto* node=static_cast<SelectStmt*>(parsed.stmt.get())->selectList.front().expr.get();
        auto* call=dynamic_cast<FunctionCallExpr*>(node);if(!call)return 2;
        ExprEvaluator evaluator;
        try {
            const bool metadata=evaluator.scalarFunctionResultType(call)=="text[]" &&
                evaluator.scalarFunctionIdentity(call).rfind("builtin",0)==0 &&
                ExprHelper::inferParsedInputType(call,{},"",nullptr,&evaluator)=="text[]";
            evaluator.bindScalarFunctions(call);
            const auto value=evaluator.eval(call,{});
            check(metadata && value.typeName=="text[]" &&
                  (item.second=="NULL"?value.isNull:!value.isNull && value.value==item.second),item.first);
        }catch(const std::exception& error){check(false,item.first+" "+error.what());}
    }
    for(const auto volatility:{'v','i'})
        for(const auto* name:{"string_to_array","\"string_to_array\"","pg_catalog.string_to_array"})
            for(const auto* args:{"'x'","'x','y'","'x','y','z'"})
                for(const auto& expected:std::vector<ExprValue>{{"text","opaque"},{"integer","7"},{"bytea","\\x00ff"}}) {
                    SQLParser parser;auto parsed=expression(parser,std::string(name)+"("+args+")");
                    auto* call=dynamic_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList.front().expr.get());
                    if(!call)return 2;
                    ExprEvaluator evaluator;int calls=0;
                    const size_t arity=call->args.size();
                    evaluator.registerFunction("string_to_array",[&](const std::vector<ExprValue>& actual){
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
    if(controls!=60){++failures;std::cerr<<"STRING_TO_ARRAY_CALLBACK_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"STRING_TO_ARRAY_CALLBACK_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
