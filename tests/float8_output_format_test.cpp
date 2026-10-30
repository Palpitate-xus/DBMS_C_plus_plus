#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include "compat/float8_native_pg18_expected.inc"

using namespace dbms;
static int checks=0,failures=0;
static void check(bool ok,const std::string& label) {
    ++checks;if(!ok) {++failures;std::cerr<<"R322_FLOAT8_NATIVE_FAIL "<<label<<'\n';}
}
static ExprValue cast(ExprEvaluator& evaluator,ExprValue input) {
    CastExpr expression;expression.typeName="double precision";
    auto parameter=std::make_unique<ParameterExpr>();parameter->slot=0;
    parameter->origin=ParameterOrigin::RuntimeCell;parameter->declaredType=input.typeName;
    expression.operand=std::move(parameter);RowContext row;row.setParameters({std::move(input)});
    return evaluator.eval(&expression,row);
}
int main() {
    static_assert(sizeof(double)==sizeof(std::uint64_t) && std::numeric_limits<double>::is_iec559);
    TypeRegistry::instance().bootstrap();ExprEvaluator evaluator;
    for(const auto& test:float8FormatCases) {
        double value;std::memcpy(&value,&test.bits,sizeof(value));char buffer[64];
        // This is only a round-tripping input spelling for a known physical
        // runtime cell. The expected output comes from actual PG18.6 d2s.c,
        // not this conversion and not a copy of the DBMS output algorithm.
        const auto encoded=std::to_chars(buffer,buffer+sizeof(buffer),value,std::chars_format::general);
        if(encoded.ec!=std::errc())return 2;
        try {
            const auto result=cast(evaluator,{"double precision",std::string(buffer,encoded.ptr)});
            check(result.typeName=="double precision",std::string(test.label)+" declared type");
            check(!result.isNull && result.value==test.expected,std::string(test.label)+" actual="+result.value+" expected="+test.expected);
            char* end=nullptr;const double decoded=std::strtod(result.value.c_str(),&end);std::uint64_t bits;
            std::memcpy(&bits,&decoded,sizeof(bits));
            check(end && !*end && (bits==test.bits || (std::isnan(decoded) && std::isnan(value))),std::string(test.label)+" physical roundtrip");
        } catch(const std::exception& error) {
            check(false,std::string(test.label)+" exception="+error.what());
            check(false,std::string(test.label)+" output unavailable");check(false,std::string(test.label)+" roundtrip unavailable");
        }
    }
    for(const std::string type:{"unknown","integer","double precision"}) {
        const auto result=cast(evaluator,{type,"",true});
        check(result.typeName=="double precision","NULL declared type");
        check(result.isNull,"NULL state");check(result.value.empty(),"NULL carries no bytes");
    }
    const auto expected=std::size(float8FormatCases)*3+9;
    check(static_cast<size_t>(checks)==expected,"all frozen cases checked");
    std::cout<<"FLOAT8_OUTPUT_FORMAT_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
