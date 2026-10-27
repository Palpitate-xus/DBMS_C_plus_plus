#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include <cstdint>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::vector<int64_t> values={INT32_MIN,-2147483647,-1073741824,-65536,-46349,-46341,-2,-1,0,1,2,3,6,42,46337,46340,46341,46349,65536,1073741824,2147483646,INT32_MAX};
    const std::vector<int64_t> wide={INT64_MIN,-INT64_MAX,-4294967296LL,-2147483648LL,-1,0,1,2,3,2147483648LL,4294967296LL,INT64_MAX};
    size_t controls=0,failures=0;
    const auto magnitude=[](int64_t value)->uint64_t {
        return value<0 ? static_cast<uint64_t>(-(value+1))+1 : static_cast<uint64_t>(value);
    };
    const auto test=[&](const std::string& fn,int64_t left,int64_t right,const std::string& lt,const std::string& rt,bool nullLeft=false,bool nullRight=false) {
        ++controls;
        const std::string resultType=lt=="bigint"||rt=="bigint" ? "bigint" : "integer";
        const uint64_t a=magnitude(left),b=magnitude(right),g=std::gcd(a,b);
        const unsigned __int128 expected=fn=="gcd" ? g : a==0||b==0 ? 0 : static_cast<unsigned __int128>(a/g)*b;
        const uint64_t maximum=resultType=="bigint" ? INT64_MAX : INT32_MAX;
        const bool null=nullLeft||nullRight,overflow=!null&&expected>maximum;
        bool good=false;
        try {
            SQLParser parser;
            const auto literal=[](int64_t n,const std::string& type,bool isNull) {
                return (isNull ? "NULL" : "'"+std::to_string(n)+"'")+"::"+type;
            };
            auto parsed=parser.parseForBinding("SELECT "+fn+"("+literal(left,lt,nullLeft)+","+literal(right,rt,nullRight)+")");
            auto* select=parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
            if(!select||select->selectList.size()!=1)throw std::runtime_error("parse failed");
            ExprEvaluator evaluator;
            auto* expression=select->selectList.front().expr.get();
            evaluator.bindScalarFunctions(expression);
            const auto result=evaluator.eval(expression,RowContext{});
            good=!overflow&&result.typeName==resultType&&result.isNull==null&&
                (null||result.value==std::to_string(static_cast<uint64_t>(expected)));
        } catch(const std::exception& error) {
            const auto* typed=dynamic_cast<const DbError*>(&error);
            good=overflow&&((typed&&typed->sqlState()=="22003")||std::string(error.what()).find("SQLSTATE 22003")!=std::string::npos);
        }
        if(!good) {
            ++failures;
            std::cerr<<"GCD_LCM_INTEGER_RANGE_FAIL "<<fn<<' '<<left<<':'<<lt<<' '<<right<<':'<<rt<<" null="<<null<<" overflow="<<overflow<<'\n';
        }
    };
    for(const auto* fn:{"gcd","lcm"}) {
        for(const auto* type:{"integer","bigint"})for(const auto a:values)for(const auto b:values)test(fn,a,b,type,type);
        for(const auto a:wide)for(const auto b:wide)test(fn,a,b,"bigint","bigint");
        for(const auto a:values)for(const auto b:values) {
            test(fn,a,b,"integer","bigint");test(fn,a,b,"bigint","integer");
        }
        for(const auto* lt:{"integer","bigint"})for(const auto* rt:{"integer","bigint"}) {
            test(fn,lt==std::string("integer")?INT32_MIN:INT64_MIN,rt==std::string("integer")?INT32_MIN:INT64_MIN,lt,rt,true,false);
            test(fn,lt==std::string("integer")?INT32_MIN:INT64_MIN,rt==std::string("integer")?INT32_MIN:INT64_MIN,lt,rt,false,true);
        }
        ++controls;
        try {
            SQLParser parser;auto parsed=parser.parseForBinding(std::string("SELECT ")+fn+"('2147483647'::integer,'2'::integer)");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)throw std::runtime_error("opaque parse failed");
            size_t calls=0;ExprEvaluator evaluator;
            evaluator.registerFunction(fn,[&](const std::vector<ExprValue>& args) {
                ++calls;if(args.size()!=2)throw std::runtime_error("opaque arity changed");
                return ExprValue("text","opaque integer range",false);
            });
            auto* expression=select->selectList.front().expr.get();evaluator.bindScalarFunctions(expression);
            if(calls)throw std::runtime_error("binding executed replacement");
            const auto result=evaluator.eval(expression,RowContext{});
            if(calls!=1||result.isNull||result.typeName!="text"||result.value!="opaque integer range")throw std::runtime_error("opaque domain changed");
        } catch(const std::exception& error) {++failures;std::cerr<<"GCD_LCM_OPAQUE_FAIL "<<fn<<' '<<error.what()<<'\n';}
    }
    if(controls!=4178)return 2;
    std::cout<<"GCD_LCM_INTEGER_RANGE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
