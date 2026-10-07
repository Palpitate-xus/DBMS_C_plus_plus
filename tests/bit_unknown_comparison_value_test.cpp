#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/ExprEvaluator.h"
#include "parser/query_binding.h"
#include "parser/parser.h"
#include <iostream>
#include <memory>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    size_t controls=0, failures=0;
    const auto require=[&](bool condition,const std::string& label) {
        ++controls;
        if(!condition) {++failures;std::cerr<<"[BIT UNKNOWN FAIL] "<<label<<'\n';}
    };
    const auto truth=[](const std::string& op,int order) {
        return op=="="?order==0:op=="<>"?order!=0:op=="<"?order<0:
               op==">"?order>0:op=="<="?order<=0:order>=0;
    };
    // Public prepared-comparison APIs receive real typed datums. UNKNOWN
    // input conversion has no implicit BIT(1) typmod, in either operand.
    const std::vector<std::string> bits={"","0","00","001","01","1","10","11"};
    for(const auto& type:{std::string("bit"),std::string("bit varying")})
        for(const bool unknownLeft:{true,false})
            for(const auto& first:bits)
                for(const auto& second:bits)
                    for(const auto& op:{std::string("="),std::string("<>"),std::string("<"),
                                        std::string(">"),std::string("<="),std::string(">=")}) {
                        const auto binding=ExprEvaluator::resolveComparison(op,
                            unknownLeft?"unknown":type,unknownLeft?type:"unknown");
                        const ExprValue left(unknownLeft?"unknown":type,first);
                        const ExprValue right(unknownLeft?type:"unknown",second);
                        const auto converted=evaluator.coerceComparison(binding,unknownLeft?left:right,unknownLeft);
                        require(!converted.isNull && converted.value==(unknownLeft?first:second),
                            "unconstrained "+type+" input '"+(unknownLeft?first:second)+"' became '"+converted.value+"'");
                        const int order=(first>second)-(first<second);
                        const auto actual=evaluator.comparePrepared(binding,left,right);
                        require(!actual.isNull && actual.asBool()==truth(op,order),first+op+second+" ("+type+")");
                        if(op=="=") {
                            const auto a=evaluator.coerceComparison(binding,left,true);
                            const auto b=evaluator.coerceComparison(binding,right,false);
                            require((ExprEvaluator::comparisonHashKey(binding,a,true)==
                                     ExprEvaluator::comparisonHashKey(binding,b,false))==(first==second),
                                    "exact BIT value/length hash identity");
                        }
                    }
    for(const auto& type:{std::string("bit"),std::string("bit varying")}) {
        const auto binding=ExprEvaluator::resolveComparison("=","unknown",type);
        require(evaluator.comparePrepared(binding,ExprValue("unknown","",true),ExprValue(type,"01")).isNull,
                "UNKNOWN NULL stays NULL");
        try {(void)evaluator.coerceComparison(binding,ExprValue("unknown","102"),true);
             require(false,"invalid binary input must reject");}
        catch(const DbError& error){require(error.sqlState()=="22P02","invalid binary SQLSTATE");}
    }
    // Real ParameterExpr binding, original AST, actual public plan/cursor and
    // SQL child providers. No SQL parameter substitution or copied evaluator.
    StorageEngine owner;
    const auto cursor=[&](const std::string& sql,const std::optional<std::string>& parameter,
                          const std::string& expected,bool nullValue,bool bindParameter) {
        std::vector<QueryBindingDatum> datums;
        if(bindParameter)datums.push_back({"unknown-bit-input","needle","unknown",{},true,parameter,1});
        auto prepared=std::make_shared<PreparedQuery>(owner.prepareBoundQuery("unused",sql,datums));
        if(bindParameter)require(prepared->parameters.size()==1 && prepared->parameters.front().typeName=="unknown" &&
            prepared->parameters.front().value==parameter.value_or(""),"real UNKNOWN datum retained at binding");
        auto plan=QueryPlanner::buildPreparedQueryPlan(&owner,"unused",prepared,prepared->ast.get());
        auto rows=QueryPlanner::makePreparedCursor(std::move(plan),prepared->output);
        std::vector<ExprValue> row;
        const bool obtained=rows->next(row);
        require(obtained && row.size()==1 && row.front().typeName=="boolean" &&
            row.front().isNull==nullValue && (nullValue || row.front().value==expected),sql);
        require(!rows->next(row),"complete single-row cursor drained");
        rows->close();
    };
    for(const auto& source:{std::string("01"),std::string("001"),std::string("")}) {
        cursor("SELECT $1=ANY(ARRAY[B'"+source+"'])",source,"t",false,true);
        cursor("SELECT $1=ANY(SELECT B'"+source+"')",source,"t",false,true);
        cursor("SELECT $1=ALL(SELECT B'"+source+"')",source,"t",false,true);
        cursor("SELECT '"+source+"'=ANY(ARRAY[B'"+source+"'])",{},"t",false,false);
    }
    cursor("SELECT $1>ALL(ARRAY[B'10'])","11","t",false,true);
    cursor("SELECT $1=ANY(ARRAY[B'00',NULL,B'01'])","01","t",false,true);
    cursor("SELECT $1=ANY(ARRAY[B'00',NULL])","01","",true,true);
    cursor("SELECT $1=ANY(ARRAY[B'01'])",{},"",true,true);
    cursor("SELECT $1=ANY(ARRAY[]::bit[])","01","f",false,true);
    cursor("SELECT $1=ALL(SELECT B'01' WHERE false)","01","t",false,true);
    try {cursor("SELECT $1=ANY(ARRAY[B'01'])","102","",false,true);
         require(false,"real parameter invalid binary input");}
    catch(const DbError& error){require(error.sqlState()=="22P02","real parameter invalid binary SQLSTATE");}
    // Explicit scalar ::BIT still has its genuine default length of one.
    SQLParser parser;
    const auto parsed=parser.parse("SELECT B'01'::bit");
    require(parsed.isValid(),"explicit BIT control parses");
    const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
    require(evaluator.eval(select->selectList.front().expr.get(),{}).value=="0","explicit BIT(1) unchanged");
    std::cout<<"[BIT UNKNOWN COMPARISON VALUE] controls="<<controls<<" failures="<<failures<<'\n';
    return failures?1:0;
}
