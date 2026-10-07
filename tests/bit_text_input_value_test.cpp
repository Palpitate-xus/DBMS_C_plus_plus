#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <iostream>
#include <memory>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    StorageEngine owner;
    SQLParser parser;
    size_t controls=0,failures=0;
    const auto require=[&](bool condition,const std::string& label) {
        ++controls;
        if(!condition){++failures;std::cerr<<"[BIT TEXT INPUT FAIL] "<<label<<'\n';}
    };
    const std::vector<std::pair<std::string,std::string>> inputs={
        {"",""},{"0","0"},{"01","01"},{"001","001"},
        {"b",""},{"B",""},{"b01","01"},{"B001","001"},
        {"x",""},{"X",""},{"x1","0001"},{"X0aF","000010101111"},
        {"x0","0000"},{"Xff","11111111"}};
    const std::vector<std::string> invalid={"b02","xg","b 01","x 1","B'01'","x-1"};
    const auto truth=[](const std::string& op,int order) {
        return op=="="?order==0:op=="<>"?order!=0:op=="<"?order<0:
            op==">"?order>0:op=="<="?order<=0:order>=0;
    };
    // Real prepared comparison APIs, both source positions, both BIT types,
    // every comparison and exact converted/hash identities. No numeric-text
    // fallback may erase a leading zero or hex nibble.
    for(const auto& type:{std::string("bit"),std::string("bit varying")})
        for(const bool unknownLeft:{false,true})
            for(const auto& input:inputs)
                for(const auto& bits:{std::string(""),std::string("0"),std::string("001"),
                    std::string("01"),std::string("0001"),std::string("1"),std::string("000010101111")})
                    for(const auto& op:{std::string("="),std::string("<>"),std::string("<"),
                        std::string(">"),std::string("<="),std::string(">=")}) {
                        try {
                            const auto binding=ExprEvaluator::resolveComparison(op,
                                unknownLeft?"unknown":type,unknownLeft?type:"unknown");
                            const ExprValue left(unknownLeft?"unknown":type,unknownLeft?input.first:bits);
                            const ExprValue right(unknownLeft?type:"unknown",unknownLeft?bits:input.first);
                            const auto converted=evaluator.coerceComparison(binding,unknownLeft?left:right,unknownLeft);
                            require(converted.typeName==binding.leftType && !converted.isNull && converted.value==input.second,
                                "full input codec "+input.first+" -> "+input.second);
                            const auto a=unknownLeft?input.second:bits,b=unknownLeft?bits:input.second;
                            const auto actual=evaluator.comparePrepared(binding,left,right);
                            require(!actual.isNull && actual.asBool()==truth(op,(a>b)-(a<b)),a+op+b);
                            if(op=="=") {
                                const auto l=evaluator.coerceComparison(binding,left,true);
                                const auto r=evaluator.coerceComparison(binding,right,false);
                                require((ExprEvaluator::comparisonHashKey(binding,l,true)==
                                    ExprEvaluator::comparisonHashKey(binding,r,false))==(a==b),"decoded full BIT hash identity");
                            }
                        } catch(const DbError& error) {require(false,"valid prepared input rejected: "+input.first+" "+error.sqlState());}
                    }
    const auto quote=[](const std::string& source) {
        std::string result="'";for(char c:source){if(c=='\'')result+='\'';result+=c;}return result+'\'';
    };
    const auto cursor=[&](const std::string& sql,const std::vector<QueryBindingDatum>& datums,
                          const std::string& type,const std::string& value,bool nullValue=false) {
        try {
            auto query=std::make_shared<PreparedQuery>(owner.prepareBoundQuery("unused",sql,datums));
            auto plan=QueryPlanner::buildPreparedQueryPlan(&owner,"unused",query,query->ast.get());
            auto rows=QueryPlanner::makePreparedCursor(std::move(plan),query->output);
            std::vector<ExprValue> row;
            const bool found=rows->next(row);
            require(found && row.size()==1 && row.front().typeName==type && row.front().isNull==nullValue &&
                (nullValue || row.front().value==value),sql);
            require(!rows->next(row),"complete cursor drained");rows->close();
        } catch(const DbError& error) {require(false,sql+" unexpected cursor error "+error.sqlState());}
    };
    for(const auto& input:inputs) {
        const auto literal=quote(input.first);
        const auto array="{01,"+(input.second.empty()?std::string("\"\""):input.second)+"}";
        for(const auto& expression:{"B'01'="+literal,literal+"=ANY(ARRAY[B'01'])",literal+" IN(B'01')"}) {
            auto parsed=parser.parse("SELECT "+expression);
            require(parsed.isValid(),"actual input AST parses");
            auto* select=static_cast<SelectStmt*>(parsed.stmt.get());
            try {
                ExprHelper::prepareArrayTypes(select->selectList.front().expr.get(),{},"unused",&owner);
                const auto value=evaluator.eval(select->selectList.front().expr.get(),{});
                require(!value.isNull && value.typeName=="boolean" && value.asBool()==(input.second=="01"),"helper implicit BIT input");
            } catch(const DbError& error) {require(false,expression+" unexpected helper error "+error.sqlState());}
        }
        cursor("SELECT ARRAY[B'01',"+literal+"]",{},"bit[]",array);
        cursor("SELECT "+literal+"::varbit",{},"bit varying",input.second);
        cursor("SELECT $1=ANY(ARRAY[B'01'])",{{"prefix-input","needle","unknown",{},true,input.first,1}},
            "boolean",input.second=="01"?"t":"f");
        const auto scalar=parser.parse("SELECT "+literal+"::bit");
        require(scalar.isValid(),"explicit default BIT parses");
        try {
            require(evaluator.eval(static_cast<SelectStmt*>(scalar.stmt.get())->selectList.front().expr.get(),{}).value==
                (input.second+"0").substr(0,1),"explicit default length remains one");
        } catch(const DbError& error) {require(false,"explicit BIT input "+input.first+" rejected "+error.sqlState());}
        const auto textAst=parser.parse("SELECT "+literal+"::text");
        require(textAst.isValid(),"actual text CAST parses");
        const auto text=evaluator.eval(static_cast<SelectStmt*>(textAst.stmt.get())->selectList.front().expr.get(),{});
        require(text.typeName=="text" && text.value==input.first,"non-BIT text bytes preserved");
    }
    for(const auto& source:invalid)
        for(const auto& type:{std::string("bit"),std::string("bit varying")}) {
            const auto binding=ExprEvaluator::resolveComparison("=","unknown",type);
            bool rejected=false;
            try {(void)evaluator.coerceComparison(binding,ExprValue("unknown",source),true);}
            catch(const DbError& error){rejected=error.sqlState()=="22P02";}
            require(rejected,"invalid input rejects "+source);
        }
    for(const auto& type:{std::string("bit"),std::string("bit varying")}) {
        const auto binding=ExprEvaluator::resolveComparison("=","unknown",type);
        require(evaluator.comparePrepared(binding,ExprValue("unknown","",true),ExprValue(type,"01")).isNull,"UNKNOWN NULL remains NULL");
        for(const auto& malformed:{std::string("b01"),std::string("x1")}) {
            bool rejected=false;
            auto cast=parser.parse("SELECT c::varbit");
            require(cast.isValid(),"actual typed-datum CAST parses");
            RowContext context;context.set("c",ExprValue(type,malformed));
            try {(void)evaluator.eval(static_cast<SelectStmt*>(cast.stmt.get())->selectList.front().expr.get(),context);}
            catch(const DbError& error){rejected=error.sqlState()=="22P02";}
            require(rejected,"typed BIT datum is canonical data, not encoded input");
        }
    }
    cursor("SELECT $1=ANY(ARRAY[B'01'])",{{"prefix-null","needle","unknown",{},true,std::nullopt,1}},"boolean","",true);
    std::cout<<"[BIT TEXT INPUT VALUE] controls="<<controls<<" failures="<<failures<<'\n';
    return failures?1:0;
}
