#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"

#include <array>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using namespace dbms;
struct Sample { std::string source, from, to, result; };

static std::string character(unsigned code) {
    std::string value;
    if(code<128)value+=static_cast<char>(code);
    else if(code<2048){value+=static_cast<char>(192|(code>>6));value+=static_cast<char>(128|(code&63));}
    else {value+=static_cast<char>(224|(code>>12));value+=static_cast<char>(128|((code>>6)&63));value+=static_cast<char>(128|(code&63));}
    return value;
}

int main() {
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++controls;if(!good){++failures;std::cerr<<"TRANSLATE_INDEX_FAILURE "<<label<<'\n';}
    };
    const auto call=[&](const std::vector<ExprValue>& args) {
        FunctionCallExpr function;function.funcName="translate";RowContext row;
        for(size_t i=0;i<args.size();++i){
            const std::string key="a"+std::to_string(i);
            auto column=std::make_unique<ColumnRefExpr>();column->column=key;
            function.args.push_back(std::move(column));row.set(key,args[i]);
        }
        return evaluator.eval(&function,row);
    };
    const auto text=[](const std::string& v){return ExprValue("text",v);};
    const auto good=[](const ExprValue& v,const std::string& expected){return !v.isNull && v.typeName=="text" && v.value==expected;};
    const auto quote=[](const std::string& value){
        std::string out="'";for(char ch:value){out+=ch;if(ch=='\'')out+=ch;}return out+"'";
    };
    const std::vector<Sample> samples={
        {"","","",""},{"","a","b",""},{"abc","","x","abc"},
        {"abc","x","y","abc"},{"abc","abc","X","X"},{"abc","abc","",""},
        {"aaaa","a","xx","xxxx"},{"aaaa","aa","b","bbbb"},
        {"aba","aba","XYZ","XYX"},{"aab","aa","XY","XXb"},
        {"abc","abca","XY","XY"},{"ab","aab","XYZ","XZ"},
        {"abab","ab","",""},{"abXXab","ab","界","界XX界"},
        {"aé界😀","é界","!","a!😀"},{"ééé","éé","界","界界界"},
        {"😀😀","😀","é","ee"},{"éé","́","!","e!e!"},
        {"é","e","x","é"},{"a b  "," ","","ab"},
        {"a'b'a","'","\"","a\"b\"a"},{"a\\b\\a","\\","'","a'b'a"},
        {"a\nb\na","\n","\t","a\tb\ta"},{"xax","x","xax","xax"},
        {"abc","abc","cba","cba"},{"ab","ab","ba","ba"},
        {"😀é中😀","😀é😀中","甲乙丙丁","甲乙丁甲"},
        {"😀é中😀","😀é😀中","甲乙","甲乙甲"},
        {"AaAa","Aa","xy","xyxy"},{"a","aa","",""},
        {"é界","é界","é","é"},{"é","é","é","é"}
    };
    for(const auto& item:samples){
        for(const auto* type:{"text","varchar"})
            check(good(call({ExprValue(type,item.source),ExprValue(type,item.from),ExprValue(type,item.to)}),item.result),"typed sample");
        const auto value=ExprHelper::evalString("translate("+quote(item.source)+","+quote(item.from)+","+quote(item.to)+")",{});
        check(value.ok && !value.isNull && value.typeName=="text" && value.value==item.result,"SQL sample");
    }
    const std::vector<std::pair<std::string,std::string>> padded={
        {"translate('ab  '::char(4),'b','!')","a!"},
        {"translate('ab  '::varchar,'b','!')","a!  "},
        {"translate('a b  ',' '::char(1),'x')","a b  "},
        {"translate('aba','b '::char(2),'X')","aXa"},
        {"translate('aba','b','X  '::char(3))","aXa"},
        {"translate('é界  '::char(4),'界','😀')","é😀"}
    };
    for(const auto& item:padded){const auto value=ExprHelper::evalString(item.first,{});check(value.ok && good(ExprValue(value.typeName,value.value,value.isNull),item.second),"CHAR argument");}
    for(unsigned mask=1;mask<8;++mask){
        std::vector<ExprValue> args={text("abc"),text("b"),text("X")};
        for(size_t i=0;i<args.size();++i)if(mask&(1U<<i))args[i]=ExprValue("text","",true);
        const auto value=call(args);check(value.isNull && value.typeName=="text","strict NULL");
    }
    for(unsigned count:{1024U,4096U,8192U}){
        std::string from;for(unsigned i=0;i<count;++i)from+=character(0x1000+i);
        const std::string to(count,'Q');
        for(bool match:{true,false}){
            const auto unit=match?character(0x1000+count-1):std::string("😀");
            std::string source;for(unsigned i=0;i<65536;++i)source+=unit;
            const auto start=std::chrono::steady_clock::now();
            const auto value=call({text(source),text(from),text(to)});
            const auto elapsed=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-start).count();
            check(good(value,match?std::string(65536,'Q'):source),"long mapping last/absent");
            // Observation only: no hardware-dependent pass threshold.
            std::cout<<"TRANSLATE_INDEX_ELAPSED_NS "<<count<<' '<<match<<' '<<elapsed<<'\n';
        }
    }
    for(int nullArgument:{-1,0,1,2}){
        std::array<int,3> effects{};FunctionCallExpr function;function.funcName="translate";
        const std::array<std::string,3> values={"aba","b","X"};
        for(size_t i=0;i<values.size();++i){
            const std::string name="translate_effect_"+std::to_string(i);
            evaluator.registerFunction(name,[&,i](const std::vector<ExprValue>&){++effects[i];return ExprValue("text",values[i],static_cast<int>(i)==nullArgument);});
            auto arg=std::make_unique<FunctionCallExpr>();arg->funcName=name;function.args.push_back(std::move(arg));
        }
        (void)ExprHelper::inferParsedResultType(&function,{},"",nullptr,&evaluator);
        const bool pure=effects==std::array<int,3>{0,0,0};const auto value=evaluator.eval(&function,{});
        check(pure && effects==std::array<int,3>{1,1,1} && (nullArgument<0?good(value,"aXa"):value.isNull),"metadata/effects");
    }
    evaluator.registerFunction("translate",[](const std::vector<ExprValue>& args){return ExprValue("text","opaque"+std::to_string(args.size()));});
    check(good(call({text("a"),text("b")}),"opaque2"),"native override arity");
    if(controls!=120){++failures;std::cerr<<"TRANSLATE_INDEX_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"TRANSLATE_INDEX_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
