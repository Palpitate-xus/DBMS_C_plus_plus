#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace {
size_t controls = 0, failures = 0;
void check(bool good, const std::string& label, const std::string& detail = "") {
    ++controls;
    if (!good) {
        ++failures;
        std::cerr << "WIDTH_BUCKET_NATIVE_FAIL " << controls << ' ' << label
                  << ' ' << detail << '\n';
    }
}
void value(const std::vector<std::string>& arguments, const std::string& wanted,
           const std::string& state = "", bool null = false) {
    std::string sql = "width_bucket(";
    for (const auto& argument : arguments) {
        if (sql.back() != '(') sql += ',';
        sql += argument;
    }
    sql += ')';
    const auto actual = dbms::ExprHelper::evalString(sql, {});
    check(state.empty()
              ? actual.ok && actual.typeName == "integer" &&
                    actual.isNull == null && (null || actual.value == wanted)
              : !actual.ok && actual.sqlState == state,
          sql, actual.typeName+" "+actual.value+" "+actual.sqlState);
    check(dbms::ExprHelper::inferResultType(sql) == "integer",
          "metadata "+sql);
}
std::string typed(const std::string& input, const std::string& type) {
    return "'"+input+"'::"+type;
}
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    for (const std::string type : {"integer", "numeric", "double precision"}) {
        for (const int64_t count : {int64_t{1}, int64_t{5}, int64_t{2147483647}}) {
            for (const bool reversed : {false, true}) {
                const int64_t low = reversed ? 10 : 0, high = reversed ? 0 : 10;
                for (const int64_t input : {-1, 0, 1, 2, 4, 5, 6, 8, 9, 10, 11}) {
                    int64_t bucket;
                    if (reversed ? input > low : input < low) bucket = 0;
                    else if (reversed ? input <= high : input >= high) bucket = count+1;
                    else if (type == "numeric")
                        bucket = (input-low)*count/(high-low)+1;
                    else {
                        const double ratio = reversed
                            ? (double(low)-double(input))/double(low-high)
                            : (double(input)-double(low))/double(high-low);
                        bucket = std::min(int64_t(count*ratio),count-1)+1;
                    }
                    value({typed(std::to_string(input),type),
                           typed(std::to_string(low),type),typed(std::to_string(high),type),
                           std::to_string(count)}, std::to_string(bucket),
                          bucket > 2147483647 ? "22003" : "");
                }
            }
        }
    }
    struct Special {
        std::array<const char*,4> args;
        const char* wanted;
        const char* state;
    };
    const std::vector<Special> special = {
        {{"0.3","0","1","10"},"4",""},
        {{"9","10","0","10"},"2",""},
        {{"Infinity","10","0","5"},"0",""},
        {{"-Infinity","10","0","5"},"6",""},
        {{"Infinity","0","10","5"},"6",""},
        {{"-Infinity","0","10","5"},"0",""},
        {{"NaN","0","10","5"},"","2201G"},
        {{"1","NaN","10","5"},"","2201G"},
        {{"1","0","NaN","5"},"","2201G"},
        {{"1","-Infinity","10","5"},"","2201G"},
        {{"1","0","Infinity","5"},"","2201G"},
        {{"1","1","1","5"},"","2201G"},
        {{"1","0","10","0"},"","2201G"},
        {{"1","0","10","-2147483648"},"","2201G"}
    };
    for (const std::string type : {"numeric","double precision"}) {
        for (const auto& item : special)
            value({typed(item.args[0],type),typed(item.args[1],type),
                   typed(item.args[2],type),item.args[3]},item.wanted,item.state);
        for (size_t index=0;index<4;++index) {
            std::vector<std::string> args = {typed("1",type),typed("0",type),typed("10",type),"5"};
            args[index]="NULL::"+std::string(index==3?"integer":type);
            value(args,"","",true);
        }
    }
    const std::vector<Special> precise = {
        {{"9007199254740993","9007199254740992","9007199254740994","2"},"2",""},
        {{"9007199254740993","9007199254740994","9007199254740992","2"},"2",""},
        {{"1e-400","0","2e-400","2"},"2",""},
        {{"1e400","0","2e400","2"},"2",""},
        {{"-1e400","-2e400","0","2"},"2",""},
        {{"0","-1e999","1e999","10"},"6",""},
        {{"0","1e999","-1e999","10"},"6",""},
        {{"0","-1e-999","1e999","10"},"1",""},
        {{"1e-999","0","2e-999","2"},"2",""},
        {{"0.9999999999999999999999999999999999999999","0","1","10"},"10",""}
    };
    for(const auto& item:precise)
        value({typed(item.args[0],"numeric"),typed(item.args[1],"numeric"),
               typed(item.args[2],"numeric"),item.args[3]},item.wanted);
    const std::vector<Special> floating = {
        {{"0","-1e308","1e308","10"},"6",""},
        {{"0","1e308","-1e308","10"},"6",""},
        {{"4e-324","0","1e-323","2"},"2",""},
        {{"0.9999999999999999","0","1","2147483647"},"2147483647",""},
        {{"-0","0","10","5"},"1",""}
    };
    for(const auto& item:floating)
        value({typed(item.args[0],"double precision"),typed(item.args[1],"double precision"),
               typed(item.args[2],"double precision"),item.args[3]},item.wanted);
    for (const std::string expression : {"width_bucket(1,0,10)",
            "width_bucket(1,0,10,5,6)","width_bucket(true,0,10,5)",
            "width_bucket('1'::text,0,10,5)","width_bucket(1,0,10,5::bigint)"}) {
        const auto actual=dbms::ExprHelper::evalString(expression,{});
        check(!actual.ok && actual.sqlState=="42883","signature "+expression,actual.sqlState);
    }
    for (const std::string sql : {"SELECT width_bucket()",
            "SELECT width_bucket('raw'::text,true)",
            "SELECT width_bucket(1,2,3,4,5)"}) {
        dbms::SQLParser parser;auto parsed=parser.parseForBinding(sql);
        const auto* select=parsed.success?dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get()):nullptr;
        const auto* call=select?dynamic_cast<const dbms::FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
        check(call!=nullptr,"opaque parse "+sql);
        if(!call)continue;
        dbms::ExprEvaluator evaluator;
        evaluator.registerFunction("width_bucket",[](const std::vector<dbms::ExprValue>& args) {
            return dbms::ExprValue("text","opaque:"+std::to_string(args.size()));
        });
        try {
            const auto result=evaluator.eval(call,{});
            check(result.typeName=="text" && result.value=="opaque:"+std::to_string(call->args.size()),
                  "opaque callback untouched "+sql,result.typeName+" "+result.value);
            check(evaluator.scalarFunctionResultType(call).empty(),"opaque result declaration "+sql);
            check(evaluator.scalarFunctionIdentity(call).rfind("native",0)==0,"opaque identity "+sql);
        } catch(const std::exception& error) {
            check(false,"opaque "+sql,error.what());
        }
    }
    constexpr size_t expected=515;
    std::cout<<"WIDTH_BUCKET_NATIVE_COMPLETE "<<controls<<" EXPECTED "<<expected
             <<" FAILURES "<<failures<<'\n';
    return controls==expected && failures==0?0:1;
}
