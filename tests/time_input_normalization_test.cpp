#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {
size_t controls=0,failures=0;
dbms::ExprEvaluator& evaluator() {
    static dbms::ExprEvaluator instance;
    return instance;
}

std::string quoted(const std::string& input) {
    std::string output="'";
    for(const char c:input){output+=c;if(c=='\'')output+=c;}
    return output+'\'';
}

std::string clockText(int64_t micros) {
    char body[32];
    std::snprintf(body,sizeof(body),"%02lld:%02lld:%02lld",
        static_cast<long long>(micros/3600000000LL),
        static_cast<long long>((micros/60000000LL)%60),
        static_cast<long long>((micros/1000000LL)%60));
    std::string result=body;
    const auto fraction=micros%1000000;
    if(fraction) {
        auto digits=std::to_string(fraction);
        digits.insert(digits.begin(),6-digits.size(),'0');
        while(digits.back()=='0')digits.pop_back();
        result+='.'+digits;
    }
    return result;
}

void check(const std::string& target,const std::string& input,const std::string& expected,
           const std::string& state="",const std::string& expectedType="") {
    ++controls;bool good=false;
    try {
        dbms::LiteralExpr literal;literal.typeName=target;literal.value=quoted(input);
        const auto result=evaluator().eval(&literal,dbms::RowContext{});
        good=state.empty() && !result.isNull && result.value==expected &&
            result.typeName==(expectedType.empty()?target:expectedType);
    } catch(const dbms::DbError& failure) { good=!state.empty() && failure.sqlState()==state; }
    catch(const std::exception&) { good=false; }
    failures+=!good;
    std::cout<<"TIME_INPUT_NATIVE "<<controls<<" target="<<target<<" pass="<<good<<'\n';
}

void null(const std::string& target) {
    ++controls;bool good=false;
    try {
        dbms::CastExpr cast;cast.typeName=target;
        auto source=std::make_unique<dbms::LiteralExpr>();source->value="NULL";
        cast.operand=std::move(source);
        const auto result=evaluator().eval(&cast,dbms::RowContext{});
        good=result.isNull && result.typeName==target;
    } catch(const std::exception&) {}
    failures+=!good;
    std::cout<<"TIME_INPUT_NATIVE "<<controls<<" NULL_TARGET="<<target<<" pass="<<good<<'\n';
}
} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::vector<std::pair<std::string,int64_t>> cases={
        {"00:00:00",0},{"00:00:00.000001",1},{"00:00:00.0000015",2},
        {"00:00:00.9999995",1000000},{"00:00:59.9999995",60000000},
        {"00:59:59.9999995",3600000000LL},{"23:59:59.9999995",86400000000LL},
        {"12:34:56",45296000000LL},{"12:34:56.123456",45296123456LL},
        {"12:34:56.0000015",45296000002LL},{"12:34:56.9999995",45297000000LL},
        {"01:02:03.1",3723100000LL},{"01:02:03.000001",3723000001LL},
        {"23:59:59.999999",86399999999LL},{"24:00:00",86400000000LL},
        {"24:00:00.0000001",86400000000LL},{"00:00:00.0000004",0},
        {"00:00:00.0000005",0},{"00:00:00.0000025",2},{"00:00:00.0000035",4},
        {"00:00:00.1234564",123456},{"00:00:00.1234565",123456},
        {"00:00:00.1234575",123458},{"03:04:05.000006",11045000006LL},
        {"0:0:0",0},{"12:34",45240000000LL},{"24:00",86400000000LL},
        {"23:59:60",86400000000LL},{"12:34:60",45300000000LL},
        {" 12:34:56.123456 ",45296123456LL}};
    for(const auto& [input,micros]:cases) {
        const auto expected=clockText(micros);
        check("time",input,expected);
        check("time[]","{\""+input+"\",NULL}","{"+expected+",NULL}");
    }
    for(const auto& [input,expected]:std::vector<std::pair<std::string,std::string>>{
            {"12:34:56+08","12:34:56"},{"12:34:56.000001+08:30","12:34:56.000001"}}) {
        check("time",input,expected);
        check("time[]","{\""+input+"\",NULL}","{"+expected+",NULL}");
    }
    for(const auto& [input,state]:std::vector<std::pair<std::string,std::string>>{
            {"bad","22007"},{"25:00:00","22008"},{"12:60:00","22008"},
            {"12:00:61","22008"},{"-01:00:00","22007"},{"infinity","22007"},
            {"24:00:00.000001","22008"}}) {
        check("time",input,"",state);
        check("time[]","{\""+input+"\",NULL}","",state);
    }
    null("time");null("time[]");
    check("time[]","{}","{}");check("time[]","{NULL,NULL}","{NULL,NULL}");
    check("time[]","[-3:-1]={0:0:0,NULL,12:34:56.9999995}","[-3:-1]={00:00:00,NULL,12:34:57}");
    check("time[]","{{0:0:0,NULL},{24:00:00,12:34:56.0000015}}","{{00:00:00,NULL},{24:00:00,12:34:56.000002}}");
    check("time[]","{{{{{{0:0:0}}}}}}","{{{{{{00:00:00}}}}}}");
    check("time without time zone","12:34","12:34:00","","time");
    check("pg_catalog.time","0:0:0","00:00:00","","time");
    check("boolean","yes","t");check("integer","01","1");
    check("date","2000-01-01","2000-01-01");
    std::cout<<"TIME_INPUT_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return cases.size()==30 && controls==90 && failures==0?0:1;
}
