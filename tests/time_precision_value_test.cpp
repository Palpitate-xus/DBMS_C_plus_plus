#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

#include <array>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace {
size_t checks=0,failures=0;
dbms::ExprEvaluator& evaluator() {static dbms::ExprEvaluator value;return value;}
std::string quote(const std::string& input) {
    std::string output="'";for(const char c:input){output+=c;if(c=='\'')output+=c;}return output+'\'';
}
std::string clockText(int64_t micros) {
    char body[32];
    std::snprintf(body,sizeof(body),"%02lld:%02lld:%02lld",
        static_cast<long long>(micros/3600000000LL),static_cast<long long>((micros/60000000LL)%60),
        static_cast<long long>((micros/1000000LL)%60));
    std::string output=body;
    if(const auto fraction=micros%1000000) {
        auto digits=std::to_string(fraction);digits.insert(digits.begin(),6-digits.size(),'0');
        while(digits.back()=='0')digits.pop_back();output+='.'+digits;
    }
    return output;
}
void check(const std::string& sql,const std::string& type,const std::string& value,
           bool null=false,const std::string& state="") {
    ++checks;bool good=false;std::string actualState;
    try {
        dbms::SQLParser parser;const auto parsed=parser.parse(sql);
        const auto* select=parsed.success?dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get()):nullptr;
        if(!select || select->selectList.size()!=1)throw dbms::DbError("XX000","invalid precision fixture AST");
        const auto actual=evaluator().eval(select->selectList.front().expr.get(),dbms::RowContext{});
        good=state.empty() && actual.typeName==type && actual.isNull==null && (null || actual.value==value);
    } catch(const dbms::DbError& error){actualState=error.sqlState();good=!state.empty() && actualState==state;}
    catch(const std::exception&) {}
    failures+=!good;
    std::cout<<"TIME_PRECISION_VALUE_NATIVE "<<checks<<' '<<sql<<" pass="<<good<<" state="<<actualState<<'\n';
}
struct Case {std::string input;std::array<int64_t,7> micros;};
} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    // Literal expected microseconds, checked independently on real PG18.
    // Input half-even to microseconds precedes positive typmod half-up.
    const std::vector<Case> cases={
        {"12:34:56.654321",{45297000000LL,45296700000LL,45296650000LL,45296654000LL,45296654300LL,45296654320LL,45296654321LL}},
        {"00:00:00.500000",{1000000,500000,500000,500000,500000,500000,500000}},
        {"00:00:00.0004996",{0,0,0,1000,500,500,500}},
        {"00:00:00.1234565",{0,100000,120000,123000,123500,123460,123456}},
        {"23:59:59.999999",{86400000000LL,86400000000LL,86400000000LL,86400000000LL,86400000000LL,86400000000LL,86399999999LL}},
        {"23:59:59.9999995",{86400000000LL,86400000000LL,86400000000LL,86400000000LL,86400000000LL,86400000000LL,86400000000LL}},
        {"0:0:0",{0,0,0,0,0,0,0}},
        {"12:34:56+08:30",{45296000000LL,45296000000LL,45296000000LL,45296000000LL,45296000000LL,45296000000LL,45296000000LL}}};
    for(const auto& item:cases)for(int precision=0;precision<=6;++precision) {
        const std::string target="time("+std::to_string(precision)+")",value=quote(item.input),expected=clockText(item.micros[precision]);
        check("SELECT CAST("+value+" AS "+target+")","time",expected);
        check("SELECT "+value+"::"+target,"time",expected);
        check("SELECT "+target+" "+value,"time",expected);
        check("SELECT CAST("+quote("{"+item.input+",NULL}")+" AS "+target+"[])","time[]","{"+expected+",NULL}");
    }
    for(int precision=0;precision<=6;++precision) {
        const std::string target="time("+std::to_string(precision)+")";
        check("SELECT CAST(NULL AS "+target+")","time","",true);
        check("SELECT CAST(NULL AS "+target+"[])","time[]","",true);
    }
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
            {"bad","22007"},{"25:00:00","22008"},{"12:60:00","22008"},{"12:00:61","22008"},
            {"-01:00:00","22007"},{"infinity","22007"},{"24:00:00.000001","22008"}})
        for(const int precision:{0,3,6}) {
            const auto target="time("+std::to_string(precision)+")";
            check("SELECT CAST("+quote(item.first)+" AS "+target+")","time","",false,item.second);
            check("SELECT CAST("+quote("{"+item.first+",NULL}")+" AS "+target+"[])","time[]","",false,item.second);
        }
    for(int precision=0;precision<=6;++precision) {
        const auto p=std::to_string(precision),expected=clockText(cases.front().micros[precision]);
        for(const auto& spelling:std::vector<std::string>{"pg_catalog.time("+p+")","pg_catalog.\"time\"("+p+")",
                "pg_catalog._time("+p+")","time("+p+") without time zone"}) {
            const bool array=spelling.find("_time")!=std::string::npos;
            check("SELECT CAST("+quote(array?"{12:34:56.654321,NULL}":"12:34:56.654321")+" AS "+spelling+")",
                  array?"time[]":"time",array?"{"+expected+",NULL}":expected);
        }
        const auto target="time("+p+")[]";
        check("SELECT CAST('[-3:-2]={12:34:56.654321,NULL}' AS "+target+")","time[]","[-3:-2]={"+expected+",NULL}");
        check("SELECT CAST('{{12:34:56.654321,NULL},{0:0:0,24:00:00}}' AS "+target+")","time[]","{{"+expected+",NULL},{00:00:00,24:00:00}}");
        check("SELECT CAST('{{{{{{12:34:56.654321}}}}}}' AS "+target+")","time[]","{{{{{{"+expected+"}}}}}}");
        check("SELECT CAST(TIME '00:00:00.500000' AS time("+p+"))","time",clockText(cases[1].micros[precision]));
    }
    check("SELECT CAST('yes' AS boolean)","boolean","t");
    check("SELECT CAST('01' AS integer)","integer","1");
    check("SELECT CAST('2000-01-01' AS date)","date","2000-01-01");
    std::cout<<"TIME_PRECISION_VALUE_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return checks==339 && !failures?0:1;
}
