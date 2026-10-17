#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace {
size_t checks=0,failures=0;
dbms::ExprEvaluator& evaluator() {static dbms::ExprEvaluator value;return value;}
std::string quote(const std::string& value) {
    std::string result="'";for(const auto c:value){result+=c;if(c=='\'')result+=c;}return result+'\'';
}
void report(bool good,const std::string& label) {
    ++checks;failures+=!good;
    std::cout<<"TIME_EXCESS_PRECISION_NATIVE "<<checks<<' '<<label<<" pass="<<good<<'\n';
}
void check(const std::string& sql,const std::string& type,const std::string& expected,
           bool null=false,const std::string& state="") {
    bool good=false;
    try {
        dbms::SQLParser parser;const auto parsed=parser.parse(sql);
        const auto* select=parsed.success?dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get()):nullptr;
        if(!select || select->selectList.size()!=1)throw dbms::DbError("XX000","invalid excess precision fixture");
        const auto value=evaluator().eval(select->selectList.front().expr.get(),dbms::RowContext{});
        good=state.empty() && value.typeName==type && value.isNull==null && (null || value.value==expected);
    } catch(const dbms::DbError& error){good=!state.empty() && error.sqlState()==state;}
    catch(const std::exception&) {}
    report(good,sql);
}
} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::vector<std::pair<std::string,std::string>> modifiers={
        {"7","7"},{"30","30"},{"2147483647","2147483647"},{"0x7","7"},
        {"0b111","7"},{"0o7","7"},{"3_0","30"}};
    const std::vector<std::pair<std::string,std::string>> values={
        {"00:00:00.1234565","00:00:00.123456"},{"00:00:00.1234567","00:00:00.123457"},
        {"23:59:59.9999995","24:00:00"},{"0:0:0","00:00:00"},
        {"12:34:56.120000","12:34:56.12"},{"12:34:56+08:30","12:34:56"}};
    for(const auto& modifier:modifiers) {
        const auto target="time("+modifier.first+")";
        for(const auto& value:values) {
            check("SELECT CAST("+quote(value.first)+" AS "+target+")","time",value.second);
            check("SELECT "+quote(value.first)+"::"+target,"time",value.second);
            check("SELECT "+target+' '+quote(value.first),"time",value.second);
            check("SELECT CAST("+quote("{"+value.first+",NULL}")+" AS "+target+"[])",
                  "time[]","{"+value.second+",NULL}");
        }
        check("SELECT CAST(NULL AS "+target+")","time","",true);
        check("SELECT CAST(NULL AS "+target+"[])","time[]","",true);
        check("SELECT CAST('[-3:-2]={00:00:00.1234567,NULL}' AS "+target+"[])",
              "time[]","[-3:-2]={00:00:00.123457,NULL}");
        check("SELECT CAST('{{00:00:00.1234565,NULL},{0:0:0,24:00:00}}' AS "+target+"[])",
              "time[]","{{00:00:00.123456,NULL},{00:00:00,24:00:00}}");
        for(const auto& bad:std::vector<std::pair<std::string,std::string>>{
                {"bad","22007"},{"25:00:00","22008"},{"12:60:00","22008"},
                {"12:00:61","22008"},{"24:00:00.000001","22008"}})
            check("SELECT CAST("+quote(bad.first)+" AS "+target+")","time","",false,bad.second);
        // The SQL projection adapter must not relax persistent schema guards.
        report(!dbms::TypeRegistry::instance().applyTypeMods("time",{modifier.second}).ok(),
               "strict registry still rejects raw precision "+modifier.second);
    }
    for(const auto& spelling:std::vector<std::string>{"pg_catalog.time(7)","pg_catalog.\"time\"(30)",
            "pg_catalog._time(7)","time(7) without time zone"})
        for(size_t i=0;i<2;++i) {
            const bool array=spelling.find("_time")!=std::string::npos;
            check("SELECT CAST("+quote(array?"{"+values[i].first+",NULL}":values[i].first)+" AS "+spelling+")",
                  array?"time[]":"time",array?"{"+values[i].second+",NULL}":values[i].second);
        }
    for(const auto precision:{"0","3","6"})
        report(dbms::TypeRegistry::instance().applyTypeMods("time",{precision}).ok(),
               std::string("strict registry accepts ")+precision);
    check("SELECT CAST('yes' AS boolean)","boolean","t");
    check("SELECT CAST('01' AS integer)","integer","1");
    check("SELECT CAST('2000-01-01' AS date)","date","2000-01-01");
    std::cout<<"TIME_EXCESS_PRECISION_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return checks==252 && !failures?0:1;
}
