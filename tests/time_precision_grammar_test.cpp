#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "parser/parser.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace {
size_t checks=0,failures=0;
void report(bool good,const std::string& label) {
    ++checks;failures+=!good;
    std::cout<<"TIME_PRECISION_GRAMMAR_NATIVE "<<checks<<' '<<label<<" pass="<<good<<'\n';
}
std::vector<std::string> forms(const std::string& modifier) {
    const auto target="TIME("+modifier+")";
    return {"SELECT CAST('00:00:00' AS "+target+") AS v", "SELECT '00:00:00'::"+target+" AS v",
        "SELECT "+target+" '00:00:00' AS v", "SELECT CAST(NULL AS "+target+"[]) AS v",
        "CREATE TABLE time_precision_rows(v "+target+")",
        "ALTER TABLE time_precision_rows ADD COLUMN v "+target,
        "ALTER TABLE time_precision_rows ALTER COLUMN v TYPE "+target,
        "CREATE DOMAIN time_precision_domain AS "+target};
}
void parse(const std::string& sql,bool wanted) {
    for(const bool strict:{false,true}) {
        bool good=false;
        try {
            dbms::SQLParser parser;const auto result=strict?parser.parseForBinding(sql):parser.parse(sql);
            good=wanted?(result.success && result.stmt):(!result.success && result.sqlState=="42601");
        } catch(const dbms::DbError&){good=false;}
        report(good,std::string(strict?"binding ":"parse ")+sql);
    }
}
} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::vector<std::string> invalid={"-1","-0","+3","6.5","1,2","'3'","\"3\"","NULL",
        "2147483648","0x80000000","0b10000000000000000000000000000000","999999999999999999999999"};
    const std::vector<std::pair<std::string,std::string>> valid={{"0","0"},{"1","1"},{"3","3"},{"6","6"},
        {"7","7"},{"0003","3"},{"0x3","3"},{"0X3","3"},{"0b11","3"},{"0B11","3"},
        {"0o3","3"},{"0O3","3"},{"0_3","3"},{"0x0_3","3"},{"0b1_1","3"},{"0o0_3","3"},
        {"0000000000000000000003","3"},{"3_0","30"},{"2147483647","2147483647"}};
    for(const auto& modifier:invalid)for(const auto& sql:forms(modifier))parse(sql,false);
    // Grammar must still accept 7: warning/clamp is a separate semantic owner.
    for(const auto& modifier:valid)for(const auto& sql:forms(modifier.first))parse(sql,true);
    for(const auto& target:std::vector<std::string>{"timestamp(3)","timestamptz(3)","numeric(8,-2)",
            "char(3)","bit(3)","pg_catalog.time(3)","pg_catalog.\"time\"(3)","pg_catalog._time(3)"})
        parse("SELECT CAST(NULL AS "+target+") AS v",true);
    for(const auto& modifier:invalid) {
        bool rejected=false;
        try {(void)dbms::SQLParser::parseTypeSpecification("TIME("+modifier+")");}
        catch(const dbms::DbError& error){rejected=error.sqlState()=="42601";}
        report(rejected,"type envelope rejects "+modifier);
    }
    for(const auto& modifier:valid) {
        const auto type=dbms::SQLParser::parseTypeSpecification("TIME("+modifier.first+")");
        report(dbms::SQLParser::toLower(type.typeName)=="time" && !type.isArray &&
               type.typeMods==std::vector<std::string>{modifier.second},"canonical integer type envelope "+modifier.first);
    }
    for(const auto& spelling:std::vector<std::string>{"TIME(3)[]","TIME(3) WITHOUT TIME ZONE","TIME(3) WITH TIME ZONE"}) {
        const auto type=dbms::SQLParser::parseTypeSpecification(spelling);
        report(type.typeMods==std::vector<std::string>{"3"} &&
               type.isArray==(spelling.find("[]")!=std::string::npos) &&
               dbms::SQLParser::toLower(type.typeName)==(spelling.find(" WITH TIME")!=std::string::npos?"timetz":"time"),
               "preserve temporal suffix "+spelling);
    }
    // Qualified/quoted generic names have a different grammar. These are
    // raw-parser controls only, not claims that negative named typmods work.
    for(const auto& spelling:std::vector<std::string>{"pg_catalog.time(-1)","pg_catalog.\"time\"(-1)","\"time\"(-1)"}) {
        const auto type=dbms::SQLParser::parseTypeSpecification(spelling);
        report(type.typeMods==std::vector<std::string>{"-1"},"generic named syntax remains distinct "+spelling);
    }
    std::cout<<"TIME_PRECISION_GRAMMAR_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return checks==549 && !failures?0:1;
}
