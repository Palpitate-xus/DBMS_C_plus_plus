#include "catalog/type_registry.h"
#include "expression/arithmetic_type.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace {
struct Signature { std::string op, left, right, result, value; };
int checks = 0, failures = 0;
void check(bool good, const std::string& label, const std::string& detail = "") {
    ++checks;
    if (!good) {
        ++failures;
        std::cerr << "TEMPORAL_TYPES_NATIVE_FAILURE " << checks << ' '
                  << label << ' ' << detail << '\n';
    }
}
std::string input(const std::string& type, bool left, bool null = false) {
    std::string value = null ? "NULL" :
        type == "integer" ? "3" :
        type == "date" ? (left ? "'2000-01-15'" : "'2000-01-14'") :
        type == "interval" ? (left ? "'1 hour'" : "'30 minutes'") :
        (left ? "'2000-01-15 12:34:56.000001" : "'2000-01-14 11:34:56.000001") +
            std::string(type == "timestamptz" ? "+00'" : "'");
    return "CAST(" + value + " AS " + type + ")";
}
}
int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::vector<Signature> cases = {
        {"+","date","interval","timestamp","2000-01-15 00:30:00"},
        {"-","date","interval","timestamp","2000-01-14 23:30:00"},
        {"+","timestamp","interval","timestamp","2000-01-15 13:04:56.000001"},
        {"-","timestamp","interval","timestamp","2000-01-15 12:04:56.000001"},
        {"+","timestamptz","interval","timestamptz","2000-01-15 13:04:56.000001+00"},
        {"-","timestamptz","interval","timestamptz","2000-01-15 12:04:56.000001+00"},
        {"+","interval","date","timestamp","2000-01-14 01:00:00"},
        {"+","interval","timestamp","timestamp","2000-01-14 12:34:56.000001"},
        {"+","interval","timestamptz","timestamptz","2000-01-14 12:34:56.000001+00"},
        {"-","date","date","integer","1"},
        {"-","timestamp","timestamp","interval","1 day 01:00:00"},
        {"-","timestamptz","timestamptz","interval","1 day 01:00:00"},
        {"-","timestamp","timestamptz","interval","1 day 01:00:00"},
        {"-","timestamptz","timestamp","interval","1 day 01:00:00"},
        {"+","date","integer","date","2000-01-18"},
        {"-","date","integer","date","2000-01-12"},
        {"+","integer","date","date","2000-01-17"},
        {"+","interval","interval","interval","01:30:00"},
        {"-","interval","interval","interval","00:30:00"}
    };
    for (const auto& c : cases) {
        const auto shared = dbms::arithmetic_detail::resultType(c.op,c.left,c.right);
        const std::string expression = input(c.left,true) + c.op + input(c.right,false);
        check(shared && *shared == c.result,"shared "+expression,shared.value_or("<none>"));
        const auto textual = dbms::ExprHelper::inferResultType(expression);
        check(textual == c.result,"text inference "+expression,textual);
        dbms::SQLParser parser;
        const auto parsed = parser.parse("SELECT "+expression);
        const auto* select = parsed.success ? dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get()) : nullptr;
        const auto ast = select && select->selectList.size() == 1
            ? dbms::ExprHelper::inferParsedResultType(select->selectList[0].expr.get()) : "<parse failed>";
        check(ast == c.result,"AST inference "+expression,ast);
        for (const auto [nullLeft,nullRight] : {std::pair{true,false},std::pair{false,true},std::pair{true,true}}) {
            const std::string sql = input(c.left,true,nullLeft) + c.op + input(c.right,false,nullRight);
            const auto value = dbms::ExprHelper::evalString(sql,{});
            check(value.ok && value.isNull &&
                  dbms::ExprHelper::canonicalResultTypeName(value.typeName) == c.result,
                  "NULL "+sql,value.typeName+" "+value.sqlState);
        }
        const auto value = dbms::ExprHelper::evalString(expression,{});
        check(value.ok && !value.isNull && value.value == c.value &&
              dbms::ExprHelper::canonicalResultTypeName(value.typeName) == c.result,
              "value "+expression,value.typeName+" "+value.value+" "+value.sqlState);
    }
    for (const auto& [op,left,right,wanted] :
         std::vector<std::tuple<std::string,std::string,std::string,std::string>>{
        {"+"," TIMESTAMP WITHOUT TIME ZONE ","INTERVAL","timestamp"},
        {"-","timestamp with time zone","interval","timestamptz"},
        {"+","interval","timestamp without time zone","timestamp"},
        {"+","datetime","interval","timestamp"}
    }) {
        const auto type = dbms::arithmetic_detail::resultType(op,left,right);
        check(type && *type == wanted,"alias "+left+op+right,type.value_or("<none>"));
    }
    for (const auto& [op,left,right] :
         std::vector<std::tuple<std::string,std::string,std::string>>{
        {"+","date","date"}, {"-","interval","date"}, {"*","date","interval"},
        {"+","timestamp","integer"}, {"+","unknown","timestamp"}
    }) check(!dbms::arithmetic_detail::resultType(op,left,right),"no guessed signature "+left+op+right);
    for (const auto& [op,left,right,wanted] :
         std::vector<std::tuple<std::string,std::string,std::string,std::string>>{
        {"+","smallint","bigint","bigint"}, {"+","real","real","real"},
        {"+","real","numeric","double precision"}, {"/","integer","integer","integer"},
        {"^","integer","integer","double precision"}
    }) {
        const auto type = dbms::arithmetic_detail::resultType(op,left,right);
        check(type && *type == wanted,"numeric unchanged "+left+op+right,type.value_or("<none>"));
    }
    constexpr int expected = 147;
    std::cout << "TEMPORAL_TYPES_NATIVE_COMPLETE " << checks << " EXPECTED "
              << expected << " FAILURES " << failures << '\n';
    return checks == expected && !failures ? 0 : 1;
}
