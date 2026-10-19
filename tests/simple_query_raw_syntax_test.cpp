#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// Pure parser controls only. Protocol regression owns prefix-message ordering.
// C++17 fallback compiles accepted old headers and uses ordinary parser behavior;
// it does not invent the new API or claim the old wire route was already fixed.
namespace {
using namespace dbms;
struct Checks {
    size_t controls=0,failures=0;
    template<class F>void test(const std::string& label,F action) {
        ++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"SIMPLE_QUERY_RAW_SYNTAX_FAIL "<<controls<<' '<<label<<" : "<<error.what()<<'\n';}
        catch(...){++failures;std::cerr<<"SIMPLE_QUERY_RAW_SYNTAX_FAIL "<<controls<<' '<<label<<" : unknown exception\n";}
    }
};
void require(bool value,const std::string& message){if(!value)throw std::runtime_error(message);}
template<class T,class=void>struct HasRawSyntax:std::false_type{};
template<class T>struct HasRawSyntax<T,std::void_t<decltype(std::declval<T&>().parseRawSyntax(std::declval<const std::string&>()))>>:std::true_type{};
template<class T>std::optional<std::string> proof(T& parser,const std::string& sql){
    if constexpr(HasRawSyntax<T>::value)return parser.parseRawSyntax(sql);
    else {const auto old=parser.parseForBinding(sql);if(!old.success)return old.error;return std::nullopt;}
}
}
int main(){
    using namespace dbms;Checks checks;SQLParser parser;
    const std::vector<std::string> bad={
        "garbage",
        "GARBAGE",
        "123",
        "'orphan'",
        "$$orphan$$",
        ")",
        "(123)",
        "_not_a_command",
        "DROP DOMAIN",
        "drop domain",
        "/* trivia */ DROP DOMAIN",
        "DROP DOMAIN IF EXISTS",
        "DROP DOMAIN \"\"",
        "DROP DOMAIN public.\"\"",
        "DROP DOMAIN x CASCADE RESTRICT",
        "DROP DOMAIN x RESTRICT CASCADE",
        "DROP DOMAIN x garbage",
        "DROP DOMAIN x CASCADE garbage",
        "SET app.key",
        "SET app.key TO",
        "SET app..key TO 1",
        "SET app. TO 1",
        "SELECT '",
        "SELECT \"",
        "/*",
        "SELECT $x$unterminated",
        "SELECT 1 /* unmatched",
        "SELECT E'abc\\'"};
    const std::vector<std::string> deferOrValid={
        "SELECT 1",
        "VALUES (1)",
        "SELECT missing_column FROM future_table",
        "CREATE TABLE future_table(id INTEGER)",
        "INSERT INTO future_table VALUES(1)",
        "CREATE SCHEMA future_schema",
        "CREATE DOMAIN future_domain AS INTEGER",
        "CREATE TYPE future_pair AS(v INTEGER)",
        "CREATE FUNCTION future_function() RETURNS INTEGER LANGUAGE SQL AS $$SELECT 7$$",
        "SELECT future_function()",
        "SET search_path=public,pg_catalog",
        "SET SESSION application_name TO 'raw;syntax'",
        "SET app.key = 'literal;value'",
        "ALTER TYPE future_pair ADD ATTRIBUTE v2 TEXT",
        "ALTER TYPE future_pair RENAME TO \"Moved.Pair\"",
        "ALTER TYPE future_pair RENAME ATTRIBUTE v TO v3",
        "ALTER TYPE future_pair ALTER ATTRIBUTE v TYPE TEXT",
        "ALTER TYPE future_pair DROP ATTRIBUTE v",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT s",
        "ROLLBACK TO SAVEPOINT s",
        "RELEASE SAVEPOINT s",
        "SELECT 'literal;value'",
        "SELECT E'escaped\\\\;value'",
        "SELECT \"semi;column\" FROM \"semi;table\"",
        "SELECT $$dollar;value$$",
        "SELECT $tag$body;-- /* literal */$tag$",
        "/* outer /* nested ; */ comment */ SELECT 1 -- trailing;",
        "WITH x AS(SELECT 1),x AS(SELECT 2) SELECT * FROM x",
        "SELECT 1 FETCH FIRST 1 ROW WITH TIES",
        "CREATE DOMAIN reached_conflict AS INTEGER NULL NOT NULL",
        "CREATE DOMAIN reached_conflict AS INTEGER NOT NULL NULL",
        "CREATE DOMAIN reached_conflict AS INTEGER UNIQUE",
        "RESTORE DATABASE later FROM '/tmp/never_execute_restore'",
        "CLEAR PLAN CACHE",
        "BACKUP DATABASE later TO '/tmp/never_execute_backup'",
        "CANCEL MAINTENANCE JOB 1",
        "DUMP DATABASE later TO '/tmp/never_execute_dump.sql'",
        "PG_SWITCH_WAL()",
        "SWITCH WAL",
        "INSERT INTO future_table VALUES(1) ON CONFLICT DO UPDATE SET id=1",
        "SELECT unknown_function(1)",
        "SELECT 1/0",
        "SELECT 'bad'::integer",
        "SELECT 1 LIMIT (1+1)",
        "SELECT 1 LIMIT unknown_function()",
        "SELECT 1 LIMIT -1",
        "SELECT 1 OFFSET (1+1)",
        "SELECT 1 OFFSET -1",
        "SELECT 1 FETCH FIRST (1+1) ROWS ONLY",
        "",
        " ",
        "-- comment ;",
        "/* comment ; */",
        "/* outer /* nested */ comment */",
        "\n--comment\n"};
    for(const auto& sql:bad)checks.test("raw/reached grammar boundary: "+sql,[&]{
        if(sql.rfind("SET ",0)==0) {
            const auto reached=parser.parseForBinding(sql);
            require(!reached.success&&reached.sqlState=="42601","reached SET grammar check changed");
            require(!proof(parser,sql),"finite preflight must defer SET to its reached owner");
        } else require(proof(parser,sql).has_value(),"raw error was not proved");
    });
    for(const auto& sql:deferOrValid)checks.test("raw syntax is valid or deferred: "+sql,[&]{require(!proof(parser,sql),"semantic/Main-owned/valid statement was rejected early");});
    size_t effects=0;ExprEvaluator evaluator;
    evaluator.registerFunction("raw_guard_effect",[&](const std::vector<ExprValue>&){++effects;return ExprValue("integer","1",false);});
    for(size_t index=0;index<16;++index)checks.test("pure parse cannot execute opaque callback "+std::to_string(index),[&]{
        const auto sql=index%2?"SELECT raw_guard_effect()":"CREATE FUNCTION untouched() RETURNS INTEGER LANGUAGE SQL AS $$SELECT raw_guard_effect()$$";
        require(!proof(parser,sql),"valid routine syntax rejected");require(effects==0,"raw syntax discovery executed callback");
    });
    checks.test("ordinary parse still reports duplicate CTE analysis",[&]{const auto result=parser.parseForBinding("WITH x AS(SELECT 1),x AS(SELECT 2) SELECT * FROM x");require(!result.success&&result.error.find("42712")!=std::string::npos,"analysis owner changed");});
    checks.test("ordinary parse still owns WITH TIES analysis",[&]{const auto result=parser.parseForBinding("SELECT 1 FETCH FIRST 1 ROW WITH TIES");require(!result.success&&result.error.find("WITH TIES")!=std::string::npos,"WITH TIES owner changed");});
    checks.test("raw context restores after proven failure",[&]{(void)proof(parser,"DROP DOMAIN x CASCADE garbage");const auto result=parser.parseForBinding("WITH x AS(SELECT 1),x AS(SELECT 2) SELECT * FROM x");require(!result.success&&result.error.find("42712")!=std::string::npos,"raw context leaked");});
    checks.test("raw context restores after deferred parser shape",[&]{(void)proof(parser,"SELECT 1 LIMIT unknown_function()");const auto result=parser.parseForBinding("WITH x AS(SELECT 1),x AS(SELECT 2) SELECT * FROM x");require(!result.success&&result.error.find("42712")!=std::string::npos,"deferred context leaked");});
    constexpr size_t expected=106;
    if(checks.controls!=expected){++checks.failures;std::cerr<<"SIMPLE_QUERY_RAW_SYNTAX_INVENTORY expected "<<expected<<" got "<<checks.controls<<'\n';}
    std::cout<<"SIMPLE_QUERY_RAW_SYNTAX_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expected<<" FAILURES "<<checks.failures<<'\n';
    return checks.failures?1:0;
}
