#include "parser/parser.h"
#include "common/DbError.h"
#include <iostream>
#include <string>
#include <vector>

namespace {
int checks = 0, failures = 0;
void check(bool ok, const std::string& label) {
    ++checks;
    if (!ok) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}
struct Case { const char* argument; bool valid; };
}
int main() {
    const std::vector<Case> cases = {
        {"1", true},
        {"1, 2", true},
        {"1/0", true},
        {"missing_argument_column", true},
        {"no_such_function(99)", true},
        {"(SELECT 1)", true},
        {"(SELECT missing_argument_column)", true},
        {"$0", true},
        {"$1", true},
        {"$2", true},
        {"$000", true},
        {"$2147483647", true},
        {"ARRAY[1,2]", true},
        {"ARRAY[]", true},
        {"ROW()", true},
        {"ROW(1,2)", true},
        {"(1,2)", true},
        {"f()", true},
        {"f(1,2)", true},
        {"missing_schema.fn(missing_column)", true},
        {"CASE WHEN true THEN 1 ELSE 2 END", true},
        {"CASE 1 WHEN 1 THEN missing_column END", true},
        {"NULL", true},
        {"true", true},
        {"UNKNOWN", true},
        {"1+2*3", true},
        {"CAST(NULL AS integer)", true},
        {"NULL::missing_type", true},
        {"missing_type 'bad'", true},
        {"CURRENT_TIMESTAMP(999)", true},
        {"MIN(1)", true},
        {"count(*)", true},
        {"sum(1) OVER ()", true},
        {"lead(1) OVER (ORDER BY 1)", true},
        {"1 IN (1,2)", true},
        {"1 = ANY(ARRAY[1,2])", true},
        {"EXISTS (SELECT 1)", true},
        {"EXISTS (SELECT 1 FROM missing_table)", true},
        {"ARRAY(SELECT 1)", true},
        {"1e9999", true},
        {"'x,y'", true},
        {"$$x,y($0)$$", true},
        {"$_tag$x,y($0)$_tag$", true},
        {"E'it\\'s a,b'", true},
        {"/* comment */1", true},
        {"1 /* tail */", true},
        {"1 -- line\n, 2", true},
        {"1 /* outer /* inner */ rest */ , 2", true},
        {"\"quoted,column\"", true},
        {"value$0", true},
        {"(SELECT)", true},
        {"", false},
        {",", false},
        {"1,", false},
        {",1", false},
        {"1,,2", false},
        {"+", false},
        {"1+", false},
        {"1 AND", false},
        {"NOT", false},
        {"(1", false},
        {"1)", false},
        {"ARRAY[1,]", false},
        {"ARRAY[1", false},
        {"ARRAY[,1]", false},
        {"[1]", false},
        {"f(,1)", false},
        {"f(1,)", false},
        {"f(1,,2)", false},
        {"(SELECT 1,)", false},
        {"(SELECT 1+)", false},
        {"1 AS x", false},
        {"1 x", false},
        {"1 FROM t", false},
        {"1 ORDER BY 1", false},
        {"SELECT 1", false},
        {"1; SELECT 2", false},
        {"CASE WHEN true THEN 1", false},
        {"CASE WHEN true 1 END", false},
        {"CASE ELSE 1 END", false},
        {"CAST(1 AS)", false},
        {"1::", false},
        {"'unterminated", false},
        {"/* comment */", false},
        {"f(1", false},
        {"(1,)", false},
        {"*", false},
        {"ROW(1,)", false},
        {"DEFAULT", true},
        {"DEFAULT+1", true},
        {"f(DEFAULT)", true},
        {"ROW(DEFAULT)", true},
        {"ARRAY[DEFAULT]", true},
        {"(SELECT FROM missing_table)", true},
        {"(SELECT WHERE true)", true},
        {"(SELECT ALL)", true},
        {"f(VARIADIC ARRAY[1,2])", true},
        {"f(a=>1)", true},
        {"f(a:=1)", true},
        {"(SELECT DISTINCT)", false},
        {"(SELECT DISTINCT ON (1))", false},
    };
    for (const auto& entry : cases) {
        const std::string sql = std::string("EXECUTE syntax_probe(") + entry.argument + ")";
        dbms::SQLParser parser;
        dbms::ParseResult parsed;
        try { parsed = parser.parseForBinding(sql); }
        catch (const dbms::DbError& error) {
            parsed.sqlState = error.sqlState(); parsed.error = error.what();
        }
        check(parsed.isValid() == entry.valid, "validity " + sql);
        if (entry.valid) {
            check(parsed.stmt && parsed.stmt->command == dbms::SqlCommand::Execute,
                  "statement kind " + sql);
        } else {
            check(!parsed.stmt, "no accepted statement " + sql);
            check(parsed.sqlState == "42601" && !parsed.error.empty(), "syntax diagnostic " + sql);
            check(parser.parseForBinding("EXECUTE syntax_probe(1)").isValid(), "parser recovery " + sql);
        }
    }
    for (const auto& sql : {"EXECUTE syntax_probe", "EXECUTE missing_name(unknown_column)",
                           "EXECUTE \"quoted name\"(1)", "EXECUTE syntax_probe USING 1"}) {
        dbms::SQLParser parser;
        check(parser.parseForBinding(sql).isValid(), std::string("accepted envelope ") + sql);
    }
    dbms::SQLParser parser;
    check(!parser.parseForBinding("SELECT $0").isValid(), "ordinary binding parameter zero unchanged");
    check(parser.parseForBinding("SELECT 1").isValid(), "ordinary SELECT remains available");
    std::cout << "EXECUTE_SYNTAX_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
