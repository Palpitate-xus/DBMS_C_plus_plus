#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    RowContext row;
    const auto evaluate = [&](const std::string& expression) {
        SQLParser parser;
        auto parsed = parser.parse("SELECT " + expression);
        auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
        assert(select && select->selectList.size() == 1);
        return evaluator.eval(select->selectList.front().expr.get(), row);
    };
    size_t failures = 0;
    for (const auto& [expression, state] : std::vector<std::pair<std::string, std::string>>{
        {"CAST('bad' AS integer)", "22P02"},
        {"CAST('2147483648' AS integer)", "22003"},
        {"CAST('32768' AS smallint)", "22003"},
        {"CAST('9223372036854775808' AS bigint)", "22003"},
        {"CAST(true AS bigint)", "42846"},
        {"CAST('bad' AS real)", "22P02"},
        {"CAST('1e100' AS real)", "22003"},
        {"CAST('bad' AS double precision)", "22P02"},
        {"CAST(true AS real)", "42846"},
        {"CAST('bad' AS numeric)", "22P02"},
        {"CAST('1000' AS numeric(3,0))", "22003"},
        {"CAST(true AS numeric)", "42846"},
        {"CAST('bad' AS money)", "22P02"},
        {"CAST(true AS money)", "42846"},
        {"CAST('bad' AS boolean)", "22P02"},
        {"CAST(1.5 AS boolean)", "42846"},
        {"CAST('bad' AS uuid)", "22P02"},
        {"CAST(1 AS uuid)", "42846"},
        {"CAST('bad' AS date)", "22007"},
        {"CAST('2024-13-01' AS date)", "22008"},
        {"CAST(true AS date)", "42846"},
        {"CAST('\\xGG' AS bytea)", "22P02"},
        {"CAST(CAST('\\x0000000001' AS bytea) AS integer)", "22003"},
        {"CAST(1 AS numeric(0))", "22023"},
        {"CAST('x' AS character(0))", "22023"},
        {"CAST('x' AS text(1))", "42601"}
    }) {
        bool precise = false;
        try {
            (void)evaluate(expression);
            std::cout << "CAST_ERROR unexpected success: " << expression << '\n';
        } catch (const DbError& error) {
            precise = error.sqlState() == state;
            const std::string suffix = " (SQLSTATE " + state + ")";
            assert(std::string(error.what()) == error.message() + suffix);
            std::cout << "CAST_ERROR " << expression << " structured " << error.sqlState() << '\n';
        } catch (const std::exception& error) {
            std::cout << "CAST_ERROR " << expression << " unstructured " << error.what() << '\n';
        }
        if (!precise) ++failures;
    }
    assert(evaluate("CAST('2147483647' AS integer)").value == "2147483647");
    assert(evaluate("CAST('-9223372036854775808' AS bigint)").value == "-9223372036854775808");
    assert(evaluate("CAST('yes' AS boolean)").value == "t");
    assert(evaluate("CAST('2024-02-29' AS date)").value == "2024-02-29");
    const auto null = evaluate("CAST(NULL AS integer)");
    assert(null.isNull && null.typeName == "integer");
    std::cout << "[CAST STRUCTURED ERROR] failures=" << failures << '\n';
    return failures ? 1 : 0;
}
