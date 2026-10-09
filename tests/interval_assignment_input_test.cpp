#include "common/DbError.h"
#include "expression/assignment_input.h"
#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    const QueryOutputColumn target{"v", "INTERVAL"};
    const auto check = [&](const std::string& sql, const std::string& type,
                           const std::string& expected) {
        SQLParser parser;
        auto parsed = parser.parseForBinding("SELECT " + sql);
        assert(parsed.success);
        const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
        assert(select && select->selectList.size() == 1);
        const auto* source = select->selectList.front().expr.get();
        const auto original = source->toString();
        std::string actual;
        try { validateAssignmentInput(target, source, type); }
        catch (const DbError& error) { actual = error.sqlState(); }
        if (actual != expected)
            std::cerr << "ASSIGNMENT_INPUT " << sql << " declared " << type
                      << " actual " << actual << " expected " << expected << '\n';
        assert(actual == expected);
        assert(source->toString() == original); // metadata never rewrites a datum
    };
    check("'1 day'", "unknown", "");
    check("'infinity'", "unknown", "");
    check("'+infinity'", "unknown", "");
    check("'-infinity'", "unknown", "");
    check("'INF'", "unknown", "22007");
    check("''", "unknown", "22007");
    check("'NULL'", "unknown", "22007");
    check("'1 day (SQLSTATE 99999)'", "unknown", "22007");
    check("'2147483648 months'", "unknown", "22015");
    check("'178956971 years'", "unknown", "22008");
    check("'-9223372036854775808 microseconds'", "unknown", "");
    check("NULL", "unknown", "");
    check("'1 day'", "text", "42804");
    check("CAST(NULL AS TEXT)", "text", "42804");
    check("1", "integer", "42804");
    check("INTERVAL '1 day'", "interval", "");
    check("INTERVAL '1 fortnight'", "interval", "22007");
    check("CAST('1 fortnight' AS INTERVAL)", "interval", "22007");
    check("CAST('2147483648 months' AS INTERVAL)", "interval", "22015");
    check("CAST(NULL AS INTERVAL)", "interval", "");
    check("'1 fortnight'::INTERVAL", "interval", "22007");
    // An already typed CAST is not an unknown input conversion. Do not fold
    // it, a function call, a scalar child, or a declared nullable column.
    check("CAST(CAST('1 fortnight' AS TEXT) AS INTERVAL)", "interval", "");
    check("CAST(2147483648 AS INTEGER)", "integer", "42804");
    check("missing_writer(1)", "interval", "");
    check("(SELECT missing_writer(1))", "interval", "");
    check("\"Nullable V\"", "interval", "");
    LiteralExpr defaultValue; defaultValue.value = "DEFAULT";
    validateAssignmentInput(target, &defaultValue, "unknown");
    validateAssignmentInput(target, nullptr, "interval");
    bool textArrayRejected = false;
    try { validateAssignmentInput({"v", "interval[]"}, nullptr, "text"); }
    catch (const DbError& error) { textArrayRejected = error.sqlState() == "42804"; }
    assert(textArrayRejected);
    validateAssignmentInput({"v", "integer"}, nullptr, "text");
    bool textNullRejected = false;
    try { validateAssignmentInput(target, nullptr, "text"); }
    catch (const DbError& error) { textNullRejected = error.sqlState() == "42804"; }
    assert(textNullRejected);
    std::cout << "[INTERVAL ASSIGNMENT INPUT] metadata-only unknown input and declared type controls passed\n";
}
