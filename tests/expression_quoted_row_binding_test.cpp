#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <set>
#include <string>

using Values = std::map<std::string, std::string>;

static void expect(const std::string& expression, const Values& row,
                   const Values& types, const std::string& expected,
                   const std::string& type, const std::set<std::string>& nulls = {},
                   bool isNull = false) {
    const auto result = dbms::ExprHelper::evalStringWithNulls(
        expression, row, nulls, types);
    if (!result.ok || result.isNull != isNull || result.typeName != type ||
        (!isNull && result.value != expected)) {
        std::cerr << expression << ": ok=" << result.ok << " null=" << result.isNull
                  << " type=" << result.typeName << " value=" << result.value
                  << " error=" << result.error << '\n';
        assert(false);
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const Values row{{"F", "2"}, {"f", "1"}};
    const Values types{{"F", "integer"}, {"f", "integer"}};
    expect("\"F\"+f", row, types, "3", "integer");
    expect("\"F\"+F", row, types, "3", "integer");
    expect("CAST(\"F\" AS INT)+f", row, types, "3", "integer");
    expect("\"F\"::INT+f", row, types, "3", "integer");
    expect("CASE WHEN \"F\"=2 THEN f+10 ELSE 99 END", row, types, "11", "integer");
    expect("coalesce(\"F\",9)+f", {{"F", ""}, {"f", "2"}}, types,
           "11", "integer", {"F"});
    expect("\"F\"+f", {{"F", ""}, {"f", "2"}}, types,
           "", "integer", {"F"}, true);
    expect("\"F\"+f", {{"F", "5000000000"}, {"f", "2"}},
           {{"F", "bigint"}, {"f", "integer"}}, "5000000002", "bigint");
    expect("\"F\"||':'||CAST(f AS TEXT)", {{"F", "00123"}, {"f", "2"}},
           {{"F", "text"}, {"f", "integer"}}, "00123:2", "text");
    expect("\"F\"||f", {{"F", ""}, {"f", "x"}},
           {{"F", "text"}, {"f", "text"}}, "x", "text");
    expect("\"Q\"\"Col\"+\"Value Name\"+\"a.b\"",
           {{"Q\"Col", "1"}, {"Value Name", "2"}, {"a.b", "3"}},
           {{"Q\"Col", "integer"}, {"Value Name", "integer"}, {"a.b", "integer"}},
           "6", "integer");
    expect("r.\"F\"+r.f", {{"r.F", "2"}, {"r.f", "1"}, {"F", "99"}},
           {{"r.F", "integer"}, {"r.f", "integer"}, {"F", "integer"}}, "3", "integer");
    expect("\"R\".\"F\"+r.f", {{"R.F", "2"}, {"r.f", "1"}},
           {{"R.F", "integer"}, {"r.f", "integer"}}, "3", "integer");
    // Scalar callers may supply only a prevalidated relation's bare columns.
    expect("r.\"F\"+r.f", row, types, "3", "integer");
    expect("NEW.id+old.id", {{"new.id", "2"}, {"old.id", "1"}},
           {{"new.id", "integer"}, {"old.id", "integer"}}, "3", "integer");
    expect("\"C\" COLLATE \"C\"", {{"C", "value"}, {"c", "wrong"}},
           {{"C", "text"}, {"c", "text"}}, "value", "text");
    expect("EXTRACT(year FROM \"D\")+year", {{"D", "2026-10-06"}, {"year", "1"}},
           {{"D", "date"}, {"year", "integer"}}, "2027", "numeric");
    expect("EXTRACT (year FROM \"D\")", {{"D", "2026-10-06"}, {"year", "bad field"}},
           {{"D", "date"}, {"year", "text"}}, "2026", "numeric");
    expect("CAST(\"F\" AS INT)", {{"F", "2"}, {"int", "99"}},
           {{"F", "integer"}, {"int", "integer"}}, "2", "integer");
    expect("\"__dbms_helper_value_0\"+\"F\"", {{"__dbms_helper_value_0", "4"}, {"F", "2"}},
           {{"__dbms_helper_value_0", "integer"}, {"F", "integer"}}, "6", "integer");
    for (const std::string expression : {"\"Missing\"", "abs(\"Missing\")", "no_such_function(\"F\")"}) {
        const auto result = dbms::ExprHelper::evalStringWithNulls(expression, row, {}, types);
        assert(!result.ok);
    }
    // A quoted identifier that guesses the private slot cannot access a value.
    const auto privateGuess = dbms::ExprHelper::evalStringWithNulls(
        "\"\x01helper_value_0\"", row, {}, types);
    assert(!privateGuess.ok);
    const auto wrongQuotedRange = dbms::ExprHelper::evalStringWithNulls(
        "r.\"F\"", {{"R.F", "2"}}, {}, {{"R.F", "integer"}});
    assert(!wrongQuotedRange.ok);
    const auto qualifiedExtractField = dbms::ExprHelper::evalStringWithNulls(
        "EXTRACT (missing.year FROM \"D\")", {{"D", "2026-10-06"}}, {}, {{"D", "date"}});
    assert(!qualifiedExtractField.ok);
    std::cout << "[EXPRESSION QUOTED ROW BINDING] exact identities, NULL and typed roles passed\n";
}
