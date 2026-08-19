// ============================================================================
// json_ops_test — PostgreSQL JSON/JSONB access operators in expressions:
//   ->   field/array index, result kept as JSON
//   ->>  same path, result as unquoted text (JSON null -> SQL NULL)
//   #>   text path 'a,b,0', result as JSON
//   #>>  same path, result as text
//   @>   containment (object/array/scalar)
//   <@   contained-by (arguments swapped @>)
// Driven through ExprHelper::evalString, i.e. the full SQL expression path.
// ============================================================================

#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>
#include <string>

using namespace dbms;

static const std::string kDoc =
    "'{\"user\":{\"name\":\"ada\",\"tags\":[\"x\",\"y\"]},\"n\":42}'";

static ExprEvalResult eval(const std::string& exprText) {
    return ExprHelper::evalString(exprText, {});
}

static void test_arrow() {
    // object field -> JSON form (string stays quoted)
    auto v = eval(kDoc + " -> 'user' -> 'name'");
    assert(v.ok && !v.isNull);
    assert(v.value == "\"ada\"");

    // ->> unquotes
    auto t = eval(kDoc + " -> 'user' ->> 'name'");
    assert(t.ok && !t.isNull);
    assert(t.value == "ada");

    // array index
    auto a = eval(kDoc + " -> 'user' -> 'tags' -> 1");
    assert(a.ok && a.value == "\"y\"");
    auto at = eval(kDoc + " -> 'user' -> 'tags' ->> 1");
    assert(at.ok && at.value == "y");

    // number field via ->> is text
    auto n = eval(kDoc + " ->> 'n'");
    assert(n.ok && n.value == "42");

    // missing key -> SQL NULL for both forms
    auto m1 = eval(kDoc + " -> 'nope'");
    assert(m1.ok && m1.isNull);
    auto m2 = eval(kDoc + " ->> 'nope'");
    assert(m2.ok && m2.isNull);

    std::cout << "[JSON-OPS] -> / ->> OK" << std::endl;
}

static void test_path() {
    auto v = eval(kDoc + " #> 'user,name'");
    assert(v.ok && v.value == "\"ada\"");
    auto t = eval(kDoc + " #>> 'user,name'");
    assert(t.ok && t.value == "ada");

    // path with array index
    auto ai = eval(kDoc + " #> 'user,tags,0'");
    assert(ai.ok && ai.value == "\"x\"");

    // missing path -> NULL
    auto m = eval(kDoc + " #> 'user,missing'");
    assert(m.ok && m.isNull);

    std::cout << "[JSON-OPS] #> / #>> OK" << std::endl;
}

static void test_contains() {
    // object containment
    auto c1 = eval(kDoc + " @> '{\"n\":42}'");
    assert(c1.ok && c1.value == "t");
    auto c2 = eval(kDoc + " @> '{\"n\":43}'");
    assert(c2.ok && c2.value == "f");
    // nested
    auto c3 = eval(kDoc + " @> '{\"user\":{\"name\":\"ada\"}}'");
    assert(c3.ok && c3.value == "t");

    // array containment
    auto c4 = eval(kDoc + " -> 'user' -> 'tags' @> '[\"x\"]'");
    assert(c4.ok && c4.value == "t");
    auto c5 = eval(kDoc + " -> 'user' -> 'tags' @> '[\"z\"]'");
    assert(c5.ok && c5.value == "f");

    // <@ is @> with swapped args
    auto c6 = eval("'{\"n\":42}' <@ " + kDoc);
    assert(c6.ok && c6.value == "t");

    // scalar containment
    auto c7 = eval("'42' @> '42'");
    assert(c7.ok && c7.value == "t");

    std::cout << "[JSON-OPS] @> / <@ OK" << std::endl;
}

static void test_precedence() {
    // JSON ops bind tighter than ||: doc->>'name' || '!' concatenates text.
    auto v = eval(kDoc + " -> 'user' ->> 'name' || '!!'");
    assert(v.ok && v.value == "ada!!");

    // comparison operates on the ->> text result
    auto b = eval(kDoc + " ->> 'n' = '42'");
    assert(b.ok && b.value == "t");

    // chained -> left-associative
    auto c = eval(kDoc + " -> 'user' -> 'tags' ->> 0");
    assert(c.ok && c.value == "x");

    std::cout << "[JSON-OPS] precedence/chaining OK" << std::endl;
}

int main() {
    test_arrow();
    test_path();
    test_contains();
    test_precedence();
    std::cout << "[JSON-OPS] all tests passed" << std::endl;
    return 0;
}
