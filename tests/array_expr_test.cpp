// ============================================================================
// array_expr_test — SQL array expression semantics:
//   ARRAY[e1,e2,...] constructor
//   arr[i]           1-based indexing (negative = from end; OOR -> NULL)
//   arr[lo:hi]       inclusive slice, open bounds, result stays an array
//   arr @> arr       element containment; <@ contained-by
//   arr[i] on nested arrays
// Driven through ExprHelper::evalString (full SQL expression path).
// ============================================================================

#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>
#include <string>

using namespace dbms;

static ExprEvalResult eval(const std::string& exprText) {
    return ExprHelper::evalString(exprText, {});
}

static void test_construct() {
    auto v = eval("ARRAY[1,2,3]");
    assert(v.ok && !v.isNull);
    assert(v.value == "{1,2,3}");

    auto s = eval("ARRAY['a','b']");
    // PostgreSQL quotes only when needed: simple identifiers/numbers stay bare.
    assert(s.ok && s.value == "{a,b}");

    // strings with spaces get quoted
    auto q = eval("ARRAY['big apple','pear']");
    assert(q.ok);
    assert(q.value == "{\"big apple\",pear}");

    // NULL element
    auto n = eval("ARRAY[1,NULL,3]");
    assert(n.ok && n.value == "{1,NULL,3}");

    // expressions as elements
    auto e = eval("ARRAY[1+1,2*3]");
    assert(e.ok && e.value == "{2,6}");

    // empty array
    auto z = eval("ARRAY[]");
    assert(z.ok);

    std::cout << "[ARRAY] ARRAY[] constructor OK" << std::endl;
}

static void test_index() {
    auto a = eval("ARRAY[10,20,30][2]");
    assert(a.ok && a.value == "20");

    // literal array text indexing
    auto b = eval("'{5,6,7}'[1]");
    assert(b.ok && b.value == "5");

    // negative index = from the end
    auto c = eval("ARRAY[10,20,30][-1]");
    assert(c.ok && c.value == "30");

    // out of range -> NULL
    auto d = eval("ARRAY[10,20][5]");
    assert(d.ok && d.isNull);

    // nested array element
    auto n = eval("ARRAY[ARRAY[1,2],ARRAY[3,4]][2]");
    assert(n.ok);
    assert(n.value == "{3,4}");

    std::cout << "[ARRAY] subscripting OK" << std::endl;
}

static void test_slice() {
    auto a = eval("ARRAY[10,20,30,40][2:3]");
    assert(a.ok && a.value == "{20,30}");

    // open upper bound
    auto b = eval("ARRAY[10,20,30,40][2:]");
    assert(b.ok && b.value == "{20,30,40}");

    // open lower bound
    auto c = eval("ARRAY[10,20,30,40][:2]");
    assert(c.ok && b.value != c.value);
    assert(c.value == "{10,20}");

    // literal array slicing
    auto d = eval("'{1,2,3,4,5}'[2:4]");
    assert(d.ok && d.value == "{2,3,4}");

    // bounds beyond ends are clamped
    auto e = eval("ARRAY[1,2][0:9]");
    assert(e.ok && e.value == "{1,2}");

    std::cout << "[ARRAY] slicing OK" << std::endl;
}

static void test_contains() {
    auto t1 = eval("ARRAY[1,2,3] @> ARRAY[2]");
    assert(t1.ok && t1.value == "t");
    auto t2 = eval("ARRAY[1,2,3] @> ARRAY[4]");
    assert(t2.ok && t2.value == "f");
    auto t3 = eval("ARRAY[1,2,3] @> ARRAY[1,2,3]");
    assert(t3.ok && t3.value == "t");

    // literal arrays
    auto t4 = eval("'{a,b,c}' @> '{b}'");
    assert(t4.ok && t4.value == "t");

    // <@ is @> swapped
    auto t5 = eval("ARRAY[2] <@ ARRAY[1,2,3]");
    assert(t5.ok && t5.value == "t");

    // nested arrays match by canonical text
    auto t6 = eval("ARRAY[ARRAY[1,2],3] @> ARRAY[ARRAY[1,2]]");
    assert(t6.ok && t6.value == "t");

    std::cout << "[ARRAY] @> / <@ OK" << std::endl;
}

static void test_json_still_works() {
    // The SQL-array vs JSON disambiguation must not break JSON containment.
    auto j = eval("'{\"n\":42}' @> '{\"n\":42}'");
    assert(j.ok && j.value == "t");
    auto j2 = eval("'{\"a\":{\"b\":1}}' @> '{\"a\":{\"b\":1}}'");
    assert(j2.ok && j2.value == "t");
    // JSON access operators still parse next to array syntax
    auto j3 = eval("'{\"k\":\"v\"}' ->> 'k'");
    assert(j3.ok && j3.value == "v");

    std::cout << "[ARRAY] JSON coexistence OK" << std::endl;
}

int main() {
    test_construct();
    test_index();
    test_slice();
    test_contains();
    test_json_still_works();
    std::cout << "[ARRAY] all tests passed" << std::endl;
    return 0;
}
