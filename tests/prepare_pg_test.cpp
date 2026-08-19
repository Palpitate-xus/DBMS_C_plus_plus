// ============================================================================
// prepare_pg_test — PostgreSQL-style prepared statement text handling:
//   PREPARE name [(types)] AS ... $n     (AS-finding, head parsing)
//   EXECUTE name(args)                   (arg splitting incl. nested/quoted)
//   $n substitution incl. out-of-range errors and multi-digit params
//   DEALLOCATE forms are dispatcher-level; helpers covered here.
// ============================================================================

#include "utils/prepared_stmts.h"
#include <cassert>
#include <iostream>

using namespace dbms;

static void test_find_as() {
    // plain
    assert(ps_findPrepareAs("q1 AS SELECT $1") != std::string::npos);
    // with type list: AS inside parens must be ignored
    std::string s = "q1 (int, text) AS SELECT $1 + $2";
    size_t p = ps_findPrepareAs(s);
    assert(p != std::string::npos);
    assert(ps_trim(s.substr(0, p)) == "q1 (int, text)");
    assert(ps_trim(s.substr(p + 4)) == "SELECT $1 + $2");
    // AS inside a quoted string must be ignored
    assert(ps_findPrepareAs("q1 AS SELECT 'a as b'") != std::string::npos);
    {
        std::string s2 = "q (text) AS SELECT $1";
        size_t p2 = ps_findPrepareAs(s2);
        assert(p2 != std::string::npos);
        // the AS inside the parens... none here; verify head+tail
        assert(ps_trim(s2.substr(0, p2)) == "q (text)");
    }
    // no AS at all
    assert(ps_findPrepareAs("q1 FROM 'SELECT ?'") == std::string::npos);
    // dollar-quoted body containing AS
    std::string s3 = "f AS SELECT run($$body as x$$)";
    size_t p3 = ps_findPrepareAs(s3);
    assert(p3 != std::string::npos);
    assert(ps_trim(s3.substr(0, p3)) == "f");
    std::cout << "[PREP-PG] AS finder OK" << std::endl;
}

static void test_head_parse() {
    std::string name;
    std::vector<std::string> types;
    assert(ps_parsePrepareHead("q1", name, types));
    assert(name == "q1" && types.empty());

    assert(ps_parsePrepareHead("q2 (int, text, numeric)", name, types));
    assert(name == "q2");
    assert(types.size() == 3);
    assert(types[0] == "int" && types[1] == "text" && types[2] == "numeric");

    assert(ps_parsePrepareHead("q3( varchar )", name, types));
    assert(name == "q3" && types.size() == 1 && types[0] == "varchar");

    assert(!ps_parsePrepareHead("  ", name, types));
    assert(!ps_parsePrepareHead("bad(unbalanced", name, types));
    std::cout << "[PREP-PG] head parser OK" << std::endl;
}

static void test_arg_split() {
    auto v = ps_splitExecuteArgs("1, 'a', 'b,c'");
    assert(v.size() == 3);
    assert(v[0] == "1" && v[1] == "'a'" && v[2] == "'b,c'");

    auto w = ps_splitExecuteArgs("42");
    assert(w.size() == 1 && w[0] == "42");

    auto x = ps_splitExecuteArgs("'x''y', ARRAY[1,2]");
    assert(x.size() == 2);
    assert(x[0] == "'x''y'");
    assert(x[1] == "ARRAY[1,2]");

    auto y = ps_splitExecuteArgs("'it''s', (1+2)*3");
    assert(y.size() == 2);
    assert(y[1] == "(1+2)*3");
    std::cout << "[PREP-PG] arg splitter OK" << std::endl;
}

static void test_dollar_subst() {
    std::string out, err;
    assert(ps_substituteDollarParams("SELECT $1", {"42"}, out, err));
    assert(out == "SELECT 42");

    assert(ps_substituteDollarParams("INSERT INTO t VALUES ($1, 'x', $2)",
                                     {"'a'", "7"}, out, err));
    assert(out == "INSERT INTO t VALUES ('a', 'x', 7)");

    // multi-digit and reuse
    assert(ps_substituteDollarParams("$1 + $10 + $1",
                                     {"1","2","3","4","5","6","7","8","9","10"}, out, err));
    assert(out == "1 + 10 + 1");

    // out of range
    assert(!ps_substituteDollarParams("SELECT $2", {"1"}, out, err));
    assert(!err.empty());

    // $ not followed by digit stays
    assert(ps_substituteDollarParams("SELECT $tag$", {"1"}, out, err));
    assert(out == "SELECT $tag$");

    // adjacency: $1$2
    assert(ps_substituteDollarParams("$1$2", {"a", "b"}, out, err));
    assert(out == "ab");
    std::cout << "[PREP-PG] $n substitution OK" << std::endl;
}

int main() {
    test_find_as();
    test_head_parse();
    test_arg_split();
    test_dollar_subst();
    std::cout << "[PREP-PG] all tests passed" << std::endl;
    return 0;
}
