// ============================================================================
// fulltext_search_test — full-text search evaluation:
//   to_tsvector: tokenize + position + canonicalize via the engine's
//     tsvector normalizer
//   to_tsquery / plainto_tsquery: word lists ANDed, operator forms kept
//   @@ : lexeme-set match, AND semantics for & queries, OR for |
//   ts_rank: matching coverage monotone in matched terms
// Driven through ExprHelper::evalString (the full SQL expression path).
// ============================================================================

#include "expression/expr_helper.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <iostream>
#include <string>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;
using namespace dbms;

static std::string ev(const std::string& expr) {
    auto r = ExprHelper::evalString(expr, {});
    assert(r.ok && !r.isNull);
    return r.value;
}

static bool evb(const std::string& expr) {
    auto r = ExprHelper::evalString(expr, {});
    assert(r.ok && !r.isNull);
    return r.value == "t";
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    // ------------------------------------------------------------------
    // 1. to_tsvector: tokens lowercased, positions assigned, canonical
    //    output format (lexemes quoted, positions merged).
    // ------------------------------------------------------------------
    std::string v = ev("to_tsvector('The Quick Brown Fox!')");
    assert(v.find("'quick'") != std::string::npos);
    assert(v.find("'brown'") != std::string::npos);
    assert(v.find("'fox'") != std::string::npos);
    assert(v.find(":1") != std::string::npos && v.find(":4") != std::string::npos);
    std::cout << "[FTS] to_tsvector OK (" << v << ")" << std::endl;

    // Duplicate lexemes merge positions.
    v = ev("to_tsvector('cat dog cat')");
    assert(v.find("'cat':1,3") != std::string::npos);
    std::cout << "[FTS] to_tsvector position merge OK (" << v << ")" << std::endl;

    // ------------------------------------------------------------------
    // 2. to_tsquery / plainto_tsquery
    // ------------------------------------------------------------------
    std::string q = ev("to_tsquery('cat & dog')");
    assert(q.find("cat") != std::string::npos && q.find("dog") != std::string::npos);
    q = ev("plainto_tsquery('cat dog')");
    assert(q.find("cat") != std::string::npos && q.find("dog") != std::string::npos);
    q = ev("to_tsquery('cat | dog')");
    assert(q.find("|") != std::string::npos);
    std::cout << "[FTS] to_tsquery OK" << std::endl;

    // ------------------------------------------------------------------
    // 3. @@ evaluation: function-produced vectors against query literals,
    //    and nested expressions on both sides.
    // ------------------------------------------------------------------
    assert(evb("to_tsvector('the quick brown fox jumps') @@ 'fox'"));
    assert(!evb("to_tsvector('the quick brown fox jumps') @@ 'cat'"));
    assert(evb("to_tsvector('the quick brown fox jumps') @@ 'fox & quick'"));
    assert(!evb("to_tsvector('the quick brown fox jumps') @@ 'fox & cat'"));
    assert(evb("to_tsvector('the quick brown fox jumps') @@ 'cat | fox'"));
    assert(evb("to_tsvector('a b c') @@ 'b & c'"));
    assert(!evb("to_tsvector('a b c') @@ 'b & d'"));
    // query built by function
    assert(evb("to_tsvector('a b c') @@ to_tsquery('a & c')"));
    assert(!evb("to_tsvector('a b c') @@ to_tsquery('a & d')"));
    std::cout << "[FTS] @@ match OK" << std::endl;

    // ------------------------------------------------------------------
    // 4. ts_rank: more matched terms -> higher rank; no match -> smallest
    // ------------------------------------------------------------------
    double d1 = std::stod(ev("ts_rank(to_tsvector('a b c'), 'a')"));
    double d2 = std::stod(ev("ts_rank(to_tsvector('a b c'), 'a & b & c')"));
    double d3 = std::stod(ev("ts_rank(to_tsvector('a b c'), 'zzz')"));
    assert(d2 > d1);
    assert(d1 > d3);
    assert(d3 >= 0.0 && d3 <= 1.0);
    std::cout << "[FTS] ts_rank OK (" << d1 << " " << d2 << " " << d3 << ")"
              << std::endl;

    // ------------------------------------------------------------------
    // 5. Stored tsvector literal round-trip: canonical output re-parses
    //    through the engine normalizer idempotently.
    // ------------------------------------------------------------------
    std::string canon = ev("to_tsvector('Zeta alpha zeta')");
    std::string re;
    assert(g_engine.normalizeTsVectorText(canon, re));
    assert(re == canon);
    std::cout << "[FTS] canonical round-trip OK" << std::endl;

    std::cout << "[FTS] all tests passed" << std::endl;
    return 0;
}
