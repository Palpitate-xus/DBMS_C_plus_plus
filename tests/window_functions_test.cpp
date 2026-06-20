// ============================================================================
// Window Function Evaluator Test — Phase 4 Wave 2
// ============================================================================

#include "expression/WindowEvaluator.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>

static dbms::WindowRow makeRow(const std::string& part, int ord) {
    dbms::WindowRow r;
    r.partitionKeys.push_back(dbms::ExprValue("text", part, false));
    r.orderKeys.push_back(dbms::ExprValue("integer", std::to_string(ord), false));
    return r;
}

static void test_ranking() {
    std::vector<dbms::WindowRow> rows = {
        makeRow("A", 10),
        makeRow("A", 20),
        makeRow("A", 20),
        makeRow("A", 30),
        makeRow("B", 5),
        makeRow("B", 15)
    };

    auto rn = dbms::WindowEvaluator::rowNumber(rows);
    assert(rn.size() == 6);
    assert(rn[0].value == "1");
    assert(rn[5].value == "2");

    auto rk = dbms::WindowEvaluator::rank(rows);
    assert(rk[0].value == "1");
    assert(rk[1].value == "2");
    assert(rk[2].value == "2");
    assert(rk[3].value == "4");

    auto dr = dbms::WindowEvaluator::denseRank(rows);
    assert(dr[0].value == "1");
    assert(dr[1].value == "2");
    assert(dr[2].value == "2");
    assert(dr[3].value == "3");

    std::cout << "[WINDOW] ranking OK" << std::endl;
}

static void test_lag_lead() {
    std::vector<dbms::WindowRow> rows = {
        makeRow("A", 10),
        makeRow("A", 20),
        makeRow("A", 30)
    };
    dbms::ExprValue def("integer", "0", false);

    auto lg = dbms::WindowEvaluator::lag(rows, 1, def);
    assert(lg[0].value == "0");
    assert(lg[1].value == "10");
    assert(lg[2].value == "20");

    auto ld = dbms::WindowEvaluator::lead(rows, 1, def);
    assert(ld[0].value == "20");
    assert(ld[1].value == "30");
    assert(ld[2].value == "0");

    std::cout << "[WINDOW] lag/lead OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_ranking();
    test_lag_lead();
    std::cout << "[WINDOW] all passed" << std::endl;
    return 0;
}
