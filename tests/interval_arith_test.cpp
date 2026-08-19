// ============================================================================
// interval_arith_test — interval arithmetic and time zones:
//   timestamp ± interval, date ± interval (calendar-aware month/day math)
//   interval ± interval, interval * n, interval / n
//   AT TIME ZONE 'zone' (offset-based zone model), timezone(zone, ts)
// ============================================================================

#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>
#include <string>

using namespace dbms;

static ExprEvalResult eval(const std::string& exprText) {
    return ExprHelper::evalString(exprText, {});
}

static void test_add_sub() {
    // days
    auto a = eval("'2024-03-10'::timestamp + '1 day'::interval");
    assert(a.ok && !a.isNull);
    assert(a.value == "2024-03-11 00:00:00");

    // month rollover with day clamp (Jan 31 + 1 month -> Feb 29, leap year)
    auto b = eval("'2024-01-31'::timestamp + '1 month'::interval");
    assert(b.ok && b.value == "2024-02-29 00:00:00");

    // non-leap clamp
    auto c = eval("'2023-01-31'::timestamp + '1 month'::interval");
    assert(c.ok && c.value == "2023-02-28 00:00:00");

    // year rollover
    auto d = eval("'2024-12-15'::timestamp + '2 months'::interval");
    assert(d.ok && d.value == "2025-02-15 00:00:00");

    // hours carry into days
    auto e = eval("'2024-03-10 23:30:00'::timestamp + '2 hours'::interval");
    assert(e.ok && e.value == "2024-03-11 01:30:00");

    // subtraction
    auto f = eval("'2024-03-10 12:00:00'::timestamp - '90 minutes'::interval");
    assert(f.ok && f.value == "2024-03-10 10:30:00");

    // across leap day
    auto g = eval("'2024-02-28'::timestamp + '2 days'::interval");
    assert(g.ok && g.value == "2024-03-01 00:00:00");

    // interval + timestamp commutes
    auto h = eval("'1 day'::interval + '2024-03-10'::timestamp");
    assert(h.ok && h.value == "2024-03-11 00:00:00");

    std::cout << "[IV] timestamp ± interval OK" << std::endl;
}

static void test_interval_ops() {
    auto a = eval("'1 day'::interval + '2 hours'::interval");
    assert(a.ok && a.value == "1 day 02:00:00");

    auto b = eval("'1 year 2 mons'::interval + '3 mons'::interval");
    assert(b.ok && b.value == "1 year 5 mons");

    auto c = eval("'90 minutes'::interval - '30 minutes'::interval");
    assert(c.ok && c.value == "01:00:00");

    auto d = eval("'1 day'::interval * 3");
    assert(d.ok && d.value == "3 days");

    auto e = eval("'2 hours'::interval / 2");
    assert(e.ok && e.value == "01:00:00");

    std::cout << "[IV] interval ± interval, * n, / n OK" << std::endl;
}

static void test_at_time_zone() {
    // UTC wall clock rendered at UTC+8
    auto a = eval("'2024-06-01 00:30:00'::timestamp AT TIME ZONE 'UTC+8'");
    assert(a.ok && !a.isNull);
    assert(a.value == "2024-06-01 08:30:00");

    // bare zone name
    auto b = eval("'2024-06-01 12:00:00'::timestamp AT TIME ZONE 'UTC'");
    assert(b.ok && b.value == "2024-06-01 12:00:00");

    // negative offset with minutes
    auto c = eval("'2024-06-01 10:00:00'::timestamp AT TIME ZONE 'UTC-05:30'");
    assert(c.ok && c.value == "2024-06-01 04:30:00");

    // function form
    auto d = eval("timezone('UTC+8', '2024-06-01 00:30:00')");
    assert(d.ok && d.value == "2024-06-01 08:30:00");

    std::cout << "[IV] AT TIME ZONE / timezone() OK" << std::endl;
}

int main() {
    test_add_sub();
    test_interval_ops();
    test_at_time_zone();
    std::cout << "[IV] all tests passed" << std::endl;
    return 0;
}
