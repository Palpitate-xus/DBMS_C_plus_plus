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
    auto datePlusDays = eval("'2024-03-10'::date + 7");
    assert(datePlusDays.ok && datePlusDays.value == "2024-03-17");

    auto daysPlusDate = eval("7 + '2024-03-10'::date");
    assert(daysPlusDate.ok && daysPlusDate.value == "2024-03-17");

    auto dateMinusDays = eval("'2024-03-10'::date - 7");
    assert(dateMinusDays.ok && dateMinusDays.value == "2024-03-03");

    auto dateUpperOverflow = eval("'9999-12-31'::date + 1");
    assert(dateUpperOverflow.ok && dateUpperOverflow.isNull);

    auto dateLowerOverflow = eval("'0001-01-01'::date - 1");
    assert(dateLowerOverflow.ok && dateLowerOverflow.isNull);

    auto dateIntegerOverflow = eval(
        "'2024-03-10'::date + 9223372036854775807");
    assert(dateIntegerOverflow.ok && dateIntegerOverflow.isNull);

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

    auto dateWithTime = eval("'2024-03-10'::date + '2 hours'::interval");
    assert(dateWithTime.ok && dateWithTime.value == "2024-03-10 02:00:00");

    auto dateWithNegativeTime = eval(
        "'2024-03-10'::date + '-04:05:00'::interval");
    assert(dateWithNegativeTime.ok &&
           dateWithNegativeTime.value == "2024-03-09 19:55:00");

    // hours carry into days
    auto e = eval("'2024-03-10 23:30:00'::timestamp + '2 hours'::interval");
    assert(e.ok && e.value == "2024-03-11 01:30:00");

    // subtraction
    auto f = eval("'2024-03-10 12:00:00'::timestamp - '90 minutes'::interval");
    assert(f.ok && f.value == "2024-03-10 10:30:00");

    // A leading minus on the hour field applies to the complete clock part,
    // including its minutes and seconds.
    auto negativeClock = eval(
        "'2024-03-10 12:00:00'::timestamp + '-04:05:06'::interval");
    assert(negativeClock.ok &&
           negativeClock.value == "2024-03-10 07:54:54");

    auto fractionalDay = eval(
        "'2024-03-10 00:00:00'::timestamp + '1.5 days'::interval");
    assert(fractionalDay.ok &&
           fractionalDay.value == "2024-03-11 12:00:00");

    // across leap day
    auto g = eval("'2024-02-28'::timestamp + '2 days'::interval");
    assert(g.ok && g.value == "2024-03-01 00:00:00");

    // interval + timestamp commutes
    auto h = eval("'1 day'::interval + '2024-03-10'::timestamp");
    assert(h.ok && h.value == "2024-03-11 00:00:00");

    auto dayOverflow = eval(
        "'2024-03-10 00:00:00'::timestamp + "
        "'9223372036854775807 days'::interval");
    assert(dayOverflow.ok && dayOverflow.isNull);

    auto timestampDomainOverflow = eval(
        "'2024-03-10 00:00:00'::timestamp + "
        "'9223372036854775807 microseconds'::interval");
    assert(timestampDomainOverflow.ok && timestampDomainOverflow.isNull);

    auto monthOverflow = eval(
        "'2024-03-10 00:00:00'::timestamp + "
        "'9223372036854775807 months'::interval");
    assert(monthOverflow.ok && monthOverflow.isNull);

    auto upperBoundary = eval(
        "'9999-12-31 23:59:59'::timestamp + '1 second'::interval");
    assert(upperBoundary.ok && upperBoundary.isNull);

    auto inRangeBoundary = eval(
        "'9999-12-30 23:59:59'::timestamp + '1 day'::interval");
    assert(inRangeBoundary.ok &&
           inRangeBoundary.value == "9999-12-31 23:59:59");

    std::cout << "[IV] timestamp ± interval OK" << std::endl;
}

static void test_interval_ops() {
    auto a = eval("'1 day'::interval + '2 hours'::interval");
    assert(a.ok && a.value == "1 day 02:00:00");

    auto b = eval("'1 year 2 mons'::interval + '3 mons'::interval");
    assert(b.ok && b.value == "1 year 5 mons");

    auto c = eval("'90 minutes'::interval - '30 minutes'::interval");
    assert(c.ok && c.value == "01:00:00");

    auto mixedSign = eval(
        "'1 day'::interval - '2 hours'::interval");
    assert(mixedSign.ok && mixedSign.value == "1 day -02:00:00");

    auto allNegative = eval(
        "'0 seconds'::interval - '1 day 2 hours'::interval");
    assert(allNegative.ok &&
           allNegative.value == "-1 day -02:00:00");

    auto mixedRoundTrip = eval(
        "('1 day'::interval - '2 hours'::interval) + "
        "'2 hours'::interval");
    assert(mixedRoundTrip.ok && mixedRoundTrip.value == "1 day");

    auto d = eval("'1 day'::interval * 3");
    assert(d.ok && d.value == "3 days");

    auto e = eval("'2 hours'::interval / 2");
    assert(e.ok && e.value == "01:00:00");

    auto divideByZero = eval("'2 hours'::interval / 0");
    assert(divideByZero.ok && divideByZero.isNull);

    auto addOverflow = eval(
        "'9223372036854775807 microseconds'::interval + "
        "'1 microsecond'::interval");
    assert(addOverflow.ok && addOverflow.isNull);

    auto subtractOverflow = eval(
        "'-9223372036854775807 microseconds'::interval - "
        "'1 microsecond'::interval");
    assert(subtractOverflow.ok && subtractOverflow.isNull);

    auto scaleOverflow = eval(
        "'3000000000 microseconds'::interval * 4000000000");
    assert(scaleOverflow.ok && scaleOverflow.isNull);

    auto safeScale = eval(
        "'4000000000 microseconds'::interval * 2");
    assert(safeScale.ok && safeScale.value == "02:13:20");

    std::cout << "[IV] interval ± interval, * n, / n OK" << std::endl;
}

static void test_at_time_zone() {
    // UTC wall clock read at a POSIX numeric zone (sign inverted: UTC+8 = UTC-8)
    auto a = eval("'2024-06-01 00:30:00'::timestamp AT TIME ZONE 'UTC+8'");
    assert(a.ok && !a.isNull);
    assert(a.value == "2024-06-01 08:30:00+00");

    // bare zone name
    auto b = eval("'2024-06-01 12:00:00'::timestamp AT TIME ZONE 'UTC'");
    assert(b.ok && b.value == "2024-06-01 12:00:00+00");

    // negative offset with minutes (POSIX sign inverted: UTC-05:30 = +05:30)
    auto c = eval("'2024-06-01 10:00:00'::timestamp AT TIME ZONE 'UTC-05:30'");
    assert(c.ok && c.value == "2024-06-01 04:30:00+00");

    // function form (naive rendering, POSIX sign inverted)
    auto d = eval("timezone('UTC+8', '2024-06-01 00:30:00')");
    assert(d.ok && d.value == "2024-05-31 16:30:00");

    // A negative result offset must render its minute component without a
    // second minus sign.
    auto e = eval(
        "timezone('UTC+05:30', "
        "'2024-06-01 10:00:00'::timestamp)");
    assert(e.ok && !e.isNull);
    assert(e.value == "2024-06-01 04:30:00-05:30");

    // PostgreSQL timezone displacements are strictly below 16 hours and the
    // minute field and separators must be well formed.
    auto invalidHour = eval(
        "timezone('UTC+16', '2024-06-01 10:00:00'::timestamp)");
    auto invalidMinute = eval(
        "timezone('UTC+05:60', '2024-06-01 10:00:00'::timestamp)");
    auto invalidShape = eval(
        "timezone('UTC+05:30:20', '2024-06-01 10:00:00'::timestamp)");
    assert(invalidHour.ok && invalidHour.isNull);
    assert(invalidMinute.ok && invalidMinute.isNull);
    assert(invalidShape.ok && invalidShape.isNull);

    std::cout << "[IV] AT TIME ZONE / timezone() OK" << std::endl;
}

int main() {
    test_add_sub();
    test_interval_ops();
    test_at_time_zone();
    std::cout << "[IV] all tests passed" << std::endl;
    return 0;
}
