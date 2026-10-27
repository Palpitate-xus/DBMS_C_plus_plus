#include "utils/plpgsql.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <iostream>

using namespace dbms;

static PlPgsqlQueryResult scalar(const ExprEvalResult& value) {
    PlPgsqlQueryResult result;
    result.ok = value.ok; result.sqlState = value.sqlState; result.message = value.error;
    if (value.ok) {
        result.rowCount = result.columnCount = 1; result.columnTypes = {value.typeName};
        result.firstRow = {value.isNull ? std::nullopt : std::optional<std::string>{value.value}};
    }
    return result;
}

int main() {
    TypeRegistry::instance().bootstrap();
    PlPgsqlHost host;
    host.evalExprTyped = [](const auto& text, const auto& vars, const auto& nulls, const auto& types) {
        return scalar(ExprHelper::evalStringWithNulls(text, vars, nulls, types));
    };
    host.coerceValueTyped = [](const std::optional<std::string>& value,
                               const std::string& sourceType, const std::string& targetType) {
        return scalar(ExprHelper::evalStringWithNulls("CAST(__input AS " + targetType + ")",
            {{"__input", value.value_or("")}}, value ? std::set<std::string>{} : std::set<std::string>{"__input"},
            {{"__input", sourceType.empty() ? "unknown" : sourceType}}));
    };
    struct Case { const char* name; const char* body; const char* expected; const char* state; bool isNull; };
    const Case cases[] = {
        {R"r277(forward)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 1..4 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(1,2,3,4,)r277", R"r277()r277", false},
        {R"r277(reverse)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN REVERSE 4..1 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(4,3,2,1,)r277", R"r277()r277", false},
        {R"r277(empty)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 4..1 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277()r277", R"r277()r277", false},
        {R"r277(reverse_empty)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN REVERSE 1..4 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277()r277", R"r277()r277", false},
        {R"r277(maximum)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 2147483647..2147483647 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(2147483647,)r277", R"r277()r277", false},
        {R"r277(minimum)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN REVERSE -2147483648..-2147483648 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(-2147483648,)r277", R"r277()r277", false},
        {R"r277(step)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 1..8 BY 3 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(1,4,7,)r277", R"r277()r277", false},
        {R"r277(reverse_step)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN REVERSE 8..1 BY 3 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(8,5,2,)r277", R"r277()r277", false},
        {R"r277(positive_overflow_exit)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 2147483646..2147483647 BY 2 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(2147483646,)r277", R"r277()r277", false},
        {R"r277(negative_overflow_exit)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN REVERSE -2147483647..-2147483648 BY 2 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(-2147483647,)r277", R"r277()r277", false},
        {R"r277(full_width_step)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN -2147483648..2147483647 BY 2147483647 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(-2147483648,-1,2147483646,)r277", R"r277()r277", false},
        {R"r277(full_width_reverse_step)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN REVERSE 2147483647..-2147483648 BY 2147483647 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(2147483647,0,-2147483647,)r277", R"r277()r277", false},
        {R"r277(numeric_rounding)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 1.6..4.4 BY 1.6 LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(2,4,)r277", R"r277()r277", false},
        {R"r277(text_input)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN '  +2  '..'4' BY '0x2' LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(2,4,)r277", R"r277()r277", false},
        {R"r277(trivia)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0;  BEGIN FOR i IN 1 /* .. BY LOOP */ .. 4 /* BY */ BY (1+1) LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(1,3,)r277", R"r277()r277", false},
        {R"r277(quoted_by)r277", R"r277(DECLARE s TEXT := ''; guard_count INT := 0; "by" INT := 2; BEGIN FOR i IN 1..4 BY "by" LOOP guard_count:=guard_count+1; IF guard_count>8 THEN EXIT; END IF; s := s || i || ','; END LOOP; RETURN s; END;)r277", R"r277(1,3,)r277", R"r277()r277", false},
        {R"r277(lower_above_int4)r277", R"r277(BEGIN FOR i IN 2147483648..2147483648 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(upper_above_int4)r277", R"r277(BEGIN FOR i IN 1..2147483648 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(lower_below_int4)r277", R"r277(BEGIN FOR i IN -2147483649..1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(upper_below_int4)r277", R"r277(BEGIN FOR i IN REVERSE 1..-2147483649 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(int8_max_not_iterator)r277", R"r277(BEGIN FOR i IN 9223372036854775807..9223372036854775807 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(huge_numeric)r277", R"r277(BEGIN FOR i IN 999999999999999999999999999999..1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(step_above_int4)r277", R"r277(BEGIN FOR i IN 4..1 BY 2147483648 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22003)r277", false},
        {R"r277(null_lower)r277", R"r277(BEGIN FOR i IN NULL..1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22004)r277", false},
        {R"r277(null_upper)r277", R"r277(BEGIN FOR i IN 1..NULL LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22004)r277", false},
        {R"r277(null_step)r277", R"r277(BEGIN FOR i IN 1..2 BY NULL LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22004)r277", false},
        {R"r277(zero_step_empty)r277", R"r277(BEGIN FOR i IN 4..1 BY 0 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22023)r277", false},
        {R"r277(negative_step)r277", R"r277(BEGIN FOR i IN 1..4 BY -1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22023)r277", false},
        {R"r277(reverse_negative_step)r277", R"r277(BEGIN FOR i IN REVERSE 4..1 BY -1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22023)r277", false},
        {R"r277(rounded_zero_step)r277", R"r277(BEGIN FOR i IN 1..4 BY 0.4 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22023)r277", false},
        {R"r277(invalid_text)r277", R"r277(BEGIN FOR i IN 'bad'..1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22P02)r277", false},
        {R"r277(range_in_literal)r277", R"r277(BEGIN FOR i IN '..'..1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22P02)r277", false},
        {R"r277(fractional_text)r277", R"r277(BEGIN FOR i IN '1.6'..4 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22P02)r277", false},
        {R"r277(fractional_text_step)r277", R"r277(BEGIN FOR i IN 1..4 BY '1.6' LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22P02)r277", false},
        {R"r277(lower_null_before_upper_error)r277", R"r277(BEGIN FOR i IN NULL..(1/0) LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22004)r277", false},
        {R"r277(lower_error_before_upper_null)r277", R"r277(BEGIN FOR i IN (1/0)..NULL LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22012)r277", false},
        {R"r277(upper_null_before_step_error)r277", R"r277(BEGIN FOR i IN 1..NULL BY (1/0) LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22004)r277", false},
        {R"r277(upper_error_before_step_null)r277", R"r277(BEGIN FOR i IN 1..(1/0) BY NULL LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22012)r277", false},
        {R"r277(evaluate_once)r277", R"r277(DECLARE s TEXT := ''; lo INT := 1; hi INT := 4; st INT := 2; BEGIN FOR i IN lo..hi BY st LOOP s := s || i || ','; lo:=100; hi:=0; st:=99; END LOOP; RETURN s; END;)r277", R"r277(1,3,)r277", R"r277()r277", false},
        {R"r277(iterator_assignment)r277", R"r277(DECLARE s TEXT := ''; BEGIN FOR i IN 1..3 LOOP s:=s||i||','; i:=99; END LOOP; RETURN s; END;)r277", R"r277(1,2,3,)r277", R"r277()r277", false},
        {R"r277(outer_null)r277", R"r277(DECLARE i TEXT := NULL; BEGIN FOR i IN 1..2 LOOP EXIT; END LOOP; RETURN i; END;)r277", R"r277()r277", R"r277()r277", true},
        {R"r277(outer_literal_null)r277", R"r277(DECLARE i TEXT := 'null'; BEGIN FOR i IN 1..2 LOOP EXIT; END LOOP; RETURN i; END;)r277", R"r277(null)r277", R"r277()r277", false},
        {R"r277(nested_iterator)r277", R"r277(DECLARE s TEXT := ''; BEGIN FOR i IN 1..2 LOOP s:=s||i||':'; FOR i IN REVERSE 3..2 LOOP s:=s||i||','; END LOOP; s:=s||i||';'; END LOOP; RETURN s; END;)r277", R"r277(1:3,2,1;2:3,2,2;)r277", R"r277()r277", false},
        {R"r277(found_nonempty)r277", R"r277(BEGIN FOR i IN 1..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r277", R"r277(yes)r277", R"r277()r277", false},
        {R"r277(found_empty)r277", R"r277(BEGIN FOUND:=TRUE; FOR i IN 2..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r277", R"r277(no)r277", R"r277()r277", false},
        {R"r277(found_not_set_inside)r277", R"r277(BEGIN FOR i IN 1..1 LOOP IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END LOOP; RETURN 'empty'; END;)r277", R"r277(no)r277", R"r277()r277", false},
        {R"r277(nested_found)r277", R"r277(BEGIN FOR i IN 1..1 LOOP FOR j IN 2..1 LOOP EXIT; END LOOP; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r277", R"r277(yes)r277", R"r277()r277", false},
        {R"r277(found_iterator_nonempty)r277", R"r277(BEGIN FOR found IN 1..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r277", R"r277(yes)r277", R"r277()r277", false},
        {R"r277(found_iterator_empty)r277", R"r277(BEGIN FOR found IN 2..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r277", R"r277(no)r277", R"r277()r277", false},
        {R"r277(found_iterator_empty_after_true)r277", R"r277(BEGIN FOUND:=TRUE; FOR found IN 2..1 LOOP EXIT; END LOOP; IF FOUND THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r277", R"r277(no)r277", R"r277()r277", false},
        {R"r277(bound_reads_outer_iterator)r277", R"r277(DECLARE i INT:=7; BEGIN FOR i IN i..i LOOP RETURN 'entered'; END LOOP; RETURN 'empty'; END;)r277", R"r277(entered)r277", R"r277()r277", false},
        {R"r277(upper_protected_range)r277", R"r277(DECLARE s TEXT:=''; BEGIN FOR i IN 1..LENGTH('..') BY LENGTH('BY') LOOP s:=s||i||','; END LOOP; RETURN s; END;)r277", R"r277(1,)r277", R"r277()r277", false},
        {R"r277(lower_protected_range)r277", R"r277(DECLARE s TEXT:=''; BEGIN FOR i IN LENGTH('..')..3 LOOP s:=s||i||','; END LOOP; RETURN s; END;)r277", R"r277(2,3,)r277", R"r277()r277", false},
        {R"r277(text_null_not_null)r277", R"r277(BEGIN FOR i IN 'null'..1 LOOP EXIT; END LOOP; RETURN 'done'; END;)r277", R"r277()r277", R"r277(22P02)r277", false},
        {R"r277(negative_numeric_rounding)r277", R"r277(DECLARE s TEXT:=''; BEGIN FOR i IN -1.6..1.6 BY 1.6 LOOP s:=s||i||','; END LOOP; RETURN s; END;)r277", R"r277(-2,0,2,)r277", R"r277()r277", false},
        {R"r277(floating_ties_even)r277", R"r277(DECLARE s TEXT:=''; BEGIN FOR i IN CAST(1.5 AS double precision)..CAST(4.5 AS double precision) BY CAST(2.5 AS double precision) LOOP s:=s||i||','; END LOOP; RETURN s; END;)r277", R"r277(2,4,)r277", R"r277()r277", false},
        {R"r277(upper_quoted_by_field)r277", R"r277(<<b>> DECLARE "by" INT:=2; s TEXT:=''; BEGIN FOR i IN 1..b."by" LOOP s:=s||i||','; END LOOP; RETURN s; END b;)r277", R"r277(1,2,)r277", R"r277()r277", false},
        {R"r277(lower_qualified_by_field)r277", R"r277(<<b>> DECLARE "by" INT:=2; s TEXT:=''; BEGIN FOR i IN b.by..3 LOOP s:=s||i||','; END LOOP; RETURN s; END b;)r277", R"r277(2,3,)r277", R"r277()r277", false},
        {R"r277(step_qualified_by_field)r277", R"r277(<<b>> DECLARE "by" INT:=2; s TEXT:=''; BEGIN FOR i IN 1..3 BY b.by LOOP s:=s||i||','; END LOOP; RETURN s; END b;)r277", R"r277(1,3,)r277", R"r277()r277", false},
        {R"r277(step_qualified_by_field_trivia)r277", R"r277(<<b>> DECLARE "by" INT:=2; s TEXT:=''; BEGIN FOR i IN 1..3 BY b /*x*/ . /*y*/ by LOOP s:=s||i||','; END LOOP; RETURN s; END b;)r277", R"r277(1,3,)r277", R"r277()r277", false},
    };
    size_t controls = 0, failures = 0;
    for (const auto& test : cases) {
        std::string value, error, state; bool isNull = false; bool ok = false;
        try { ok = PlPgsql::run(test.body, {}, host, value, error, nullptr, &isNull, nullptr, &state); }
        catch (const std::exception& exception) { error = std::string("uncaught: ") + exception.what(); }
        const bool expectedOk = !*test.state;
        ++controls;
        if (ok != expectedOk || state != test.state) {
            ++failures; std::cerr << "INTEGER_FOR_NATIVE_FAIL " << test.name << " ok=" << ok
                << " state=" << state << " error=" << error << '\n';
        }
        ++controls;
        if (expectedOk && (!ok || isNull != test.isNull || (!isNull && value != test.expected))) {
            ++failures; std::cerr << "INTEGER_FOR_NATIVE_VALUE_FAIL " << test.name << " value=" << value
                << " isNull=" << isNull << " error=" << error << '\n';
        }
    }
    std::cout << "PLPGSQL_INTEGER_FOR_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    size_t apiControls = 0, apiFailures = 0;
    const auto apiCheck = [&](const char* label, const std::function<bool()>& action) {
        ++apiControls;
        try {
            if (action()) return;
        } catch (const std::exception& error) {
            std::cerr << "INTEGER_FOR_API_EXCEPTION " << label << " " << error.what() << '\n';
        }
        ++apiFailures; std::cerr << "INTEGER_FOR_API_FAIL " << label << '\n';
    };
    // The parser rejects malformed ranges before any reached host statement.
    for (const std::string interval : {"..1", "1..", "1..2 BY /*none*/", "1..2 BY 1 BY 1", "1"}) {
        apiCheck(interval.c_str(), [&] {
            auto probe = host; size_t effects = 0;
            probe.execStmt = [&](const auto&, const auto&) { ++effects; return true; };
            std::string value, error, state;
            const bool ok = PlPgsql::run("BEGIN PERFORM 1; FOR i IN " + interval +
                " LOOP EXIT; END LOOP; RETURN 'done'; END;", {}, probe,
                value, error, nullptr, nullptr, nullptr, &state);
            return !ok && state == "42601" && effects == 0;
        });
    }
    // Observable host calls prove both evaluate-once and short-circuit order;
    // final values alone cannot distinguish repeated volatile evaluation.
    for (int mode = 0; mode < 3; ++mode) {
        apiCheck("control expression/coercion order", [&] {
            std::vector<std::string> events;
            auto probe = host;
            probe.evalExprTyped = [&](const auto& expression, const auto&, const auto&, const auto&) {
                PlPgsqlQueryResult result;
                events.push_back(expression);
                if (mode == 2 && expression == "lower_control()") {
                    result.sqlState = "22012"; result.message = "lower failed"; return result;
                }
                result.ok = true; result.rowCount = result.columnCount = 1;
                result.columnTypes = {"integer"};
                result.firstRow = {mode == 1 && expression == "lower_control()" ? std::nullopt :
                    std::optional<std::string>{expression == "lower_control()" ? "1" : expression == "upper_control()" ? "3" : "2"}};
                return result;
            };
            probe.coerceValueTyped = [&](const auto& value, const auto& source, const auto& target) {
                events.push_back("cast:" + value.value_or("NULL"));
                return host.coerceValueTyped(value, source, target);
            };
            std::string value, error, state;
            const bool ok = PlPgsql::run("BEGIN FOR i IN lower_control()..upper_control() BY step_control() "
                "LOOP EXIT; END LOOP; RETURN 'done'; END;", {}, probe,
                value, error, nullptr, nullptr, nullptr, &state);
            if (mode == 0) return ok && value == "done" && state.empty() &&
                events == std::vector<std::string>{"lower_control()", "cast:1", "upper_control()", "cast:3", "step_control()", "cast:2"};
            if (mode == 1) return !ok && state == "22004" &&
                events == std::vector<std::string>{"lower_control()", "cast:NULL"};
            return !ok && state == "22012" && events == std::vector<std::string>{"lower_control()"};
        });
    }
    apiCheck("invalid coercion metadata", [&] {
        auto probe = host;
        probe.coerceValueTyped = [](const auto&, const auto&, const auto&) {
            PlPgsqlQueryResult result; result.ok = true; result.rowCount = 1; result.columnCount = 2;
            result.firstRow = {std::string("1")}; return result;
        };
        std::string value, error, state;
        return !PlPgsql::run("BEGIN FOR i IN 1..2 LOOP EXIT; END LOOP; RETURN 'done'; END;",
            {}, probe, value, error, nullptr, nullptr, nullptr, &state) && state == "XX000";
    });
    struct FallbackCase { const char* interval; const char* expected; const char* state; };
    for (const auto& test : {FallbackCase{"1..3 BY 2", "1", ""}, {"REVERSE 3..1 BY 2", "3", ""},
        {"1.6..2.4 BY 1.6", "2", ""}, {"'0x2'..2 BY 1", "2", ""},
        {"NULL..1", "", "22004"}, {"1..2 BY 0", "", "22023"},
        {"9223372036854775807..9223372036854775807", "", "22003"}, {"'bad'..1", "", "22P02"}, {"(1.6+1)..4", "3", ""},
        {"1..3 BY (0.4+0.0)", "", "22023"}}) {
        apiCheck(test.interval, [&] {
            std::string value, error, state;
            const bool ok = PlPgsql::run(std::string("BEGIN FOR i IN ") + test.interval +
                " LOOP RETURN i; END LOOP; RETURN 'empty'; END;", {}, PlPgsqlHost{},
                value, error, nullptr, nullptr, nullptr, &state);
            return ok == !*test.state && state == test.state && (!ok || value == test.expected);
        });
    }
    std::cout << "PLPGSQL_INTEGER_FOR_API_COMPLETE " << apiControls << " FAILURES " << apiFailures << '\n';
    if (apiControls != 19 || apiFailures) return 1;

    return failures ? 1 : 0;
}
