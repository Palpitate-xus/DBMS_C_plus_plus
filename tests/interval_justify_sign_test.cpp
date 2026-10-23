#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "utils/interval.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {
constexpr long long day = 86400000000LL;
struct Fields { long long months, days, micros; };
dbms::ExprValue evaluate(dbms::ExprEvaluator& evaluator, const std::string& name,
                         const std::string& input, bool null = false) {
    dbms::FunctionCallExpr call; call.funcName = name;
    auto argument = std::make_unique<dbms::ColumnRefExpr>(); argument->column = "v";
    call.args.push_back(std::move(argument));
    dbms::RowContext row; row.set("v", dbms::ExprValue("interval", input, null));
    return evaluator.eval(&call, row);
}
std::string input(const Fields& fields) {
    return std::to_string(fields.months) + " months " + std::to_string(fields.days) +
        " days " + std::to_string(fields.micros) + " microseconds";
}
Fields expected(Fields fields, int mode) {
    // Independent normal-form oracle: divide the conserved scalar quantity
    // for exactly the fields this function normalizes. Inputs here are small
    // enough that all intermediate/final fields are representable.
    if (mode == 0) {
        const __int128 total = static_cast<__int128>(fields.days) * day + fields.micros;
        fields.days = total / day; fields.micros = total % day;
    } else if (mode == 1) {
        const __int128 total = static_cast<__int128>(fields.months) * 30 + fields.days;
        fields.months = total / 30; fields.days = total % 30;
    } else {
        const __int128 total = (static_cast<__int128>(fields.months) * 30 + fields.days) * day + fields.micros;
        fields.months = total / (30 * day);
        fields.days = total % (30 * day) / day;
        fields.micros = total % day;
    }
    return fields;
}
} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "INTERVAL_JUSTIFY_SIGN_FAIL " << label << '\n'; }
    };
    const std::vector<std::string> names = {"justify_hours", "justify_days", "justify_interval"};
    for (const long long months : {-13LL, -1LL, 0LL, 1LL, 13LL})
        for (const long long days : {-61LL, -30LL, -1LL, 0LL, 1LL, 30LL, 61LL})
            for (const long long micros : {-day-1, -day, -3600000000LL, -1LL, 0LL, 1LL, 3600000000LL, day, day+1}) {
                const Fields fields{months, days, micros};
                for (int mode = 0; mode < 3; ++mode) {
                    const auto value = evaluate(evaluator, names[mode], input(fields));
                    const auto parsed = dbms::parseIntervalInput(value.value);
                    const auto want = expected(fields, mode);
                    const auto repeated = evaluate(evaluator, names[mode], value.value);
                    check(!value.isNull && parsed.ok && parsed.months == want.months && parsed.days == want.days &&
                          parsed.micros == want.micros && !repeated.isNull && repeated.value == value.value,
                          names[mode] + " exact conserved normal form and idempotence: " + input(fields));
                }
            }
    for (const auto& name : names) {
        check(evaluate(evaluator, name, "invalid payload", true).isNull, name + " strict NULL");
        for (const auto* infinity : {"infinity", "-infinity"}) {
            const auto value = evaluate(evaluator, name, infinity);
            check(!value.isNull && value.value == infinity, name + " preserves infinity");
        }
    }
    for (const Fields fields : std::vector<Fields>{{2147483647,0,0},{-2147483648LL,0,0},
            {0,2147483647,0},{0,-2147483648LL,0},{0,0,9223372036854775807LL},{0,0,(-9223372036854775807LL-1)}}) {
        for (int mode = 0; mode < 3; ++mode) {
            const auto value = evaluate(evaluator, names[mode], input(fields));
            const auto parsed = dbms::parseIntervalInput(value.value);
            const auto want = expected(fields, mode);
            check(!value.isNull && parsed.ok && parsed.months == want.months && parsed.days == want.days && parsed.micros == want.micros,
                  names[mode] + " full-width field endpoint: " + input(fields));
        }
    }
    struct Edge { const char* name; Fields fields; const char* state; Fields result; };
    const std::vector<Edge> edges = {
        {"justify_hours", {0,2147483647,day}, "22008", {}},
        {"justify_hours", {0,-2147483648LL,-day}, "22008", {}},
        {"justify_days", {2147483647,30,0}, "22008", {}},
        {"justify_days", {-2147483648LL,-30,0}, "22008", {}},
        {"justify_interval", {2147483647,30,0}, "22008", {}},
        {"justify_interval", {-2147483648LL,-30,0}, "22008", {}},
        {"justify_interval", {0,2147483647,day}, "", {71582788,8,0}},
        {"justify_interval", {0,-2147483648LL,-day}, "", {-71582788,-9,0}},
        {"justify_interval", {2147483647,30,-1}, "22008", {}},
        {"justify_interval", {-2147483648LL,-30,1}, "22008", {}},
        {"justify_days", {0,0,(-9223372036854775807LL-1)}, "", {0,0,(-9223372036854775807LL-1)}},
    };
    for (const auto& edge : edges) {
        bool good = false;
        try {
            const auto value = evaluate(evaluator, edge.name, input(edge.fields));
            const auto parsed = dbms::parseIntervalInput(value.value);
            good = !*edge.state && !value.isNull && parsed.ok && parsed.months == edge.result.months &&
                parsed.days == edge.result.days && parsed.micros == edge.result.micros;
        } catch (const dbms::DbError& error) { good = *edge.state && error.sqlState() == edge.state; }
        check(good, std::string(edge.name) + " representability/pre-normalization: " + input(edge.fields));
    }
    int effects = 0;
    evaluator.registerFunction("justify_input_effect", [&](const std::vector<dbms::ExprValue>&) {
        ++effects; return dbms::ExprValue("interval", "1 mon -1 day");
    });
    dbms::FunctionCallExpr call; call.funcName = "justify_interval";
    auto effect = std::make_unique<dbms::FunctionCallExpr>(); effect->funcName = "justify_input_effect";
    call.args.push_back(std::move(effect));
    const auto value = evaluator.eval(&call, dbms::RowContext{});
    check(effects == 1 && value.value == "29 days", "input expression evaluated once");
    evaluator.registerFunction("justify_interval", [](const std::vector<dbms::ExprValue>&) {
        return dbms::ExprValue("interval", "2 days");
    });
    check(evaluate(evaluator, "justify_interval", "1 mon -1 day").value == "2 days", "native callback override");
    if (controls != 985) { ++failures; std::cerr << "INTERVAL_JUSTIFY_BAD_REACH " << controls << '\n'; }
    std::cout << "INTERVAL_JUSTIFY_SIGN_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
