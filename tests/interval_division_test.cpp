#include "expression/ExprEvaluator.h"
#include "common/DbError.h"
#include "utils/interval.h"
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

// PG18.6 fixed datum results. RuntimeCell parameters prevent constant folding
// from replacing the interval arithmetic owner exercised by window AVG.
int main() {
    using namespace dbms;
    struct Case { const char *label,*interval,*factor,*state; bool null;
                  long long months,days,micros; };
    const std::vector<Case> cases={
        {"half-month", "1 mon", "2", "", false, 0LL, 15LL, 0LL},
        {"half-day", "1 day", "2", "", false, 0LL, 0LL, 43200000000LL},
        {"mixed-calendar", "2 mons 1 day", "2", "", false, 1LL, 0LL, 43200000000LL},
        {"fraction-month", "1 mon", "7", "", false, 0LL, 4LL, 24685689600LL},
        {"fraction-day", "1 day", "7", "", false, 0LL, 0LL, 12342857143LL},
        {"negative-month", "-1 mon", "2", "", false, 0LL, -15LL, 0LL},
        {"negative-divisor", "1 mon", "-2", "", false, 0LL, -15LL, 0LL},
        {"both-negative", "-1 mon", "-2", "", false, 0LL, 15LL, 0LL},
        {"mixed-signs", "1 mon -1 day 1 second", "2", "", false, 0LL, 15LL, -43199500000LL},
        {"no-upward-justify", "1 mon 29 days", "2", "", false, 0LL, 29LL, 43200000000LL},
        {"month-day-third", "1 mon 1 day", "3", "", false, 0LL, 10LL, 28800000000LL},
        {"month-fraction-boundary", "1 mon", "1.00000001", "", false, 0LL, 30LL, 0LL},
        {"day-fraction-boundary", "1 day", "1.000000000001", "", false, 0LL, 1LL, 0LL},
        {"micro-half-even-zero", "1 microsecond", "2", "", false, 0LL, 0LL, 0LL},
        {"micro-half-even-up", "3 microseconds", "2", "", false, 0LL, 0LL, 2LL},
        {"micro-half-even-down", "5 microseconds", "2", "", false, 0LL, 0LL, 2LL},
        {"micro-negative-even-zero", "-1 microsecond", "2", "", false, 0LL, 0LL, 0LL},
        {"micro-negative-even-up", "-3 microseconds", "2", "", false, 0LL, 0LL, -2LL},
        {"micro-mixed-carry", "1 day 1 microsecond", "2", "", false, 0LL, 0LL, 43200000000LL},
        {"zero", "0 seconds", "3", "", false, 0LL, 0LL, 0LL},
        {"positive-infinity-factor", "1 mon 1 day 1 second", "Infinity", "", false, 0LL, 0LL, 0LL},
        {"negative-infinity-factor", "1 mon 1 day 1 second", "-Infinity", "", false, 0LL, 0LL, 0LL},
        {"minimum-time", "-9223372036854775808 microseconds", "1", "", false, 0LL, 0LL, std::numeric_limits<long long>::lowest()},
        {"large-time-half", "9223372036854775806 microseconds", "2", "", false, 0LL, 0LL, 4611686018427387904LL},
        {"null-interval", nullptr, "2", "", true, 0LL, 0LL, 0LL},
        {"null-factor", "1 mon", nullptr, "", true, 0LL, 0LL, 0LL},
        {"null-both", nullptr, nullptr, "", true, 0LL, 0LL, 0LL},
        {"null-before-zero", nullptr, "0", "", true, 0LL, 0LL, 0LL},
        {"zero-error", "1 mon", "0", "22012", false, 0LL, 0LL, 0LL},
        {"negative-zero-error", "1 mon", "-0", "22012", false, 0LL, 0LL, 0LL},
        {"nan-error", "1 mon", "NaN", "22008", false, 0LL, 0LL, 0LL},
        {"month-overflow", "2147483647 mons", "0.5", "22008", false, 0LL, 0LL, 0LL},
        {"day-overflow", "2147483647 days", "0.5", "22008", false, 0LL, 0LL, 0LL},
        {"time-overflow", "9223372036854775807 microseconds", "0.5", "22008", false, 0LL, 0LL, 0LL},
        {"time-negative-overflow", "-9223372036854775808 microseconds", "-1", "22008", false, 0LL, 0LL, 0LL},
        {"time-double-boundary", "9223372036854775807 microseconds", "1", "22008", false, 0LL, 0LL, 0LL},
        {"calendar-near-limit", "1 mon 2147483647 days", "0.9999999999", "", false, 1LL, 2147483647LL, 18554260254LL},
        {"cascade-positive-overflow", "1 mon 2147483647 days", "1.0000000001", "22008", false, 0LL, 0LL, 0LL},
        {"cascade-negative-overflow", "-1 mon -2147483648 days", "1.0000000001", "22008", false, 0LL, 0LL, 0LL},
    };
    size_t controls=0,failures=0;
    ExprEvaluator evaluator;
    for (const auto& test:cases) {
        BinaryOpExpr expression;expression.op="/";
        auto left=std::make_unique<ParameterExpr>();left->slot=0;left->declaredType="interval";
        auto right=std::make_unique<ParameterExpr>();right->slot=1;right->declaredType="double precision";
        expression.left=std::move(left);expression.right=std::move(right);
        RowContext context;
        context.setParameters({ExprValue("interval",test.interval?test.interval:"",!test.interval),
                               ExprValue("double precision",test.factor?test.factor:"",!test.factor)});
        bool good=false;std::string state,message,value;
        try {
            const auto result=evaluator.eval(&expression,context);value=result.value;
            const auto datum=parseIntervalInput(value);
            good=std::string(test.state).empty() && result.typeName=="interval" &&
                 result.isNull==test.null && (test.null || (datum.ok &&
                 datum.months==test.months && datum.days==test.days && datum.micros==test.micros));
        } catch(const DbError& error) {
            state=error.sqlState();message=error.message();
            good=state==test.state && !state.empty() &&
                 message==(state=="22012"?"division by zero":"interval out of range");
        } catch(const std::exception& error) {message=error.what();}
        good=good && context.parameterCount()==2 &&
             context.parameter(0).typeName=="interval" && context.parameter(0).isNull==!test.interval &&
             context.parameter(0).value==(test.interval?test.interval:"") &&
             context.parameter(1).typeName=="double precision" && context.parameter(1).isNull==!test.factor &&
             context.parameter(1).value==(test.factor?test.factor:"");
        ++controls;failures+=!good;
        if(!good)std::cerr<<"INTERVAL_DIVISION_FAILED "<<test.label<<" value="<<value<<" state="<<state<<" message="<<message<<'\n';
    }
    std::cout<<"INTERVAL_DIVISION_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return controls==39 && failures==0?0:1;
}
