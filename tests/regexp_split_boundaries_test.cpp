#include "catalog/type_registry.h"
#include "common/SqlArrayText.h"
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {
struct Sample {
    std::string source, pattern;
    std::vector<std::string> fields;
    std::string flags;
};
dbms::ExprValue evaluate(dbms::ExprEvaluator& evaluator,
                         const std::vector<dbms::ExprValue>& arguments) {
    dbms::FunctionCallExpr call;
    call.funcName = "regexp_split_to_array";
    dbms::RowContext row;
    for (size_t i = 0; i < arguments.size(); ++i) {
        auto column = std::make_unique<dbms::ColumnRefExpr>();
        column->column = "a" + std::to_string(i);
        row.set(column->column, arguments[i]);
        call.args.push_back(std::move(column));
    }
    return evaluator.eval(&call, row);
}
std::string array(const std::vector<std::string>& fields) {
    std::string result = "{";
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i) result += ',';
        result += dbms::sql_array_text::quote(fields[i]);
    }
    return result + '}';
}
} // namespace

int main() {
    using dbms::ExprValue;
    dbms::TypeRegistry::instance().bootstrap();
    dbms::ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) {
            ++failures;
            std::cerr << "REGEXP_SPLIT_BOUNDARIES_FAIL " << label << '\n';
        }
    };
    const std::vector<Sample> samples = {
        {"", ",", {""}}, {"", "", {""}}, {"abc", ",", {"abc"}},
        {"a,", ",", {"a", ""}}, {",a", ",", {"", "a"}},
        {",,", ",", {"", "", ""}}, {"a,,b,", ",", {"a", "", "b", ""}},
        {"abc", "", {"a", "b", "c"}}, {"abc", "x*", {"a", "b", "c"}},
        {"aaa", "a*", {"", ""}}, {"aba", "a*", {"", "b", ""}},
        {"a", "a?", {"", ""}}, {"abc", "b*", {"a", "c"}},
        {"abbc", "b*", {"a", "c"}}, {"abc", "^", {"abc"}},
        {"abc", "$", {"abc"}}, {"abc", "^|$", {"abc"}},
        {"abc", "(^)|($)", {"abc"}}, {"a\nb\n", "\n", {"a", "b", ""}},
        {"a,b,", "(,)", {"a", "b", ""}}, {"aaXbbX", "x", {"aa", "bb", ""}, "i"},
        {"é界😀", "", {"é", "界", "😀"}}, {"é界😀", "x*", {"é", "界", "😀"}},
        {"é界😀", "界", {"é", "😀"}}, {"é界😀界", "界", {"é", "😀", ""}},
        {"éé", "", {"e", "́", "é"}}, {"NULL,\"x\",a\\b,", ",", {"NULL", "\"x\"", "a\\b", ""}},
        {"a<>b<>", "<>", {"a", "b", ""}}, {"a  b  ", " +", {"a", "b", ""}},
    };
    for (const auto* type : {"text", "varchar"}) {
        for (const auto& sample : samples) {
            std::vector<ExprValue> args = {ExprValue(type, sample.source), ExprValue(type, sample.pattern)};
            if (!sample.flags.empty()) args.emplace_back(type, sample.flags);
            const auto value = evaluate(evaluator, args);
            check(!value.isNull && value.value == array(sample.fields),
                  std::string(type) + ": " + sample.source + " / " + sample.pattern);
        }
    }
    check(evaluate(evaluator, {ExprValue("character", "a,b,  "), ExprValue("character", ",  ")}).value == "{a,b,\"\"}",
          "CHAR trailing blanks removed before split, trailing field retained");
    for (size_t i = 0; i < 3; ++i) {
        std::vector<ExprValue> args = {ExprValue("text", "abc"), ExprValue("text", "("), ExprValue("text", "z")};
        args[i] = ExprValue("text", "malformed payload", true);
        check(evaluate(evaluator, args).isNull, "strict NULL does not inspect invalid pattern/flags");
    }
    for (const auto& invalid : std::vector<std::pair<std::vector<ExprValue>, std::string>>{
             {{ExprValue("text", "abc"), ExprValue("text", "(")}, "2201B"},
             {{ExprValue("text", "abc"), ExprValue("text", ","), ExprValue("text", "g")}, "22023"},
             {{ExprValue("text", "abc"), ExprValue("text", ","), ExprValue("text", "z")}, "22023"}}) {
        bool rejected = false;
        try { (void)evaluate(evaluator, invalid.first); }
        catch (const std::exception& error) { rejected = std::string(error.what()).find(invalid.second) != std::string::npos; }
        check(rejected, "existing error state " + invalid.second);
    }
    int effects = 0;
    evaluator.registerFunction("split_effect", [&](const std::vector<ExprValue>&) {
        ++effects; return ExprValue("text", "a,");
    });
    dbms::FunctionCallExpr outer;
    outer.funcName = "regexp_split_to_array";
    auto inner = std::make_unique<dbms::FunctionCallExpr>(); inner->funcName = "split_effect";
    outer.args.push_back(std::move(inner));
    auto delimiter = std::make_unique<dbms::ColumnRefExpr>(); delimiter->column = "p";
    outer.args.push_back(std::move(delimiter));
    dbms::RowContext row; row.set("p", ExprValue("text", ","));
    const auto effectValue = evaluator.eval(&outer, row);
    check(effects == 1 && effectValue.value == "{a,\"\"}", "input expression evaluated exactly once");
    evaluator.registerFunction("regexp_split_to_array", [](const std::vector<ExprValue>&) {
        return ExprValue("ARRAY", "{opaque}");
    });
    check(evaluate(evaluator, {ExprValue("text", "a,"), ExprValue("text", ",")}).value == "{opaque}", "native callback override retained");
    if (controls != 67) { ++failures; std::cerr << "REGEXP_SPLIT_BAD_REACH " << controls << '\n'; }
    std::cout << "REGEXP_SPLIT_BOUNDARIES_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
