#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "REVERSE_LINEAR_FAILURE " << label << '\n'; }
    };
    const auto reverse = [&](const ExprValue& value) {
        FunctionCallExpr call;
        call.funcName = "reverse";
        auto column = std::make_unique<ColumnRefExpr>();
        column->column = "v";
        call.args.push_back(std::move(column));
        RowContext row;
        row.set("v", value);
        return evaluator.eval(&call, row);
    };
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"", ""}, {"a", "a"}, {"abc", "cba"}, {"é", "é"},
        {"界", "界"}, {"😀", "😀"}, {"aé界😀", "😀界éa"}, {"é", "́e"}
    };
    for (const auto* type : {"text", "varchar", "character", "bpchar"}) {
        for (const auto& item : cases) {
            const auto value = reverse(ExprValue(type, item.first));
            check(!value.isNull && value.typeName == "text" && value.value == item.second,
                  std::string(type) + " code points");
            const auto twice = reverse(value);
            check(!twice.isNull && twice.value == item.first, std::string(type) + " involution");
        }
        const auto value = reverse(ExprValue(type, "", true));
        check(value.isNull && value.typeName == "text", std::string(type) + " typed NULL");
    }
    const auto spaces = reverse(ExprValue("text", " a  "));
    check(spaces.value == "  a ", "TEXT retains trailing spaces");
    for (const auto& item : std::vector<std::pair<std::string, std::string>>{
             {"\\x", "\\x"}, {"\\x00c3a9ff", "\\xffa9c300"}, {"\\x000000", "\\x000000"}}) {
        const auto value = reverse(ExprValue("bytea", item.first));
        check(!value.isNull && value.typeName == "bytea" && value.value == item.second, "BYTEA byte reversal");
    }
    // Record observations without machine-dependent timing assertions. The
    // complete value is checked at each size, not just its length or prefix.
    for (const bool multibyte : {false, true}) for (const size_t count : {2048U, 4096U, 8192U}) {
        std::string input, expected;
        for (size_t i = 0; i < count; ++i) {
            input += multibyte ? "aé界😀" : "abcd";
            expected += multibyte ? "😀界éa" : "dcba";
        }
        const auto started = std::chrono::steady_clock::now();
        const auto value = reverse(ExprValue("text", input));
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started).count();
        std::cout << "REVERSE_LINEAR_OBSERVATION " << multibyte << ' ' << count << ' ' << elapsed << '\n';
        check(!value.isNull && value.typeName == "text" && value.value == expected, "complete large value");
    }
    int effects = 0;
    evaluator.registerFunction("reverse_input_effect", [&](const std::vector<ExprValue>&) {
        ++effects;
        return ExprValue("text", "aé界😀");
    });
    FunctionCallExpr call;
    call.funcName = "reverse";
    auto input = std::make_unique<FunctionCallExpr>();
    input->funcName = "reverse_input_effect";
    call.args.push_back(std::move(input));
    (void)ExprHelper::inferParsedResultType(&call, {}, "", nullptr, &evaluator);
    const bool pure = effects == 0;
    const auto value = evaluator.eval(&call, {});
    check(pure && effects == 1 && value.typeName == "text" && value.value == "😀界éa", "input evaluated once");
    evaluator.registerFunction("reverse", [](const std::vector<ExprValue>&) { return ExprValue("text", "opaque"); });
    check(reverse(ExprValue("text", "abc")).value == "opaque", "native override retained");
    if (controls != 80) { ++failures; std::cerr << "REVERSE_LINEAR_BAD_INVENTORY " << controls << '\n'; }
    std::cout << "REVERSE_LINEAR_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
