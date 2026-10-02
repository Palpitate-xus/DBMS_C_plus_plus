#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

int main() {
    dbms::ExprEvaluator evaluator;
    const std::vector<std::tuple<std::string, std::string, std::string>> cases = {
        {"0.5", "FM999", "1"}, {"-0.5", "FM999", "-1"},
        {"1.005", "FM999.00", "1.01"}, {"-1.005", "FM999.00", "-1.01"},
        {"9.995", "FM99.00", "10.00"}, {"-9.995", "FM99.00", "-10.00"},
        {"9007199254740993", "FM9999999999999999", "9007199254740993"},
        {"9223372036854775807", "FM9999999999999999999", "9223372036854775807"},
        {"9007199254740993.005", "FM9999999999999999.00", "9007199254740993.01"},
        {"1.125", "FM999V99", "113"}, {"-1.125", "FM999V99", "-113"},
        {"0.005", "FM999V99", "1"}, {"-0.005", "FM999V99", "-1"},
        {"999.995", "FM99.00", "##.##"}, {"-999.995", "FM99.00", "-##.##"}
    };
    for (const auto& [number, format, expected] : cases) {
        dbms::RowContext context;
        context.set("n", dbms::ExprValue("numeric", number, false));
        context.set("f", dbms::ExprValue("text", format, false));
        dbms::FunctionCallExpr expression;
        expression.funcName = "to_char";
        for (const auto& name : {"n", "f"}) {
            auto reference = std::make_unique<dbms::ColumnRefExpr>();
            reference->column = name;
            expression.args.push_back(std::move(reference));
        }
        const auto value = evaluator.eval(&expression, context);
        assert(!value.isNull && value.typeName == "text" && value.value == expected);
    }
    std::cout << "[TO CHAR EXACT ROUNDING] passed\n";
}
