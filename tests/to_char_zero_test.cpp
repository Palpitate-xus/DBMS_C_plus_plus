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
        {"0", "999", "   0"}, {"-0.004", "999", "   0"},
        {"0", "FM999", "0"}, {"0", "0999", " 0000"},
        {"0", "999.99", "    .00"}, {"-0.004", "999.99", "    .00"},
        {"0", "FM999.99", "0."}, {"-0.004", "FM999.99", "0."},
        {"0", "FM999.00", ".00"}, {"0", "FM999.09", ".0"},
        {"0", "FM999.90", ".00"}, {"0", "FM0999.99", "0000."},
        {"0.5", "999.99", "    .50"}, {"-0.5", "999.99", "   -.50"},
        {"0.5", "FM999.99", ".5"}, {"-0.5", "FM999.99", "-.5"},
        {"0.5", "FM999.00", ".50"}, {"-0.5", "FM999.00", "-.50"},
        {"0.5", "FM0999.99", "0000.5"}, {"-0.5", "FM0999.99", "-0000.5"},
        {"3.1", "FM999.99", "3.1"}, {"3.1", "FM999.00", "3.10"},
        {"-3.1", "999.99", "  -3.10"},
        {"482", "SG9999", "+ 482"}, {"-482", "SG9999", "- 482"},
        {"1234", "SG9999", "+1234"}, {"-1234", "SG9999", "-1234"},
        {"482", "9999SG", " 482+"}, {"-482", "9999SG", " 482-"},
        {"482", "FMSG9999", "+482"}, {"-482", "FM9999SG", "482-"},
        {"482", "9999.00SG", " 482.00+"}, {"-482", "9999.00SG", " 482.00-"}
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
    std::cout << "[TO CHAR ZERO] passed\n";
}
