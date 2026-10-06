#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"

#include <cassert>
#include <iostream>
#include <tuple>
#include <vector>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    bool correct = true;
    for (const auto& control : std::vector<std::tuple<std::string, std::string, std::string>>{
        {"sum", "smallint", "bigint"}, {"sum", "integer", "bigint"},
        {"sum", "bigint", "numeric"}, {"sum", "numeric", "numeric"},
        {"sum", "real", "real"}, {"sum", "double precision", "double precision"},
        {"sum", "money", "money"}, {"sum", "interval", "interval"},
        {"avg", "integer", "numeric"}, {"avg", "real", "double precision"},
        {"avg", "interval", "interval"}}) {
        dbms::FunctionCallExpr aggregate;
        aggregate.funcName = std::get<0>(control);
        auto input = std::make_unique<dbms::ParameterExpr>();
        input->declaredType = std::get<1>(control);
        aggregate.args.push_back(std::move(input));
        const auto actual = dbms::ExprHelper::inferParsedResultType(&aggregate);
        if (actual != std::get<2>(control))
            std::cerr << aggregate.funcName << '(' << std::get<1>(control) << "): "
                      << actual << " expected " << std::get<2>(control) << '\n';
        correct = correct && actual == std::get<2>(control);
    }
    assert(correct);
    std::cout << "[AGGREGATE RESULT TYPE METADATA] declared nullable parameter types passed\n";
}
