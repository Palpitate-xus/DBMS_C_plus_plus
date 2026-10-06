#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();SQLParser parser;ExprEvaluator evaluator;
    for(const auto& sql:{"SELECT INTERVAL '1 mon'=INTERVAL '30 days'",
        "SELECT INTERVAL '1 day'=INTERVAL '24 hours'",
        "SELECT INTERVAL '1 mon'>INTERVAL '29 days'",
        "SELECT INTERVAL '-1 mon'=INTERVAL '-30 days'"}) {
        auto parsed=parser.parse(sql);assert(parsed.isValid());
        const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
        const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});
        assert(value.typeName=="boolean" && !value.isNull && value.asBool());
    }
    const auto result=ExprHelper::evalString("CASE INTERVAL '1 mon' WHEN INTERVAL '30 days' THEN 1 ELSE 2 END",{},{});
    assert(result.ok && result.value=="1");
    std::cout<<"[INTERVAL COMPARISON VALUE] shared value codec and simple CASE\n";
}
