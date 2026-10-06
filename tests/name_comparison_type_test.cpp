#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();SQLParser parser;ExprEvaluator evaluator;
    for(const auto& sql:{"SELECT '1'::NAME='01'::NAME","SELECT '10'::NAME>'2'::NAME",
        "SELECT '1'::NAME='01'::TEXT","SELECT '1'::TEXT='01'::NAME"}) {
        auto parsed=parser.parse(sql);assert(parsed.isValid());
        const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
        const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});
        assert(value.typeName=="boolean" && !value.isNull && !value.asBool());
    }
    for(const auto& sql:{"SELECT 'B'::NAME<'a'::NAME","SELECT 'B'::NAME<'a'::TEXT",
        "SELECT 'B'::TEXT<'a'::NAME"}) {
        auto parsed=parser.parse(sql);assert(parsed.isValid());
        const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
        const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});
        assert(!value.isNull && value.asBool());
    }
    const auto result=ExprHelper::evalString("CASE '1'::NAME WHEN '01'::NAME THEN 1 ELSE 2 END",{},{});
    assert(result.ok && result.value=="2");
    std::cout<<"[NAME COMPARISON TYPE] typed numeric-looking names remain textual\n";
}
