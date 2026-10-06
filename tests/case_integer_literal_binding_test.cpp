#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"2147483647","integer"},{"2147483648","bigint"},
        {"9223372036854775807","bigint"},{"9223372036854775808","numeric"},
        {"-2147483648","integer"},{"-2147483649","bigint"},
        {"-9223372036854775808","bigint"},{"-9223372036854775809","numeric"},
        {"-(-2147483648)","bigint"},{"-(-9223372036854775808)","numeric"}}) {
        auto query=prepareQuery("SELECT "+item.first,{},metadata);
        assert(query.output.at(0).type==item.second);
        auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
        const auto* literal=dynamic_cast<LiteralExpr*>(select->selectList[0].expr.get());
        assert(literal && literal->typeName.empty());
        ExprEvaluator evaluator;
        const auto value=evaluator.eval(literal,RowContext{});
        assert(value.typeName==item.second && !value.isNull);
        query=prepareQuery("SELECT CASE "+item.first+" WHEN "+item.first+" THEN 1 ELSE 2 END",{},metadata);
        select=dynamic_cast<SelectStmt*>(query.ast.get());
        const auto* conditional=dynamic_cast<CaseExpr*>(select->selectList[0].expr.get());
        assert((conditional->simpleComparisonTypes.at(0)==std::make_pair(item.second,item.second)));
    }
    // Explicit narrowing remains an executable CAST, not a lexical number.
    auto query=prepareQuery("SELECT -(CAST(-2147483648 AS INT))",{},metadata);
    auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
    assert(dynamic_cast<UnaryOpExpr*>(select->selectList[0].expr.get()));
    bool rejected=false;
    try { ExprEvaluator evaluator;(void)evaluator.eval(select->selectList[0].expr.get(),RowContext{}); }
    catch(const DbError& error) { rejected=error.sqlState()=="22003"; }
    catch(const std::runtime_error& error) {
        // Existing unary creation sites are a separate structured-error
        // follow-up. Preserve the exact current overflow contract here.
        rejected=std::string(error.what())=="integer out of range (SQLSTATE 22003)";
    }
    assert(rejected);
    std::cout<<"decimal constants and lexical signs retain static widths\n";
}
