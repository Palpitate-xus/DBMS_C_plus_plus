#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    ExprEvaluator evaluator;
    for (const auto& control : std::vector<std::tuple<std::string,std::string,std::string>>{
        {"ARRAY[B'01',B'001',B'',NULL]", "bit[]", "{01,001,\"\",NULL}"},
        {"ARRAY[B'01','001']", "bit[]", "{01,001}"},
        {"ARRAY[B'01'::bit(3),B'10101'::bit(5)]", "bit[]", "{010,10101}"},
        {"ARRAY[ARRAY[B'01',B'001'],ARRAY[B'10',B'101']]", "bit[]", "{{01,001},{10,101}}"},
        {"ARRAY[B'01'::varbit,B'001'::varbit]", "bit varying[]", "{01,001}"}}) {
        const std::string sql = "SELECT " + std::get<0>(control);
        auto prepared = std::make_shared<PreparedQuery>(prepareQuery(sql,{},{}));
        const auto* select = static_cast<SelectStmt*>(prepared->ast.get());
        const auto value = evaluator.eval(select->selectList.front().expr.get(),{});
        std::cerr << "[BIT array] " << sql << " actual=" << value.value << '\n';
        assert(!value.isNull && value.typeName==std::get<1>(control) && value.value==std::get<2>(control));
        const auto elements = ExprEvaluator::arrayElements(value);
        assert(elements.size()==(value.value.find("NULL")!=std::string::npos?4:
                               value.value.find("{{")!=std::string::npos?4:2));
    }
    // Constructor coercion is implicit. Explicit scalar BIT without a
    // modifier still has PostgreSQL's length-one default.
    const auto scalar = parser.parse("SELECT B'01'::bit");
    assert(scalar.isValid());
    assert(evaluator.eval(static_cast<SelectStmt*>(scalar.stmt.get())->selectList.front().expr.get(),{}).value=="0");
    std::cout << "[BIT ARRAY CONSTRUCTOR VALUE] implicit elements retain lengths and explicit typmods passed\n";
}
