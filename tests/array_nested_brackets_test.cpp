#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::SQLParser parser;
    for (const std::string expression : {"ARRAY[[1,NULL],[2,3]]",
                                        "ARRAY[ARRAY[1,NULL],ARRAY[2,3]]"}) {
        auto parsed = parser.parseForBinding("SELECT " + expression);
        assert(parsed.success);
        const auto* statement = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
        assert(statement && statement->selectList.size() == 1);
        dbms::ExprEvaluator evaluator;
        const auto value = evaluator.eval(statement->selectList[0].expr.get(),dbms::RowContext{});
        assert(value.typeName == "integer[]" && value.value == "{{1,NULL},{2,3}}");
        assert(parser.parse("INSERT INTO t VALUES(" + expression + ")").success);
    }
    for (const std::string expression : {"ARRAY[[1,],[2,3]]","ARRAY[[1,2],[3,4]",
                                        "ARRAY[1,,2]","[1,2]"}) {
        const auto parsed = parser.parseForBinding("SELECT " + expression);
        if (parsed.success) std::cerr << "unexpected successful parse: " << expression << '\n';
        assert(!parsed.success);
    }
    std::cout << "[ARRAY NESTED BRACKETS] passed\n";
}
