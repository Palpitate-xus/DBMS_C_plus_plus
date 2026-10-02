#include "commands/TableManage.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    dbms::TableSchema schema;
    for (const std::string expression : {"FALSE", "NULL", "(NULL)",
            "NOT TRUE", "FALSE OR NULL", "1=0"}) {
        dbms::StorageEngine::Condition condition;
        condition.op = "typedexpr";
        condition.value = expression;
        assert(!dbms::StorageEngine::evalConditionOnRow(condition, "", schema));
    }
    for (const std::string expression : {"TRUE", "NOT FALSE", "TRUE AND TRUE", "1=1"}) {
        dbms::StorageEngine::Condition condition;
        condition.op = "typedexpr";
        condition.value = expression;
        assert(dbms::StorageEngine::evalConditionOnRow(condition, "", schema));
    }
    assert(dbms::ExprHelper::inferResultType("FALSE OR NULL") == "boolean");
    assert(dbms::ExprHelper::evalString("NULL", {}).isNull);
    std::cout << "[CONSTANT BOOLEAN PREDICATE] passed\n";
}
