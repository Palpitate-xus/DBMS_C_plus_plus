#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const auto& sql : {
            "UPDATE t SET a=1,a=2",
            "UPDATE t SET A=1,a=2",
            "UPDATE t SET a=1,\"a\"=2",
            "UPDATE t SET a=1,\"A\"=2"}) {
        const auto result = parser.parseForBinding(sql);
        assert(result.success);
        const auto* update = dynamic_cast<const dbms::UpdateStmt*>(result.stmt.get());
        assert(update);
        std::cerr << sql << " retained targets=" << update->setClauses.size() << '\n';
        assert(update->setClauses.size() == 2);
    }
    const auto result = parser.parseForBinding("UPDATE t SET a=DEFAULT,a=2");
    assert(result.success);
    const auto* update = dynamic_cast<const dbms::UpdateStmt*>(result.stmt.get());
    assert(update && update->setClauses.size() == 2);
    const auto* value = dynamic_cast<const dbms::LiteralExpr*>(update->setClauses.front().second.get());
    assert(value && value->value == "default");
    assert(value->sourceBegin != std::string::npos && value->sourceEnd > value->sourceBegin);
    std::cout << "[UPDATE DUPLICATE TARGET] all original assignment sites retained\n";
}
