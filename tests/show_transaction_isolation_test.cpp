#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const char* sql : {"SHOW transaction_isolation", "SHOW transaction_isolation;",
                            "SHOW TRANSACTION ISOLATION LEVEL;",
                            "SHOW transaction /* nested /* x */ comment */ isolation level;",
                            "SHOW \"TRANSACTION_ISOLATION\"; -- tail"}) {
        auto parsed = parser.parse(sql);
        assert(parsed.success);
        const auto* shown = dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get());
        assert(shown && shown->isShow && shown->name == "transaction_isolation");
    }
    for (const char* sql : {"SHOW transaction_isolation junk", "SHOW TRANSACTION",
                            "SHOW TRANSACTION ISOLATION", "SHOW TRANSACTION ISOLATION LEVEL junk",
                            "SHOW transaction_isolation; SELECT 1"}) {
        assert(!parser.parse(sql).success);
    }
    // Other existing SHOW grammar remains unchanged by this narrow repair.
    auto ordinary = parser.parse("SHOW timezone");
    assert(ordinary.success);
    const auto* shown = dynamic_cast<const dbms::SetStmt*>(ordinary.stmt.get());
    assert(shown && shown->isShow && shown->name == "timezone");
    std::cout << "[SHOW TRANSACTION ISOLATION] passed\n";
}
