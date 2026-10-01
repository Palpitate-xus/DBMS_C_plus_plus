#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const auto& sql : {"DROP TABLE comma_a comma_b", "DROP TABLE comma_a,",
                            "DROP TABLE ,comma_a", "DROP TABLE IF EXISTS",
                            "DROP TABLE comma_a CASCADE comma_b",
                            "DROP TABLE comma_a RESTRICT CASCADE",
                            "DROP TABLE comma_a,,comma_b", "DROP TABLE 123"}) {
        const auto parsed = parser.parse(sql);
        assert(!parsed.success);
    }
    const auto parsed = parser.parse(
        "DROP TABLE IF EXISTS public.comma_a,\"comma,b\" RESTRICT");
    assert(parsed.success);
    const auto* drop = dynamic_cast<const dbms::DropStmt*>(parsed.stmt.get());
    assert(drop && drop->ifExists && !drop->cascade);
    assert(drop->objectNames.size() == 2);
    assert(drop->objectNames[0] == "public.comma_a");
    assert(drop->objectNames[1] == "\"comma,b\"");
    const auto cascade = parser.parse("DROP TABLE comma_a,comma_b CASCADE");
    assert(cascade.success);
    assert(dynamic_cast<const dbms::DropStmt*>(cascade.stmt.get())->cascade);
    std::cout << "[DROP TABLE LIST PARSER] passed" << std::endl;
}
