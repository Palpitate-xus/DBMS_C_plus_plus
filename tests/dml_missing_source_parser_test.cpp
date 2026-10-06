#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const char* sql : {
             "UPDATE t SET v = 1 FROM",
             "UPDATE t SET v = 1 FROM a JOIN",
             "UPDATE t SET v = 1 FROM a LEFT JOIN",
             "DELETE FROM t USING",
             "DELETE FROM t USING a JOIN",
             "DELETE FROM t USING a FULL JOIN"}) {
        const auto parsed = parser.parse(sql);
        if (parsed.success) std::cerr << "accepted: " << sql << '\n';
        assert(!parsed.success);
    }
    assert(parser.parse("UPDATE t SET v = 1").success);
    assert(parser.parse("DELETE FROM t").success);
    const auto update = parser.parse("UPDATE t SET v = a.v FROM a WHERE t.id = a.id");
    assert(update.success);
    assert(dynamic_cast<const dbms::UpdateStmt*>(update.stmt.get())->fromClause);
    const auto deletion = parser.parse("DELETE FROM t USING a WHERE t.id = a.id");
    assert(deletion.success);
    assert(dynamic_cast<const dbms::DeleteStmt*>(deletion.stmt.get())->usingClause);
    std::cout << "[DML MISSING SOURCE PARSER] passed\n";
}
