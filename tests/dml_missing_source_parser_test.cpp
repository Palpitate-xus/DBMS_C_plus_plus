#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const char* sql : {
             "UPDATE t SET v = 1 FROM",
             "UPDATE t SET v = 1 FROM a JOIN",
             "UPDATE t SET v = 1 FROM a LEFT JOIN",
             "UPDATE t SET v = 1 FROM a JOIN b",
             "UPDATE t SET v = 1 FROM a LEFT JOIN b",
             "DELETE FROM t USING",
             "DELETE FROM t USING a JOIN",
             "DELETE FROM t USING a FULL JOIN",
             "DELETE FROM t USING a INNER JOIN b",
             "DELETE FROM t USING a RIGHT JOIN b",
             "DELETE FROM t USING a JOIN b CROSS JOIN c"}) {
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
    assert(parser.parse(
        "UPDATE t SET v = 1 FROM a CROSS JOIN b WHERE t.id = a.id").success);
    assert(parser.parse(
        "UPDATE t SET v = 1 FROM a NATURAL LEFT JOIN b WHERE t.id = a.id").success);
    assert(parser.parse(
        "DELETE FROM t USING a JOIN b ON a.id = b.id WHERE t.id = a.id").success);
    assert(parser.parse(
        "UPDATE t SET v = 1 FROM a JOIN b USING (id) WHERE t.id = a.id").success);
    std::cout << "[DML MISSING SOURCE PARSER] passed\n";
}
