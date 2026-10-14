#include "parser/parser.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    dbms::SQLParser parser;
    size_t checked = 0;
    const std::vector<std::string> valid = {
        "INSERT INTO dst WITH q AS (SELECT 1) SELECT * FROM q",
        "INSERT INTO dst(id) WITH q AS (VALUES(1)) SELECT * FROM q RETURNING id",
        "INSERT INTO dst WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<3) SELECT n FROM q",
        "INSERT INTO dst WITH q AS (SELECT 1) SELECT * FROM q ON CONFLICT DO NOTHING",
        "INSERT INTO dst WITH q AS (SELECT 1), r AS (SELECT * FROM q) SELECT * FROM r ON CONFLICT(id) DO UPDATE SET id=excluded.id RETURNING id",
        "INSERT INTO dst OVERRIDING SYSTEM VALUE WITH q AS (SELECT 1) SELECT * FROM q"
    };
    const std::vector<std::string> invalid = {
        "INSERT INTO dst WITH q AS (SELECT 1) DELETE FROM dst",
        "INSERT INTO dst WITH q AS (SELECT 1) UPDATE dst SET id=2",
        "INSERT INTO dst WITH q AS (SELECT 1) INSERT INTO dst VALUES(1)",
        "INSERT INTO dst WITH q AS (SELECT 1)",
        "INSERT INTO dst WITH q AS SELECT 1 SELECT 1",
        "INSERT INTO dst WITH q AS (SELECT 1), SELECT 1"
    };
    for (bool binding : {false, true}) {
        for (const auto& sql : valid) {
            const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            ++checked;
            std::cout << "[INSERT WITH QUERY SOURCE] binding=" << binding
                      << " valid=" << parsed.isValid() << " sql=" << sql
                      << " error=" << parsed.error << std::endl;
            assert(parsed.isValid());
            const auto* insert = dynamic_cast<const dbms::InsertStmt*>(parsed.stmt.get());
            assert(insert && dynamic_cast<const dbms::SelectStmt*>(insert->selectSource.get()));
            if (sql.find("RETURNING") != std::string::npos) assert(!insert->returning.empty());
            if (sql.find("ON CONFLICT") != std::string::npos) assert(!insert->conflictAction.empty());
        }
        for (const auto& sql : invalid) {
            const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            ++checked;
            std::cout << "[INSERT WITH QUERY SOURCE NEGATIVE] binding=" << binding
                      << " valid=" << parsed.isValid() << " sql=" << sql
                      << " error=" << parsed.error << std::endl;
            assert(!parsed.isValid());
        }
    }
    assert(checked == 24);
    std::cout << "[INSERT WITH QUERY SOURCE] complete24 source grammar/AST/binding/conflict/RETURNING controls passed\n";
}
