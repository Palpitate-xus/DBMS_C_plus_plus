#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const std::string sql : {
             "WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1",
             "WITH C AS(SELECT 1),\"c\" AS(SELECT 2) SELECT 1",
             "WITH \"a\"\"b\" AS(SELECT 1),\"a\"\"b\" AS(SELECT 2) SELECT 1",
             "WITH RECURSIVE c AS(SELECT 1),c AS(SELECT 2) SELECT 1",
             "WITH c(id) AS MATERIALIZED(SELECT 1),c(id) AS NOT MATERIALIZED(SELECT 2) SELECT 1",
             "WITH x AS(WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1) SELECT 1",
             "SELECT * FROM (WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1) AS d",
             "WITH written AS(INSERT INTO t VALUES(1) RETURNING id),"
             "x AS(WITH c AS(SELECT 1),c AS(SELECT 2) SELECT 1) SELECT 1"}) {
        const auto parsed = parser.parse(sql);
        if (parsed.success || parsed.error.find("(SQLSTATE 42712)") == std::string::npos) {
            std::cerr << "duplicate CTE accepted or misdiagnosed: " << sql
                      << " error=" << parsed.error << '\n';
            return 1;
        }
    }
    for (const std::string sql : {
             "WITH c AS(SELECT 1),\"C\" AS(SELECT 2) SELECT 1",
             "WITH c AS(SELECT 1),x AS(WITH c AS(SELECT 2) SELECT 1) SELECT 1",
             "SELECT 'WITH c AS(SELECT 1),c AS(SELECT 2)'",
             "SELECT $$WITH c AS(SELECT 1),c AS(SELECT 2)$$",
             "SELECT 1 /* WITH c AS(SELECT 1),c AS(SELECT 2) */"}) {
        const auto parsed = parser.parse(sql);
        assert(parsed.success);
    }
    assert(!dbms::SQLParser::duplicateCteName("SELECT 'WITH c AS(SELECT 1),c AS(SELECT 2)"));
    assert(!dbms::SQLParser::duplicateCteName("WITH c AS NOT(SELECT 1),c AS(SELECT 2) SELECT 1"));
    std::string nested = "SELECT 1";
    for (int i = 0; i < 300; ++i) nested = "WITH c AS(" + nested + ") SELECT 1";
    assert(!dbms::SQLParser::duplicateCteName(nested));
    std::cout << "[CTE DUPLICATE NAME PARSER] passed\n";
}
