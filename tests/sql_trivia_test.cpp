#include "common/SqlTrivia.h"
#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    const std::string body = "SELECT '-- /* literal */' AS data;";
    for (const std::string prefix : {"", " \t", "/* preamble */ ", "-- preamble\n",
                                    "/* outer /* nested */ end */ ", "-- first\r\n/* next */ "}) {
        const std::string sql = prefix + body;
        const size_t offset = dbms::skipLeadingSqlTrivia(sql);
        assert(offset == prefix.size());
        assert(sql.substr(offset) == body);
        assert(dbms::SQLParser::classify(sql) == dbms::SqlCommand::Select);
        assert(dbms::SQLParser::classify(prefix + "BEGIN READ ONLY;") == dbms::SqlCommand::Begin);
        auto parsed = dbms::SQLParser().parse(sql);
        assert(parsed.success);
        const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
        assert(select && select->selectList.size() == 1);
        assert(select->selectList.front().alias == "data");
        const auto* literal = dynamic_cast<const dbms::LiteralExpr*>(select->selectList.front().expr.get());
        assert(literal && literal->value == "'-- /* literal */'");
    }
    for (const std::string sql : {"", "-- comment", "/* comment */", " \t\r\n"}) {
        assert(dbms::skipLeadingSqlTrivia(sql) == sql.size());
    }
    for (const std::string sql : {"/*", "/* unclosed", "/* outer /* inner */"}) {
        assert(dbms::skipLeadingSqlTrivia(sql) == std::string::npos);
        assert(dbms::SQLParser::classify(sql) == dbms::SqlCommand::Unknown);
    }
    assert(dbms::skipLeadingSqlTrivia("'/* literal */'") == 0);
    assert(dbms::skipLeadingSqlTrivia("\"-- identifier\"") == 0);
    assert(dbms::skipLeadingSqlTrivia("x /* not leading */") == 0);
    std::cout << "[SQL TRIVIA] passed\n";
}
