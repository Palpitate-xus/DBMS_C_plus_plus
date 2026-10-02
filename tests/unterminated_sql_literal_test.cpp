#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    for (const std::string sql : {"SELECT 'open", "SELECT 'doubled''",
            "SELECT E'open", "SELECT E'escaped\\'", "SELECT \"open",
            "SELECT \"doubled\"\"", "SELECT $$open", "SELECT $tag$open$other$"}) {
        assert(!dbms::SQLParser::lexicalError(sql).empty());
        const auto parsed = dbms::SQLParser().parse(sql);
        assert(!parsed.success && parsed.error.find("unterminated") != std::string::npos);
    }
    for (const std::string sql : {R"(SELECT 'slash\' AS v;)",
            R"(SELECT E'it\'s';)", R"(SELECT E'quote\'';)", "SELECT 'it''s' AS v;",
            "SELECT 1 AS \"double\"\"quote\";", "SELECT $$/* data ' \" */$$;",
            "SELECT $tag$body$tag$;", "SELECT 1 AS foo$tag$bar$tag$;"}) {
        assert(dbms::SQLParser::lexicalError(sql).empty());
    }
    assert(dbms::SQLParser::lexicalError(R"(SELECT 'slash\'; /* open)") ==
           "unterminated block comment");
    assert(dbms::SQLParser::tokenize(R"(SELECT 'slash\' AS v;)") ==
           (std::vector<std::string>{"SELECT", "'slash\\'", "AS", "v", ";"}));
    assert(dbms::SQLParser::tokenize("SELECT 1 AS foo$tag$bar$tag$;") ==
           (std::vector<std::string>{"SELECT", "1", "AS", "foo$tag$bar$tag$", ";"}));
    assert(dbms::SQLParser::tokenize(R"(SELECT E'quote\'';)") ==
           (std::vector<std::string>{"SELECT", "'quote'''", ";"}));
    std::cout << "[UNTERMINATED SQL LITERAL] passed\n";
}
