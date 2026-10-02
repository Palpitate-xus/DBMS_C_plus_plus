#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    for (const std::string sql : {"SELECT 1 /*", "SELECT 1 /* outer /* inner */",
            "BEGIN /* open", "SHOW transaction_isolation /* open",
            "SET TRANSACTION READ ONLY /* open", "/* outer /* inner */"}) {
        assert(dbms::SQLParser::lexicalError(sql) == "unterminated block comment");
        const auto parsed = dbms::SQLParser().parse(sql);
        assert(!parsed.success && parsed.error == "unterminated block comment");
    }
    for (const std::string sql : {"SELECT '/* not comment';", "SELECT $$/* data$$;",
            "SELECT \"/* identifier\";", "SELECT 1 -- /* line EOF",
            "SELECT /* outer /* inner */ end */ 1;", "-- line\rSELECT 1;"}) {
        assert(dbms::SQLParser::lexicalError(sql).empty());
    }
    std::cout << "[UNTERMINATED SQL COMMENT] passed\n";
}
