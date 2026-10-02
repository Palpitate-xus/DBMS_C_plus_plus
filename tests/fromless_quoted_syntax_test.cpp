#include "common/SqlSyntax.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    for (const std::string sql : {R"(E'it\'s, AS WHERE LIMIT OFFSET' AS v)",
            R"('it''s, AS WHERE LIMIT OFFSET' AS v)",
            R"($Tag$AS WHERE LIMIT OFFSET $tag$ ' inside$Tag$ AS v)",
            R"(1 AS "comma, quote"" name")", "1 /* AS /* WHERE */ LIMIT */ AS v"}) {
        assert(dbms::findTopLevelSqlKeyword(sql, "as") == sql.rfind("AS"));
        assert(dbms::findTopLevelSqlKeyword(sql, "where") == std::string::npos);
        assert(dbms::findTopLevelSqlKeyword(sql, "limit") == std::string::npos);
        const auto protectedBytes = dbms::sqlProtectedBytes(sql);
        for (size_t i = 0; i < sql.size(); ++i) {
            if (sql[i] == ',') assert(protectedBytes[i]);
        }
    }
    const std::string sql = R"(E'where\'limit' AS v WHERE TRUE LIMIT 1 OFFSET 0)";
    assert(dbms::findTopLevelSqlKeyword(sql, "where") == sql.find("WHERE"));
    assert(dbms::findTopLevelSqlKeyword(sql, "limit") == sql.find("LIMIT"));
    assert(dbms::findTopLevelSqlKeyword(sql, "offset") == sql.find("OFFSET"));
    assert(dbms::findTopLevelSqlKeyword("CAST(1 AS text) AS v", "as") == 16);
    assert(dbms::findTopLevelSqlKeyword("ARRAY['where','limit'] AS v", "where") == std::string::npos);
    assert(dbms::findTopLevelSqlKeyword("offset_value AS v", "offset") == std::string::npos);
    assert(dbms::findTopLevelSqlKeyword("1 AS foo$tag$bar;", "as") == 2);
    assert(dbms::findTopLevelSqlKeyword("1 -- AS\r AS v", "as") == 9);
    assert(dbms::findTopLevelSqlKeyword(R"('slash\' AS v)", "as") == 9);
    std::cout << "[FROMLESS QUOTED SYNTAX] passed\n";
}
