#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    const std::vector<std::string> expected = {"SELECT", "1", "AS", "n", ",",
        "'-- /* literal */'", "AS", "data", ";"};
    for (const std::string separator : {" ", "/* outer /* inner */ end */",
            "/* outer /* inner /* deep */ end */ end */", "-- ignored\n", "-- ignored\r", "-- ignored\r\n"}) {
        const std::string sql = "SELECT" + separator + "1 AS n," + separator +
            "'-- /* literal */' AS data;";
        assert(dbms::SQLParser::tokenize(sql) == expected);
        auto parsed = dbms::SQLParser().parse(sql);
        const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
        assert(parsed.success && select && select->selectList.size() == 2);
        assert(select->selectList[0].alias == "n" && select->selectList[1].alias == "data");
    }
    const auto quoted = dbms::SQLParser::tokenize("SELECT \"a/*b*/\", $$-- /* dollar */$$;");
    assert(quoted.size() == 5 && quoted[1] == "\"a/*b*/\"" && quoted[3] == "'-- /* dollar */'");
    assert(dbms::SQLParser::tokenize("SELECT 1 -- EOF").size() == 2);
    std::cout << "[NESTED SQL COMMENTS] passed\n";
}
