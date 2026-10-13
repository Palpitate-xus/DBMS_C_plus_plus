#include "parser/parser.h"
#include <cassert>
#include <iostream>
#include <vector>

int main() {
    dbms::SQLParser parser;
    const std::vector<std::string> valid{
        "INSERT INTO t VALUES(1)", "insert /* note */ into t(id) values($1)",
        "INSERT INTO t DEFAULT VALUES", "INSERT INTO public.\"into\" SELECT 1",
        "DELETE FROM t WHERE id=1 RETURNING id",
        "DELETE FROM ONLY t AS d WHERE d.id=1 RETURNING d.id",
        "delete /* note */ from \"from\" RETURNING *",
        "DELETE FROM t USING u WHERE t.id=u.id"};
    const std::vector<std::string> invalid{
        "INSERT t VALUES(1)", "insert /* note */ t(id) values($1)",
        "INSERT t DEFAULT VALUES", "INSERT public.t SELECT 1",
        "INSERT \"into\" VALUES(1)", "DELETE t WHERE id=1 RETURNING id",
        "DELETE ONLY t AS d WHERE d.id=1 RETURNING d.id",
        "delete /* note */ t RETURNING id", "DELETE \"from\" WHERE id=1",
        "DELETE t USING u WHERE t.id=u.id"};
    for (bool binding : {false, true}) {
        for (const auto& sql : valid) {
            const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            assert(parsed.isValid());
        }
        for (const auto& sql : invalid) {
            const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            std::cout << "REQUIRED_KEYWORD " << binding << ' ' << sql
                      << " valid=" << parsed.isValid() << " state=" << parsed.sqlState << std::endl;
            assert(!parsed.isValid() && parsed.sqlState == "42601");
        }
    }
    std::cout << "[DML REQUIRED KEYWORDS] all 36 parser controls passed\n";
}
