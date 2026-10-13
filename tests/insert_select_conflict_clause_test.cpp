#include "parser/parser.h"
#include <iostream>
#include <string>
#include <vector>

int main() {
    dbms::SQLParser parser;
    size_t checks = 0, failures = 0;
    const std::vector<std::string> sources{
        "SELECT 1, 10", "SELECT 1, 10 WHERE FALSE",
        "SELECT s.id,s.v FROM source_rows s WHERE TRUE",
        "SELECT s.id,s.v FROM source_rows s JOIN peer p ON s.id=p.id WHERE TRUE",
        "SELECT (SELECT 1), 10 WHERE FALSE",
        "SELECT 1, length('ON CONFLICT RETURNING')",
        "SELECT 1, length($q$ON CONFLICT RETURNING$q$)",
        "SELECT 1,10 UNION ALL SELECT 2,20"};
    for (bool binding : {false, true}) {
        for (const auto& source : sources) {
            for (const auto& action : std::vector<std::string>{
                    "ON CONFLICT DO NOTHING",
                    "ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE TRUE"}) {
                const std::string sql = "INSERT INTO target_rows(id,v) " + source + " " + action + " RETURNING id,v";
                const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
                const auto* insert = parsed.isValid() ? dynamic_cast<const dbms::InsertStmt*>(parsed.stmt.get()) : nullptr;
                const bool good = insert && insert->selectSource && insert->returning.size() == 2 &&
                    insert->conflictAction == (action.find("UPDATE") != std::string::npos ? "DO UPDATE" : "DO NOTHING");
                ++checks;
                if (!good) ++failures;
                std::cout << "INSERT_SELECT_CONFLICT " << binding << ' ' << sql << " PASS " << good
                          << " ERROR " << parsed.error << '\n';
            }
        }
        for (const auto& sql : std::vector<std::string>{
                "INSERT INTO target_rows SELECT 1,10 ON /* between */ CONFLICT(id) DO NOTHING",
                "INSERT INTO target_rows SELECT 1,10 RETURNING id",
                "INSERT INTO target_rows SELECT 1,10 ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE FALSE"}) {
            const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            ++checks;
            if (!parsed.isValid()) ++failures;
            std::cout << "INSERT_SELECT_CONFLICT_CONTROL " << binding << ' ' << sql << " PASS " << parsed.isValid()
                      << " ERROR " << parsed.error << '\n';
        }
        if (binding) {
            for (const auto& sql : std::vector<std::string>{
                    "INSERT INTO target_rows SELECT 1,10 ON CONFLICT",
                    "INSERT INTO target_rows SELECT 1,10 ON CONFLICT(id) DO UPDATE",
                    "INSERT INTO target_rows SELECT 1,10 ON CONFLICT(id) DO NOTHING garbage",
                    "INSERT INTO target_rows SELECT 1,10 ON CONFLICT(id) DO UPDATE SET"}) {
                const auto parsed = parser.parseForBinding(sql);
                ++checks;
                if (parsed.isValid()) ++failures;
                std::cout << "INSERT_SELECT_CONFLICT_REJECT " << sql << " PASS " << !parsed.isValid() << '\n';
            }
        }
    }
    std::cout << "INSERT_SELECT_CONFLICT_CHECKS " << checks << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
