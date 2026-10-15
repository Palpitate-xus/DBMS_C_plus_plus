// SHOW's supported finite configuration name has one canonical metadata key.
#include "parser/parser.h"
#include <iostream>
#include <string>
#include <vector>

int main() {
    dbms::SQLParser parser;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& sql) {
        ++controls;
        if (!good) ++failures;
        std::cout << "SHOW_PARALLEL_GRAMMAR " << sql << " pass=" << good << '\n';
    };
    for (const auto* sql : {"SHOW max_parallel_workers_per_gather",
            "SHOW max_parallel_workers_per_gather;", "SHOW MAX_PARALLEL_WORKERS_PER_GATHER",
            "SHOW \"MAX_PARALLEL_WORKERS_PER_GATHER\";", "SHOW \"max_parallel_workers_per_gather\"",
            "SHOW /* outer /* nested */ end */ max_parallel_workers_per_gather; -- tail"}) {
        auto parsed = parser.parse(sql);
        const auto* shown = parsed.success ? dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get()) : nullptr;
        check(shown && shown->isShow && shown->name == "max_parallel_workers_per_gather", sql);
    }
    for (const auto* sql : {"SHOW max_parallel_workers_per_gather junk",
            "SHOW \"MAX_PARALLEL_WORKERS_PER_GATHER\"; junk"})
        check(!parser.parse(sql).success, sql);
    for (const auto* sql : {"SHOW timezone", "SHOW TRANSACTION ISOLATION LEVEL"})
        check(parser.parse(sql).success, sql);
    if (controls != 10) return 2;
    std::cout << "SHOW_PARALLEL_GRAMMAR_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
