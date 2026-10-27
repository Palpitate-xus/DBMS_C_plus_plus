#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "Session.h"

#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "EXCLUSION_STATUS_NATIVE_FAIL " << label << '\n'; }
    };
    const std::string database = "__t_r275_exclusion_sqlstate";
    if (g_engine.createDatabase(database) != DBStatus::OK) {
        std::cerr << "CONTROL_ERROR: could not create private test database\n";
        return 2;
    }
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = database;
    DdlExecutor ddl;
    struct Case { std::string type, method, op, first, second, conflict, replacement; };
    const std::vector<Case> cases{
        {"TEXT", "btree", "=", "x", "y", "x", "z"},
        {"VARCHAR(32)", "btree", "=", "aa", "bb", "aa", "cc"},
        {"INT", "btree", "=", "1", "2", "1", "3"},
        {"int4range", "gist", "&&", "[1,5)", "[10,15)", "[2,3)", "[20,25)"},
    };
    for (size_t index = 0; index < cases.size(); ++index) {
        const auto& item = cases[index];
        const std::string table = "exclusion_" + std::to_string(index);
        const bool ddlError = ddl.executeSql("CREATE TABLE " + table +
            "(id INT PRIMARY KEY,v " + item.type + ",CONSTRAINT ex_" + std::to_string(index) +
            " EXCLUDE USING " + item.method + "(v WITH " + item.op + "))", session);
        check(!ddlError, table + "/ddl");
        auto status = g_engine.insert(database, table, {{"id", "1"}, {"v", item.first}});
        check(status == DBStatus::OK, table + "/first-insert");
        status = g_engine.insert(database, table, {{"id", "2"}, {"v", item.second}});
        check(status == DBStatus::OK, table + "/second-insert");
        const auto before = g_engine.query(database, table, {}, {"id", "v"});
        check(before.size() == 2, table + "/seed-values");
        status = g_engine.insert(database, table, {{"id", "3"}, {"v", item.conflict}});
        check(status == DBStatus::EXCLUSION_VIOLATION, table + "/insert-status=" + std::to_string(static_cast<int>(status)));
        check(g_engine.query(database, table, {}, {"id", "v"}) == before, table + "/failed-insert-preserves-rows");
        status = g_engine.update(database, table, {{"v", item.conflict}}, {"=id 2"});
        check(status == DBStatus::EXCLUSION_VIOLATION, table + "/update-status=" + std::to_string(static_cast<int>(status)));
        check(g_engine.query(database, table, {}, {"id", "v"}) == before, table + "/failed-update-preserves-rows");
        status = g_engine.update(database, table, {{"v", item.replacement}}, {"=id 2"});
        check(status == DBStatus::OK, table + "/valid-update");
        const auto after = g_engine.query(database, table, {}, {"id", "v"});
        check(after.size() == 2 && after != before, table + "/valid-update-visible");
    }
    // The runner owns a fresh private CWD. Let the engine finish writing
    // before that private directory is removed; do not delete live heaps.
    std::cout << "EXCLUSION_STATUS_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return controls == 40 && failures == 0 ? 0 : 1;
}
