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
        if (!good) { ++failures; std::cerr << "EXCLUSION_COMMAND_NATIVE_FAIL " << label << '\n'; }
    };
    const std::string database = "__t_r276_exclusion_command";
    if (g_engine.createDatabase(database) != DBStatus::OK) {
        std::cerr << "CONTROL_ERROR: private database creation failed\n";
        return 2;
    }
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    DdlExecutor ddl;
    struct Case { std::string type, method, op, value; };
    const std::vector<Case> cases{
        {"TEXT", "btree", "=", "x"}, {"VARCHAR(32)", "btree", "=", "yy"},
        {"INT", "btree", "=", "7"}, {"int4range", "gist", "&&", "[1,5)"},
    };
    for (size_t index = 0; index < cases.size(); ++index) {
        const auto& item = cases[index];
        const std::string table = "command_" + std::to_string(index);
        const bool ddlError = ddl.executeSql("CREATE TABLE " + table +
            "(id INT PRIMARY KEY,v " + item.type + ",CONSTRAINT ex_" + std::to_string(index) +
            " EXCLUDE USING " + item.method + "(v WITH " + item.op + ") NOT DEFERRABLE)", session);
        check(!ddlError, table + "/ddl");
        auto status = g_engine.beginTransaction(database);
        check(status == DBStatus::OK, table + "/begin");
        check(g_engine.beginSqlCommand(), table + "/command-begin");
        status = g_engine.insert(database, table, {{"id", "1"}, {"v", item.value}});
        check(status == DBStatus::OK, table + "/first-insert");
        check(g_engine.query(database, table, {}, {"id", "v"}).empty(), table + "/ordinary-query-hides-current-command");
        status = g_engine.insert(database, table, {{"id", "2"}, {"v", item.value}});
        check(status == DBStatus::EXCLUSION_VIOLATION, table + "/constraint-sees-earlier-current-write");
        check(g_engine.query(database, table, {}, {"id", "v"}).empty(), table + "/query-snapshot-still-unchanged");
        check(g_engine.finishSqlCommand(), table + "/command-finish");
        check(g_engine.query(database, table, {}, {"id", "v"}).size() == 1, table + "/completed-command-has-one-row");
        status = g_engine.rollbackTransaction();
        check(status == DBStatus::OK, table + "/rollback");
        check(g_engine.query(database, table, {}, {"id", "v"}).empty(), table + "/rollback-restores-empty-table");
    }
    std::cout << "EXCLUSION_COMMAND_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return controls == 44 && failures == 0 ? 0 : 1;
}
