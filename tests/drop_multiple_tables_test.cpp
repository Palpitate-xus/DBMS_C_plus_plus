#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("drop_multiple_tables");
    cleanupTestDb("drop_multiple_tables");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 424243;
    dbms::DdlExecutor ddl;
    const auto create = [&](const std::string& name, int value) {
        assert(!ddl.executeSql("CREATE TABLE " + name + "(id INT)", session));
        assert(g_engine.insert(database, name, {{"id", std::to_string(value)}}) == dbms::DBStatus::OK);
    };
    create("a", 1);
    create("b", 2);
    assert(!ddl.executeSql("DROP TABLE a,b", session));
    assert(!g_engine.tableExists(database, "a") && !g_engine.tableExists(database, "b"));
    create("a", 1);
    create("b", 2);
    assert(ddl.executeSql("DROP TABLE a,missing", session));
    assert(g_engine.query(database, "a", {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.query(database, "b", {}, {"id"}) == std::vector<std::string>{"2 "});
    assert(!ddl.executeSql("DROP TABLE a,a", session));
    assert(!g_engine.tableExists(database, "a") && g_engine.tableExists(database, "b"));
    assert(!ddl.executeSql("DROP TABLE IF EXISTS missing,b", session));
    assert(!g_engine.tableExists(database, "b"));
    create("c", 3);
    assert(!ddl.executeSql("CREATE VIEW wrong_kind AS SELECT 42 AS id", session));
    assert(ddl.executeSql("DROP TABLE c,wrong_kind", session));
    assert(g_engine.query(database, "c", {}, {"id"}) == std::vector<std::string>{"3 "});
    assert(g_engine.viewExists(database, "wrong_kind"));
    assert(!ddl.executeSql("CREATE TEMP TABLE temporary_a(id INT) ON COMMIT PRESERVE ROWS", session));
    const std::string temporary = tempTablePrefix(session, "temporary_a");
    assert(g_engine.insert(database, temporary, {{"id", "7"}}) == dbms::DBStatus::OK);
    assert(ddl.executeSql("DROP TABLE temporary_a,missing", session));
    assert(session.tempTables.count("temporary_a") == 1);
    assert(session.tempTableOnCommit.at("temporary_a") == "preserve");
    assert(g_engine.query(database, temporary, {}, {"id"}) == std::vector<std::string>{"7 "});
    assert(!ddl.executeSql("DROP TABLE IF EXISTS missing,temporary_a", session));
    assert(session.tempTables.count("temporary_a") == 0);
    assert(!g_engine.tableExists(database, temporary));
    cleanupTestDb("drop_multiple_tables");
    std::cout << "[DROP MULTIPLE TABLES] passed" << std::endl;
}
