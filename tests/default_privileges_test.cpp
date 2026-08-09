#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) {
    if (std::filesystem::exists(db)) std::filesystem::remove_all(db);
}

int main() {
    const std::string db = testDbPath("default_privileges");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
    dbms::DdlExecutor ddl;

    // The typed path accepts comma-separated table privileges and is idempotent.
    assert(!ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "GRANT SELECT, INSERT ON TABLES TO alice", s));
    assert(!ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "GRANT SELECT ON TABLES TO alice", s));
    assert(g_engine.getDefaultPrivileges(db).size() == 2);

    assert(!ddl.executeSql("CREATE TABLE default_acl_one (id INT)", s));
    assert(g_engine.hasPermission(db, "default_acl_one", "alice",
                                  dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasPermission(db, "default_acl_one", "alice",
                                  dbms::StorageEngine::TablePrivilege::Insert));

    // REVOKE changes future defaults and does not rewrite an existing table ACL.
    assert(!ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE SELECT ON TABLES FROM alice", s));
    assert(g_engine.hasPermission(db, "default_acl_one", "alice",
                                  dbms::StorageEngine::TablePrivilege::Select));
    assert(!ddl.executeSql("CREATE TABLE default_acl_two (id INT)", s));
    assert(!g_engine.hasPermission(db, "default_acl_two", "alice",
                                   dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasPermission(db, "default_acl_two", "alice",
                                  dbms::StorageEngine::TablePrivilege::Insert));

    // Unsupported object types and grant-option mutations fail closed.
    assert(ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO alice", s));
    assert(ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO alice WITH GRANT OPTION", s));

    assert(!ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES GRANT ALL PRIVILEGES ON TABLES TO alice", s));
    assert(!ddl.executeSql(
        "ALTER DEFAULT PRIVILEGES REVOKE ALL PRIVILEGES ON TABLES FROM alice", s));
    for (const auto& entry : g_engine.getDefaultPrivileges(db)) {
        assert(std::get<4>(entry) != "alice");
    }

    cleanup(db);
    std::cout << "[DEFAULT PRIVILEGES] TABLE GRANT/REVOKE OK" << std::endl;
    return 0;
}
