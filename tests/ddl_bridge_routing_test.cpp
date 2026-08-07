#include "commands/DdlExecutor.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "permissions.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_bridge_handles_create_table() {
    std::string db = testDbPath("ddl_route_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    std::string sql = "create table rt1 (id integer primary key, name varchar(100))";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    bool handled = false;
    bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
    assert(handled);
    assert(!err);
    assert(g_engine.tableExists(db, "rt1"));

    // Running the same CREATE again must error exactly once (no double-exec bug).
    err = dbms::tryDdlBridge(sql, cmd, s, handled);
    assert(handled);
    assert(err);
    assert(g_engine.tableExists(db, "rt1"));

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge handles CREATE TABLE and single-runs it OK" << std::endl;
}

static void test_bridge_falls_back_for_unhandled() {
    std::string db = testDbPath("ddl_route_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    // CREATE CAST is not in the bridge set.
    std::string sql = "create cast (int as text) with function int2text";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    bool handled = false;
    bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
    assert(!handled);
    assert(!err);

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge falls back for unhandled DDL OK" << std::endl;
}

static void test_bridge_fails_closed_on_parse_error() {
    Session s;
    setupSession(s, "");

    // The caller normally supplies classify(sql); use an explicit bridge-owned
    // command here to exercise the parse-error contract independently.
    bool handled = false;
    bool err = dbms::tryDdlBridge("", dbms::SqlCommand::CreateTable, s, handled);
    assert(handled);
    assert(err);

    std::cout << "[DDL-ROUTE] parse errors fail closed" << std::endl;
}

static void test_bridge_handles_ctas() {
    std::string db = testDbPath("ddl_route_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    // First create a source table.
    std::string src = "create table src (id int)";
    dbms::SqlCommand srcCmd = dbms::SQLParser::classify(src);
    bool handled = false;
    bool err = dbms::tryDdlBridge(src, srcCmd, s, handled);
    assert(handled && !err);

    std::string sql = "create table ctas_dst as select * from src";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    err = dbms::tryDdlBridge(sql, cmd, s, handled);
    // CTAS is now handled by the DDL AST bridge.
    assert(handled);
    assert(!err);
    assert(g_engine.tableExists(db, "ctas_dst"));

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge handles CTAS OK" << std::endl;
}

static void test_bridge_handles_typed_alter_table() {
    std::string db = testDbPath("ddl_route_alter");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    auto run = [&](const std::string& sql) {
        bool handled = false;
        auto cmd = dbms::SQLParser::classify(sql);
        bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
        assert(handled);
        assert(!err);
    };

    run("CREATE TABLE alter_t (id INT, name VARCHAR(20))");
    run("ALTER TABLE alter_t ADD COLUMN score INT");
    run("ALTER TABLE alter_t ADD COLUMN IF NOT EXISTS score INT");
    assert(g_engine.getTableSchema(db, "alter_t").len == 3);
    run("ALTER TABLE alter_t ALTER COLUMN score SET DEFAULT 10");
    run("ALTER TABLE alter_t ALTER COLUMN score SET NOT NULL");
    run("ALTER TABLE alter_t ADD PRIMARY KEY (id)");
    run("ALTER TABLE alter_t DROP CONSTRAINT IF EXISTS alter_t_pkey");
    run("ALTER TABLE alter_t DROP CONSTRAINT IF EXISTS missing_constraint");
    run("ALTER TABLE alter_t RENAME COLUMN name TO full_name");
    auto renamed = g_engine.getTableSchema(db, "alter_t");
    assert(renamed.cols[1].dataName == "full_name");
    run("ALTER TABLE alter_t RENAME TO alter_t_v2");
    assert(g_engine.tableExists(db, "alter_t_v2"));

    dbms::SQLParser parser;
    auto parsed = parser.parse("ALTER TABLE alter_t_v2 RENAME TO alter_t_v3");
    assert(parsed.success);
    auto* alter = dynamic_cast<dbms::AlterTableStmt*>(parsed.stmt.get());
    assert(alter && alter->subCommands.size() == 1);
    assert(alter->subCommands[0].action == dbms::AlterTableStmt::Action::RenameTable);

    cleanup(db);
    std::cout << "[DDL-ROUTE] typed ALTER TABLE bridge OK" << std::endl;
}

static void test_bridge_handles_catalog_auth_ddl() {
    Session s;
    setupSession(s, "");
    const std::string userName = "route_auth_user";
    const std::string roleName = "route_auth_role";
    const std::string renamedRole = "route_auth_role_renamed";
    dropRole(userName);
    dropRole(roleName);
    dropRole(renamedRole);

    auto run = [&](const std::string& sql) {
        bool handled = false;
        const auto cmd = dbms::SQLParser::classify(sql);
        const bool err = dbms::tryDdlBridge(sql, cmd, s, handled);
        assert(handled);
        assert(!err);
    };

    run("CREATE USER Route_Auth_User WITH PASSWORD 'MiXeD-Case-9!' LOGIN");
    auto account = authCatalog().getAuthIdByName(userName);
    assert(account && account->rolcanlogin);
    assert(verifyUserPassword(userName, "MiXeD-Case-9!"));

    run("ALTER USER Route_Auth_User WITH NOLOGIN PASSWORD 'SeCoNd-Secret-8!'");
    account = authCatalog().getAuthIdByName(userName);
    assert(account && !account->rolcanlogin);
    run("ALTER USER Route_Auth_User WITH LOGIN");
    assert(verifyUserPassword(userName, "SeCoNd-Secret-8!"));

    run("CREATE ROLE Route_Auth_Role");
    run("ALTER ROLE Route_Auth_Role RENAME TO Route_Auth_Role_Renamed");
    assert(!roleExists(roleName));
    assert(roleExists(renamedRole));
    run("DROP ROLE Route_Auth_Role_Renamed");
    run("DROP USER Route_Auth_User");
    assert(!roleExists(userName));

    std::cout << "[DDL-ROUTE] catalog auth DDL and literal case preservation OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bridge_handles_create_table();
    test_bridge_falls_back_for_unhandled();
    test_bridge_fails_closed_on_parse_error();
    test_bridge_handles_ctas();
    test_bridge_handles_typed_alter_table();
    test_bridge_handles_catalog_auth_ddl();
    std::cout << "[DDL-ROUTE] all passed" << std::endl;
    return 0;
}
