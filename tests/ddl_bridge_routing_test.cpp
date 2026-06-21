#include "commands/DdlExecutor.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_bridge_handles_create_table() {
    std::string db = "ddl_route_t1";
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
    std::string db = "ddl_route_t2";
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

static void test_bridge_falls_back_for_ctas() {
    std::string db = "ddl_route_t3";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);

    // First create a source table so the legacy CTAS path has something to select.
    std::string src = "create table src (id int)";
    dbms::SqlCommand srcCmd = dbms::SQLParser::classify(src);
    bool handled = false;
    bool err = dbms::tryDdlBridge(src, srcCmd, s, handled);
    assert(handled && !err);

    std::string sql = "create table ctas_dst as select * from src";
    dbms::SqlCommand cmd = dbms::SQLParser::classify(sql);
    err = dbms::tryDdlBridge(sql, cmd, s, handled);
    // CTAS is intentionally left on legacy inline path.
    assert(!handled);
    assert(!err);

    cleanup(db);
    std::cout << "[DDL-ROUTE] bridge falls back for CTAS OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bridge_handles_create_table();
    test_bridge_falls_back_for_unhandled();
    test_bridge_falls_back_for_ctas();
    std::cout << "[DDL-ROUTE] all passed" << std::endl;
    return 0;
}
