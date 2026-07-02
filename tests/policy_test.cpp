#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_policy_all() {
    std::string db = testDbPath("policy_all");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    bool err = ddl.executeSql("CREATE POLICY p ON t", s);
    assert(!err);

    auto policies = g_engine.getPolicies(db, "t");
    assert(policies.size() == 1);
    assert(policies[0].name == "p");
    assert(policies[0].cmd == "ALL");

    cleanup(db);
    std::cout << "[POLICY] ALL OK" << std::endl;
}

static void test_create_policy_using_and_check() {
    std::string db = testDbPath("policy_check");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, owner VARCHAR(50))", s));
    assert(!ddl.executeSql("CREATE POLICY p ON t FOR UPDATE TO public USING (owner = current_user) WITH CHECK (owner = current_user)", s));

    auto policies = g_engine.getPolicies(db, "t");
    assert(policies.size() == 1);
    assert(policies[0].cmd == "UPDATE");
    assert(!policies[0].usingExpr.empty());
    assert(!policies[0].withCheckExpr.empty());

    cleanup(db);
    std::cout << "[POLICY] USING + WITH CHECK OK" << std::endl;
}

static void test_create_policy_insert() {
    std::string db = testDbPath("policy_insert");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE POLICY p ON t FOR INSERT WITH CHECK (id > 0)", s));

    auto policies = g_engine.getPolicies(db, "t");
    assert(policies.size() == 1);
    assert(policies[0].cmd == "INSERT");
    assert(policies[0].withCheckExpr.find("id") != std::string::npos);

    cleanup(db);
    std::cout << "[POLICY] INSERT OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_policy_all();
    test_create_policy_using_and_check();
    test_create_policy_insert();
    std::cout << "[POLICY] all passed" << std::endl;
    return 0;
}
