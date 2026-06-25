#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

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

static std::string trimRight(const std::string& s) {
    size_t end = s.find_last_not_of(" \t\n\r");
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

static void test_ctas_select_star() {
    std::string db = "ctas_star";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (id INT PRIMARY KEY, name VARCHAR(50), score INT)", s));
    assert(g_engine.insert(db, "src", {{"id", "1"}, {"name", "alice"}, {"score", "90"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "src", {{"id", "2"}, {"name", "bob"}, {"score", "80"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE TABLE dst AS SELECT * FROM src", s));

    auto rows = g_engine.query(db, "dst", {}, {"id", "name", "score"});
    assert(rows.size() == 2);
    assert(trimRight(rows[0]) == "1 alice 90" || trimRight(rows[0]) == "2 bob 80");

    cleanup(db);
    std::cout << "[CTAS] SELECT * OK" << std::endl;
}

static void test_ctas_select_columns() {
    std::string db = "ctas_cols";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (id INT PRIMARY KEY, name VARCHAR(50), score INT)", s));
    assert(g_engine.insert(db, "src", {{"id", "1"}, {"name", "alice"}, {"score", "90"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE TABLE dst AS SELECT id, name FROM src", s));

    auto rows = g_engine.query(db, "dst", {}, {"id", "name"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "1 alice");

    cleanup(db);
    std::cout << "[CTAS] SELECT columns OK" << std::endl;
}

static void test_ctas_with_where() {
    std::string db = "ctas_where";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (id INT PRIMARY KEY, score INT)", s));
    assert(g_engine.insert(db, "src", {{"id", "1"}, {"score", "90"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "src", {{"id", "2"}, {"score", "70"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE TABLE dst AS SELECT * FROM src WHERE score = 90", s));

    auto rows = g_engine.query(db, "dst", {}, {"id", "score"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "1 90");

    cleanup(db);
    std::cout << "[CTAS] WHERE OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_ctas_select_star();
    test_ctas_select_columns();
    test_ctas_with_where();
    std::cout << "[CTAS] all passed" << std::endl;
    return 0;
}
