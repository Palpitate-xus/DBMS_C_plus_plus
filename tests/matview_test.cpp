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

static void test_create_matview_select_star() {
    std::string db = testDbPath("matview_star");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t", s));

    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    assert(g_engine.tableExists(db, backing));
    assert(g_engine.isMaterializedView(db, "mv"));
    assert(!g_engine.getMaterializedViewSQL(db, "mv").empty());

    auto rows = g_engine.query(db, backing, {}, {"id", "name"}, {});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[MATVIEW] CREATE SELECT * OK" << std::endl;
}

static void test_create_matview_select_columns() {
    std::string db = testDbPath("matview_cols");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50), score INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"score", "90"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}, {"score", "80"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT id, score FROM t", s));

    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    assert(g_engine.tableExists(db, backing));

    auto rows = g_engine.query(db, backing, {}, {"id", "score"}, {});
    assert(rows.size() == 2);
    // Verify name column is not present in backing table schema.
    auto schema = g_engine.getTableSchema(db, backing);
    bool hasName = false;
    for (size_t i = 0; i < schema.len; ++i) {
        if (schema.cols[i].dataName == "name") hasName = true;
    }
    assert(!hasName);

    cleanup(db);
    std::cout << "[MATVIEW] CREATE SELECT columns OK" << std::endl;
}

static void test_create_matview_where() {
    std::string db = testDbPath("matview_where");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, score INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"score", "50"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"score", "90"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT id FROM t WHERE score > 60", s));

    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    auto rows = g_engine.query(db, backing, {}, {"id"}, {});
    assert(rows.size() == 1);
    assert(rows[0].find("2") != std::string::npos);

    cleanup(db);
    std::cout << "[MATVIEW] CREATE WHERE OK" << std::endl;
}

static void test_create_matview_reversed_projection() {
    std::string db = testDbPath("matview_rev");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // Columns id,name,age: selecting "age, id" reverses schema order, which the
    // old set-order mapping got wrong (values landed in the wrong columns).
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20), age INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"age", "30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}, {"age", "25"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT age, id FROM t", s));
    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");

    // Backing has only age,id and rows mapped correctly: query in backing schema
    // order. The backing schema column order follows the projection (age, id).
    auto rows = g_engine.query(db, backing, {}, {}, {});
    assert(rows.size() == 2);
    std::set<std::string> got(rows.begin(), rows.end());
    // age then id, each value with trailing space.
    assert(got.count("30 1 "));
    assert(got.count("25 2 "));

    cleanup(db);
    std::cout << "[MATVIEW] reversed projection mapping OK" << std::endl;
}

static void test_create_matview_with_no_data() {
    std::string db = testDbPath("matview_nodata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WITH NO DATA", s));
    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    assert(g_engine.tableExists(db, backing));         // structure created
    auto schema = g_engine.getTableSchema(db, backing);
    assert(schema.len == 2);                            // both columns present
    auto rows = g_engine.query(db, backing, {}, {}, {});
    assert(rows.empty());                               // but no rows

    cleanup(db);
    std::cout << "[MATVIEW] WITH NO DATA OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_matview_select_star();
    test_create_matview_select_columns();
    test_create_matview_where();
    test_create_matview_reversed_projection();
    test_create_matview_with_no_data();
    std::cout << "[MATVIEW] all passed" << std::endl;
    return 0;
}
