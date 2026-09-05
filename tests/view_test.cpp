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

static void test_create_view() {
    std::string db = testDbPath("view_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE VIEW v AS SELECT id, name FROM t", s));
    assert(g_engine.viewExists(db, "v"));
    std::string sql = g_engine.getViewSQL(db, "v");
    assert(!sql.empty());
    assert(g_engine.getViewBaseTable(db, "v") == "t");

    cleanup(db);
    std::cout << "[VIEW] create/select OK" << std::endl;
}

static void test_create_or_replace_view() {
    std::string db = testDbPath("view_replace");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50), score INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"score", "90"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE VIEW v AS SELECT id, name FROM t", s));
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW v AS SELECT id, score FROM t", s));

    assert(g_engine.getViewBaseTable(db, "v") == "t");
    std::string sql = g_engine.getViewSQL(db, "v");
    assert(sql.find("score") != std::string::npos);

    cleanup(db);
    std::cout << "[VIEW] or replace OK" << std::endl;
}

static void test_view_with_check_option() {
    std::string db = testDbPath("view_wco");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, score INT)", s));
    assert(!ddl.executeSql("CREATE VIEW v AS SELECT * FROM t WHERE score > 0 WITH CHECK OPTION", s));
    assert(g_engine.viewExists(db, "v"));

    cleanup(db);
    std::cout << "[VIEW] with check option OK" << std::endl;
}

static void test_view_existence_requires_metadata_file() {
    const std::string db = testDbPath("view_metadata_kind");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    const fs::path fakeView = g_engine.viewsDir(db) / "fake.view";
    fs::create_directories(fakeView);
    assert(!g_engine.viewExists(db, "fake"));
    assert(g_engine.getViewSQL(db, "fake").empty());

    cleanup(db);
    std::cout << "[VIEW] metadata object type validation OK" << std::endl;
}

static void test_schema_qualified_view_name() {
    const std::string db = testDbPath("view_qualified_name");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));
    assert(ddl.executeSql(
        "CREATE VIEW missing_schema.v AS SELECT id FROM t", s));
    assert(!g_engine.viewExists(db, "missing_schema.v"));
    assert(!g_engine.viewExists(db, "v"));

    assert(!ddl.executeSql("CREATE SCHEMA reporting", s));
    assert(!ddl.executeSql(
        "CREATE VIEW reporting.v AS SELECT id FROM t", s));
    assert(g_engine.viewExists(db, "reporting.v"));
    assert(!g_engine.viewExists(db, "v"));
    assert(g_engine.getViewBaseTable(db, "reporting.v") == "t");
    assert(g_engine.dropView(db, "reporting.v") == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[VIEW] schema-qualified name OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_view();
    test_create_or_replace_view();
    test_view_with_check_option();
    test_view_existence_requires_metadata_file();
    test_schema_qualified_view_name();
    std::cout << "[VIEW] all passed" << std::endl;
    return 0;
}
