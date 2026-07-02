#include "commands/DdlExecutor.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "table_schema.h"
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

static const dbms::Column* findCol(const dbms::TableSchema& t, const std::string& name) {
    for (size_t i = 0; i < t.len; ++i)
        if (t.cols[i].dataName == name) return &t.cols[i];
    return nullptr;
}

static void test_like_basic() {
    std::string db = testDbPath("like_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL DEFAULT 'x')", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src)", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    assert(schema.len == 2);
    const dbms::Column* id = findCol(schema, "id");
    const dbms::Column* name = findCol(schema, "name");
    assert(id && name);
    // Plain LIKE: column structure + NOT NULL copied, but NOT PK or DEFAULT.
    assert(id->isPrimaryKey == false);
    assert(name->isNull == false);            // NOT NULL preserved
    assert(name->defaultValue.empty());       // default NOT copied
    cleanup(db);
    std::cout << "[LIKE] basic structure copy OK" << std::endl;
}

static void test_like_including_defaults() {
    std::string db = testDbPath("like_defaults");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT, label VARCHAR(20) DEFAULT 'hi')", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src INCLUDING DEFAULTS)", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    const dbms::Column* label = findCol(schema, "label");
    assert(label);
    assert(!label->defaultValue.empty());     // default copied
    cleanup(db);
    std::cout << "[LIKE] INCLUDING DEFAULTS OK" << std::endl;
}

static void test_like_including_all() {
    std::string db = testDbPath("like_all");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE src (id INT PRIMARY KEY, amt INT DEFAULT 5)", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src INCLUDING ALL)", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    const dbms::Column* id = findCol(schema, "id");
    const dbms::Column* amt = findCol(schema, "amt");
    assert(id && amt);
    assert(id->isPrimaryKey == true);         // PK copied with INCLUDING ALL
    assert(!amt->defaultValue.empty());       // default copied
    cleanup(db);
    std::cout << "[LIKE] INCLUDING ALL OK" << std::endl;
}

static void test_like_plus_extra_column() {
    std::string db = testDbPath("like_extra");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE src (a INT, b INT)", s));
    assert(!ddl.executeSql("CREATE TABLE cpy (LIKE src, c VARCHAR(10))", s));

    auto schema = g_engine.getTableSchema(db, "cpy");
    assert(schema.len == 3);
    assert(findCol(schema, "a") && findCol(schema, "b") && findCol(schema, "c"));
    cleanup(db);
    std::cout << "[LIKE] LIKE + extra column OK" << std::endl;
}

static void test_like_missing_source() {
    std::string db = testDbPath("like_missing");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    // Source does not exist -> error (true).
    assert(ddl.executeSql("CREATE TABLE cpy (LIKE nope)", s));
    assert(!g_engine.tableExists(db, "cpy"));
    cleanup(db);
    std::cout << "[LIKE] missing source rejected OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_like_basic();
    test_like_including_defaults();
    test_like_including_all();
    test_like_plus_extra_column();
    test_like_missing_source();
    std::cout << "[LIKE] all passed" << std::endl;
    return 0;
}
