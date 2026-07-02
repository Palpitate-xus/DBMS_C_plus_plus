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

static std::string trimRight(const std::string& s) {
    size_t end = s.find_last_not_of(" \t\n\r");
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

static void test_enum_basic() {
    std::string db = testDbPath("enum_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')", s);
    assert(!err);

    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, m mood)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"m", "happy"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"m", "sad"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"m", "angry"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id", "m"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[ENUM] basic OK" << std::endl;
}

static void test_enum_update() {
    std::string db = testDbPath("enum_update");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TYPE color AS ENUM ('red', 'green', 'blue')", s);
    assert(!err);
    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, c color)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"c", "red"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"c", "green"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"c", "yellow"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ENUM] update OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_enum_basic();
    test_enum_update();
    std::cout << "[ENUM] all passed" << std::endl;
    return 0;
}
