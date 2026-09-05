#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
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

static std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static void test_domain_check() {
    std::string db = testDbPath("domain_check");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE DOMAIN pos_int AS INT CHECK (VALUE > 0)", s);
    assert(!err);

    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x pos_int)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"x", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"x", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"x", "-5"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id", "x"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "1 10");

    cleanup(db);
    std::cout << "[DOMAIN] check OK" << std::endl;
}

static void test_domain_default() {
    std::string db = testDbPath("domain_default");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE DOMAIN dflt_int AS INT DEFAULT 42", s);
    assert(!err);

    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x dflt_int)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"x"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "42");

    cleanup(db);
    std::cout << "[DOMAIN] default OK" << std::endl;
}

static void test_domain_update() {
    std::string db = testDbPath("domain_update");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE DOMAIN small_text AS VARCHAR(50) CHECK (length(VALUE) <= 5)", s);
    assert(!err);

    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x small_text)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"x", "hi"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"x", "hello"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"x", "too long"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[DOMAIN] update OK" << std::endl;
}

static void test_domain_metadata_rewrites_are_atomic() {
    const std::string db = testDbPath("domain_metadata_atomicity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::StorageEngine::DomainInfo first;
    first.name = "first_domain";
    first.baseType = "int";
    first.defaultValue = "1";
    dbms::StorageEngine::DomainInfo second = first;
    second.name = "second_domain";
    second.defaultValue = "2";
    assert(g_engine.createDomain(db, first) == dbms::DBStatus::OK);
    assert(g_engine.createDomain(db, second) == dbms::DBStatus::OK);

    const fs::path path = fs::path(db) / ".domains";
    const fs::path savedPath = fs::path(db) / ".domains.saved";
    const std::string originalBytes = readFile(path);
    assert(!originalBytes.empty());
    fs::rename(path, savedPath);
    assert(fs::create_directory(path));

    dbms::StorageEngine::DomainInfo changed = first;
    changed.defaultValue = "42";
    assert(g_engine.alterDomain(db, first.name, changed) ==
           dbms::DBStatus::IO_ERROR);
    assert(g_engine.dropDomain(db, first.name) == dbms::DBStatus::IO_ERROR);
    fs::remove(path);
    fs::rename(savedPath, path);
    assert(readFile(path) == originalBytes);

    assert(g_engine.alterDomain(db, first.name, changed) ==
           dbms::DBStatus::OK);
    dbms::StorageEngine reloaded;
    assert(reloaded.getDomain(db, first.name).defaultValue == "42");
    assert(reloaded.getDomain(db, second.name).defaultValue == "2");

    assert(g_engine.dropDomain(db, first.name) == dbms::DBStatus::OK);
    dbms::StorageEngine afterDrop;
    assert(afterDrop.getDomain(db, first.name).name.empty());
    assert(afterDrop.getDomain(db, second.name).defaultValue == "2");

    cleanup(db);
    std::cout << "[DOMAIN] metadata rewrites are atomic OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_domain_check();
    test_domain_default();
    test_domain_update();
    test_domain_metadata_rewrites_are_atomic();
    std::cout << "[DOMAIN_FULL] all passed" << std::endl;
    return 0;
}
