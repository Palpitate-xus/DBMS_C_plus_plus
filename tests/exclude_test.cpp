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

static void test_exclude_equality() {
    std::string db = "exclude_eq_t";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, room VARCHAR(10), CONSTRAINT no_dup_room EXCLUDE USING btree (room WITH =))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"room", "101"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"room", "102"}}) == dbms::DBStatus::OK);
    // Duplicate room should be rejected.
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"room", "101"}}) == dbms::DBStatus::INVALID_VALUE);

    // Updating to a conflicting room should be rejected.
    assert(g_engine.update(db, "t", {{"room", "101"}}, {"=id 2"}) == dbms::DBStatus::INVALID_VALUE);
    // Updating to a new room is OK.
    assert(g_engine.update(db, "t", {{"room", "103"}}, {"=id 2"}) == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[EXCLUDE] equality OK" << std::endl;
}

static void test_exclude_range_overlap() {
    std::string db = "exclude_range_t";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, room VARCHAR(10), during int4range, "
        "CONSTRAINT no_overlap EXCLUDE USING gist (during WITH &&))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"room", "101"}, {"during", "[1,5)"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"room", "102"}, {"during", "[5,10)"}}) == dbms::DBStatus::OK);
    // Overlapping range should be rejected.
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"room", "103"}, {"during", "[3,7)"}}) == dbms::DBStatus::INVALID_VALUE);
    // Adjacent non-overlapping range is OK.
    assert(g_engine.insert(db, "t", {{"id", "4"}, {"room", "104"}, {"during", "[10,15)"}}) == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[EXCLUDE] range overlap OK" << std::endl;
}

static void test_exclude_drop_table_cleanup() {
    std::string db = "exclude_drop_t";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, room VARCHAR(10), CONSTRAINT no_dup_room EXCLUDE (room WITH =))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"room", "101"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("DROP TABLE t", s));
    // Recreate table with same constraint name; should not fail due to stale metadata.
    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, room VARCHAR(10), CONSTRAINT no_dup_room EXCLUDE (room WITH =))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"room", "101"}}) == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[EXCLUDE] drop table cleanup OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_exclude_equality();
    test_exclude_range_overlap();
    test_exclude_drop_table_cleanup();
    std::cout << "[EXCLUDE] all passed" << std::endl;
    return 0;
}
