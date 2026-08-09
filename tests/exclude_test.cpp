#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_exclude_equality() {
    std::string db = testDbPath("exclude_eq_t");
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
    std::string db = testDbPath("exclude_range_t");
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
    std::string db = testDbPath("exclude_drop_t");
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

static void test_alter_exclude_typed_bridge() {
    std::string db = testDbPath("exclude_alter_t");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, room VARCHAR(10))", s));

    const auto executeAlter = [&](const std::string& sql) {
        bool handled = false;
        const bool error = dbms::tryDdlBridge(
            sql, dbms::SqlCommand::AlterTable, s, handled);
        assert(handled);
        return error;
    };

    assert(!executeAlter(
        "ALTER TABLE t ADD CONSTRAINT no_dup_room EXCLUDE USING btree (room WITH =)"));
    auto exclusions = g_engine.getExclusionConstraints(db, "t");
    assert(exclusions.size() == 1 && exclusions[0].name == "no_dup_room");
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"room", "101"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"room", "101"}}) == dbms::DBStatus::INVALID_VALUE);

    assert(!executeAlter("ALTER TABLE t DROP CONSTRAINT no_dup_room"));
    assert(g_engine.getExclusionConstraints(db, "t").empty());
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"room", "101"}}) == dbms::DBStatus::OK);

    // The parser must preserve a named EXCLUDE constraint through its
    // dedicated sub-parser, otherwise the typed executor cannot persist it.
    dbms::SQLParser parser;
    auto parsed = parser.parse(
        "ALTER TABLE t ADD CONSTRAINT named_excl EXCLUDE (room WITH =)");
    assert(parsed.success);
    auto* alter = dynamic_cast<dbms::AlterTableStmt*>(parsed.stmt.get());
    assert(alter && alter->subCommands.size() == 1);
    assert(alter->subCommands[0].constraint.name == "named_excl");

    cleanup(db);
    std::cout << "[EXCLUDE] typed ALTER ADD/DROP bridge OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_exclude_equality();
    test_exclude_range_overlap();
    test_exclude_drop_table_cleanup();
    test_alter_exclude_typed_bridge();
    std::cout << "[EXCLUDE] all passed" << std::endl;
    return 0;
}
