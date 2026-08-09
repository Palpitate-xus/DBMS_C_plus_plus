#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) {
    if (std::filesystem::exists(db)) std::filesystem::remove_all(db);
}

static size_t rowCount(const std::string& db, const std::string& table) {
    size_t count = 0;
    g_engine.forEachRow(db, table,
                        [&](uint32_t, uint16_t, const char*, size_t) { ++count; });
    return count;
}

int main() {
    const std::string db = testDbPath("truncate");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
    dbms::DdlExecutor ddl;

    auto parsed = dbms::SQLParser().parse(
        "TRUNCATE TABLE parent, child RESTART IDENTITY CASCADE");
    assert(parsed.success);
    const auto* truncate = dynamic_cast<const dbms::TruncateStmt*>(parsed.stmt.get());
    assert(truncate && truncate->tableNames.size() == 2);
    assert(truncate->restartIdentity && truncate->cascade);
    assert(!dbms::SQLParser().parse("TRUNCATE TABLE parent,").success);
    assert(!dbms::SQLParser().parse(
        "TRUNCATE TABLE parent RESTART IDENTITY CONTINUE IDENTITY").success);

    assert(!ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent(id))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE grandchild (id INT PRIMARY KEY, child_id INT, "
        "FOREIGN KEY (child_id) REFERENCES child(id))", s));
    assert(g_engine.insert(db, "parent", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "child", {{"id", "2"}, {"parent_id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "grandchild", {{"id", "3"}, {"child_id", "2"}}) == dbms::DBStatus::OK);

    // RESTRICT validates dependencies before modifying any relation.
    assert(ddl.executeSql("TRUNCATE TABLE parent RESTRICT", s));
    assert(rowCount(db, "parent") == 1);
    assert(rowCount(db, "child") == 1);
    assert(rowCount(db, "grandchild") == 1);

    // CASCADE recursively includes all FK dependants.
    assert(!ddl.executeSql("TRUNCATE TABLE parent CASCADE", s));
    assert(rowCount(db, "parent") == 0);
    assert(rowCount(db, "child") == 0);
    assert(rowCount(db, "grandchild") == 0);

    assert(!ddl.executeSql("CREATE TABLE independent (id INT)", s));
    assert(!ddl.executeSql("CREATE TABLE independent_two (id INT)", s));
    assert(g_engine.insert(db, "independent", {{"id", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "independent_two", {{"id", "11"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("TRUNCATE TABLE independent, independent_two", s));
    assert(rowCount(db, "independent") == 0);
    assert(rowCount(db, "independent_two") == 0);

    assert(!ddl.executeSql(
        "CREATE TABLE identity_table (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, msg TEXT)", s));
    assert(g_engine.insert(db, "identity_table", {{"msg", "a"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("TRUNCATE TABLE identity_table RESTART IDENTITY", s));
    assert(g_engine.insert(db, "identity_table", {{"msg", "b"}}) == dbms::DBStatus::OK);
    const auto identityRows = g_engine.query(db, "identity_table", {}, {"id"});
    assert(identityRows.size() == 1 && identityRows.front().find("1") == 0);

    // Table ownership is sufficient; superuser/admin status is not required.
    s.permission = 0;
    assert(!ddl.executeSql("TRUNCATE TABLE identity_table", s));

    cleanup(db);
    std::cout << "[TRUNCATE] typed multi-table/restrict/cascade/identity OK" << std::endl;
    return 0;
}
