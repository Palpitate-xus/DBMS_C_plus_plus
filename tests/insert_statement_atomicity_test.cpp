#include "catalog/type_registry.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

bool runDml(const std::string& sql, Session& session) {
    bool handled = false;
    const bool error = dbms::tryDmlBridge(
        sql, dbms::SQLParser::classify(sql), session, handled, sql);
    assert(handled);
    return error;
}

void assertPresent(const std::string& database, const std::string& id) {
    assert(g_engine.query(database, "target", {"=id " + id}, {"id"})
               .size() == 1);
}

void assertMissing(const std::string& database, const std::string& id) {
    assert(g_engine.query(database, "target", {"=id " + id}, {"id"})
               .empty());
}

void test_multirow_insert_is_atomic_inside_explicit_transaction() {
    const std::string testName = "insert_statement_atomicity";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema target;
    target.tablename = "target";
    target.formatVersion = 2;
    target.append(dbms::makeIntColumn("id", false, 4, true));
    target.append(dbms::makeVarCharColumn("value", false, 40));
    assert(g_engine.createTable(database, target) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "target", {{"id", "1"}, {"value", "existing"}}) ==
           dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "target", {{"id", "2"}, {"value", "survivor"}}) ==
           dbms::DBStatus::OK);
    assert(runDml(
        "INSERT INTO target VALUES (3, 'must-rollback'), (1, 'duplicate')",
        session));
    assert(g_engine.inTransaction());
    assertPresent(database, "1");
    assertPresent(database, "2");
    assertMissing(database, "3");
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertPresent(database, "2");
    assertMissing(database, "3");

    // The bridge is also safe when embedded without main.cpp's top-level
    // statement transaction: it owns and rolls back an internal transaction.
    assert(runDml(
        "INSERT INTO target VALUES (4, 'must-rollback'), (1, 'duplicate')",
        session));
    assert(!g_engine.inTransaction());
    assertMissing(database, "4");

    dbms::TableSchema source = target;
    source.tablename = "source";
    assert(g_engine.createTable(database, source) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "source", {{"id", "5"}, {"value", "selected"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "source", {{"id", "1"}, {"value", "duplicate"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(runDml(
        "INSERT INTO target (id, value) SELECT id, value FROM source", session));
    assert(g_engine.inTransaction());
    assertMissing(database, "5");
    assertPresent(database, "2");
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertMissing(database, "5");

    dbms::TableSchema upsertTarget = target;
    upsertTarget.tablename = "upsert_target";
    upsertTarget.cols[1].isUnique = true;
    assert(g_engine.createTable(database, upsertTarget) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "upsert_target",
               {{"id", "1"}, {"value", "original"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "upsert_target",
               {{"id", "2"}, {"value", "occupied"}}) ==
           dbms::DBStatus::OK);
    const auto originalValue = g_engine.query(
        database, "upsert_target", {"=id 1"}, {"value"});

    // The statement boundary also covers changes made by ON CONFLICT. The
    // first input updates id=1, then the second input conflicts with a
    // different UNIQUE key and makes the whole statement fail.
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(runDml(
        "INSERT INTO upsert_target VALUES (1, 'changed'), (3, 'occupied') "
        "ON CONFLICT (id) DO UPDATE SET value = excluded.value",
        session));
    assert(g_engine.inTransaction());
    const auto rolledBackValue = g_engine.query(
        database, "upsert_target", {"=id 1"}, {"value"});
    assert(rolledBackValue == originalValue);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_multirow_insert_is_atomic_inside_explicit_transaction();
    finalCleanupTestData();
    std::cout << "[INSERT STATEMENT ATOMICITY] all row sources rolled back OK"
              << std::endl;
    return 0;
}
