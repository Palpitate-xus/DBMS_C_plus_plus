#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "temp_prepared_access";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 956956;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE permanent_rows(id INT PRIMARY KEY);", session));
    assert(!ddl.executeSql("CREATE TEMP TABLE local_rows(id SERIAL PRIMARY KEY);", session));
    const std::string physical = tempTablePrefix(session, "local_rows");
    assert(g_engine.tableExists(database, physical));

    auto assertRejected = [&](const std::string& xid) {
        std::cout << "[TEMP PREPARE NATIVE] " << xid << std::endl;
        assert(g_engine.prepareTransaction(xid) == dbms::DBStatus::FEATURE_NOT_SUPPORTED);
        assert(!g_engine.inTransaction());
        const auto prepared = g_engine.listPreparedTransactions();
        assert(std::find(prepared.begin(), prepared.end(), xid) == prepared.end());
        assert(g_engine.query(database, "permanent_rows", {}, {"id"}).empty());
        assert(g_engine.query(database, physical, {}, {"id"}).empty());
    };
    auto beginPermanentWrite = [&]() {
        assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, "permanent_rows", {{"id", "1"}}) == dbms::DBStatus::OK);
    };

    // An empty TEMP read has no row-undo entry, but it still prevents a
    // backend-independent prepared transaction and must roll back all writes.
    beginPermanentWrite();
    assert(g_engine.query(database, physical, {}, {"id"}).empty());
    assertRejected("temp_prepare_empty_read");

    // The access flag cannot be restored by user or statement savepoints.
    beginPermanentWrite();
    assert(g_engine.savepoint("child") == dbms::DBStatus::OK);
    assert(g_engine.query(database, physical, {}, {"id"}).empty());
    assert(g_engine.rollbackToSavepoint("child") == dbms::DBStatus::OK);
    assertRejected("temp_prepare_child_read");

    beginPermanentWrite();
    assert(g_engine.savepoint("child") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, physical, {{"id", "9"}}) == dbms::DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("child") == dbms::DBStatus::OK);
    assertRejected("temp_prepare_child_write");

    beginPermanentWrite();
    assert(g_engine.savepoint("child") == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TEMP TABLE child_rows(id INT);", session));
    assert(g_engine.rollbackToSavepoint("child") == dbms::DBStatus::OK);
    assert(!g_engine.tableExists(database, tempTablePrefix(session, "child_rows")));
    assertRejected("temp_prepare_child_creation");

    beginPermanentWrite();
    dbms::notePreparedTemporaryObjectAccess(
        "WITH read_temp AS (SELECT id FROM local_rows) SELECT id FROM read_temp;", session);
    assertRejected("temp_prepare_analysis");

    beginPermanentWrite();
    dbms::notePreparedTemporaryObjectAccess(
        "SELECT (SELECT id FROM local_rows LIMIT 1);", session);
    assertRejected("temp_prepare_scalar_analysis");

    beginPermanentWrite();
    dbms::notePreparedTemporaryObjectAccess(
        "SELECT id FROM (SELECT id FROM local_rows) AS nested;", session);
    assertRejected("temp_prepare_derived_analysis");

    beginPermanentWrite();
    dbms::notePreparedTemporaryObjectAccess(
        "WITH local_rows AS (SELECT id FROM local_rows) SELECT id FROM local_rows;", session);
    assertRejected("temp_prepare_nonrecursive_cte_analysis");

    beginPermanentWrite();
    dbms::notePreparedTemporaryObjectAccess(
        "WITH local_rows AS (SELECT 7 AS id) SELECT id FROM local_rows;", session);
    dbms::notePreparedTemporaryObjectAccess(
        "SELECT '(SELECT id FROM local_rows)' AS data;", session);
    assert(g_engine.prepareTransaction("temp_prepare_cte_shadow") == dbms::DBStatus::OK);
    assert(g_engine.rollbackPrepared("temp_prepare_cte_shadow") == dbms::DBStatus::OK);

    // Sequence calls don't write the heap undo log either. In particular a
    // session-local lastval() must inspect the real sequence persistence.
    beginPermanentWrite();
    assert(g_engine.nextval(database, "local_rows_id_seq") == 1);
    assertRejected("temp_prepare_nextval");
    beginPermanentWrite();
    assert(g_engine.currval(database, "local_rows_id_seq") == 1);
    assertRejected("temp_prepare_currval");
    beginPermanentWrite();
    assert(g_engine.lastval() == 1);
    assertRejected("temp_prepare_lastval");
    beginPermanentWrite();
    assert(g_engine.setval(database, "local_rows_id_seq", 20, false) == 20);
    assertRejected("temp_prepare_setval");
    assert(g_engine.nextval(database, "local_rows_id_seq") == 20);

    // State resets at a real top-level boundary, not whenever a TEMP object
    // merely exists in the backend. Both completion directions remain valid.
    beginPermanentWrite();
    assert(g_engine.prepareTransaction("temp_prepare_permanent_rollback") == dbms::DBStatus::OK);
    assert(!g_engine.inTransaction());
    assert(g_engine.rollbackPrepared("temp_prepare_permanent_rollback") == dbms::DBStatus::OK);
    assert(g_engine.query(database, "permanent_rows", {}, {"id"}).empty());
    beginPermanentWrite();
    assert(g_engine.prepareTransaction("temp_prepare_permanent_commit") == dbms::DBStatus::OK);
    assert(g_engine.commitPrepared("temp_prepare_permanent_commit") == dbms::DBStatus::OK);
    const auto rows = g_engine.query(database, "permanent_rows", {}, {"id"});
    assert(rows.size() == 1);
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[TEMP PREPARED ACCESS] passed\n";
}
