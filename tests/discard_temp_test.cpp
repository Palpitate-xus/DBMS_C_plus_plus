#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "commands/SequenceStorageName.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::SQLParser parser;
    for (const auto& sql : {"DISCARD TEMP", "DISCARD TEMPORARY;",
                           "/* x */ DISCARD\nTEMP;"}) {
        auto parsed = parser.parse(sql);
        const auto* discard = dynamic_cast<const dbms::DiscardStmt*>(parsed.stmt.get());
        assert(parsed.success && discard);
        assert(discard->target == dbms::DiscardStmt::Target::Temp);
        assert(discard->toString() == "DISCARD TEMP");
    }
    for (const auto& sql : {"DISCARD", "DISCARD TEMP extra", "DISCARD 'temp'",
                           "DISCARD \"temp\"", "DISCARD ALL extra", "DISCARD TEMP;;"}) {
        assert(!parser.parse(sql).success);
    }
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "discard_temp_transaction";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 943943;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TEMP TABLE discarded(id SERIAL);", session));
    auto& catalog = g_engine.catalogService().get(database);
    const dbms::Oid namespaceOid = catalog.findTempNamespace(session.pid)->oid;
    const dbms::Oid tableOid = catalog.findClassByName("discarded", namespaceOid)->oid;
    const dbms::Oid sequenceOid = catalog.findClassByName("discarded_id_seq", namespaceOid)->oid;
    const std::string sequenceStorage = dbms::sequenceStorageName(
        sessionTempSchemaName(session), "discarded_id_seq");
    const auto discardObjects = [&] {
        dbms::DdlTransaction transaction(session);
        assert(transaction.enableSnapshotRollback());
        assert(transaction.begin());
        transaction.markSnapshotDirty();
        assert(g_engine.dropSessionTemporaryObjects(database, session.pid, true));
        session.tempTables.clear();
        session.tempTableOnCommit.clear();
        session.tempTablesCreatedInTransaction.clear();
        assert(transaction.commit());
        assert(g_engine.catalogService().get(database).findTempNamespace(session.pid)->oid == namespaceOid);
        assert(!g_engine.tableExists(database, tempTablePrefix(session, "discarded")));
        assert(!g_engine.sequenceExists(database, sequenceStorage));
    };
    const auto restored = [&] {
        auto& restoredCatalog = g_engine.catalogService().get(database);
        assert(restoredCatalog.findTempNamespace(session.pid)->oid == namespaceOid);
        assert(restoredCatalog.findClass(tableOid));
        assert(restoredCatalog.findClass(sequenceOid));
        assert(g_engine.sequenceExists(database, sequenceStorage));
        assert(session.tempNamespaceCreated && session.tempTables.count("discarded"));
    };
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    discardObjects();
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    restored();
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.savepoint("before_discard") == dbms::DBStatus::OK);
    discardObjects();
    assert(g_engine.rollbackToSavepoint("before_discard") == dbms::DBStatus::OK);
    restored();
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[DISCARD TEMP] passed\n";
}
