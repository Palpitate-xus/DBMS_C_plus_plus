#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "temp_index_catalog";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 948948;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TEMP TABLE indexed_temp(id INT,v TEXT);", session));
    auto& catalog = g_engine.catalogService().get(database);
    const auto* nameSpace = catalog.findTempNamespace(session.pid);
    assert(nameSpace);
    const auto namespaceOid = nameSpace->oid;
    const auto tableOid = catalog.findClassByName("indexed_temp", namespaceOid)->oid;
    assert(!ddl.executeSql("CREATE INDEX indexed_temp_index ON indexed_temp(id);", session));
    auto& created = g_engine.catalogService().get(database);
    const auto* index = created.findClassByName("indexed_temp_index", namespaceOid);
    assert(index && index->relkind == 'i' && index->relpersistence == 't');
    const auto indexOid = index->oid;
    bool dependency = false;
    for (const auto& edge : created.findDepends(dbms::PgClassOid_Class, indexOid))
        if (edge.refclassid == dbms::PgClassOid_Class && edge.refobjid == tableOid &&
            edge.deptype == 'a') dependency = true;
    assert(dependency);
    assert(created.findClass(tableOid)->relhasindex);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.savepoint("before_drop") == dbms::DBStatus::OK);
    assert(!ddl.executeSql("DROP INDEX pg_temp.indexed_temp_index;", session));
    assert(!g_engine.catalogService().get(database).findClass(indexOid));
    assert(g_engine.rollbackToSavepoint("before_drop") == dbms::DBStatus::OK);
    assert(g_engine.catalogService().get(database).findClass(indexOid));
    assert(g_engine.getNamedIndex(database, tempTablePrefix(session, "indexed_temp"),
                                  "indexed_temp_index"));
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!ddl.executeSql("DROP INDEX indexed_temp_index;", session));
    assert(!g_engine.catalogService().get(database).findClass(indexOid));
    assert(!ddl.executeSql("CREATE INDEX indexed_temp_index ON indexed_temp(id);", session));
    assert(!ddl.executeSql("DROP TABLE indexed_temp;", session));
    assert(!g_engine.catalogService().get(database).findClassByName(
        "indexed_temp_index", namespaceOid));
    assert(!ddl.executeSql("CREATE TEMP TABLE cleanup_temp(id INT);", session));
    assert(!ddl.executeSql("CREATE INDEX cleanup_temp_index ON cleanup_temp(id);", session));
    const auto cleanupTableOid = g_engine.catalogService().get(database)
        .findClassByName("cleanup_temp", namespaceOid)->oid;
    const auto cleanupIndexOid = g_engine.catalogService().get(database)
        .findClassByName("cleanup_temp_index", namespaceOid)->oid;
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.savepoint("before_cleanup") == dbms::DBStatus::OK);
    {
        dbms::DdlTransaction transaction(session);
        assert(transaction.enableSnapshotRollback());
        assert(transaction.begin());
        transaction.markSnapshotDirty();
        assert(g_engine.dropSessionTemporaryObjects(database, session.pid, true));
        session.tempTables.clear();
        session.tempTableOnCommit.clear();
        session.tempTablesCreatedInTransaction.clear();
        assert(transaction.commit());
    }
    assert(!g_engine.catalogService().get(database).findClass(cleanupTableOid));
    assert(!g_engine.catalogService().get(database).findClass(cleanupIndexOid));
    assert(g_engine.catalogService().get(database).findTempNamespace(session.pid)->oid == namespaceOid);
    assert(g_engine.rollbackToSavepoint("before_cleanup") == dbms::DBStatus::OK);
    assert(g_engine.catalogService().get(database).findClass(cleanupTableOid));
    assert(g_engine.catalogService().get(database).findClass(cleanupIndexOid));
    assert(g_engine.tableExists(database, tempTablePrefix(session, "cleanup_temp")));
    assert(g_engine.getNamedIndex(database, tempTablePrefix(session, "cleanup_temp"),
                                  "cleanup_temp_index"));
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[TEMP INDEX CATALOG] passed\n";
}
