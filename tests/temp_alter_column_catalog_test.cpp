#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/SequenceStorageName.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "temp_alter_column_catalog";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 940940;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TEMP TABLE changing_temp(id SERIAL, v VARCHAR(10));", session));
    auto& initialCatalog = g_engine.catalogService().get(database);
    const auto* nameSpace = initialCatalog.findTempNamespace(session.pid);
    assert(nameSpace);
    const dbms::Oid namespaceOid = nameSpace->oid;
    const dbms::Oid tableOid = initialCatalog.findClassByName("changing_temp", namespaceOid)->oid;
    const dbms::Oid sequenceOid = initialCatalog.findClassByName("changing_temp_id_seq", namespaceOid)->oid;
    const std::string sequenceStorage = dbms::sequenceStorageName(
        sessionTempSchemaName(session), "changing_temp_id_seq");
    assert(!ddl.executeSql("ALTER TABLE changing_temp ADD COLUMN note VARCHAR(5);", session));
    const auto* note = g_engine.catalogService().get(database).findAttribute(tableOid, "note");
    assert(note && note->attnum == 3 && note->atttypmod == 9);
    assert(!ddl.executeSql("ALTER TABLE changing_temp RENAME COLUMN id TO new_id;", session));
    auto& renamedCatalog = g_engine.catalogService().get(database);
    assert(!renamedCatalog.findAttribute(tableOid, "id"));
    assert(renamedCatalog.findAttribute(tableOid, "new_id")->attnum == 1);
    dbms::SequenceInfo sequence;
    assert(g_engine.getSequenceInfo(database, sequenceStorage, sequence) == dbms::DBStatus::OK);
    assert(sequence.ownedByColumn == "new_id");
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.savepoint("before_column_drop") == dbms::DBStatus::OK);
    assert(!ddl.executeSql("ALTER TABLE changing_temp DROP COLUMN new_id;", session));
    assert(!g_engine.catalogService().get(database).findClass(sequenceOid));
    assert(!g_engine.sequenceExists(database, sequenceStorage));
    assert(g_engine.rollbackToSavepoint("before_column_drop") == dbms::DBStatus::OK);
    assert(g_engine.catalogService().get(database).findClass(sequenceOid));
    assert(g_engine.catalogService().get(database).findAttribute(tableOid, "new_id"));
    assert(g_engine.sequenceExists(database, sequenceStorage));
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!ddl.executeSql("ALTER TABLE pg_temp.changing_temp DROP COLUMN new_id;", session));
    assert(!g_engine.catalogService().get(database).findClass(sequenceOid));
    assert(!g_engine.catalogService().get(database).findAttribute(tableOid, "new_id"));
    assert(!g_engine.sequenceExists(database, sequenceStorage));
    assert(!ddl.executeSql("DROP TABLE changing_temp;", session));
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[TEMP ALTER COLUMN CATALOG] passed\n";
}
