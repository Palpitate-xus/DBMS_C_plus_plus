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
    const std::string name = "temp_serial_owned_sequence";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 606060;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TEMP TABLE serial_temp(id SERIAL PRIMARY KEY, v TEXT);", session));
    auto& catalog = g_engine.catalogService().get(database);
    const auto* nameSpace = catalog.findTempNamespace(session.pid);
    assert(nameSpace && session.tempNamespaceCreated);
    const dbms::Oid namespaceOid = nameSpace->oid;
    const auto* table = catalog.findClassByName("serial_temp", namespaceOid);
    assert(table && table->relpersistence == 't');
    const dbms::Oid tableOid = table->oid;
    const auto* sequence = catalog.findClassByName("serial_temp_id_seq", namespaceOid);
    assert(sequence && sequence->relkind == 'S' && sequence->relpersistence == 't');
    const dbms::Oid sequenceOid = sequence->oid;
    bool owned = false;
    for (const auto& dependency : catalog.findRefs(dbms::PgClassOid_Class, tableOid, -1)) {
        if (dependency.objid == sequenceOid && dependency.refobjsubid == 1 && dependency.deptype == 'a') owned = true;
    }
    assert(owned);
    const std::string storageName = dbms::sequenceStorageName(sessionTempSchemaName(session), "serial_temp_id_seq");
    assert(g_engine.sequenceExists(database, storageName));
    assert(!ddl.executeSql("ALTER TABLE serial_temp RENAME TO renamed_temp;", session));
    assert(g_engine.catalogService().get(database).findClassByName("renamed_temp", namespaceOid)->oid == tableOid);
    assert(!g_engine.catalogService().get(database).findClassByName("serial_temp", namespaceOid));
    assert(!ddl.executeSql("DROP TABLE renamed_temp;", session));
    assert(!g_engine.catalogService().get(database).findClass(tableOid));
    assert(!g_engine.catalogService().get(database).findClass(sequenceOid));
    assert(!g_engine.sequenceExists(database, storageName));
    assert(!ddl.executeSql("CREATE TEMP TABLE another_temp(id SERIAL);", session));
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    assert(!g_engine.catalogService().get(database).findTempNamespace(session.pid));
    assert(!g_engine.tableExists(database, tempTablePrefix(session, "another_temp")));
    assert(!g_engine.sequenceExists(database, dbms::sequenceStorageName(sessionTempSchemaName(session), "another_temp_id_seq")));
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[TEMP SERIAL OWNED SEQUENCE] passed\n";
}
