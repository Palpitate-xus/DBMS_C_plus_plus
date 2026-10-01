#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
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
    const std::string database = testDbPath("alter_sequence_quoted_options");
    cleanupTestDb("alter_sequence_quoted_options");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA \"alter space\"", session));
    assert(!ddl.executeSql("CREATE TABLE \"alter space\".\"table.dot\"(\"id value\" BIGINT,\"id\"\"quote\" BIGINT)", session));
    assert(!ddl.executeSql("CREATE SEQUENCE \"alter space\".\"seq.dot\"", session));
    assert(!ddl.executeSql("ALTER SEQUENCE \"alter space\".\"seq.dot\" OWNED BY \"alter space\".\"table.dot\".\"id value\"", session));
    assert(!ddl.executeSql("ALTER SEQUENCE \"alter space\".\"seq.dot\" RENAME TO \"renamed seq\"", session));
    assert(!ddl.executeSql("ALTER SEQUENCE \"alter space\".\"renamed seq\" OWNED BY \"alter space\".\"table.dot\".\"id\"\"quote\" RESTART WITH 23", session));
    dbms::SequenceInfo info;
    const std::string physical = dbms::sequenceStorageName("alter space", "renamed seq");
    assert(g_engine.getSequenceInfo(database, physical, info) == dbms::DBStatus::OK);
    assert(info.ownedByColumn == "id\"quote");
    assert(g_engine.nextval(database, physical) == 23);
    auto& catalog = g_engine.catalogService().get(database);
    const auto* ns = catalog.findNamespaceByName("alter space");
    assert(ns);
    const auto* table = catalog.findClassByName("table.dot", ns->oid);
    const auto* sequence = catalog.findClassByName("renamed seq", ns->oid);
    assert(table && sequence);
    const auto tableOid = table->oid;
    const auto sequenceOid = sequence->oid;
    bool owned = false;
    for (const auto& dependency : catalog.findRefs(dbms::PgClassOid_Class, tableOid, -1)) {
        if (dependency.objid == sequenceOid && dependency.refobjsubid == 2 && dependency.deptype == 'a')
            owned = true;
    }
    assert(owned);
    assert(!ddl.executeSql("ALTER SEQUENCE \"alter space\".\"renamed seq\" OWNED BY NONE", session));
    for (const auto& dependency : catalog.findRefs(dbms::PgClassOid_Class, tableOid, -1))
        assert(dependency.objid != sequenceOid);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("alter_sequence_quoted_options");
    std::cout << "[ALTER SEQUENCE QUOTED OPTIONS] passed" << std::endl;
}
