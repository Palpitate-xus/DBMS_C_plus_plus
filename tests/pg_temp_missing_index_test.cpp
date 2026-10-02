#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "pg_temp_missing_index";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 942942;
    dbms::DdlExecutor ddl;
    const auto error = [&](const std::string& sql, const char* expected) {
        std::ostringstream output;
        auto* previous = std::cout.rdbuf(output.rdbuf());
        const bool failed = ddl.executeSql(sql, session);
        std::cout.rdbuf(previous);
        assert(failed && output.str().find(expected) != std::string::npos);
    };
    error("DROP INDEX pg_temp.missing_index;", "SQLSTATE 3F000");
    assert(!ddl.executeSql("DROP INDEX IF EXISTS pg_temp.missing_index;", session));
    assert(!session.tempNamespaceCreated);
    assert(!g_engine.catalogService().get(database).findTempNamespace(session.pid));
    error("DROP INDEX missing_schema.missing_index;", "SQLSTATE 3F000");
    error("DROP INDEX \"PG_TEMP\".missing_index;", "SQLSTATE 3F000");
    assert(!ddl.executeSql("CREATE TEMP TABLE temporary_index_owner(id INT);", session));
    const auto namespaceOid = g_engine.catalogService().get(database)
        .findTempNamespace(session.pid)->oid;
    error("DROP INDEX pg_temp.missing_index;", "SQLSTATE 42704");
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid, true));
    session.tempTables.clear();
    assert(session.tempNamespaceCreated);
    assert(g_engine.catalogService().get(database)
        .findTempNamespace(session.pid)->oid == namespaceOid);
    error("DROP INDEX pg_temp.missing_index;", "SQLSTATE 42704");
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    assert(!g_engine.catalogService().get(database).findTempNamespace(session.pid));
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[PG TEMP MISSING INDEX] passed\n";
}
