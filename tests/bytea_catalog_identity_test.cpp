#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "bytea_catalog_identity";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    auto* previous = currentSession();
    setCurrentSession(&session);
    struct Restore { Session* value; ~Restore() { setCurrentSession(value); } } restore{previous};
    DdlExecutor ddl;
    size_t controls = 0;
    const auto check = [&](StorageEngine& engine) {
        const auto catalog = engine.catalogService().acquire(database);
        const auto relation = catalog->resolveRelationSnapshot("bytes", {"public"});
        assert(relation);
        const auto* attribute = catalog->findAttribute(relation->oid, "b");
        assert(attribute && attribute->atttypid == 17 && attribute->attlen == -1);
        const auto query = engine.prepareBoundQuery(database,
            "SELECT id FROM bytes WHERE b LIKE '__'::bytea");
        assert(query.output.size() == 1 && query.output.front().type == "integer");
        const auto output = engine.prepareBoundQuery(database, "SELECT b FROM bytes");
        assert(output.output.size() == 1 && output.output.front().typeOid == 17);
        ++controls;
    };
    assert(!ddl.executeSql("CREATE TABLE bytes(id INT,b BYTEA)", session));
    check(g_engine);
    assert(g_engine.getTableSchema(database, "bytes").cols[1].dataType == "blob");
    assert(!ddl.executeSql("ALTER TABLE bytes ADD COLUMN other INT", session));
    check(g_engine);
    assert(g_engine.catalogService().persistAll());
    g_engine.catalogService().evict(database);
    check(g_engine);
    {
        StorageEngine cold;
        check(cold);
    }
    {
        // Reconstruct the old producer's catalog row, without changing heap
        // bytes or deleting a possibly independently referenced user type.
        const auto catalog = g_engine.catalogService().acquire(database);
        const auto ns = catalog->getNamespaceByName("public");
        assert(ns);
        PgTypeRow legacy;
        legacy.typname = "blob";
        legacy.typnamespace = ns->oid;
        legacy.typlen = -1;
        const auto legacyOid = catalog->createType(legacy);
        assert(legacyOid != 17 && legacyOid != INVALID_OID);
        const auto relation = catalog->resolveRelationSnapshot("bytes", {"public"});
        assert(relation);
        auto attributes = catalog->findAttributes(relation->oid);
        for (auto& attribute : attributes)
            if (attribute.attname == "b") attribute.atttypid = legacyOid;
        assert(catalog->replaceAttributes(relation->oid, attributes));
        assert(catalog->persistAll());
    }
    std::string state;
    try { (void)g_engine.prepareBoundQuery(database, "SELECT b FROM bytes WHERE b LIKE '__'::bytea"); }
    catch (const DbError& error) { state = error.sqlState(); }
    assert(state == "42883");
    ++controls;
    assert(!ddl.executeSql("ALTER TABLE bytes ALTER COLUMN b TYPE BYTEA", session));
    check(g_engine);
    assert(!ddl.executeSql("CREATE SCHEMA bytea_owner", session));
    assert(!ddl.executeSql("CREATE TYPE bytea_owner.blob AS ENUM('x')", session));
    assert(!ddl.executeSql("CREATE TABLE bytea_owner.user_value(v bytea_owner.blob)", session));
    const auto query = g_engine.prepareBoundQuery(database, "SELECT v FROM bytea_owner.user_value");
    assert(query.output.size() == 1 && query.output.front().typeOid != 17);
    ++controls;
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[BYTEA CATALOG IDENTITY] " << controls
              << " declaration/cold/ALTER/user-type controls passed\n";
}
