#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

int main() {
    using namespace dbms;
    namespace fs = std::filesystem;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const auto database = testDbPath("catalog_public_marker_scaffold");
    cleanupTestDb("catalog_public_marker_scaffold");
    // The server's existing cluster scaffold is a database (tlist.lst), but
    // has not yet initialized either namespace catalog or schema marker.
    fs::create_directories(fs::path(database) / "pg_catalog");
    { std::ofstream tables(fs::path(database) / "tlist.lst"); assert(tables); }
    assert(engine.databaseExists(database));
    assert(!engine.schemaExists(database, "public"));
    auto& catalog = engine.catalogService().get(database);
    assert(catalog.findNamespaceByName("public"));
    assert(engine.schemaExists(database, "public"));
    assert(fs::is_regular_file(fs::symlink_status(fs::path(database) / ".schema_public")));
    assert(catalog.persistAll());
    assert(engine.dropSchema(database, "public", true) == DBStatus::OK);
    engine.catalogService().evict(database);
    assert(!engine.catalogService().get(database).findNamespaceByName("public"));
    assert(!engine.schemaExists(database, "public"));
    assert(engine.createSchema(database, "public") == DBStatus::OK);
    const auto recreated = engine.catalogService().get(database).findNamespaceByName("public")->oid;
    engine.catalogService().evict(database);
    assert(engine.catalogService().get(database).findNamespaceByName("public")->oid == recreated);
    assert(engine.schemaExists(database, "public"));
    assert(engine.dropDatabase(database) == DBStatus::OK);

    // An occupied marker is not replaced, nor hidden by a synthetic public
    // answer; failure cannot publish a half-initialized namespace catalog.
    const auto blocked = testDbPath("catalog_public_marker_blocked");
    cleanupTestDb("catalog_public_marker_blocked");
    fs::create_directories(fs::path(blocked) / "pg_catalog");
    { std::ofstream tables(fs::path(blocked) / "tlist.lst"); assert(tables); }
    fs::create_directory(fs::path(blocked) / ".schema_public");
    std::string state;
    try { (void)engine.catalogService().get(blocked); }
    catch (const DbError& error) { state = error.sqlState(); }
    assert(state == "58030");
    assert(fs::is_directory(fs::path(blocked) / ".schema_public"));
    assert(!fs::exists(fs::path(blocked) / "pg_catalog/pg_namespace.cat"));
    assert(!engine.catalogService().has(blocked));
    fs::remove(fs::path(blocked) / ".schema_public");
    assert(engine.catalogService().get(blocked).findNamespaceByName("public"));
    assert(engine.schemaExists(blocked, "public"));
    assert(engine.dropDatabase(blocked) == DBStatus::OK);
    std::cout << "[CATALOG PUBLIC MARKER] fresh scaffold, committed DROP/recreate, occupied marker and retry passed\n";
}
