// Cluster-catalog bootstrap keeps its historical directory-only contract.
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "permissions.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    namespace fs = std::filesystem;
    TypeRegistry::instance().bootstrap();
    assert(!fs::exists("info"));
    const auto name = "reserved_auth_bootstrap_parent";
    const auto db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    std::string state;
    try { (void)g_engine.catalogService().get("missing_ordinary_database"); }
    catch (const DbError& error) { state = error.sqlState(); }
    assert(state == "3D000");
    assert(!fs::exists("missing_ordinary_database"));
    assert(!g_engine.catalogService().has("missing_ordinary_database"));

    assert(g_engine.beginTransaction(db, true) == DBStatus::OK);
    auto& cluster = authCatalog();
    assert(g_engine.inTransaction() && g_engine.transactionDatabase() == db);
    assert(fs::is_directory(g_engine.dbPath("info") / "pg_catalog"));
    assert(!fs::exists("info/tlist.lst"));
    assert(!fs::exists("info/.schema_public"));
    assert(!g_engine.databaseExists("info"));
    PgAuthIdRow role;
    role.rolname = "preserved_cluster_role";
    role.rolsuper = false;
    role.rolcanlogin = false;
    role.rolpassword = "preserved_test_digest";
    const auto oid = cluster.createAuthId(role);
    assert(cluster.persistAll());
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    g_engine.catalogService().evict("info");
    const auto* restored = authCatalog().findAuthId(oid);
    assert(restored && restored->rolname == role.rolname &&
           restored->rolpassword == role.rolpassword &&
           restored->rolcanlogin == role.rolcanlogin &&
           restored->rolsuper == role.rolsuper);
    assert(!fs::exists("info/tlist.lst"));
    assert(!fs::exists("info/.schema_public"));

    // An existing partial cluster directory and its role bytes are retained;
    // an occupied catalog target is a typed I/O failure, not silent repair.
    g_engine.catalogService().evict("info");
    fs::rename("info/pg_catalog", "info/catalog_saved");
    { std::ofstream occupied("info/pg_catalog"); occupied << "occupied"; }
    state.clear();
    try { (void)authCatalog(); }
    catch (const DbError& error) { state = error.sqlState(); }
    assert(state == "58030");
    assert(fs::is_regular_file("info/pg_catalog"));
    fs::remove("info/pg_catalog");
    fs::rename("info/catalog_saved", "info/pg_catalog");
    restored = authCatalog().findAuthId(oid);
    assert(restored && restored->rolpassword == role.rolpassword);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    g_engine.catalogService().evict("info");
    fs::remove_all("info");
    std::cout << "[RESERVED AUTH CATALOG BOOTSTRAP] exact directory-only lifecycle controls passed\n";
}
