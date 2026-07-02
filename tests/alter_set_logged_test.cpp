// ============================================================================
// ALTER TABLE SET LOGGED / SET UNLOGGED test — Phase 4 Wave 4.27f
// alterTableSetLogged flips the persisted isUnlogged flag (metadata only; data
// is kept). The owner / cluster / replica-identity sidecar subcommands run
// through the string dispatch in main.cpp and are covered by binary E2E instead.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_set_logged_unlogged() {
    std::string db = testDbPath("setlogged");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, n INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"n", "10"}}) == dbms::DBStatus::OK);

    // Default: logged.
    assert(!g_engine.getTableSchema(db, "t").isUnlogged);

    // SET UNLOGGED.
    assert(g_engine.alterTableSetLogged(db, "t", false) == dbms::DBStatus::OK);
    assert(g_engine.getTableSchema(db, "t").isUnlogged);

    // SET LOGGED back; data still present.
    assert(g_engine.alterTableSetLogged(db, "t", true) == dbms::DBStatus::OK);
    assert(!g_engine.getTableSchema(db, "t").isUnlogged);
    size_t n = 0;
    g_engine.forEachRow(db, "t", [&](uint32_t, uint16_t, const char*, size_t) { ++n; });
    assert(n == 1);

    // Unknown table rejected.
    assert(g_engine.alterTableSetLogged(db, "ghost", false) == dbms::DBStatus::TABLE_NOT_FOUND);

    cleanup(db);
    std::cout << "[SETLOGGED] set logged/unlogged OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_set_logged_unlogged();
    std::cout << "[SETLOGGED] all passed" << std::endl;
    return 0;
}
