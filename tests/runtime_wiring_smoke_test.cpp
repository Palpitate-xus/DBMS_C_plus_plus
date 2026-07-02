#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
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

static bool hasClass(dbms::CatalogManager& cat, const std::string& name) {
    const auto* ns = cat.findNamespaceByName("public");
    if (!ns) return false;
    return cat.findClassByName(name, ns->oid) != nullptr;
}

static void test_runtime_wiring_smoke() {
    std::string db = testDbPath("runtime_wiring_t1");
    cleanup(db);

    Session s;
    setupSession(s, "");
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE DATABASE " + db, s);
    assert(!err);

    s.currentDB = db;
    err = ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(50))", s);
    assert(!err);

    err = ddl.executeSql("CREATE INDEX i ON t(id)", s);
    assert(!err);

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    assert(hasClass(cat, "t"));
    assert(hasClass(cat, "i"));

    auto classes = cat.listClasses();
    bool sawT = false, sawI = false;
    for (const auto& cls : classes) {
        if (cls.relname == "t" && cls.relkind == 'r') sawT = true;
        if (cls.relname == "i" && cls.relkind == 'i') sawI = true;
    }
    assert(sawT);
    assert(sawI);

    err = ddl.executeSql("DROP TABLE t CASCADE", s);
    assert(!err);
    assert(!hasClass(cat, "t"));
    assert(!hasClass(cat, "i"));

    err = ddl.executeSql("DROP DATABASE " + db, s);
    assert(!err);
    assert(!g_engine.catalogService().has(db));

    cleanup(db);
    std::cout << "[RUNTIME-WIRING] smoke test OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_runtime_wiring_smoke();
    std::cout << "[RUNTIME-WIRING] all passed" << std::endl;
    return 0;
}
