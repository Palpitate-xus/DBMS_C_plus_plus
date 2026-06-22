#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_list_accessors() {
    std::string db = "pgcat_view_t1";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    ddl.executeSql("CREATE TABLE pgcat_tbl (id INT, name VARCHAR(50))", s);

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);

    auto namespaces = cat.listNamespaces();
    assert(!namespaces.empty());
    bool hasPublic = false;
    for (const auto& ns : namespaces) {
        if (ns.nspname == "public") hasPublic = true;
    }
    assert(hasPublic);

    auto classes = cat.listClasses();
    bool hasTable = false;
    for (const auto& cls : classes) {
        if (cls.relname == "pgcat_tbl" && cls.relkind == 'r') {
            hasTable = true;
            assert(cls.relnatts == 2);
        }
    }
    assert(hasTable);

    auto types = cat.listTypes();
    assert(!types.empty());

    // Evict before cleanup so the destructor does not recreate the directory.
    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[PG-CATALOG-VIEW] list accessors OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_list_accessors();
    std::cout << "[PG-CATALOG-VIEW] all passed" << std::endl;
    return 0;
}
