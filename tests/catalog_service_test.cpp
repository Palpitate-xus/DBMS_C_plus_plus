#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void test_bootstrap_and_cache() {
    std::string db = "catalog_service_t1";
    cleanup(db);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::CatalogService& svc = engine.catalogService();
    dbms::CatalogManager& cat = svc.get(db);

    const auto* nsPublic = cat.findNamespaceByName("public");
    const auto* nsCat = cat.findNamespaceByName("pg_catalog");
    assert(nsPublic != nullptr);
    assert(nsCat != nullptr);

    dbms::CatalogManager& cat2 = svc.get(db);
    assert(&cat == &cat2); // cache hit returns same instance

    cleanup(db);
    std::cout << "[CATALOG-SVC] bootstrap and cache OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bootstrap_and_cache();
    std::cout << "[CATALOG-SVC] all passed" << std::endl;
    return 0;
}
