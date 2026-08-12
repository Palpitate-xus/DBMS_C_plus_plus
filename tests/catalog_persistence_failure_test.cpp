// Catalog persistence must report I/O failures and leave the previous target
// file intact when an atomic replacement cannot be completed.

#include "catalog/catalog.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    const std::filesystem::path catalogDir = "catalog_persistence_failure_dir";
    std::filesystem::remove_all(catalogDir);

    CatalogManager catalog(catalogDir.string());
    assert(catalog.createNamespace("public", 1) != 0);
    assert(catalog.persistAll());

    const auto namespacePath = catalogDir / "pg_namespace.cat";
    std::filesystem::remove(namespacePath);
    std::filesystem::create_directory(namespacePath);
    assert(!catalog.persistAll());
    assert(std::filesystem::is_directory(namespacePath));

    std::filesystem::remove_all(catalogDir);
    std::cout << "[CATALOG PERSISTENCE] failure propagation OK\n";
    return 0;
}
