#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const auto ordinary = testDbPath("catalog_not_a_database");
    assert(!std::filesystem::exists(ordinary));
    std::filesystem::create_directory(ordinary);
    assert(!engine.databaseExists(ordinary));
    std::string state;
    try { (void)engine.catalogService().get(ordinary); }
    catch (const DbError& error) { state = error.sqlState(); }
    assert(state == "3D000");
    assert(!std::filesystem::exists(std::filesystem::path(ordinary) / "pg_catalog"));
    assert(!engine.catalogService().has(ordinary));
    std::filesystem::remove(ordinary);
    std::cout << "[CATALOG MISSING DATABASE GUARD] existing ordinary directory cannot bootstrap a database catalog\n";
}
