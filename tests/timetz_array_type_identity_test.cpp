#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    for (const auto* spelling : {"timetz[]", "time with time zone[]"}) {
        const auto oid = dbms::mapBuiltinTypeNameToOid(spelling);
        if (oid != 1270) std::cerr << spelling << " OID " << oid << " expected1270\n";
        assert(oid == 1270 && dbms::isBuiltinTypeOid(oid));
    }
    const std::string name = "timetz_array_type_identity";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    const auto check = [&]() {
        auto& catalog = g_engine.catalogService().get(database);
        const auto* scalar = catalog.findType(1266);
        const auto* array = catalog.findType(1270);
        assert(scalar && scalar->typname == "timetz" && scalar->typlen == 12);
        assert(array && array->typname == "_timetz" && array->typnamespace == 11);
        assert(array->typlen == -1 && array->typcategory == 'A' && array->typelem == 1266);
        assert(catalog.findTypeByName("_timetz", 11)->oid == 1270);
    };
    check();
    assert(g_engine.catalogService().persistAll());
    g_engine.catalogService().evict(database);
    check();
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "[TIMETZ ARRAY TYPE IDENTITY] passed\n";
}
