#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "json_xml_catalog_type_identity";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    const auto check = [&]() {
        auto& catalog = g_engine.catalogService().get(database);
        for (const auto [scalarOid, arrayOid] : {std::pair<dbms::Oid,dbms::Oid>{114,199}, {142,143}}) {
            const auto* scalar = catalog.findType(scalarOid);
            const auto* array = catalog.findType(arrayOid);
            if (!scalar) std::cerr << "missing scalar builtin OID " << scalarOid << '\n';
            assert(scalar && scalar->typnamespace == 11 && scalar->typlen == -1);
            assert(scalar->typcategory == 'U');
            assert(array && array->typnamespace == 11 && array->typlen == -1);
            assert(array->typcategory == 'A' && array->typelem == scalarOid);
            assert(array->typname == "_"+scalar->typname);
        }
    };
    check();
    assert(g_engine.catalogService().persistAll());
    g_engine.catalogService().evict(database);
    check();
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "[JSON XML CATALOG TYPE IDENTITY] passed\n";
}
