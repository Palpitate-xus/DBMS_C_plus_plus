#include "catalog/CatalogService.h"
#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <future>
#include <iostream>

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("restore_catalog_staging_db");
    const std::string backup = testDbPath("restore_catalog_staging_image");
    {
        dbms::StorageEngine engine;
        assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
        assert(engine.physicalBackup(database, backup));
        assert(engine.createSchema(database, "live_only") == dbms::DBStatus::OK);
        auto& warm = engine.catalogService().get(database);
        assert(warm.getNamespaceByName("live_only"));
        assert(engine.catalogService().persistAll(database));
        const auto original = engine.catalogService().acquire(database);

        // An invalid or cancelled staging operation must not throw away the
        // readable current generation when no live directory was replaced.
        assert(!engine.physicalRestore(database, database));
        assert(engine.catalogService().acquire(database) == original);
        assert(!engine.physicalRestore(database, backup, [](uint64_t) { return false; }));
        assert(engine.catalogService().acquire(database) == original);

        bool observed = false;
        bool readable = false;
        const bool restored = engine.physicalRestore(database, backup, [&](uint64_t) {
            if (observed) return true;
            observed = true;
            // This is a real foreign thread while restore owns the database
            // exclusively. The old directory has not yet been published over.
            auto observer = std::async(std::launch::async, [&] {
                assert(!engine.catalogReadOwnership(database).mayReadCurrentFiles);
                try {
                    const auto current = engine.catalogService().acquire(database);
                    return current == original && bool(current->getNamespaceByName("live_only"));
                } catch (const dbms::DbError& error) {
                    std::cerr << "RESTORE_STAGING_CATALOG_SQLSTATE " << error.sqlState() << '\n';
                    return false;
                }
            });
            readable = observer.get();
            return true;
        });
        assert(restored && observed && readable);
        const auto replacement = engine.catalogService().acquire(database);
        assert(replacement != original);
        assert(!replacement->getNamespaceByName("live_only"));
        assert(original->getNamespaceByName("live_only"));
    }
    finalCleanupTestData();
    std::cout << "[RESTORE CATALOG STAGING] original, cancellation, foreign reader and publication passed\n";
}
