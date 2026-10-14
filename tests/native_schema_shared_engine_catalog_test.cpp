#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <set>
#include <thread>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const auto name = "schema_same_engine_backend";
    const auto database = testDbPath(name);
    cleanupTestDb(name);
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createSchema(database, "committed_namespace") == DBStatus::OK);
    const auto committedLease = engine.catalogService().acquire(database);
    const auto* committedRow = committedLease->findNamespaceByName("committed_namespace");
    assert(committedRow);
    const auto committedOid = committedRow->oid;
    const std::set<std::string> observed = {
        "committed_namespace", "uncommitted_namespace", "before_sp",
        "after_sp", "committed_replacement"
    };
    bool valid = true;
    const auto read = [&](const std::set<std::string>& expected, const char* phase) {
        std::set<std::string> actual, fromLease, fromExists, fromNames;
        std::string state;
        std::thread backend([&] {
            try {
                for (const auto& row : engine.catalogService().metadataSnapshot(database).namespaces)
                    if (observed.count(row.nspname)) actual.insert(row.nspname);
                const auto reader = engine.catalogService().acquire(database);
                for (const auto& schema : observed)
                    if (reader->findNamespaceByName(schema)) fromLease.insert(schema);
                for (const auto& schema : observed)
                    if (engine.schemaExists(database, schema)) fromExists.insert(schema);
                for (const auto& schema : engine.getSchemaNames(database))
                    if (observed.count(schema)) fromNames.insert(schema);
            } catch (const DbError& error) { state = error.sqlState(); }
            engine.endBackendSession();
        });
        backend.join();
        const bool passed = state.empty() && actual == expected && fromLease == expected &&
            fromExists == expected && fromNames == expected;
        std::cout << "[SAME ENGINE CATALOG] " << phase << " state=" << state
                  << " snapshot_count=" << actual.size() << " lease_count=" << fromLease.size()
                  << " exists_count=" << fromExists.size() << " names_count=" << fromNames.size()
                  << " expected_count=" << expected.size() << " PASS=" << passed << std::endl;
        valid = valid && passed;
    };
    assert(engine.beginTransaction(database, true) == DBStatus::OK);
    assert(engine.dropSchema(database, "committed_namespace", false) == DBStatus::OK);
    assert(engine.createSchema(database, "uncommitted_namespace") == DBStatus::OK);
    read({"committed_namespace"}, "uncommitted writer");
    assert(engine.rollbackTransaction() == DBStatus::OK);
    read({"committed_namespace"}, "after full rollback");

    assert(engine.beginTransaction(database, true) == DBStatus::OK);
    assert(engine.createSchema(database, "before_sp") == DBStatus::OK);
    assert(engine.savepoint("namespace_owner") == DBStatus::OK);
    assert(engine.createSchema(database, "after_sp") == DBStatus::OK);
    assert(engine.rollbackToSavepoint("namespace_owner") == DBStatus::OK);
    // The restored physical image includes before_sp, but it still belongs
    // to this parent transaction, not the reader's committed namespace.
    read({"committed_namespace"}, "after savepoint restore before commit");
    assert(engine.commitTransaction() == DBStatus::OK);
    read({"committed_namespace", "before_sp"}, "after savepoint parent commit");

    assert(engine.beginTransaction(database, true) == DBStatus::OK);
    assert(engine.dropSchema(database, "committed_namespace", false) == DBStatus::OK);
    assert(engine.createSchema(database, "committed_replacement") == DBStatus::OK);
    read({"committed_namespace", "before_sp"}, "before replacement commit");
    assert(engine.commitTransaction() == DBStatus::OK);
    read({"committed_replacement", "before_sp"}, "after replacement commit");
    if (valid) assert(committedRow->oid == committedOid &&
                      committedRow->nspname == "committed_namespace");
    assert(engine.dropDatabase(database) == DBStatus::OK);
    return valid ? 0 : 1;
}
