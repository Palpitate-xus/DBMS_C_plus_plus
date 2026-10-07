#include "commands/TableManage.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <iostream>
#include <set>
#include <string>

using namespace dbms;

static TableSchema schema(const std::string& name) {
    TableSchema result;
    result.tablename = name;
    result.append(makeIntColumn("id", false, 4, true));
    return result;
}

static void expectIds(StorageEngine& engine, const std::string& database,
                      const std::string& table, const std::set<std::string>& expected) {
    const auto rows = engine.query(database, table, {}, {"id"});
    const std::set<std::string> actual(rows.begin(), rows.end());
    std::cerr << "[HEAP WAL GENERATION] " << table << " rows=" << rows.size()
              << " expected=" << expected.size() << '\n';
    assert(rows.size() == expected.size() && actual == expected);
}

static void runScenario(const std::string& scenario) {
    const std::string database = "__t_heap_wal_generation_" + scenario;
    uint64_t originalIdentity = 0;
    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        assert(engine.createTable(database, schema("items")) == DBStatus::OK);
        originalIdentity = engine.getTableSchema(database, "items").physicalRelationId;
        assert(originalIdentity != 0);
        assert(engine.insert(database, "items", {{"id", "99"}}) == DBStatus::OK);
        if (scenario == "rename" || scenario == "reuse") {
            assert(engine.alterTableRenameTable(database, "items", "archived") == DBStatus::OK);
            assert(engine.getTableSchema(database, "archived").physicalRelationId == originalIdentity);
            if (scenario == "rename") {
                assert(engine.insert(database, "archived", {{"id", "100"}}) == DBStatus::OK);
                expectIds(engine, database, "archived", {"99 ", "100 "});
            } else {
                assert(engine.createTable(database, schema("items")) == DBStatus::OK);
                assert(engine.getTableSchema(database, "items").physicalRelationId > originalIdentity);
                expectIds(engine, database, "items", {});
                expectIds(engine, database, "archived", {"99 "});
            }
        } else if (scenario == "drop") {
            assert(engine.dropTable(database, "items") == DBStatus::OK);
            assert(engine.createTable(database, schema("items")) == DBStatus::OK);
            assert(engine.getTableSchema(database, "items").physicalRelationId > originalIdentity);
            expectIds(engine, database, "items", {});
        } else if (scenario == "truncate") {
            assert(engine.truncateTable(database, "items") == DBStatus::OK);
            assert(engine.alterTableRenameTable(database, "items", "archived") == DBStatus::OK);
            assert(engine.createTable(database, schema("items")) == DBStatus::OK);
            assert(engine.getTableSchema(database, "items").physicalRelationId > originalIdentity);
            assert(engine.insert(database, "items", {{"id", "100"}}) == DBStatus::OK);
            expectIds(engine, database, "items", {"100 "});
            expectIds(engine, database, "archived", {});
        } else if (scenario == "rollback") {
            assert(engine.beginTransaction(database) == DBStatus::OK);
            engine.preserveTransactionBackupOnRollback(true);
            assert(engine.createTransactionBackup());
            // Direct metadata APIs retain the public manual-snapshot
            // contract. Opt in to the same lifecycle as the SQL DDL owner.
            engine.restoreTransactionBackupBeforeRowUndo(true);
            engine.markTransactionBackupDirty();
            assert(engine.alterTableRenameTable(database, "items", "archived") == DBStatus::OK);
            assert(engine.rollbackTransaction() == DBStatus::OK);
            assert(engine.tableExists(database, "items"));
            assert(!engine.tableExists(database, "archived"));
            expectIds(engine, database, "items", {"99 "});
            assert(engine.getTableSchema(database, "items").physicalRelationId == originalIdentity);
        } else {
            assert(false && "unknown scenario");
        }
    }
    StorageEngine recovered;
    if (scenario == "rename") {
        expectIds(recovered, database, "archived", {"99 ", "100 "});
    } else if (scenario == "reuse") {
        expectIds(recovered, database, "items", {});
        expectIds(recovered, database, "archived", {"99 "});
    } else if (scenario == "drop") {
        expectIds(recovered, database, "items", {});
    } else if (scenario == "truncate") {
        expectIds(recovered, database, "items", {"100 "});
        expectIds(recovered, database, "archived", {});
    } else {
        expectIds(recovered, database, "items", {"99 "});
        assert(!recovered.tableExists(database, "archived"));
    }
    std::cout << "[HEAP WAL GENERATION] " << scenario << " survived recovery\n";
}

int main(int argc, char** argv) {
    if (argc > 1) runScenario(argv[1]);
    else for (const char* scenario : {"rename", "reuse", "drop", "truncate", "rollback"})
        runScenario(scenario);
}
