#include "TableManage.h"
#include "Config.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

dbms::Config g_config;
using namespace dbms;

static bool listContains(const std::vector<std::string>& list, const std::string& name) {
    for (const auto& s : list) if (s == name) return true;
    return false;
}

int main() {
    const std::string dbname = "catalog_snap_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    StorageEngine engine;
    OpResult r = engine.createDatabase(dbname);
    if (r != OpResult::Success) {
        std::cerr << "createDatabase failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    TableSchema tbl;
    tbl.tablename = "base";
    tbl.append(makeIntColumn("id", false, 0, true));
    tbl.append(makeVarCharColumn("name", false, 20, false));
    r = engine.createTable(dbname, tbl);
    if (r != OpResult::Success) {
        std::cerr << "createTable failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Test 1: getTableNames snapshot isolation
    {
        // Create concurrent engine BEFORE starting any transaction: the StorageEngine
        // constructor runs WAL recovery on databases with incomplete transactions.
        StorageEngine engine2;

        r = engine.beginTransaction(dbname);
        assert(r == OpResult::Success);

        auto namesBefore = engine.getTableNames(dbname);
        assert(listContains(namesBefore, "base"));
        assert(!listContains(namesBefore, "newtable"));

        // Create another table from the concurrent engine instance
        r = engine2.beginTransaction(dbname);
        assert(r == OpResult::Success);
        TableSchema tbl2;
        tbl2.tablename = "newtable";
        tbl2.append(makeIntColumn("id", false, 0, true));
        r = engine2.createTable(dbname, tbl2);
        assert(r == OpResult::Success);
        r = engine2.commitTransaction();
        assert(r == OpResult::Success);

        // engine1 still in transaction, should see snapshot view
        auto namesDuring = engine.getTableNames(dbname);
        assert(listContains(namesDuring, "base"));
        assert(!listContains(namesDuring, "newtable"));

        r = engine.commitTransaction();
        assert(r == OpResult::Success);

        // After commit, new transaction sees new table
        r = engine.beginTransaction(dbname);
        assert(r == OpResult::Success);
        auto namesAfter = engine.getTableNames(dbname);
        assert(listContains(namesAfter, "base"));
        assert(listContains(namesAfter, "newtable"));
        engine.commitTransaction();

        std::cout << "[CATALOG SNAPSHOT] getTableNames isolation OK\n";
    }

    // Test 2: getTableSchema snapshot isolation
    {
        // Create concurrent engine before starting any transaction (see test 1 comment).
        StorageEngine engine2;

        r = engine.beginTransaction(dbname);
        assert(r == OpResult::Success);

        TableSchema schemaBefore = engine.getTableSchema(dbname, "base");
        assert(schemaBefore.len == 2);

        // Alter table from the concurrent engine instance
        r = engine2.beginTransaction(dbname);
        assert(r == OpResult::Success);
        Column newCol = makeIntColumn("age", true, 0, false);
        r = engine2.alterTableAddColumn(dbname, "base", newCol);
        assert(r == OpResult::Success);
        r = engine2.commitTransaction();
        assert(r == OpResult::Success);

        // engine1 still sees old schema
        TableSchema schemaDuring = engine.getTableSchema(dbname, "base");
        assert(schemaDuring.len == 2);

        r = engine.commitTransaction();
        assert(r == OpResult::Success);

        // New transaction sees new schema
        r = engine.beginTransaction(dbname);
        assert(r == OpResult::Success);
        TableSchema schemaAfter = engine.getTableSchema(dbname, "base");
        assert(schemaAfter.len == 3);
        engine.commitTransaction();

        std::cout << "[CATALOG SNAPSHOT] getTableSchema isolation OK\n";
    }

    // Test 3: rollback clears catalog snapshot
    {
        r = engine.beginTransaction(dbname);
        assert(r == OpResult::Success);

        TableSchema snap = engine.getTableSchema(dbname, "base");
        assert(snap.len == 3);

        Column another = makeIntColumn("score", true, 0, false);
        r = engine.alterTableAddColumn(dbname, "base", another);
        assert(r == OpResult::Success);

        // Within same transaction, own DDL is visible (cache miss on modified schema)
        TableSchema withOwn = engine.getTableSchema(dbname, "base");
        assert(withOwn.len == 4);

        r = engine.rollbackTransaction();
        assert(r == OpResult::Success);

        // After rollback, the catalog snapshot must be cleared so the next read
        // fetches the current file state. Note: DDL (ALTER TABLE) is not yet
        // undone by rollbackTransaction(), so the schema file still has 4 columns.
        r = engine.beginTransaction(dbname);
        assert(r == OpResult::Success);
        TableSchema afterRollback = engine.getTableSchema(dbname, "base");
        assert(afterRollback.len == 4); // snapshot was cleared, read from disk
        engine.commitTransaction();

        std::cout << "[CATALOG SNAPSHOT] rollback clears snapshot OK\n";
    }

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[CATALOG SNAPSHOT] all passed\n";
    return 0;
}
