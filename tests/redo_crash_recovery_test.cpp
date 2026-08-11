// Crash recovery test: verify redo/undo via WAL page images.

#include "TableManage.h"
#include "Config.h"
#include <filesystem>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

static bool rowContains(const std::vector<std::string>& rows, const std::string& substring) {
    for (const auto& row : rows) {
        if (row.find(substring) != std::string::npos) return true;
    }
    return false;
}

static void setupTable(StorageEngine& engine, const std::string& dbname) {
    assert(engine.createDatabase(dbname) == DBStatus::OK);
    TableSchema tbl;
    tbl.tablename = "t";
    tbl.formatVersion = 2;
    tbl.append(makeIntColumn("id", false, 0, true));
    tbl.append(makeVarCharColumn("name", false, 50, false));
    assert(engine.createTable(dbname, tbl) == DBStatus::OK);
}

int main() {
    std::string dbname = "redo_crash_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    // Scenario 1: uncommitted insert, crash (object destruction), recover -> row not visible.
    {
        StorageEngine engine;
        setupTable(engine, dbname);
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        vals["name"] = "uncommitted";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        vals["id"] = "3";
        vals["name"] = "uncommitted-again";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        // No commit/rollback - simulate crash by destroying engine.
    }

    {
        StorageEngine engine;
        auto rows = engine.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "uncommitted"));
        assert(!rowContains(rows, "uncommitted-again"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(dbname, "t");
        assert(pk != nullptr);
        assert(!pk->search("1", rid));
        assert(!pk->search("3", rid));
        assert(engine.checkpoint(dbname));
        std::cout << "[REDO CRASH] uncommitted insert rolled back by recovery OK\n";
    }

    // Scenario 2: committed insert, crash, recover -> row visible.
    {
        StorageEngine engine;
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "2";
        vals["name"] = "committed";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    {
        StorageEngine engine;
        auto rows = engine.query(dbname, "t", {}, {"id", "name"});
        assert(rowContains(rows, "committed"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(dbname, "t");
        assert(pk != nullptr);
        assert(pk->search("2", rid));
        std::cout << "[REDO CRASH] committed insert recovered OK\n";
    }

    // Scenario 3: committed delete, crash, recover -> row gone.
    {
        StorageEngine engine;
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.remove(dbname, "t", {"id = '2'"}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    {
        StorageEngine engine;
        auto rows = engine.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "committed"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(dbname, "t");
        assert(pk != nullptr);
        assert(!pk->search("2", rid));
        std::cout << "[REDO CRASH] committed delete recovered OK\n";
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[REDO CRASH] all passed\n";
    return 0;
}
