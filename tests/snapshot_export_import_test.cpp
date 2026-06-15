#include "TableManage.h"
#include "Config.h"
#include "dbms_defs.h"
#include <cassert>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>

dbms::Config g_config;
using namespace dbms;

static bool rowContains(const std::vector<std::string>& rows, const std::string& substring) {
    for (const auto& row : rows) {
        if (row.find(substring) != std::string::npos) return true;
    }
    return false;
}

int main() {
    const std::string dbname = "snapshot_ei_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    // Test 1: Snapshot struct roundtrip
    {
        Snapshot s;
        s.version = 1;
        s.xmin = 10;
        s.xmax = 100;
        s.curCid = 5;
        s.activeXids = {12, 15, 20};
        s.subxip = {30, 31};

        std::string bytes = s.exportToBytes();
        auto opt = Snapshot::importFromBytes(bytes);
        assert(opt.has_value());
        Snapshot t = *opt;
        assert(t.version == s.version);
        assert(t.xmin == s.xmin);
        assert(t.xmax == s.xmax);
        assert(t.curCid == s.curCid);
        assert(t.activeXids == s.activeXids);
        assert(t.subxip == s.subxip);
        std::cout << "[SNAPSHOT EI] struct roundtrip OK\n";
    }

    // Test 2: Reject invalid magic
    {
        Snapshot s;
        s.xmin = 1; s.xmax = 2;
        std::string bytes = s.exportToBytes();
        bytes[0] = 0xFF; // corrupt magic
        auto opt = Snapshot::importFromBytes(bytes);
        assert(!opt.has_value());
        std::cout << "[SNAPSHOT EI] invalid magic rejected OK\n";
    }

    // Test 3: Reject truncated data
    {
        Snapshot s;
        s.activeXids = {1, 2, 3};
        std::string bytes = s.exportToBytes();
        bytes.resize(bytes.size() - 8);
        auto opt = Snapshot::importFromBytes(bytes);
        assert(!opt.has_value());
        std::cout << "[SNAPSHOT EI] truncated data rejected OK\n";
    }

    // Test 4: StorageEngine export/import with visibility.
    // Create all engine instances before starting any transaction: the StorageEngine
    // constructor scans the current directory and runs WAL recovery on any database
    // with an incomplete transaction, which would interfere with the active tx.
    {
        StorageEngine engine1;
        StorageEngine engine2;
        StorageEngine engine3;

        DBStatus r = engine1.createDatabase(dbname);
        if (r != DBStatus::OK) {
            std::cerr << "createDatabase failed: " << static_cast<int>(r) << "\n";
            return 1;
        }

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.append(makeIntColumn("id", false, 0, true));
        tbl.append(makeVarCharColumn("name", false, 20, false));
        r = engine1.createTable(dbname, tbl);
        if (r != DBStatus::OK) {
            std::cerr << "createTable failed: " << static_cast<int>(r) << "\n";
            return 1;
        }

        r = engine1.beginTransaction(dbname);
        if (r != DBStatus::OK) {
            std::cerr << "beginTransaction failed: " << static_cast<int>(r) << "\n";
            return 1;
        }

        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        vals["name"] = "alice";
        r = engine1.insert(dbname, "t", vals);
        if (r != DBStatus::OK) {
            std::cerr << "insert failed: " << static_cast<int>(r) << "\n";
            return 1;
        }

        // Engine1 sees its own uncommitted insert
        auto rows = engine1.query(dbname, "t", {}, {"id", "name"});
        assert(rowContains(rows, "alice"));

        std::string snapBytes = engine1.exportSnapshot();
        assert(!snapBytes.empty());

        // Engine2 imports the snapshot and should not see engine1's uncommitted row
        r = engine2.beginTransaction(dbname);
        if (r != DBStatus::OK) {
            std::cerr << "engine2 begin failed: " << static_cast<int>(r) << "\n";
            return 1;
        }
        bool ok = engine2.importSnapshot(snapBytes);
        assert(ok);

        rows = engine2.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "alice"));

        engine1.commitTransaction();

        // Flush engine1's dirty pages so engine3 can read the committed row from disk.
        // (Each StorageEngine instance has its own page allocator cache.)
        engine1.getPageAllocator(dbname, "t")->flush();

        // Engine2 still uses imported snapshot (repeatable read), still invisible
        rows = engine2.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "alice"));

        engine2.commitTransaction();

        // Engine3 with fresh snapshot sees committed row
        r = engine3.beginTransaction(dbname);
        assert(r == DBStatus::OK);
        rows = engine3.query(dbname, "t", {}, {"id", "name"});
        assert(rowContains(rows, "alice"));
        engine3.commitTransaction();

        std::cout << "[SNAPSHOT EI] engine export/import visibility OK\n";
    }

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[SNAPSHOT EI] all passed\n";
    return 0;
}
