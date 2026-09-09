#include "storage/PageAllocator.h"
#include "TableManage.h"
#include "Config.h"
#include "dbms_defs.h"
#include <cassert>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

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
        s.version = 2;
        s.database = "snapshot_ei_db";
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
        assert(t.database == s.database);
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
        s.version = 2;
        s.database = "snapshot_ei_db";
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
        s.version = 2;
        s.database = "snapshot_ei_db";
        s.activeXids = {1, 2, 3};
        std::string bytes = s.exportToBytes();
        bytes.resize(bytes.size() - 8);
        auto opt = Snapshot::importFromBytes(bytes);
        assert(!opt.has_value());
        std::cout << "[SNAPSHOT EI] truncated data rejected OK\n";
    }

    // Test 3b: Reject unsupported versions, trailing bytes, and invalid XIDs.
    {
        Snapshot s;
        s.version = 2;
        s.database = "snapshot_ei_db";
        s.xmin = 1;
        s.xmax = 2;
        std::string bytes = s.exportToBytes();
        assert(!bytes.empty());
        bytes[4] = 3;
        assert(!Snapshot::importFromBytes(bytes).has_value());

        bytes = s.exportToBytes();
        bytes.push_back('\0');
        assert(!Snapshot::importFromBytes(bytes).has_value());

        Snapshot invalid = s;
        invalid.activeXids = {0};
        assert(invalid.exportToBytes().empty());
        std::cout << "[SNAPSHOT EI] malformed payloads rejected OK\n";
    }

    // Test 4: StorageEngine export/import with visibility.
    // Create all engine instances before starting any transaction: the StorageEngine
    // constructor scans the current directory and runs WAL recovery on any database
    // with an incomplete transaction, which would interfere with the active tx.
    {
        StorageEngine engine1;
        StorageEngine engine2;
        StorageEngine engine3;
        StorageEngine engine4;

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

        // Exercise a non-zero exporter command ID so the serialized field
        // cannot silently remain a placeholder.
        assert(engine1.beginSqlCommand());
        assert(engine1.finishSqlCommand());
        assert(engine1.currentCommandId() == 1);
        std::string snapBytes = engine1.exportSnapshot();
        assert(!snapBytes.empty());
        auto exported = Snapshot::importFromBytes(snapBytes);
        assert(exported.has_value());
        assert(exported->curCid == engine1.currentCommandId());

        // Engine2 imports the snapshot and should not see engine1's uncommitted row
        r = engine2.beginTransaction(dbname);
        if (r != DBStatus::OK) {
            std::cerr << "engine2 begin failed: " << static_cast<int>(r) << "\n";
            return 1;
        }
        bool ok = engine2.importSnapshot(snapBytes);
        assert(ok);

        // A snapshot is database-scoped and can only be installed before the
        // importing transaction has read or written anything.
        assert(!engine2.importSnapshot(snapBytes));

        rows = engine2.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "alice"));
        assert(!engine2.importSnapshot(snapBytes));

        engine1.commitTransaction();

        // Flush engine1's dirty pages so engine3 can read the committed row from disk.
        // (Each StorageEngine instance has its own page allocator cache.)
        engine1.getPageAllocator(dbname, "t")->flush();

        // Engine2 still uses imported snapshot (repeatable read), still invisible
        rows = engine2.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "alice"));

        engine2.commitTransaction();

        // A transaction that has already written cannot replace its snapshot.
        assert(engine4.beginTransaction(dbname) == DBStatus::OK);
        vals["id"] = "2";
        vals["name"] = "bob";
        assert(engine4.insert(dbname, "t", vals) == DBStatus::OK);
        assert(!engine4.importSnapshot(snapBytes));
        assert(engine4.rollbackTransaction() == DBStatus::OK);

        // A snapshot from another database must be rejected even when the
        // receiving transaction has not executed a statement yet.
        const std::string otherDb = "snapshot_ei_other_db";
        assert(engine1.createDatabase(otherDb) == DBStatus::OK);
        assert(engine1.beginTransaction(otherDb) == DBStatus::OK);
        assert(!engine1.importSnapshot(snapBytes));
        assert(engine1.rollbackTransaction() == DBStatus::OK);
        std::filesystem::remove_all(otherDb);

        // Engine3 with fresh snapshot sees committed row
        r = engine3.beginTransaction(dbname);
        assert(r == DBStatus::OK);
        rows = engine3.query(dbname, "t", {}, {"id", "name"});
        assert(rowContains(rows, "alice"));
        engine3.commitTransaction();

        std::cout << "[SNAPSHOT EI] engine export/import visibility OK\n";
    }

    // Test 5: READ COMMITTED refreshes once per SQL command, not once per
    // physical relation scan.  Otherwise a JOIN/CTE/subquery can observe a
    // transaction that commits between two scans in the same statement.
    {
        const std::string rcDb = "snapshot_rc_db";
        std::filesystem::remove_all(rcDb);
        {
        StorageEngine engine;
        assert(engine.createDatabase(rcDb) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.append(makeIntColumn("id", false, 0, true));
        tbl.append(makeVarCharColumn("name", false, 20, false));
        assert(engine.createTable(rcDb, tbl) == DBStatus::OK);

        engine.setIsolationLevel(IsolationLevel::READ_COMMITTED);
        assert(engine.beginTransaction(rcDb) == DBStatus::OK);
        assert(engine.beginSqlCommand());
        auto rows = engine.query(rcDb, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "concurrent"));

        std::thread writer([&] {
            assert(engine.beginTransaction(rcDb) == DBStatus::OK);
            std::map<std::string, std::string> vals{
                {"id", "1"}, {"name", "concurrent"}};
            assert(engine.insert(rcDb, "t", vals) == DBStatus::OK);
            assert(engine.commitTransaction() == DBStatus::OK);
        });
        writer.join();

        rows = engine.query(rcDb, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "concurrent"));
        assert(engine.finishSqlCommand());

        assert(engine.beginSqlCommand());
        rows = engine.query(rcDb, "t", {}, {"id", "name"});
        assert(rowContains(rows, "concurrent"));
        assert(engine.finishSqlCommand());
        assert(engine.commitTransaction() == DBStatus::OK);
        std::cout << "[SNAPSHOT EI] READ COMMITTED statement snapshot OK\n";
        }
        std::filesystem::remove_all(rcDb);
    }

    // Test 6: PostgreSQL accepts READ UNCOMMITTED but gives it READ
    // COMMITTED visibility and statement-snapshot behavior.
    {
        const std::string ruDb = "snapshot_ru_db";
        std::filesystem::remove_all(ruDb);
        {
        StorageEngine engine;
        assert(engine.createDatabase(ruDb) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.append(makeIntColumn("id", false, 0, true));
        tbl.append(makeVarCharColumn("name", false, 20, false));
        assert(engine.createTable(ruDb, tbl) == DBStatus::OK);

        assert(engine.beginTransaction(ruDb) == DBStatus::OK);
        assert(engine.insert(ruDb, "t", {{"id", "1"}, {"name", "before"}})
               == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        engine.setIsolationLevel(IsolationLevel::READ_UNCOMMITTED);
        assert(engine.beginTransaction(ruDb) == DBStatus::OK);
        assert(engine.beginSqlCommand());
        auto rows = engine.query(ruDb, "t", {}, {"id", "name"});
        assert(rowContains(rows, "before"));
        assert(!engine.setIsolationLevel(IsolationLevel::SERIALIZABLE));
        assert(engine.getIsolationLevel() == IsolationLevel::READ_UNCOMMITTED);

        std::thread writer([&] {
            assert(engine.beginTransaction(ruDb) == DBStatus::OK);
            assert(engine.insert(ruDb, "t", {{"id", "2"}, {"name", "during"}})
                   == DBStatus::OK);
            assert(engine.commitTransaction() == DBStatus::OK);
        });
        writer.join();

        rows = engine.query(ruDb, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "during"));
        assert(engine.finishSqlCommand());
        assert(engine.beginSqlCommand());
        rows = engine.query(ruDb, "t", {}, {"id", "name"});
        assert(rowContains(rows, "during"));
        assert(engine.finishSqlCommand());
        assert(engine.commitTransaction() == DBStatus::OK);
        std::cout << "[SNAPSHOT EI] READ UNCOMMITTED maps to READ COMMITTED OK\n";
        }
        std::filesystem::remove_all(ruDb);
    }

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[SNAPSHOT EI] all passed\n";
    return 0;
}
