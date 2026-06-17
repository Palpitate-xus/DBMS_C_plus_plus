// Storage parameter fillfactor test.

#include "TableManage.h"
#include "Config.h"
#include <filesystem>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "fillfactor_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        // Table with default fillfactor (100) - rows can pack tightly.
        TableSchema tbl100;
        tbl100.tablename = "t100";
        tbl100.formatVersion = 2;
        tbl100.append(makeVarCharColumn("payload", false, 2000, false));
        assert(engine.createTable(dbname, tbl100) == DBStatus::OK);

        // Table with fillfactor 50 - only ~50% of each page can be used.
        TableSchema tbl50;
        tbl50.tablename = "t50";
        tbl50.formatVersion = 2;
        tbl50.append(makeVarCharColumn("payload", false, 2000, false));
        tbl50.storageParams["fillfactor"] = "50";
        assert(engine.createTable(dbname, tbl50) == DBStatus::OK);
        assert(engine.setStorageParams(dbname, "t50", tbl50.storageParams) == DBStatus::OK);

        // Insert 4 rows of ~2000 bytes into each table.
        std::string payload(2000, 'x');
        for (int i = 0; i < 4; ++i) {
            assert(engine.beginTransaction(dbname) == DBStatus::OK);
            std::map<std::string, std::string> vals;
            vals["payload"] = payload;
            assert(engine.insert(dbname, "t100", vals) == DBStatus::OK);
            assert(engine.commitTransaction() == DBStatus::OK);

            assert(engine.beginTransaction(dbname) == DBStatus::OK);
            vals["payload"] = payload;
            assert(engine.insert(dbname, "t50", vals) == DBStatus::OK);
            assert(engine.commitTransaction() == DBStatus::OK);
        }

        uint32_t pages100 = engine.tableNumPages(dbname, "t100");
        uint32_t pages50 = engine.tableNumPages(dbname, "t50");

        std::cout << "[FILLFACTOR] t100 pages=" << pages100
                  << " t50 pages=" << pages50 << "\n";

        // With fillfactor 100, rows pack as densely as possible.
        // With fillfactor 50 each page can only use ~50% of space,
        // so the same row count needs more pages than fillfactor 100.
        assert(pages50 > pages100);

        // Verify all rows are still queryable.
        auto rows100 = engine.query(dbname, "t100", {}, {"payload"});
        auto rows50 = engine.query(dbname, "t50", {}, {"payload"});
        assert(rows100.size() == 4);
        assert(rows50.size() == 4);
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[FILLFACTOR] all passed\n";
    return 0;
}
