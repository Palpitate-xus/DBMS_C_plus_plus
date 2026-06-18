// TOAST chunked relation test.

#include "TableManage.h"
#include "Config.h"
#include <filesystem>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "toast_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeVarCharColumn("payload", false, 10000, false));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // Verify TOAST relation and index files were created.
        assert(std::filesystem::exists(std::filesystem::path(dbname) / "t.toast.dt"));
        assert(std::filesystem::exists(std::filesystem::path(dbname) / "t.toast.idx"));

        std::string largeValue(10000, 'a');

        // Insert a row with a large value that exceeds the TOAST threshold.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["payload"] = largeValue;
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        // Query should return the original large value.
        auto rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(largeValue) != std::string::npos);
        std::cout << "[TOAST] insert + query large value OK\n";

        // Update with another large value.
        std::string updatedValue(12000, 'b');
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> newVals;
        newVals["payload"] = updatedValue;
        assert(engine.update(dbname, "t", newVals, {}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(updatedValue) != std::string::npos);
        assert(rows[0].find(largeValue) == std::string::npos);
        std::cout << "[TOAST] update large value OK\n";

        // Delete the row.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.remove(dbname, "t", {}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.empty());
        std::cout << "[TOAST] delete large value OK\n";
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[TOAST] all passed\n";
    return 0;
}
