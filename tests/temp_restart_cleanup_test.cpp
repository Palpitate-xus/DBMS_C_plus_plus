#include "TableManage.h"
#include "Config.h"
#include "utils/Session.h"
#include <cassert>
#include <filesystem>
#include <iostream>

dbms::Config g_config;

int main() {
    using namespace dbms;
    const std::string dbname = "temp_restart_cleanup_db";
    const std::string logicalName = "stale";
    const std::string physicalName = "__tmp_424242_" + logicalName;
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema table;
        table.tablename = physicalName;
        table.append(makeIntColumn("id", false, 0, true));

        Session session;
        session.pid = 424242;
        setCurrentSession(&session);
        assert(engine.createTable(dbname, table) == DBStatus::OK);
        setCurrentSession(nullptr);
        assert(engine.tableExists(dbname, physicalName));
    }

    // A new StorageEngine represents a post-restart backend.  Session temp
    // relations must not survive, including their persistent relation-list
    // entry and any orphaned physical files.
    {
        StorageEngine restarted;
        assert(!restarted.tableExists(dbname, physicalName));
        for (const auto& name : restarted.getTableNames(dbname)) {
            assert(name != physicalName);
        }
        for (const auto& entry : std::filesystem::directory_iterator(dbname)) {
            assert(entry.path().filename().string().rfind("__tmp_424242_", 0) != 0);
        }
    }

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove(".txnid");
    std::cout << "[TEMP RESTART] stale session temporary relations cleaned up\n";
    return 0;
}
