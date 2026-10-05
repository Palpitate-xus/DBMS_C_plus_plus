#include "access/IndexFileUtil.h"
#include "storage/CommitLog.h"
#include "TableManage.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string dbname = "__t_database_lifecycle";
    std::filesystem::remove_all(dbname);

    const std::string failedCreate = "__t_database_lifecycle_sync_failure";
    std::filesystem::remove_all(failedCreate);
    // Four atomic child-file publications sync the new database directory;
    // the following parent sync must be the point that publishes CREATE DB.
    dbms::index_file::failDirectorySyncAfterForTesting(4);
    assert(g_engine.createDatabase(failedCreate) == dbms::DBStatus::IO_ERROR);
    assert(!std::filesystem::exists(failedCreate));
    assert(g_engine.createDatabase(failedCreate) == dbms::DBStatus::OK);
    assert(g_engine.dropDatabase(failedCreate) == dbms::DBStatus::OK);

    const std::string failedDrop = "__t_database_lifecycle_drop_sync_failure";
    std::filesystem::remove_all(failedDrop);
    assert(g_engine.createDatabase(failedDrop) == dbms::DBStatus::OK);
    dbms::index_file::failNextDirectorySyncForTesting();
    assert(g_engine.dropDatabase(failedDrop) == dbms::DBStatus::IO_ERROR);
    assert(!std::filesystem::exists(failedDrop));

    assert(g_engine.createDatabase(dbname) == dbms::DBStatus::OK);
    assert(std::filesystem::is_regular_file(std::filesystem::path(dbname) / "tlist.lst"));
    assert(std::filesystem::is_regular_file(std::filesystem::path(dbname) / ".charset"));
    assert(std::filesystem::is_regular_file(std::filesystem::path(dbname) / ".schema_public"));
    {
        std::ifstream charset(std::filesystem::path(dbname) / ".charset");
        std::string value;
        assert(std::getline(charset, value));
        assert(value == "utf8");
    }
    auto* oldClog = g_engine.getCommitLog(dbname);
    assert(oldClog != nullptr);
    oldClog->setStatus(42, dbms::CommitLog::Status::Committed);
    assert(oldClog->getStatus(42) == dbms::CommitLog::Status::Committed);

    assert(g_engine.dropDatabase(dbname) == dbms::DBStatus::OK);
    assert(!std::filesystem::exists(dbname));

    // A recreated database must not observe the deleted database's cached
    // commit status or receive a late flush from its old CommitLog object.
    assert(g_engine.createDatabase(dbname) == dbms::DBStatus::OK);
    auto* newClog = g_engine.getCommitLog(dbname);
    assert(newClog != nullptr);
    assert(newClog->getStatus(42) == dbms::CommitLog::Status::InProgress);

    assert(g_engine.dropDatabase(dbname) == dbms::DBStatus::OK);
    std::cout << "[DATABASE-LIFECYCLE] cache eviction and same-name recreate OK\n";
    return 0;
}
