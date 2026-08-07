#include "TableManage.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string dbname = "__t_database_lifecycle";
    std::filesystem::remove_all(dbname);

    assert(g_engine.createDatabase(dbname) == dbms::DBStatus::OK);
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
