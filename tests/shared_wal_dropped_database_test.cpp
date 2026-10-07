#include "commands/TableManage.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main() {
    using namespace dbms;
    const std::string database = "__t_shared_wal_dropped_database";
    StorageEngine first, second;
    assert(first.createDatabase(database) == DBStatus::OK);
    auto* before = first.getWAL(database);
    assert(before && second.getWAL(database) == before);
    assert(first.dropDatabase(database) == DBStatus::OK);
    auto* after = second.getWAL(database);
    std::cerr << "[DROPPED WAL] manager=" << (after != nullptr)
              << " resurrected=" << std::filesystem::exists(database) << '\n';
    assert(after == nullptr);
    assert(!std::filesystem::exists(database));
    assert(first.createDatabase(database) == DBStatus::OK);
    assert(first.getWAL(database) && first.getWAL(database) == second.getWAL(database));
    std::cout << "[DROPPED WAL] stale caller does not resurrect database\n";
}
