#include "process/RuntimeStats.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <vector>

int main() {
    dbms::resetRuntimeStats();

    constexpr int workers = 4;
    constexpr int queriesPerWorker = 100;
    std::vector<std::thread> threads;
    for (int worker = 0; worker < workers; ++worker) {
        threads.emplace_back([] {
            for (int i = 0; i < queriesPerWorker; ++i) {
                dbms::recordQueryExecution("SELECT 1", 1.0, "db1", true, 1);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    dbms::recordQueryExecution("COMMIT", 0.5, "db1", true);
    dbms::recordQueryExecution("ROLLBACK", 0.5, "db1", true);
    dbms::recordQueryExecution("BAD SQL", 2.0, "db1", false);
    dbms::recordQueryExecution("SELECT 1", 3.0, "db2", true, 2);

    dbms::recordTableMutation("db1", "users", dbms::TableMutation::Insert, 3);
    dbms::recordTableMutation("db1", "users", dbms::TableMutation::Update, 2);
    dbms::recordTableMutation("db1", "users", dbms::TableMutation::Delete, 1);
    dbms::recordTableScan("db1", "partial", 7, false, false);
    uint64_t estimatedRows = 0;
    assert(!dbms::getRuntimeLiveRowEstimate("db1", "partial", estimatedRows));
    dbms::recordTableScan("db1", "users", 2, false, true);
    dbms::recordTableScan("db1", "users", 1, true, false);

    const auto db1 = dbms::getRuntimeDatabaseStats("db1");
    assert(db1.size() == 1);
    assert(db1[0].queries == workers * queriesPerWorker + 3);
    assert(db1[0].failedQueries == 1);
    assert(db1[0].xactCommit == 1);
    assert(db1[0].xactRollback == 1);
    assert(db1[0].tupReturned == workers * queriesPerWorker);

    const auto tables = dbms::getRuntimeTableStats("db1");
    assert(tables.size() == 2);
    const auto users = std::find_if(
        tables.begin(), tables.end(), [](const auto& table) {
            return table.relname == "users";
        });
    assert(users != tables.end());
    assert(users->seqScan == 1);
    assert(users->seqTupRead == 2);
    assert(users->idxScan == 1);
    assert(users->idxTupFetch == 1);
    assert(users->nTupIns == 3);
    assert(users->nTupUpd == 2);
    assert(users->nTupDel == 1);
    assert(users->nLiveTup == 2);
    assert(users->liveTupEstimateValid);
    assert(dbms::getRuntimeLiveRowEstimate("db1", "users", estimatedRows));
    assert(estimatedRows == 2);

    const std::filesystem::path persisted = "runtime_stats_persist_test.bin";
    std::filesystem::remove(persisted);
    std::filesystem::remove(persisted.string() + ".lock");
    assert(dbms::persistRuntimeStats("db1", persisted));
    dbms::resetRuntimeStats();
    assert(dbms::loadRuntimeStats("db1", persisted));
    const auto restoredDb = dbms::getRuntimeDatabaseStats("db1");
    assert(restoredDb.size() == 1);
    assert(restoredDb[0].queries == workers * queriesPerWorker + 3);
    const auto restoredTables = dbms::getRuntimeTableStats("db1");
    assert(restoredTables.size() == 2);
    const auto restoredUsers = std::find_if(
        restoredTables.begin(), restoredTables.end(), [](const auto& table) {
            return table.relname == "users";
        });
    assert(restoredUsers != restoredTables.end());
    assert(restoredUsers->nLiveTup == 2);

    dbms::resetRuntimeTableStats("db1", "users");
    assert(dbms::persistRuntimeStats("db1", persisted));
    dbms::resetRuntimeStats();
    assert(dbms::loadRuntimeStats("db1", persisted));
    const auto afterDropTables = dbms::getRuntimeTableStats("db1");
    assert(std::find_if(
               afterDropTables.begin(), afterDropTables.end(), [](const auto& table) {
                   return table.relname == "users";
               }) == afterDropTables.end());

    {
        std::ofstream corrupt(persisted, std::ios::binary | std::ios::trunc);
        corrupt << "corrupt";
    }
    dbms::resetRuntimeStats();
    assert(!dbms::loadRuntimeStats("db1", persisted));
    std::filesystem::remove(persisted);
    std::filesystem::remove(persisted.string() + ".lock");

    dbms::resetRuntimeStats();
    assert(dbms::getRuntimeDatabaseStats().empty());
    assert(dbms::getRuntimeTableStats().empty());
    std::cout << "[RUNTIME-STATS] concurrency, database and table counters OK\n";
    return 0;
}
