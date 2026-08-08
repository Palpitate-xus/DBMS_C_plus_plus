#include "process/RuntimeStats.h"

#include <cassert>
#include <iostream>
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
    dbms::recordTableScan("db1", "users", 2, false, true);

    const auto db1 = dbms::getRuntimeDatabaseStats("db1");
    assert(db1.size() == 1);
    assert(db1[0].queries == workers * queriesPerWorker + 3);
    assert(db1[0].failedQueries == 1);
    assert(db1[0].xactCommit == 1);
    assert(db1[0].xactRollback == 1);
    assert(db1[0].tupReturned == workers * queriesPerWorker);

    const auto tables = dbms::getRuntimeTableStats("db1");
    assert(tables.size() == 1);
    assert(tables[0].seqScan == 1);
    assert(tables[0].seqTupRead == 2);
    assert(tables[0].nTupIns == 3);
    assert(tables[0].nTupUpd == 2);
    assert(tables[0].nTupDel == 1);
    assert(tables[0].nLiveTup == 2);

    dbms::resetRuntimeStats();
    assert(dbms::getRuntimeDatabaseStats().empty());
    assert(dbms::getRuntimeTableStats().empty());
    std::cout << "[RUNTIME-STATS] concurrency, database and table counters OK\n";
    return 0;
}
