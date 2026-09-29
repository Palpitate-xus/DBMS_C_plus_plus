#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;
using dbms::DBStatus;

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("aborted_snapshot_recovery");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == DBStatus::OK);
    assert(engine.createSequence(database, "counter") == DBStatus::OK);

    assert(engine.beginTransaction(database, true) == DBStatus::OK);
    const uint64_t abortedXid = engine.currentTxnId();
    assert(abortedXid != 0);
    assert(engine.rollbackTransaction() == DBStatus::OK);

    // Simulate an older process leaving a valid transaction image after a
    // durable ABORT record, before unlinking its completed snapshot.
    const fs::path staleBackup = database + ".txn_backup." +
        std::to_string(abortedXid);
    assert(!fs::exists(staleBackup));
    assert(engine.physicalBackup(database, staleBackup.string()));
    assert(engine.nextval(database, "counter") == 1);

    dbms::StorageEngine restarted;
    assert(!fs::exists(staleBackup));
    assert(restarted.nextval(database, "counter") == 2);

    assert(restarted.dropDatabase(database) == DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[ABORTED SNAPSHOT RECOVERY] passed" << std::endl;
    return 0;
}
