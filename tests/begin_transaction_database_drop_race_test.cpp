#include "TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <thread>

int main() {
    namespace fs = std::filesystem;
    const std::string db = testDbPath("begin_transaction_drop_race");
    const std::string moved = db + ".moved";
    fs::remove_all(db);
    fs::remove_all(moved);
    dbms::StorageEngine engine;
    assert(engine.createDatabase(db) == dbms::DBStatus::OK);
    assert(engine.beginTransaction(db, true) == dbms::DBStatus::OK);

    std::atomic<bool> started{false};
    std::atomic<bool> finished{false};
    dbms::DBStatus result = dbms::DBStatus::OK;
    std::thread starter([&] {
        started.store(true, std::memory_order_release);
        result = engine.beginTransaction(db);
        if (result == dbms::DBStatus::OK) engine.rollbackTransaction();
        finished.store(true, std::memory_order_release);
    });
    while (!started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(!finished.load(std::memory_order_acquire));

    // A normal DROP/RENAME also takes this exclusive database lock. Replace
    // the owned name with a symlink while it is held. The lock holder can
    // still flush its open files through the link, but a waiting BEGIN must
    // reject that name as a different, invalid database generation.
    fs::rename(db, moved);
    fs::create_directory_symlink(moved, db);
    assert(engine.commitTransaction() == dbms::DBStatus::OK);
    starter.join();
    assert(result == dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(!engine.databaseExists(db));
}
