#include "commands/TableManage.h"
#include "storage/WAL.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

using namespace dbms;
namespace fs = std::filesystem;

static void retainedOwnedDirectory() {
    const std::string db = "__t_retained_wal_namespace";
    const std::string moved = db + ".moved";
    StorageEngine engine;
    engine.setBackgroundIntervals(1, 300000);
    assert(engine.createDatabase(db) == DBStatus::OK);
    WALManager* pinned = engine.getWAL(db);
    assert(pinned && pinned->refersToCurrentDirectory());
    assert(engine.beginTransaction(db, true) == DBStatus::OK);
    fs::rename(db, moved);
    fs::create_directory_symlink(moved, db);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    assert(!engine.databaseExists(db));
    // This is still the exact directory opened while the holder acquired
    // ownership. No new namespace lookup may deny the owner's terminal WAL.
    assert(pinned->refersToCurrentDirectory());
    assert(engine.getWAL(db) == pinned);
    assert(engine.commitTransaction() == DBStatus::OK);
    // The terminal exception does not make the symlink a valid public DB.
    assert(engine.getWAL(db) == nullptr);
    assert(engine.beginTransaction(db) == DBStatus::DATABASE_NOT_FOUND);
    fs::remove(db);
    fs::rename(moved, db);
    assert(engine.databaseExists(db));
}

static void unrelatedDirectoryIsNotTheOwner() {
    const std::string db = "__t_retained_wal_wrong_generation";
    const std::string moved = db + ".moved";
    const std::string other = "__t_retained_wal_unrelated";
    StorageEngine engine;
    assert(engine.createDatabase(db) == DBStatus::OK);
    assert(engine.createDatabase(other) == DBStatus::OK);
    WALManager* pinned = engine.getWAL(db);
    WALManager* unrelated = engine.getWAL(other);
    assert(pinned && unrelated && pinned != unrelated);
    const Lsn unrelatedBoundary = unrelated->currentWriteLsn();
    assert(engine.beginTransaction(db, true) == DBStatus::OK);
    fs::rename(db, moved);
    fs::create_directory_symlink(other, db);
    assert(!pinned->refersToCurrentDirectory());
    assert(engine.getWAL(db) == nullptr);
    assert(unrelated->currentWriteLsn() == unrelatedBoundary);
    fs::remove(db);
    fs::rename(moved, db);
    assert(engine.getWAL(db) == pinned);
    assert(engine.commitTransaction() == DBStatus::OK);
}

static void validPublicNameIsNotTheOwnedGeneration() {
    const std::string db = "__t_retained_wal_regular_generation";
    const std::string moved = db + ".moved";
    const std::string other = "__t_retained_wal_regular_other";
    StorageEngine engine;
    assert(engine.createDatabase(db) == DBStatus::OK);
    assert(engine.createDatabase(other) == DBStatus::OK);
    WALManager* pinned = engine.getWAL(db);
    WALManager* unrelated = engine.getWAL(other);
    assert(pinned && unrelated && pinned != unrelated);
    const Lsn unrelatedBoundary = unrelated->currentWriteLsn();
    assert(engine.beginTransaction(db, true) == DBStatus::OK);
    fs::rename(db, moved);
    fs::rename(other, db);
    assert(engine.databaseExists(db));
    assert(!pinned->refersToCurrentDirectory());
    assert(engine.getWAL(db) == nullptr);
    assert(unrelated->currentWriteLsn() == unrelatedBoundary);
    fs::rename(db, other);
    fs::rename(moved, db);
    assert(engine.getWAL(db) == pinned);
    assert(engine.commitTransaction() == DBStatus::OK);
}

int main() {
    retainedOwnedDirectory();
    unrelatedDirectoryIsNotTheOwner();
    validPublicNameIsNotTheOwnedGeneration();
    std::cout << "[RETAINED WAL NAMESPACE] same physical owner commits; public and foreign names rejected\n";
}
