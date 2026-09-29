#include "TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <filesystem>
#include <thread>

int main() {
    namespace fs = std::filesystem;
    const std::string source = testDbPath("backup_target_lock_source");
    const std::string target = testDbPath("backup_target_lock_target");
    fs::remove_all(source);
    fs::remove_all(target);

    dbms::StorageEngine other;
    dbms::StorageEngine engine;
    assert(engine.createDatabase(source) == dbms::DBStatus::OK);

    std::atomic<bool> checked{false};
    dbms::DBStatus createResult = dbms::DBStatus::OK;
    const auto progress = [&](uint64_t) {
        if (!checked.exchange(true)) {
            std::thread creator([&] {
                createResult = other.createDatabase(target);
            });
            creator.join();
        }
        return true;
    };
    const bool backedUp = engine.physicalBackup(source, target, progress);
    assert(checked.load());
    assert(createResult == dbms::DBStatus::DATABASE_IN_USE);
    assert(backedUp);
    assert(!engine.databaseExists(target));
    assert(fs::is_regular_file(fs::path(target) / ".dbms_physical_backup"));

    fs::remove_all(target);
    assert(other.createDatabase(target) == dbms::DBStatus::OK);
    assert(other.dropDatabase(target) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(source) == dbms::DBStatus::OK);
}
