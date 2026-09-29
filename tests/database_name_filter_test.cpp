#include "TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

int main() {
    namespace fs = std::filesystem;
    const std::vector<std::string> names = {
        testDbPath("name_filter.archivebar"),
        testDbPath("name_filter.txn_backupbar"),
        testDbPath("name_filter_parent"),
        testDbPath("name_filter_parent.txn_backup.7"),
        ".name_filter_hidden"};
    for (const auto& name : names) fs::remove_all(name);

    {
        dbms::StorageEngine engine;
        for (const auto& name : names) {
            assert(engine.createDatabase(name) == dbms::DBStatus::OK);
            assert(engine.databaseExists(name));
        }
    }

    // Recovery must inspect every real database even if its name resembles
    // an internal sidecar. Otherwise malformed lifecycle state is skipped.
    const fs::path lifecycle =
        fs::path(names.front()) / ".unlogged_lifecycle";
    {
        std::ofstream corrupt(lifecycle, std::ios::binary | std::ios::trunc);
        corrupt << "corrupt lifecycle";
    }
    bool refusedCorruptDatabase = false;
    try {
        dbms::StorageEngine engine;
    } catch (const std::runtime_error&) {
        refusedCorruptDatabase = true;
    }
    assert(refusedCorruptDatabase);
    {
        std::ofstream restored(lifecycle, std::ios::binary | std::ios::trunc);
        restored << "DBMS_UNLOGGED_LIFECYCLE_V1\nCLEAN\n";
    }

    dbms::StorageEngine engine;
    const auto listed = engine.getDatabaseNames();
    for (const auto& name : names) {
        assert(std::find(listed.begin(), listed.end(), name) != listed.end());
    }

    // A physical transaction image contains a table list but carries the
    // backup marker, so structural identification must continue excluding it.
    const fs::path snapshot = testDbPath("name_filter_snapshot.txn_backup.7");
    fs::create_directory(snapshot);
    std::ofstream(snapshot / "tlist.lst");
    std::ofstream(snapshot / ".dbms_physical_backup") << "snapshot";
    assert(!engine.databaseExists(snapshot.string()));
    const auto withSnapshot = engine.getDatabaseNames();
    assert(std::find(withSnapshot.begin(), withSnapshot.end(),
                     snapshot.string()) == withSnapshot.end());

    for (const auto& name : names) {
        assert(engine.dropDatabase(name) == dbms::DBStatus::OK);
    }
    fs::remove_all(snapshot);
}
