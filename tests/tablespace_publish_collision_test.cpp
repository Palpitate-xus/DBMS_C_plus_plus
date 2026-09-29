#include "commands/TableManage.h"
#include "interfaces/table_schema.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using dbms::DBStatus;

static dbms::TableSchema simpleTable() {
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 2));
    return table;
}

static void assertSourceIntact(dbms::StorageEngine& engine,
                               const std::string& database) {
    const auto tablespace = engine.getTableSchema(database, "items").tablespace;
    assert(tablespace.empty() || tablespace == "pg_default");
    assert(fs::is_regular_file(fs::path(database) / "items.dt"));
    size_t rows = 0;
    engine.forEachRow(database, "items",
                      [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);
}

int main() {
    cleanupAllTestData();
    dbms::StorageEngine engine;

    const std::string sameDatabase = testDbPath("ts_publish_same");
    const fs::path sameLocation =
        fs::absolute(testDbPath("ts_publish_same_location"));
    fs::remove_all(sameLocation);
    assert(engine.createDatabase(sameDatabase, "utf8") == DBStatus::OK);
    assert(engine.createTablespace(sameDatabase, "fast", sameLocation.string()) ==
           DBStatus::OK);
    assert(engine.createTable(sameDatabase, simpleTable()) == DBStatus::OK);
    assert(engine.insert(sameDatabase, "items", {{"id", "7"}}) == DBStatus::OK);
    bool directCollision = false;
    const auto directResult = engine.alterTableTablespace(
        sameDatabase, "items", "fast",
        [&](const fs::path& destination, bool staged) {
            if (staged || destination.filename() != "items.dt") return;
            std::ofstream output(destination, std::ios::binary);
            output << "unrelated-destination";
            assert(output.good());
            directCollision = true;
        });
    assert(directCollision);
    assert(directResult == DBStatus::INVALID_VALUE);
    const fs::path directDestination = sameLocation / sameDatabase / "items.dt";
    std::ifstream directInput(directDestination, std::ios::binary);
    std::string directBytes;
    std::getline(directInput, directBytes);
    assert(directBytes == "unrelated-destination");
    assertSourceIntact(engine, sameDatabase);
    fs::remove(directDestination);
    assert(engine.dropTable(sameDatabase, "items") == DBStatus::OK);
    assert(engine.dropTablespace(sameDatabase, "fast") == DBStatus::OK);
    assert(engine.dropDatabase(sameDatabase) == DBStatus::OK);
    fs::remove_all(sameLocation);

    struct stat localDevice {};
    struct stat sharedMemoryDevice {};
    if (::stat(".", &localDevice) == 0 &&
        ::stat("/dev/shm", &sharedMemoryDevice) == 0 &&
        localDevice.st_dev != sharedMemoryDevice.st_dev) {
        const std::string crossDatabase = testDbPath("ts_publish_cross");
        const fs::path crossLocation = fs::path("/dev/shm") /
            ("dbms-ts-publish-" + std::to_string(::getpid()));
        fs::remove_all(crossLocation);
        assert(engine.createDatabase(crossDatabase, "utf8") == DBStatus::OK);
        assert(engine.createTablespace(
                   crossDatabase, "cross", crossLocation.string()) == DBStatus::OK);
        assert(engine.createTable(crossDatabase, simpleTable()) == DBStatus::OK);
        assert(engine.insert(crossDatabase, "items", {{"id", "9"}}) ==
               DBStatus::OK);
        bool staleStagingCreated = false;
        fs::path staleStaging;
        const auto staleResult = engine.alterTableTablespace(
            crossDatabase, "items", "cross",
            [&](const fs::path& destination, bool staged) {
                if (staged || staleStagingCreated) return;
                // This is the first cross-device move in this isolated
                // process, so the generated sequence suffix is zero.
                staleStaging = fs::path(
                    destination.string() + ".tablespace_move." +
                    std::to_string(::getpid()) + ".0");
                assert(fs::create_directory(staleStaging));
                std::ofstream output(staleStaging / "keep", std::ios::binary);
                output << "preexisting-staging";
                assert(output.good());
                staleStagingCreated = true;
            });
        assert(staleStagingCreated);
        assert(staleResult != DBStatus::OK);
        std::ifstream staleInput(staleStaging / "keep", std::ios::binary);
        std::string staleBytes;
        std::getline(staleInput, staleBytes);
        assert(staleBytes == "preexisting-staging");
        assertSourceIntact(engine, crossDatabase);
        fs::remove_all(staleStaging);

        bool stagedCollision = false;
        const auto stagedResult = engine.alterTableTablespace(
            crossDatabase, "items", "cross",
            [&](const fs::path& destination, bool staged) {
                if (!staged || destination.filename() != "items.dt") return;
                assert(fs::create_directory(destination));
                std::ofstream output(destination / "keep", std::ios::binary);
                output << "unrelated-staged-target";
                assert(output.good());
                stagedCollision = true;
            });
        assert(stagedCollision);
        assert(stagedResult != DBStatus::OK);
        const fs::path stagedDestination =
            crossLocation / crossDatabase / "items.dt";
        std::ifstream stagedInput(stagedDestination / "keep", std::ios::binary);
        std::string stagedBytes;
        std::getline(stagedInput, stagedBytes);
        assert(stagedBytes == "unrelated-staged-target");
        assertSourceIntact(engine, crossDatabase);
        fs::remove_all(stagedDestination);
        assert(engine.dropTable(crossDatabase, "items") == DBStatus::OK);
        assert(engine.dropTablespace(crossDatabase, "cross") == DBStatus::OK);
        assert(engine.dropDatabase(crossDatabase) == DBStatus::OK);
        fs::remove_all(crossLocation);
    }

    finalCleanupTestData();
    std::cout << "[TABLESPACE PUBLISH COLLISION] passed" << std::endl;
    return 0;
}
