// test_link_flags: -Wl,--wrap=fsync

#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "access/IndexFileUtil.h"
#include "test_utils.h"

#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <unistd.h>

namespace {
std::atomic<bool> failStagedFileSync{false};
}

extern "C" int __real_fsync(int descriptor);
extern "C" int __wrap_fsync(int descriptor) {
    if (failStagedFileSync.load(std::memory_order_relaxed)) {
        const std::string link = "/proc/self/fd/" + std::to_string(descriptor);
        std::array<char, 4096> buffer;
        const ssize_t length = ::readlink(link.c_str(), buffer.data(), buffer.size());
        if (length > 0) {
            const std::string_view path(buffer.data(), static_cast<size_t>(length));
            constexpr std::string_view suffix = "/backup_payload/item-97";
            if (path.find(".backup_staging.") != std::string_view::npos &&
                path.size() >= suffix.size() &&
                path.substr(path.size() - suffix.size()) == suffix &&
                failStagedFileSync.exchange(false, std::memory_order_relaxed)) {
                errno = EIO;
                return -1;
            }
        }
    }
    return __real_fsync(descriptor);
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("backup_replacement_source");
    const std::string backup = testDbPath("backup_replacement_image");

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema obsolete;
    obsolete.tablename = "obsolete_table";
    obsolete.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    obsolete.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, obsolete) == dbms::DBStatus::OK);
    assert(engine.insert(database, "obsolete_table", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(engine.physicalBackup(database, backup));

    assert(engine.dropTable(database, "obsolete_table") ==
           dbms::DBStatus::OK);
    dbms::TableSchema current;
    current.tablename = "current_table";
    current.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    current.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, current) == dbms::DBStatus::OK);
    assert(engine.insert(database, "current_table", {{"id", "2"}}) ==
           dbms::DBStatus::OK);

    // Exercise a staging tree large enough to sync several files together.
    // A payload sync failure must leave the previously published backup.
    const auto extra = engine.dbPath(database) / "backup_payload";
    assert(std::filesystem::create_directory(extra));
    for (int index = 0; index < 160; ++index) {
        std::ofstream payload(extra / ("item-" + std::to_string(index)));
        payload << std::string(static_cast<size_t>(index), 'x');
        assert(payload.good());
    }
    failStagedFileSync.store(true, std::memory_order_relaxed);
    assert(!engine.physicalBackup(database, backup));
    assert(!failStagedFileSync.load(std::memory_order_relaxed));
    assert(std::filesystem::is_regular_file(
        std::filesystem::path(backup) / "obsolete_table.stc"));
    assert(!std::filesystem::exists(std::filesystem::path(backup) / "backup_payload"));

    // Fail the parent-directory sync immediately after the atomic exchange.
    // The old generation must be exchanged back before failure is reported.
    bool sawIncompleteStaging = false;
    bool armed = false;
    const auto stagingPrefix = "." + backup + ".backup_staging.";
    const auto failAtPublication = [&](uint64_t bytesCopied) {
        for (const auto& entry : std::filesystem::directory_iterator(".")) {
            const auto name = entry.path().filename().string();
            if (name.rfind(stagingPrefix, 0) != 0) continue;
            const auto marker = entry.path() / ".dbms_physical_backup";
            std::ifstream markerInput(marker, std::ios::binary);
            const std::string markerBytes{std::istreambuf_iterator<char>(markerInput), {}};
            if (markerBytes != "DBMS_PHYSICAL_BACKUP_V3\n") {
                if (std::filesystem::is_regular_file(entry.path() / "tlist.lst")) {
                    assert(!engine.databaseExists(name));
                    sawIncompleteStaging = true;
                }
                continue;
            }
            if (bytesCopied != 0 ||
                !std::filesystem::is_regular_file(entry.path() / ".dbms_backup_manifest")) {
                continue;
            }
            assert(!armed && "the operation has exactly one completed staging directory");
            assert(std::filesystem::is_regular_file(entry.path() / "current_table.stc"));
            assert(std::filesystem::is_regular_file(
                std::filesystem::path(backup) / "obsolete_table.stc"));
            armed = true;
            dbms::index_file::failNextDirectorySyncForTesting();
        }
        return true;
    };
    assert(!engine.physicalBackup(database, backup, failAtPublication));
    assert(sawIncompleteStaging && "observed the copied table list before backup completion");
    assert(armed && "the new generation reached the final progress callback");
    assert(std::filesystem::is_regular_file(
        std::filesystem::path(backup) / "obsolete_table.stc"));
    assert(!std::filesystem::exists(
        std::filesystem::path(backup) / "current_table.stc"));

    // Reusing a destination must replace its complete generation, not merge
    // the new source into files left by the previous backup.
    assert(engine.physicalBackup(database, backup));

    dbms::TableSchema afterBackup;
    afterBackup.tablename = "after_backup";
    afterBackup.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    afterBackup.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, afterBackup) == dbms::DBStatus::OK);
    assert(engine.physicalRestore(database, backup));
    assert(engine.tableExists(database, "current_table"));
    assert(!engine.tableExists(database, "obsolete_table"));
    assert(!engine.tableExists(database, "after_backup"));
    for (int index = 0; index < 160; ++index)
        assert(std::filesystem::file_size(extra / ("item-" + std::to_string(index))) ==
               static_cast<uintmax_t>(index));

    finalCleanupTestData();
    std::cout << "[PHYSICAL BACKUP REPLACEMENT] passed\n";
    return 0;
}
