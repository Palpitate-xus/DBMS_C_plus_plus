#include "access/IndexFileUtil.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {
enum class PublicationFault { None, Rename, ParentSync };
enum class PublicationStage { None, InjectFailure, DurableRetry };
thread_local PublicationFault publicationFault = PublicationFault::None;
thread_local PublicationStage publicationStage = PublicationStage::None;
thread_local std::string publicationTarget;
thread_local fs::path publicationParent;
thread_local struct stat publicationDirectory {};
thread_local unsigned publicationHits = 0;
thread_local unsigned injectedFailures = 0;
thread_local unsigned durableRetries = 0;
thread_local unsigned successfulDirectorySyncs = 0;
thread_local unsigned competingThreadSyncs = 0;

void armPublicationFault(const fs::path& path, PublicationFault fault) {
    assert(publicationFault == PublicationFault::None);
    assert(publicationStage == PublicationStage::None);
    publicationTarget = path.string();
    publicationParent = path.parent_path();
    assert(::stat(publicationParent.c_str(), &publicationDirectory) == 0);
    assert(S_ISDIR(publicationDirectory.st_mode));
    publicationHits = 0;
    injectedFailures = 0;
    durableRetries = 0;
    successfulDirectorySyncs = 0;
    competingThreadSyncs = 0;
    publicationFault = fault;
}
} // namespace

// Interpose only in this standalone test executable. No production flag or
// header changes are needed, so the ordinary native-test runner exercises the
// fault as well. The real Linux syscalls are otherwise passed through. The
// target is the exact tlist.lst publication, not a count of unrelated CREATE
// barriers (physical IDs, heap markers, or derived-map receipts).
extern "C" int rename(const char* source, const char* target) noexcept {
    assert(publicationStage == PublicationStage::None);
    const auto fault = publicationFault;
    const bool selected = fault != PublicationFault::None &&
        std::strcmp(target, publicationTarget.c_str()) == 0;
    if (selected && fault == PublicationFault::Rename) {
        publicationFault = PublicationFault::None;
        ++publicationHits;
        errno = EIO;
        return -1;
    }
    const int result = static_cast<int>(
        ::syscall(SYS_renameat, AT_FDCWD, source, AT_FDCWD, target));
    if (selected && result == 0) {
        publicationFault = PublicationFault::None;
        ++publicationHits;
        publicationStage = PublicationStage::InjectFailure;

        // Deterministically run another thread's real sync of this same
        // directory before the selected thread can reach its first fsync.
        // It must not consume the selected thread's pending error.
        bool otherThreadSynced = false;
        std::thread other([&, directory = publicationDirectory,
                           parent = publicationParent]() {
            assert(publicationStage == PublicationStage::None);
            publicationDirectory = directory;
            assert(dbms::index_file::syncDirectory(parent));
            assert(injectedFailures == 0 && durableRetries == 0);
            assert(publicationStage == PublicationStage::None);
            otherThreadSynced = successfulDirectorySyncs == 1;
        });
        other.join();
        assert(otherThreadSynced);
        ++competingThreadSyncs;
        assert(publicationStage == PublicationStage::InjectFailure);
        assert(injectedFailures == 0 && durableRetries == 0);
        assert(dbms::index_file::directorySyncFailureCountForTesting.load() == 0);
    }
    return result;
}

extern "C" int fsync(int fd) {
    struct stat owner {};
    const bool selectedDirectory = ::fstat(fd, &owner) == 0 &&
        S_ISDIR(owner.st_mode) && owner.st_dev == publicationDirectory.st_dev &&
        owner.st_ino == publicationDirectory.st_ino;
    // A later heap/index publication cannot masquerade as the tlist retry.
    assert(publicationStage == PublicationStage::None || selectedDirectory);
    if (publicationStage == PublicationStage::InjectFailure) {
        ++injectedFailures;
        publicationStage = PublicationStage::DurableRetry;
        errno = EIO;
        return -1;
    }
    const int result = static_cast<int>(::syscall(SYS_fsync, fd));
    if (selectedDirectory && result == 0) {
        ++successfulDirectorySyncs;
        if (publicationStage == PublicationStage::DurableRetry) {
            ++durableRetries;
            publicationStage = PublicationStage::None;
        }
    }
    return result;
}

static std::string readBytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static std::vector<std::string> readTableNames(const fs::path& path) {
    const std::string bytes = readBytes(path);
    assert(bytes.size() % dbms::MAX_TABLE_NAME_LEN == 0);
    std::vector<std::string> names;
    for (size_t offset = 0; offset < bytes.size();
         offset += dbms::MAX_TABLE_NAME_LEN) {
        const size_t end = bytes.find('\0', offset);
        names.push_back(bytes.substr(
            offset, (end == std::string::npos
                        ? offset + dbms::MAX_TABLE_NAME_LEN : end) - offset));
    }
    return names;
}

static dbms::TableSchema makeTable(const std::string& name) {
    dbms::TableSchema table;
    table.tablename = name;
    table.append(dbms::makeIntColumn("id", true, 2));
    return table;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("table_list_atomicity");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    assert(engine.createTable(database, makeTable("alpha")) ==
           dbms::DBStatus::OK);
    assert(engine.createTable(database, makeTable("remove_me")) ==
           dbms::DBStatus::OK);
    assert(engine.createTable(database, makeTable("omega")) ==
           dbms::DBStatus::OK);

    const fs::path tableList = fs::path(database) / "tlist.lst";
    const std::vector<std::string> originalNames{
        "alpha", "remove_me", "omega"};
    const std::string originalTableList = readBytes(tableList);
    assert(engine.insert(database, "alpha", {{"id", "11"}}) ==
           dbms::DBStatus::OK);
    assert(engine.insert(database, "remove_me", {{"id", "22"}}) ==
           dbms::DBStatus::OK);
    assert(engine.insert(database, "omega", {{"id", "33"}}) ==
           dbms::DBStatus::OK);

    const auto expectSurvivingRows = [&]() {
        assert(engine.query(database, "alpha", {}, {"id"}) ==
               std::vector<std::string>{"11 "});
        assert(engine.query(database, "remove_me", {}, {"id"}) ==
               std::vector<std::string>{"22 "});
        assert(engine.query(database, "omega", {}, {"id"}) ==
               std::vector<std::string>{"33 "});
    };
    const auto expectUnpublished = [&]() {
        assert(dbms::index_file::directorySyncFailureCountForTesting.load() == 0);
        assert(readBytes(tableList) == originalTableList);
        assert(readTableNames(tableList) == originalNames);
        assert(engine.getTableNames(database) == originalNames);
        assert(!engine.tableExists(database, "created"));
        for (const auto& entry : fs::directory_iterator(database)) {
            const auto name = entry.path().filename().string();
            assert(name != "created" && name.rfind("created.", 0) != 0 &&
                   name.rfind("created#", 0) != 0);
            assert(name.rfind("tlist.lst.tmp.", 0) != 0);
        }
        expectSurvivingRows();
    };

    // Keep the original injection. Its second barrier now belongs to the
    // database physical-ID highwater, before any schema/heap/tlist publish.
    // This is a legitimate CREATE failure, not a tlist retry opportunity.
    dbms::index_file::failDirectorySyncAfterForTesting(1);
    std::string earlyError;
    assert(engine.createTable(database, makeTable("created"), &earlyError) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(earlyError == "could not allocate durable physical relation identity");
    expectUnpublished();
    assert(engine.createTable(database, makeTable("created")) ==
           dbms::DBStatus::OK);
    assert(engine.getTableSchema(database, "created").physicalRelationId >
           engine.getTableSchema(database, "omega").physicalRelationId);
    assert(engine.dropTable(database, "created") == dbms::DBStatus::OK);
    expectUnpublished();

    // Failure before rename must not accept the old (different) generation
    // as a successful publication, or leave a half-created physical owner.
    armPublicationFault(tableList, PublicationFault::Rename);
    std::string publishError;
    assert(engine.createTable(database, makeTable("created"), &publishError) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(publishError == "could not update table list");
    assert(publicationFault == PublicationFault::None && publicationHits == 1);
    assert(injectedFailures == 0 && durableRetries == 0);
    assert(competingThreadSyncs == 0 && publicationStage == PublicationStage::None);
    expectUnpublished();

    // Fail the actual tlist parent sync only after its successful rename.
    // CREATE must confirm the exact bytes and perform a real durable retry.
    armPublicationFault(tableList, PublicationFault::ParentSync);
    std::ostringstream createDiagnostics;
    auto* previousCerr = std::cerr.rdbuf(createDiagnostics.rdbuf());
    const auto createStatus = engine.createTable(
        database, makeTable("created"));
    std::cerr.rdbuf(previousCerr);
    assert(createStatus == dbms::DBStatus::OK);
    assert(publicationFault == PublicationFault::None && publicationHits == 1);
    assert(injectedFailures == 1 && durableRetries == 1);
    assert(competingThreadSyncs == 1 && publicationStage == PublicationStage::None);
    assert(dbms::index_file::directorySyncFailureCountForTesting.load() == 0);
    assert(createDiagnostics.str().find(
               "failed to flush heap allocator") == std::string::npos);
    assert(dbms::index_file::syncDirectory(database));
    assert((readTableNames(tableList) ==
            std::vector<std::string>{"alpha", "remove_me", "omega",
                                     "created"}));
    expectSurvivingRows();
    assert(engine.insert(database, "created", {{"id", "44"}}) ==
           dbms::DBStatus::OK);
    assert(engine.query(database, "created", {}, {"id"}) ==
           std::vector<std::string>{"44 "});

    // DROP TABLE uses the same atomic publication path; no interrupted
    // rewrite may truncate the unrelated surviving table names.
    armPublicationFault(tableList, PublicationFault::ParentSync);
    assert(engine.dropTable(database, "remove_me") == dbms::DBStatus::OK);
    assert(publicationFault == PublicationFault::None && publicationHits == 1);
    assert(injectedFailures == 1 && durableRetries == 1);
    assert(competingThreadSyncs == 1 && publicationStage == PublicationStage::None);
    assert(dbms::index_file::directorySyncFailureCountForTesting.load() == 0);
    assert(dbms::index_file::syncDirectory(database));
    assert((readTableNames(tableList) ==
            std::vector<std::string>{"alpha", "omega", "created"}));
    assert(!engine.tableExists(database, "remove_me"));
    assert(engine.query(database, "alpha", {}, {"id"}) ==
           std::vector<std::string>{"11 "});
    assert(engine.query(database, "omega", {}, {"id"}) ==
           std::vector<std::string>{"33 "});
    assert(engine.query(database, "created", {}, {"id"}) ==
           std::vector<std::string>{"44 "});
    {
        dbms::StorageEngine reloaded;
        assert((reloaded.getTableNames(database) ==
                std::vector<std::string>{"alpha", "omega", "created"}));
        assert(!reloaded.tableExists(database, "remove_me"));
        assert(reloaded.query(database, "alpha", {}, {"id"}) ==
               std::vector<std::string>{"11 "});
        assert(reloaded.query(database, "omega", {}, {"id"}) ==
               std::vector<std::string>{"33 "});
        assert(reloaded.query(database, "created", {}, {"id"}) ==
               std::vector<std::string>{"44 "});
    }

    assert(engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[TABLE LIST ATOMICITY] passed\n";
    return 0;
}
