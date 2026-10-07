#include "commands/TableManage.h"
#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"
#include "storage/DerivedMapPublication.h"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string db = "__t_derived_map_clean_peer";
static bool fallback = false;
#ifdef DBMS_TEST_PUBLICATION_FALLBACK
extern "C" int __real_fsetxattr(int, const char*, const void*, size_t, int);
extern "C" int __wrap_fsetxattr(int fd, const char* name, const void* bytes, size_t size, int flags) {
    if (fallback && std::string(name) == derived_map_publication::attribute) { errno = ENOTSUP; return -1; }
    return __real_fsetxattr(fd, name, bytes, size, flags);
}
extern "C" ssize_t __real_fgetxattr(int, const char*, void*, size_t);
extern "C" ssize_t __wrap_fgetxattr(int fd, const char* name, void* bytes, size_t size) {
    if (fallback && std::string(name) == derived_map_publication::attribute) { errno = ENOTSUP; return -1; }
    return __real_fgetxattr(fd, name, bytes, size);
}
#endif

static std::filesystem::path mapFile(const std::string& name) {
    std::filesystem::path found;
    for (const auto& item : std::filesystem::recursive_directory_iterator(".")) {
        if (item.path().filename() == name && item.path().parent_path().filename() == db) {
            assert(found.empty());
            found = item.path();
        }
    }
    assert(!found.empty());
    return found;
}

int main(int argc, char** argv) {
    fallback = argc >= 2 && std::string(argv[argc - 1]) == "sidecar";
    StorageEngine engine;
    engine.setBackgroundIntervals(1000000, 1000000);
    if (argc >= 2 && std::string(argv[1]) == "writer") {
        const auto table = engine.getTableSchema(db, "items");
        auto* allocator = engine.getPageAllocator(db, "items");
        assert(allocator);
        auto* bytes = allocator->fetchPage(1);
        assert(bytes);
        PageWrapper page(bytes, allocator->pageSize(), table.formatVersion);
        const auto actual = static_cast<uint8_t>(page.freeSpace() * 100 / allocator->pageSize());
        allocator->unpinPage(1);
        engine.getFSM(db, "items")->setFreePercent(1, actual);
        engine.getVM(db, "items")->setAllVisible(1, false);
        assert(engine.getFSM(db, "items")->flushChecked());
        assert(engine.getVM(db, "items")->flushChecked());
        return 0;
    }
    assert(engine.createDatabase(db) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 2, false));
    assert(engine.createTable(db, table) == DBStatus::OK);
    assert(engine.beginTransaction(db) == DBStatus::OK);
    assert(engine.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(engine.commitTransaction() == DBStatus::OK);
    // A conservative free-space underestimate and all-visible committed page
    // are legitimate old derived state; the child publishes actual page data.
    engine.getFSM(db, "items")->setFreePercent(1, 0);
    engine.getVM(db, "items")->setAllVisible(1, true);
    assert(engine.getFSM(db, "items")->flushChecked());
    assert(engine.getVM(db, "items")->flushChecked());
    const auto child = ::fork();
    assert(child >= 0);
    if (child == 0) { ::execl(argv[0], argv[0], "writer", fallback ? "sidecar" : "xattr", nullptr); _exit(127); }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    // No parent map read occurs between the valid peer publication and backup.
    const auto backup = engine.physicalBackup(db, "snapshot-after-peer");
    std::cout << "backup_after_valid_actual_page_peer=" << backup << std::endl;
    assert(backup);
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);

    const auto vm = mapFile("items.vm");
    const int fd = ::open(vm.c_str(), O_RDWR | O_CLOEXEC);
    assert(fd >= 0);
    uint8_t original = 0;
    assert(::pread(fd, &original, 1, 0) == 1);
    const uint8_t raw = static_cast<uint8_t>(original | 2);
    assert(raw != original && ::pwrite(fd, &raw, 1, 0) == 1 && ::fsync(fd) == 0);
    assert(!engine.physicalBackup(db, "raw-map-must-not-backup"));
    assert(!engine.getVM(db, "items")->isAllVisible(1));
    assert(::pwrite(fd, &original, 1, 0) == 1 && ::fsync(fd) == 0 && ::close(fd) == 0);
    assert(engine.physicalBackup(db, "raw-restored-exact-map"));
    assert(engine.insert(db, "items", {{"id", "2"}}) == DBStatus::OK);
    assert(engine.physicalRestore(db, "snapshot-after-peer"));
    assert(engine.query(db, "items", {}, {"id"}).size() == 1);
    assert(engine.alterTableRenameTable(db, "items", "renamed") == DBStatus::OK);
    assert(engine.getFSM(db, "renamed") && engine.getVM(db, "renamed"));
    assert(engine.physicalBackup(db, "after-rename"));
    if (fallback) {
        // Interrupted temporary receipt belongs to this exact relation too.
        const auto target = mapFile("renamed.vm").string() + ".map_publication.tmp.123.1";
        const int temporary = ::open(target.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        assert(temporary >= 0 && ::close(temporary) == 0);
        assert(engine.dropTable(db, "renamed") == DBStatus::OK);
        assert(!std::filesystem::exists(target));
        for (const auto& item : std::filesystem::recursive_directory_iterator(".")) {
            if (item.path().parent_path().filename() == db)
                assert(item.path().filename().string().find("renamed.vm.map_publication") != 0);
        }
    }
}
