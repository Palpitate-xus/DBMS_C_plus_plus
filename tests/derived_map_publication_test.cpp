#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include "storage/DerivedMapPublication.h"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
static bool fallback = false;
static std::string fault;

#ifdef DBMS_TEST_PUBLICATION_FAULT
static bool partialWritten = false;
static std::string descriptorName(int fd) {
    char name[4096];
    const auto count = ::readlink(("/proc/self/fd/" + std::to_string(fd)).c_str(), name, sizeof(name));
    return count < 0 ? "" : std::string(name, static_cast<size_t>(count));
}
extern "C" int __real_fsetxattr(int, const char*, const void*, size_t, int);
extern "C" int __wrap_fsetxattr(int fd, const char* name, const void* value, size_t size, int flags) {
    if (fault == "stamp") { errno = EIO; return -1; }
    if (fallback) { errno = ENOTSUP; return -1; }
    const auto result = __real_fsetxattr(fd, name, value, size, flags);
    if (fault == "stamp-crash") _exit(93);
    return result;
}
extern "C" ssize_t __real_fgetxattr(int, const char*, void*, size_t);
extern "C" ssize_t __wrap_fgetxattr(int fd, const char* name, void* value, size_t size) {
    if (fallback) { errno = ENOTSUP; return -1; }
    return __real_fgetxattr(fd, name, value, size);
}
extern "C" int __real_fsync(int);
extern "C" int __wrap_fsync(int fd) {
    struct stat owner{};
    const auto name = descriptorName(fd);
    const bool receipt = name.find(".map_publication") != std::string::npos;
    const bool directory = ::fstat(fd, &owner) == 0 && S_ISDIR(owner.st_mode);
    if ((fault == "source-sync" && !receipt && !directory) ||
        (fault == "receipt-sync" && receipt) || (fault == "directory-sync" && directory)) {
        errno = EIO; return -1;
    }
    return __real_fsync(fd);
}
extern "C" ssize_t __real_pwrite(int, const void*, size_t, off_t);
extern "C" ssize_t __wrap_pwrite(int fd, const void* bytes, size_t size, off_t offset) {
    if (fault == "partial-data") {
        if (partialWritten) { errno = EIO; return -1; }
        partialWritten = true;
        return __real_pwrite(fd, bytes, std::min<size_t>(size, 1), offset);
    }
    return __real_pwrite(fd, bytes, size, offset);
}
extern "C" ssize_t __real_write(int, const void*, size_t);
extern "C" ssize_t __wrap_write(int fd, const void* bytes, size_t size) {
    if (fault == "receipt-write" && descriptorName(fd).find(".map_publication") != std::string::npos) {
        errno = EIO; return -1;
    }
    const auto result = __real_write(fd, bytes, size);
    if (fault == "receipt-write-crash" && descriptorName(fd).find(".map_publication") != std::string::npos)
        _exit(91);
    return result;
}
extern "C" int __real_renameat(int, const char*, int, const char*);
extern "C" int __wrap_renameat(int from, const char* source, int to, const char* target) {
    if (fault == "receipt-rename") { errno = EIO; return -1; }
    const auto result = __real_renameat(from, source, to, target);
    if (fault == "receipt-rename-crash") _exit(92);
    return result;
}
#endif

static void child(const char* executable, const std::string& path,
                  const std::string& failure = "", bool sidecar = false, int expected = 0) {
    const auto pid = ::fork();
    assert(pid >= 0);
    if (pid == 0) {
        ::execl(executable, executable, "writer", path.c_str(), failure.c_str(),
                sidecar ? "sidecar" : "xattr", nullptr);
        _exit(127);
    }
    int status = 0;
    assert(::waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == expected);
}

static void rawByte(const std::string& path, uint8_t value) {
    const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    assert(fd >= 0 && ::pwrite(fd, &value, 1, 0) == 1 && ::fsync(fd) == 0);
    assert(::close(fd) == 0);
}

static void strictRawEdit() {
    dbms::VisibilityMap map("raw.vm");
    assert(map.open());
    map.setAllVisible(1, false);
    map.setAllVisible(9, false);
    assert(map.flushChecked());
    rawByte("raw.vm", 2); // A well-shaped but unproven AllVisible bit.
    assert(!map.flushChecked() && !map.quiescentForSnapshot());
    assert(!map.refreshPublishedChecked() && !map.isAllVisible(1));
    map.setAllVisible(2, true);
    assert(!map.flushChecked()); // Do not stamp arbitrary foreign bits while dirty.
    assert(!map.refreshPublishedChecked());
    rawByte("raw.vm", 0);
    assert(map.flushChecked()); // Retry keeps only our pending bit 2.
    assert(!map.isAllVisible(1) && map.isAllVisible(2));
}

static void validPeer(const char* executable, bool sidecar) {
    fallback = sidecar;
    const std::string path = sidecar ? "peer-sidecar.vm" : "peer-xattr.vm";
    dbms::VisibilityMap map(path);
    assert(map.open());
    map.setAllVisible(1, false);
    assert(map.flushChecked());
    child(executable, path, "", sidecar);
    assert(!map.flushChecked() && !map.quiescentForSnapshot());
    assert(map.refreshPublishedChecked() && map.flushChecked() && map.isAllVisible(1));
    if (sidecar) {
        assert(fs::exists(path + dbms::derived_map_publication::suffix));
        fs::create_hard_link(path, "hardlink.vm");
        child(executable, "hardlink.vm", "", true);
        // Force a new complete publication on the alias, not the old receipt.
        dbms::VisibilityMap alias("hardlink.vm");
        assert(alias.open());
        alias.setAllVisible(2, true);
        assert(alias.flushChecked());
        // Same-process aliases share a cache, so use a fresh exec on the alias.
        child(executable, "hardlink.vm", "alias", true);
        assert(!map.flushChecked());
        assert(map.refreshPublishedChecked() && map.isAllVisible(2));
        assert(map.isAllVisible(17));
        fs::rename(path, "renamed.vm");
        assert(!map.refreshPublishedChecked()); // Old pathname is not its owner.
        fs::rename("hardlink.vm.map_publication", "renamed.vm.map_publication");
        dbms::VisibilityMap renamed("renamed.vm");
        assert(renamed.open() && renamed.isAllVisible(1) && renamed.isAllVisible(2));
    }
    fallback = false;
}

#ifdef DBMS_TEST_PUBLICATION_FAULT
static void failedProducer(const char* executable, const std::string& failure, bool sidecar) {
    fallback = sidecar;
    const auto path = failure + (sidecar ? "-sidecar.vm" : "-xattr.vm");
    dbms::VisibilityMap map(path);
    assert(map.open());
    map.setAllVisible(1, false);
    map.setAllVisible(9, false);
    assert(map.flushChecked());
    child(executable, path, failure, sidecar);
    const bool hasCandidate = failure == "source-sync" || failure == "directory-sync";
    if (hasCandidate) {
        fault = failure;
        assert(!map.refreshPublishedChecked() && !map.quiescentForSnapshot());
        fault.clear();
        // Producer failure is NOT success; the consumer must independently
        // complete durable sync and stable byte/receipt checks before success.
        assert(map.refreshPublishedChecked() && map.isAllVisible(1));
    } else {
        assert(!map.refreshPublishedChecked() && !map.isAllVisible(1));
    }
    fallback = false;
}

static void retryProducer(const std::string& failure, bool sidecar) {
    fallback = sidecar;
    const auto path = "retry-" + failure + (sidecar ? "-sidecar.fsm" : "-xattr.fsm");
    dbms::FreeSpaceMap map(path);
    assert(map.open());
    map.setFreePercent(0, 31);
    map.setFreePercent(1, 73);
    fault = failure;
    partialWritten = false;
    assert(!map.flushChecked() && !map.quiescentForSnapshot());
    fault.clear();
    assert(map.flushChecked() && map.quiescentForSnapshot());
    assert(map.getFreePercent(0) == 31 && map.getFreePercent(1) == 73);
    fallback = false;
}

static void destroyedProducer(const std::string& failure, bool sidecar) {
    fallback = sidecar;
    const auto path = "destroyed-" + failure + (sidecar ? "-sidecar.fsm" : "-xattr.fsm");
    {
        auto map = std::make_unique<dbms::FreeSpaceMap>(path);
        assert(map->open());
        map->setFreePercent(0, 31);
        map->setFreePercent(1, 73);
        fault = failure;
        partialWritten = false;
        assert(!map->flushChecked());
        // Both destructor retries also fail. No surviving map object is
        // allowed to carry the only pending state into the later retry.
        map.reset();
        fault.clear();
    }
    dbms::FreeSpaceMap retry(path);
    assert(retry.open() && !retry.quiescentForSnapshot());
    assert(retry.getFreePercent(0) == 31 && retry.getFreePercent(1) == 73);
    assert(retry.flushChecked() && retry.quiescentForSnapshot());
    fallback = false;
}

static void abruptReceipt(const char* executable, const std::string& failure, int expected) {
    fallback = failure != "stamp-crash";
    const auto path = failure + ".vm";
    dbms::VisibilityMap map(path);
    assert(map.open());
    map.setAllVisible(1, false);
    map.setAllVisible(9, false);
    assert(map.flushChecked());
    child(executable, path, failure, fallback, expected);
    if (failure == "receipt-write-crash") {
        assert(!map.refreshPublishedChecked());
        bool temporary = false;
        for (const auto& item : fs::directory_iterator("."))
            temporary |= item.path().filename().string().find(path + ".map_publication.tmp.") == 0;
        assert(temporary);
        child(executable, path, "alias", true); // Actual cooperating publication cleans only its own temporary.
    } else {
        fault = fallback ? "directory-sync" : "source-sync";
        assert(!map.refreshPublishedChecked());
        fault.clear();
    }
    assert(map.refreshPublishedChecked() && map.isAllVisible(1));
    fallback = false;
}

static void crossDirectoryFallback(const char* executable) {
    fallback = true;
    dbms::VisibilityMap map("cross.vm");
    assert(map.open());
    map.setAllVisible(1, false);
    assert(map.flushChecked());
    fs::create_directory("other-directory");
    fs::create_hard_link("cross.vm", "other-directory/cross.vm");
    child(executable, "other-directory/cross.vm", "", true);
    // Without inode-attached xattrs there is no safe discovery of arbitrary
    // other directories. Keep the old owner fail-closed instead of trusting mtime.
    assert(!map.refreshPublishedChecked() && !map.flushChecked() && !map.isAllVisible(1));
    fallback = false;
}
#endif

static void copiedOwner() {
    dbms::VisibilityMap map("copy.vm");
    assert(map.open());
    map.setAllVisible(1, true);
    assert(map.flushChecked());
    fs::copy_file("copy.vm", "replacement.vm");
    const int copied = ::open("replacement.vm", O_RDWR | O_CLOEXEC);
    assert(copied >= 0);
    // Backup/restore tools need not preserve xattrs. A copied file is a new
    // owner, and a copied old receipt cannot authorize a stale old cache.
    (void)::fremovexattr(copied, dbms::derived_map_publication::attribute);
    ::close(copied);
    fs::rename("replacement.vm", "copy.vm");
    assert(!map.refreshPublishedChecked() && !map.quiescentForSnapshot());
    dbms::VisibilityMap cold("copy.vm");
    assert(cold.open() && cold.isAllVisible(1)); // Legacy byte format is unchanged.
}

int main(int argc, char** argv) {
    if (argc == 5 && std::string(argv[1]) == "writer") {
        fallback = std::string(argv[4]) == "sidecar";
        dbms::VisibilityMap map(argv[2]);
        assert(map.open());
        map.setAllVisible(1, true);
        map.setAllVisible(9, false); // Two bytes exercise an interrupted data write.
        fault = argv[3];
        if (fault == "alias") { map.setAllVisible(17, true); fault.clear(); }
        const bool result = map.flushChecked();
        assert(result == fault.empty());
        _exit(0);
    }
    strictRawEdit();
    validPeer(argv[0], false);
#ifdef DBMS_TEST_PUBLICATION_FAULT
    validPeer(argv[0], true);
    for (const auto& failure : {"stamp", "source-sync", "partial-data"}) {
        failedProducer(argv[0], failure, false);
        retryProducer(failure, false);
        destroyedProducer(failure, false);
    }
    for (const auto& failure : {"receipt-write", "receipt-sync", "receipt-rename", "directory-sync", "source-sync"}) {
        failedProducer(argv[0], failure, true);
        retryProducer(failure, true);
        destroyedProducer(failure, true);
    }
    abruptReceipt(argv[0], "receipt-write-crash", 91);
    abruptReceipt(argv[0], "receipt-rename-crash", 92);
    abruptReceipt(argv[0], "stamp-crash", 93);
    crossDirectoryFallback(argv[0]);
    for (const auto& item : fs::directory_iterator("."))
        assert(item.path().filename().string().find(".map_publication.tmp.") == std::string::npos);
#endif
    copiedOwner();
    std::cout << "cooperative receipts require stable bytes and checked durable consumer sync\n";
}
