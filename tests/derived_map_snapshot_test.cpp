#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include <cassert>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

#ifdef DBMS_TEST_FSYNC_FAULT
static bool failSync = false;
extern "C" int __real_fsync(int);
extern "C" int __wrap_fsync(int fd) {
    if (failSync) { failSync = false; errno = EIO; return -1; }
    return __real_fsync(fd);
}
#endif

static struct stat generation(const fs::path& file) {
    struct stat value{};
    assert(::stat(file.c_str(), &value) == 0);
    return value;
}

template<class Map, class Mutate>
static void verify(const std::string& path, Mutate mutate) {
    Map map(path);
    assert(!map.quiescentForSnapshot());
    assert(map.open());
    assert(map.quiescentForSnapshot());
    mutate(map);
    assert(!map.quiescentForSnapshot());
#ifdef DBMS_TEST_FSYNC_FAULT
    failSync = true;
    assert(!map.flushChecked());
    assert(!failSync);
    // Successful pwrite is not a successful durable flush.
    assert(!map.quiescentForSnapshot());
#endif
    assert(map.flushChecked());
    assert(map.quiescentForSnapshot());
    const auto before = generation(path);
    assert(map.flushChecked());
    map.close();
    const auto after = generation(path);
    assert(before.st_ino == after.st_ino && before.st_dev == after.st_dev);
    assert(before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
           before.st_mtim.tv_nsec == after.st_mtim.tv_nsec);
    assert(before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
           before.st_ctim.tv_nsec == after.st_ctim.tv_nsec);
    assert(!map.quiescentForSnapshot());
    assert(map.open());
    assert(map.quiescentForSnapshot());
    // A raw physical edit cannot be mistaken for our cached durable preimage.
    {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        out.put('z');
    }
    assert(!map.quiescentForSnapshot());
    assert(!map.flushChecked());
    map.close();
    assert(map.open());
    assert(map.quiescentForSnapshot());
    fs::rename(path, path + ".old");
    {
        std::ofstream out(path, std::ios::binary);
        out.put('y');
    }
    assert(!map.open());
    assert(!map.flushChecked());
    assert(!map.quiescentForSnapshot());
    map.close();
    assert(map.open());
    assert(map.quiescentForSnapshot());
    map.close();
    fs::create_symlink(path, path + ".link");
    Map linked(path + ".link");
    assert(!linked.open());
    assert(!linked.quiescentForSnapshot());
}

int main() {
    verify<dbms::FreeSpaceMap>("snapshot.fsm", [](auto& map) { map.setFreePercent(1, 31); });
    verify<dbms::VisibilityMap>("snapshot.vm", [](auto& map) { map.setAllVisible(1, true); });
    std::cout << "derived map durable snapshot owner and failure checks OK\n";
}
