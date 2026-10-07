#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include <cassert>
#include <cerrno>
#include <iostream>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>

#ifdef DBMS_TEST_PWRITE_FAULT
static bool failWrite = false;
extern "C" ssize_t __real_pwrite(int, const void*, size_t, off_t);
extern "C" ssize_t __wrap_pwrite(int fd, const void* bytes, size_t size, off_t offset) {
    if (failWrite) { errno = EIO; return -1; }
    return __real_pwrite(fd, bytes, size, offset);
}
#endif

template<class Map, class Set, class Get>
static void retry(const char* name, Set set, Get get) {
    Map map(name);
    assert(map.open());
    set(map);
    assert(!map.quiescentForSnapshot());
#ifdef DBMS_TEST_PWRITE_FAULT
    failWrite = true;
    map.close();
    failWrite = false;
    assert(map.open());
    // Failed close must not forget the only pending update.
    assert(get(map));
#endif
    assert(map.flushChecked());
    map.close();
    Map cold(name);
    assert(cold.open() && get(cold));
}

#ifdef DBMS_TEST_PWRITE_FAULT
static size_t openDescriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        (void)entry; ++count;
    }
    return count;
}

template<class Map, class Set, class Get>
static void destroyedOwner(const char* name, Set set, Get get) {
    const auto before = openDescriptors();
    {
        auto map = std::make_unique<Map>(name);
        assert(map->open());
        set(*map);
        failWrite = true;
        map.reset(); // No live map object can accidentally carry the pending cell.
        failWrite = false;
    }
    assert(openDescriptors() == before + 1); // The only failed actual owner is pinned.
    {
        Map retry(name);
        assert(retry.open() && get(retry));
        assert(!retry.quiescentForSnapshot());
        assert(retry.flushChecked() && retry.quiescentForSnapshot());
        assert(openDescriptors() == before + 1); // Success released the old owner.
    }
    assert(openDescriptors() == before);
}

static void actualOwnerAndGc() {
    const auto before = openDescriptors();
    {
        dbms::FreeSpaceMap retired("failed-owner.fsm");
        assert(retired.open());
        retired.setFreePercent(1, 73);
        failWrite = true;
        retired.close();
        failWrite = false;
    }
    std::filesystem::rename("failed-owner.fsm", "failed-owner.retired");
    {
        dbms::FreeSpaceMap current("failed-owner.fsm");
        assert(current.open());
        assert(current.getFreePercent(1) == 255); // New file is not the failed old inode.
        current.setFreePercent(1, 14);
        assert(current.flushChecked());
    }
    {
        dbms::FreeSpaceMap actualOld("failed-owner.retired");
        assert(actualOld.open() && actualOld.getFreePercent(1) == 73);
        assert(actualOld.flushChecked());
    }
    assert(openDescriptors() == before);
    {
        dbms::VisibilityMap abandoned("failed-unlinked.vm");
        assert(abandoned.open());
        abandoned.setAllVisible(1, true);
        failWrite = true;
        abandoned.close();
        failWrite = false;
    }
    assert(openDescriptors() == before + 1);
    std::filesystem::remove("failed-unlinked.vm"); // Actual unlink, not retire intent.
    for (unsigned i = 0; i < 128; ++i) {
        dbms::FreeSpaceMap fresh("gc-" + std::to_string(i) + ".fsm");
        assert(fresh.open());
    }
    assert(openDescriptors() == before);
}
#endif

int main() {
    retry<dbms::FreeSpaceMap>("close-retry.fsm", [](auto& map) { map.setFreePercent(1, 73); },
                             [](auto& map) { return map.getFreePercent(1) == 73; });
    retry<dbms::VisibilityMap>("close-retry.vm", [](auto& map) { map.setAllVisible(1, true); },
                              [](auto& map) { return map.isAllVisible(1); });
#ifdef DBMS_TEST_PWRITE_FAULT
    destroyedOwner<dbms::FreeSpaceMap>("destroyed.fsm", [](auto& map) { map.setFreePercent(1, 73); },
                                      [](auto& map) { return map.getFreePercent(1) == 73; });
    destroyedOwner<dbms::VisibilityMap>("destroyed.vm", [](auto& map) { map.setAllVisible(1, true); },
                                       [](auto& map) { return map.isAllVisible(1); });
    actualOwnerAndGc();
#endif
    std::cout << "failed derived map close retains pending retry\n";
}
