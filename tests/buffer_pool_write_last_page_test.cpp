// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Explicit single-page writeback must honor the installed structural ordering.
#include "storage/BufferPool.h"
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
constexpr size_t pageSize = 128;
enum class Fault { None, BarrierFalse, BarrierThrow, DataWrite, FirstSync,
                   StructuralWrite, LastSync };
struct Event { char operation; uint32_t page; bool failed; };
std::array<Event, 32> events{};
size_t eventCount = 0;
bool armed = false;
dev_t targetDevice{};
ino_t targetInode{};
uint32_t structuralPage = 0;
Fault fault = Fault::None;
size_t syncCalls = 0;

void require(bool good, const char* label) {
    if (!good) throw std::runtime_error(label);
}
bool target(int fd) {
    struct stat status{};
    return armed && ::fstat(fd, &status) == 0 &&
           status.st_dev == targetDevice && status.st_ino == targetInode;
}
void event(char operation, uint32_t page, bool failed) {
    if (eventCount == events.size()) ::_exit(85);
    events[eventCount++] = {operation, page, failed};
}
std::string trace() {
    std::string result;
    for (size_t i = 0; i < eventCount; ++i) {
        if (i) result += ',';
        result += events[i].operation;
        if (events[i].operation == 'W') result += std::to_string(events[i].page);
        if (events[i].failed) result += '!';
    }
    return result;
}
void arm(Fault selected = Fault::None) {
    eventCount = syncCalls = 0;
    fault = selected;
    armed = true;
}
std::array<char, pageSize * 4> disk(const std::string& path) {
    std::array<char, pageSize * 4> result{};
    const int fd = ::open(path.c_str(), O_RDONLY);
    require(fd >= 0, "disk open");
    const auto n = ::pread(fd, result.data(), result.size(), 0);
    require(::close(fd) == 0 && n == static_cast<ssize_t>(result.size()), "disk read");
    return result;
}
void prepare(const std::string& path) {
    std::array<char, pageSize * 4> bytes{};
    for (size_t page = 0; page < 4; ++page)
        std::memset(bytes.data() + page * pageSize, 'a' + page, pageSize);
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    require(fd >= 0, "prepare open");
    struct stat status{};
    require(::fstat(fd, &status) == 0, "prepare stat");
    targetDevice = status.st_dev;
    targetInode = status.st_ino;
    require(::pwrite(fd, bytes.data(), bytes.size(), 0) == static_cast<ssize_t>(bytes.size()), "prepare write");
    require(::fsync(fd) == 0 && ::close(fd) == 0, "prepare durable");
}
void cache(dbms::BufferPool& pool, uint32_t page, char value, bool dirty) {
    char* bytes = pool.fetchPage(page);
    require(bytes, "cache fetch");
    if (dirty) {
        std::memset(bytes, value, pageSize);
        pool.markDirty(page);
    }
    pool.unpinPage(page);
}
bool pageEquals(const std::array<char, pageSize * 4>& bytes, uint32_t page, char value) {
    for (size_t i = 0; i < pageSize; ++i)
        if (bytes[page * pageSize + i] != value) return false;
    return true;
}
}

extern "C" ssize_t pwrite(int fd, const void* bytes, size_t length, off_t offset) {
    using Original = ssize_t(*)(int, const void*, size_t, off_t);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "pwrite"));
    if (!original) ::_exit(86);
    if (target(fd)) {
        const auto page = static_cast<uint32_t>(offset / pageSize);
        const bool fail = (fault == Fault::DataWrite && page == 1) ||
                          (fault == Fault::StructuralWrite && page == structuralPage);
        event('W', page, fail);
        if (fail) { errno = EIO; return -1; }
    }
    return original(fd, bytes, length, offset);
}
extern "C" int fsync(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "fsync"));
    if (!original) ::_exit(86);
    if (target(fd)) {
        ++syncCalls;
        const bool fail = (fault == Fault::FirstSync && syncCalls == 1) ||
                          (fault == Fault::LastSync && syncCalls == 2);
        event('S', 0, fail);
        if (fail) { errno = EIO; return -1; }
    }
    return original(fd);
}

static bool ordered(const std::string& path, uint32_t structural,
                    bool initiallyDirty, Fault selected, bool structuralFirst) {
    structuralPage = structural;
    prepare(path);
    dbms::BufferPool pool(path, 3, pageSize);
    require(pool.open(), "ordered open");
    pool.setWriteLastPage(structural);
    if (structuralFirst) cache(pool, structural, 'H', initiallyDirty);
    cache(pool, 1, 'X', true);
    cache(pool, 2, 'Y', true);
    if (!structuralFirst) cache(pool, structural, 'H', initiallyDirty);
    const auto before = disk(path);
    size_t barriers = 0;
    bool barrierBeforeWrites = true;
    pool.setWritebackBarrier([&] {
        ++barriers;
        barrierBeforeWrites = barrierBeforeWrites && eventCount == 0 && disk(path) == before;
        if (selected == Fault::BarrierThrow) throw std::runtime_error("write-last-barrier");
        return selected != Fault::BarrierFalse;
    });
    arm(selected);
    bool ok = false, caught = false;
    try { ok = pool.flushPage(structural); }
    catch (const std::runtime_error& error) { caught = std::string(error.what()) == "write-last-barrier"; }
    armed = false;
    std::string expected;
    switch (selected) {
        case Fault::None: expected = "W1,W2,S,W" + std::to_string(structural) + ",S"; break;
        case Fault::BarrierFalse: case Fault::BarrierThrow: break;
        case Fault::DataWrite: expected = "W1!,W2,S"; break;
        case Fault::FirstSync: expected = "W1,W2,S!"; break;
        case Fault::StructuralWrite: expected = "W1,W2,S,W" + std::to_string(structural) + "!"; break;
        case Fault::LastSync: expected = "W1,W2,S,W" + std::to_string(structural) + ",S!"; break;
    }
    const auto after = disk(path);
    bool good = ok == (selected == Fault::None) &&
                caught == (selected == Fault::BarrierThrow) &&
                barriers == 1 && barrierBeforeWrites && trace() == expected;
    const bool failed = selected != Fault::None;
    good = good && pool.isPageDirty(1) == failed && pool.isPageDirty(2) == failed &&
           pool.isPageDirty(structural) == failed;
    if (selected == Fault::BarrierFalse || selected == Fault::BarrierThrow)
        good = good && after == before;
    if (selected == Fault::DataWrite || selected == Fault::FirstSync)
        good = good && pageEquals(after, structural, static_cast<char>('a' + structural));
    std::cout << "WRITE_LAST_CASE structural=" << structural << " dirty=" << initiallyDirty
              << " first=" << structuralFirst << " fault=" << static_cast<int>(selected)
              << " result=" << ok << " caught=" << caught << " trace=" << trace()
              << " expected=" << expected << " good=" << good << '\n';
    pool.setWritebackBarrier({});
    arm();
    const bool retry = pool.flushPage(structural);
    armed = false;
    const auto final = disk(path);
    const char structuralValue = initiallyDirty ? 'H' : static_cast<char>('a' + structural);
    // A successful first attempt leaves siblings clean, so its forced retry
    // has only the two ordering barriers and structural rewrite.
    const std::string expectedRetry = failed ?
        "W1,W2,S,W" + std::to_string(structural) + ",S" :
        "S,W" + std::to_string(structural) + ",S";
    good = good && retry && trace() == expectedRetry;
    good = good && pageEquals(final, 1, 'X') && pageEquals(final, 2, 'Y') &&
           pageEquals(final, structural, structuralValue) &&
           !pool.isPageDirty(1) && !pool.isPageDirty(2) && !pool.isPageDirty(structural);
    pool.close(false);
    dbms::BufferPool reader(path, 3, pageSize);
    require(reader.openExisting(), "reader open");
    for (const auto page : {1U, 2U, structural}) {
        char* bytes = reader.fetchPage(page);
        const char value = page == 1 ? 'X' : page == 2 ? 'Y' : structuralValue;
        good = good && bytes;
        if (bytes) for (size_t i = 0; i < pageSize; ++i) good = good && bytes[i] == value;
        if (bytes) reader.unpinPage(page);
    }
    reader.close(false);
    return good;
}

static bool ordinary(const std::string& path, bool configured, bool dirty) {
    prepare(path);
    dbms::BufferPool pool(path, 3, pageSize);
    require(pool.open(), "ordinary open");
    if (configured) pool.setWriteLastPage(0);
    cache(pool, 0, 'H', true);
    cache(pool, 1, 'X', dirty);
    const auto before = disk(path);
    size_t barriers = 0;
    pool.setWritebackBarrier([&] { ++barriers; return true; });
    arm(); const bool ok = pool.flushPage(1); armed = false;
    const auto after = disk(path);
    bool good = ok && barriers == 1 && trace() == "W1" &&
                pageEquals(after, 0, 'a') && pageEquals(after, 1, dirty ? 'X' : 'b') &&
                pool.isPageDirty(0) && !pool.isPageDirty(1);
    if (!dirty) good = good && after == before;
    arm(); const bool absent = pool.flushPage(3); armed = false;
    good = good && absent && trace().empty() && barriers == 1;
    pool.close(false);
    return good;
}

static bool isolatedStructural(const std::string& path, uint32_t structural,
                               bool dirty, bool evictedData) {
    structuralPage = structural;
    prepare(path);
    dbms::BufferPool pool(path, 2, pageSize);
    require(pool.open(), "isolated open");
    pool.setWriteLastPage(structural);
    cache(pool, structural, 'H', dirty);
    if (evictedData) {
        char* held = pool.fetchPage(structural);
        require(held, "hold structural");
        cache(pool, 1, 'X', true);
        cache(pool, 2, 'Y', false); // evicts the dirty ordinary page first
        pool.unpinPage(structural);
        require(pageEquals(disk(path), 1, 'X'), "data actually evicted");
    }
    arm(); const bool ok = pool.flushPage(structural); armed = false;
    const auto after = disk(path);
    const bool good = ok && trace() == "S,W" + std::to_string(structural) + ",S" &&
        pageEquals(after, structural, dirty ? 'H' : static_cast<char>('a' + structural)) &&
        !pool.isPageDirty(structural);
    pool.close(false);
    return good;
}

int main() {
    char pattern[] = "/tmp/dbms-write-last-page.XXXXXX";
    char* made = ::mkdtemp(pattern);
    if (!made) return 120;
    const std::filesystem::path directory(made);
    try {
        require(!dbms::PageCrypto::enabled(), "ordinary unencrypted fixture");
        size_t controls = 0, failures = 0;
        const auto check = [&](bool ok, const char* label) {
            ++controls;
            if (!ok) { ++failures; std::cerr << "WRITE_LAST_FAIL " << label << '\n'; }
        };
        const auto path = [&] { return (directory / (std::to_string(controls) + ".data")).string(); };
        for (uint32_t structural : {0U, 3U})
            for (bool dirty : {false, true})
                for (bool first : {false, true})
                    for (Fault selected : {Fault::None, Fault::BarrierFalse, Fault::BarrierThrow,
                            Fault::DataWrite, Fault::FirstSync, Fault::StructuralWrite, Fault::LastSync})
                        check(ordered(path(), structural, dirty, selected, first), "ordered structural flush/retry");
        for (bool configured : {false, true}) for (bool dirty : {false, true})
            check(ordinary(path(), configured, dirty), "ordinary forced write and absent no-op");
        for (uint32_t structural : {0U, 3U}) for (bool dirty : {false, true}) for (bool evicted : {false, true})
            check(isolatedStructural(path(), structural, dirty, evicted), "isolated structural/evicted data");
        require(controls == 68, "complete control inventory");
        std::filesystem::remove_all(directory);
        std::cout << "BUFFER_WRITE_LAST_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        armed = false;
        std::cerr << "BUFFER_WRITE_LAST_DRIVER_EXCEPTION " << error.what() << '\n';
        std::filesystem::remove_all(directory);
        return 120;
    }
}
