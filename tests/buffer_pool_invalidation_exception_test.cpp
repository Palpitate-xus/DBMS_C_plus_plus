// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Exhaust the allocations actually observed during pinned-page invalidation.
#include "storage/BufferPool.h"

#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <new>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {
thread_local bool traceAllocations = false;
thread_local bool traceReads = false;
thread_local size_t allocationCount = 0, failOrdinal = 0, readCount = 0;
size_t serial = 0;

void require(bool good, const char* message) {
    if (!good) throw std::runtime_error(message);
}

struct Configuration {
    size_t pages;
    int pins;
    bool dirty;
    bool mixed;
    bool all;
};

char diskValue(size_t page) { return static_cast<char>('A' + page); }
char memoryValue(size_t page, bool dirty) {
    return dirty ? static_cast<char>('a' + page) : diskValue(page);
}

void prepare(const std::string& path, size_t pages) {
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    require(fd >= 0, "prepare open");
    char bytes[128];
    for (size_t page = 0; page <= pages; ++page) {
        std::memset(bytes, diskValue(page), sizeof(bytes));
        require(::pwrite(fd, bytes, sizeof(bytes), static_cast<off_t>(page * sizeof(bytes))) ==
                    static_cast<ssize_t>(sizeof(bytes)), "prepare write");
    }
    require(::pread(fd, bytes, 1, 0) == 1 && ::close(fd) == 0, "prepare warm IO");
}

bool sameFrames(const std::vector<dbms::BufferPool::FrameInfo>& before,
                const std::vector<dbms::BufferPool::FrameInfo>& after) {
    if (before.size() != after.size()) return false;
    for (size_t i = 0; i < before.size(); ++i) {
        const auto& a = before[i];
        const auto& b = after[i];
        if (a.pageId != b.pageId || a.dirty != b.dirty || a.pinCount != b.pinCount ||
            a.usageCount != b.usageCount || a.valid != b.valid) return false;
    }
    return true;
}

bool persisted(const std::string& path, size_t pages, bool dirty) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    require(fd >= 0, "inspect persisted file");
    bool good = true;
    for (size_t page = 1; page <= pages; ++page) {
        char value = 0;
        good = (::pread(fd, &value, 1, static_cast<off_t>(page * 128)) == 1 &&
                value == memoryValue(page, dirty)) && good;
    }
    require(::close(fd) == 0, "inspect close");
    return good;
}
}

void* operator new(std::size_t size) {
    if (traceAllocations && ++allocationCount == failOrdinal) throw std::bad_alloc();
    if (void* value = std::malloc(size ? size : 1)) return value;
    throw std::bad_alloc();
}
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete[](void* value) noexcept { std::free(value); }
void operator delete[](void* value, std::size_t) noexcept { std::free(value); }

extern "C" ssize_t pread(int fd, void* bytes, size_t size, off_t offset) {
    using Original = ssize_t(*)(int, void*, size_t, off_t);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pread"));
    if (!original) ::_exit(86);
    if (traceReads) ++readCount;
    return original(fd, bytes, size, offset);
}

static std::pair<size_t, bool> exercise(Configuration config, size_t ordinal) {
    const std::string path = "invalidation-exception-" + std::to_string(++serial) + ".data";
    prepare(path, config.pages);
    dbms::BufferPool pool(path, config.pages, 128);
    require(pool.open(), "exercise open");
    std::vector<char*> pointers(config.pages + 1);
    std::vector<int> pinCounts(config.pages + 1);
    for (size_t page = 1; page <= config.pages; ++page) {
        char* value = pool.fetchPage(static_cast<uint32_t>(page));
        require(value && value[0] == diskValue(page), "seed cached page");
        pointers[page] = value;
        value[0] = memoryValue(page, config.dirty);
        if (config.dirty) pool.markDirty(static_cast<uint32_t>(page));
        const int pins = config.mixed && page % 2 == 0 ? 0 : config.pins;
        pinCounts[page] = pins;
        if (pins == 0) pool.unpinPage(static_cast<uint32_t>(page));
        for (int i = 1; i < pins; ++i) {
            require(pool.fetchPage(static_cast<uint32_t>(page)) == value, "repeat pin");
        }
    }
    const auto before = pool.getFrameInfo();
    pool.resetStats();
    allocationCount = readCount = 0;
    failOrdinal = ordinal;
    traceReads = traceAllocations = true;
    bool caught = false;
    try {
        if (config.all) pool.invalidateAll();
        else pool.invalidatePage(1);
    } catch (const std::bad_alloc&) {
        caught = true;
    }
    traceAllocations = false;
    const size_t allocations = allocationCount;
    bool good = readCount == 0;
    if (ordinal) {
        good = caught && allocations == ordinal && sameFrames(before, pool.getFrameInfo()) && good;
        // Exercise mappings as well as frame metadata; unpin must still route
        // to the original owner, and no new read may replace dirty bytes.
        for (size_t page = 1; page <= config.pages; ++page) {
            char* current = pool.fetchPage(static_cast<uint32_t>(page));
            good = (current && current == pointers[page] &&
                    current[0] == memoryValue(page, config.dirty)) && good;
            if (current) pool.unpinPage(static_cast<uint32_t>(page));
        }
        const auto counters = pool.statistics();
        good = (readCount == 0 && counters.hits == config.pages && counters.misses == 0) && good;
    } else {
        good = !caught && allocations > 0 && allocations <= 32 && good;
        for (size_t page = 1; page <= config.pages; ++page) {
            if (pinCounts[page]) good = (pointers[page][0] == memoryValue(page, config.dirty)) && good;
        }
    }
    traceReads = false;
    for (size_t page = 1; page <= config.pages; ++page) {
        for (int i = 0; i < pinCounts[page]; ++i) pool.unpinPage(static_cast<uint32_t>(page));
    }
    const bool flushed = pool.flush();
    const bool idle = pool.quiescentForSnapshot();
    const bool disk = !ordinal || persisted(path, config.pages, config.dirty);
    good = flushed && idle && disk && good;
    // A failed invalidation must not strand a frame even after every caller
    // has released its original pins.
    char* other = pool.fetchPage(static_cast<uint32_t>(config.pages + 1));
    good = (other != nullptr) && good;
    if (other) pool.unpinPage(static_cast<uint32_t>(config.pages + 1));
    std::cout << "BUFFER_INVALIDATION_CASE " << config.pages << ' ' << config.pins << ' '
              << config.dirty << ' ' << config.mixed << ' ' << config.all << " ordinal=" << ordinal
              << " allocations=" << allocations << " caught=" << caught << " idle=" << idle
              << " disk=" << disk << " good=" << good << '\n';
    return {allocations, good};
}

static bool noPins(bool all, bool dirty) {
    const std::string path = "invalidation-no-pins-" + std::to_string(++serial) + ".data";
    prepare(path, 2);
    dbms::BufferPool pool(path, 2, 128);
    require(pool.open(), "no pins open");
    for (uint32_t page : {1U, 2U}) {
        char* value = pool.fetchPage(page);
        require(value, "no pins fetch");
        if (dirty) { value[0] = 'd'; pool.markDirty(page); }
        pool.unpinPage(page);
    }
    allocationCount = 0;
    failOrdinal = 1;
    traceAllocations = true;
    if (all) pool.invalidateAll();
    else pool.invalidatePage(1);
    traceAllocations = false;
    const bool good = allocationCount == 0 && !pool.isPageDirty(1) && pool.flush() && pool.quiescentForSnapshot();
    return good;
}

static bool absentPage(bool opened) {
    dbms::BufferPool pool("invalidation-absent-" + std::to_string(++serial) + ".data", 1, 128);
    if (opened) require(pool.open(), "absent open");
    allocationCount = 0;
    failOrdinal = 1;
    traceAllocations = true;
    pool.invalidatePage(17);
    traceAllocations = false;
    return allocationCount == 0 && pool.getFrameInfo().empty();
}

int main() {
    try {
        size_t controls = 0, failures = 0, faultControls = 0;
        const auto check = [&](bool good, const char* message) {
            ++controls;
            if (!good) { ++failures; std::cerr << "BUFFER_INVALIDATION_EXCEPTION_FAIL " << message << '\n'; }
        };
        for (size_t pages : {1U, 2U, 4U}) for (int pins : {1, 3}) for (bool dirty : {false, true})
            for (bool mixed : {false, true}) for (bool all : {false, true}) {
                const Configuration config{pages, pins, dirty, mixed, all};
                const auto trace = exercise(config, 0);
                check(trace.second, "successful invalidation allocation trace");
                require(trace.first > 0 && trace.first <= 32, "complete allocation trace bound");
                for (size_t ordinal = 1; ordinal <= trace.first; ++ordinal) {
                    ++faultControls;
                    check(exercise(config, ordinal).second, "allocation failure leaves cache usable");
                }
            }
        for (bool all : {false, true}) for (bool dirty : {false, true}) check(noPins(all, dirty), "unpinned invalidation needs no allocation");
        for (bool opened : {false, true}) check(absentPage(opened), "absent page needs no allocation");
        require(controls == 54 + faultControls, "complete invalidation inventory");
        std::cout << "BUFFER_INVALIDATION_EXCEPTION_NATIVE_COMPLETE " << controls << " FAILURES " << failures
                  << " FAULTS " << faultControls << '\n';
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        traceAllocations = traceReads = false;
        std::cerr << "BUFFER_INVALIDATION_EXCEPTION_DRIVER_ERROR " << error.what() << '\n';
        return 120;
    }
}
