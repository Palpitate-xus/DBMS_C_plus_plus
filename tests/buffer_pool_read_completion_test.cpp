// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Real file reads, with deterministic legal short transfers and errno faults.
#include "storage/BufferPool.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {
constexpr size_t pageSize = 128;
struct Step { size_t maximum; int error = 0; };
struct Scenario {
    const char* name;
    uint32_t page;
    std::vector<Step> steps;
    bool fail = false;
    bool reject = false;
    size_t expectedCalls = 0;
};
thread_local bool armed = false;
thread_local dev_t targetDevice = 0;
thread_local ino_t targetInode = 0;
thread_local const std::vector<Step>* script = nullptr;
thread_local size_t scriptIndex = 0, calls = 0, remaining = 0;
thread_local off_t expectedOffset = 0;
thread_local bool traceGood = true;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
char pattern(size_t index) { return static_cast<char>('!' + index % 89); }
bool matches(const char* data, size_t present) {
    if (!data) return false;
    for (size_t i = 0; i < pageSize; ++i)
        if (data[i] != (i < present ? pattern(i) : '\0')) return false;
    return true;
}
void prepare(const std::string& path) {
    std::vector<char> bytes(2 * pageSize + 17, 'H');
    for (size_t i = 0; i < pageSize; ++i) bytes[pageSize + i] = pattern(i);
    for (size_t i = 0; i < 17; ++i) bytes[2 * pageSize + i] = pattern(i);
    const int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
    require(fd >= 0, "fixture open");
    require(::pwrite(fd, bytes.data(), bytes.size(), 0) ==
            static_cast<ssize_t>(bytes.size()), "fixture write");
    struct stat status {};
    require(::fstat(fd, &status) == 0, "fixture identity");
    targetDevice = status.st_dev;
    targetInode = status.st_ino;
    char warm;
    require(::pread(fd, &warm, 1, 0) == 1, "resolve real pread");
    require(::close(fd) == 0, "fixture close");
}
void arm(const Scenario& scenario) {
    script = &scenario.steps;
    scriptIndex = calls = 0;
    remaining = pageSize;
    expectedOffset = static_cast<off_t>(scenario.page) * pageSize;
    traceGood = true;
    armed = true;
}
}

extern "C" ssize_t pread(int fd, void* data, size_t size, off_t offset) {
    using Original = ssize_t(*)(int, void*, size_t, off_t);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pread"));
    if (!original) ::_exit(86);
    struct stat status {};
    if (!armed || ::fstat(fd, &status) != 0 ||
        status.st_dev != targetDevice || status.st_ino != targetInode)
        return original(fd, data, size, offset);
    ++calls;
    traceGood = traceGood && offset == expectedOffset && size == remaining;
    const Step step = scriptIndex < script->size()
        ? (*script)[scriptIndex++] : Step{size, 0};
    if (step.error) {
        errno = step.error;
        return -1;
    }
    // Only cap a real transfer: EOF and all returned bytes come from libc.
    const auto result = original(fd, data, std::min(size, step.maximum), offset);
    if (result > 0) {
        expectedOffset += result;
        remaining -= static_cast<size_t>(result);
    }
    return result;
}

static bool runCase(const Scenario& scenario) {
    const std::string path = std::string(scenario.name) + ".data";
    prepare(path);
    dbms::BufferPool pool(path, 1, pageSize);
    require(pool.open(), "pool open");
    size_t validations = 0;
    bool validatorBytes = true;
    pool.setPageValidator([&](uint32_t page, const char* bytes) {
        ++validations;
        validatorBytes = validatorBytes && page == 1 && matches(bytes, pageSize);
        return !scenario.reject;
    });
    arm(scenario);
    char* page = pool.fetchPage(scenario.page);
    armed = false;
    const size_t firstCalls = calls;
    const auto statistics = pool.statistics();
    const bool failed = scenario.fail || scenario.reject;
    const size_t present = scenario.page == 1 ? pageSize : scenario.page == 2 ? 17 : 0;
    const bool valueGood = failed ? page == nullptr : matches(page, present);
    const bool validationGood = validations == (scenario.page == 1 && !scenario.fail ? 1 : 0)
        && validatorBytes;
    const bool ioGood = traceGood && (!scenario.expectedCalls || firstCalls == scenario.expectedCalls);
    const bool accountingGood = statistics.hits == 0 && statistics.misses == 1;
    bool cacheGood = true;
    if (page) {
        arm(scenario);
        char* cached = pool.fetchPage(scenario.page);
        armed = false;
        cacheGood = cached == page && calls == 0 && pool.statistics().hits == 1;
        if (cached) pool.unpinPage(scenario.page);
        pool.unpinPage(scenario.page);
    }
    const bool cleanupGood = !failed || pool.getFrameInfo().empty();
    // A retry after a hard error or validator rejection must use a free frame.
    pool.setPageValidator({});
    pool.invalidateAll();
    char* retry = pool.fetchPage(scenario.page);
    const bool retryGood = matches(retry, present);
    if (retry) pool.unpinPage(scenario.page);
    bool pinsGood = true;
    for (const auto& frame : pool.getFrameInfo()) pinsGood = pinsGood && frame.pinCount == 0;
    pool.close();
    require(pool.open(), "pool reopen");
    char* reopened = pool.fetchPage(1);
    const bool reopenGood = matches(reopened, pageSize);
    if (reopened) pool.unpinPage(1);
    const bool good = valueGood && validationGood && ioGood && accountingGood &&
        cacheGood && cleanupGood && retryGood && pinsGood && reopenGood;
    std::cout << "BUFFER_READ_COMPLETION_CASE " << scenario.name << " good=" << good
        << " reads=" << firstCalls << " validations=" << validations
        << " value=" << valueGood << " IO=" << ioGood << " cleanup=" << cleanupGood << '\n';
    return good;
}

int main() {
    try {
        const std::vector<Scenario> cases = {
            {"full", 1, {}, false, false, 1},
            {"eintr-first", 1, {{0, EINTR}}, false, false, 2},
            {"eintr-repeated", 1, {{0, EINTR}, {0, EINTR}, {0, EINTR}}, false, false, 4},
            {"short-first", 1, {{11}}, false, false, 2},
            {"short-fragments", 1, {{1}, {2}, {7}, {13}}, false, false, 5},
            {"short-eintr", 1, {{11}, {0, EINTR}}, false, false, 3},
            {"eintr-short-eintr", 1, {{0, EINTR}, {11}, {0, EINTR}}, false, false, 4},
            {"one-byte", 1, std::vector<Step>(pageSize, Step{1}), false, false, pageSize},
            {"validator-reject", 1, {}, false, true, 1},
            {"short-validator-reject", 1, {{11}}, false, true, 2},
            {"hard-error", 1, {{0, EIO}}, true, false, 1},
            {"partial-hard-error", 1, {{11}, {0, EIO}}, true, false, 2},
            {"eintr-hard-error", 1, {{0, EINTR}, {0, EIO}}, true, false, 2},
            {"partial-eintr-hard-error", 1, {{11}, {0, EINTR}, {0, EIO}}, true, false, 3},
            {"partial-eof", 2, {}},
            {"fragmented-eof", 2, {{3}, {4}}, false, false, 4},
            {"fragmented-eintr-eof", 2, {{3}, {0, EINTR}, {4}}, false, false, 5},
            {"empty-eof", 3, {}, false, false, 1},
            {"eintr-empty-eof", 3, {{0, EINTR}}, false, false, 2},
        };
        size_t failures = 0;
        for (const auto& scenario : cases) if (!runCase(scenario)) ++failures;
        std::cout << "BUFFER_READ_COMPLETION_NATIVE_COMPLETE " << cases.size()
                  << " FAILURES " << failures << '\n';
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        armed = false;
        std::cerr << "BUFFER_READ_COMPLETION_DRIVER_EXCEPTION " << error.what() << '\n';
        return 120;
    }
}
