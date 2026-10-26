// test_sources: src/storage/LargeObject.cpp
// Bound allocation-failure injection to allocations while our object fd is live.
#include "storage/LargeObject.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
enum class Flow { Read, Construct, CreateCollision, Export };
constexpr size_t noFailure = std::numeric_limits<size_t>::max();
thread_local bool faultThread = false;
struct FaultState {
    bool armed = false, closeError = false;
    char path[1024] {};
    dev_t device = 0;
    ino_t inode = 0;
    int liveFd = -1, watchedFd = -1;
    size_t selected = noFailure, allocations = 0, thrown = 0, closes = 0, closeErrors = 0;
    std::array<size_t, 256> sizes {};
} fault;

void require(bool good, const char* label) {
    if (!good) throw std::runtime_error(label);
}
bool isTarget(int fd) {
    struct stat status {};
    return ::fstat(fd, &status) == 0 && status.st_dev == fault.device && status.st_ino == fault.inode;
}
void beforeAllocation(size_t bytes) {
    if (!faultThread || !fault.armed || fault.liveFd < 0) return;
    const int saved = errno;
    const bool relevant = isTarget(fault.liveFd);
    errno = saved;
    if (!relevant) return;
    if (fault.allocations == fault.sizes.size()) ::_exit(86);
    const size_t ordinal = fault.allocations++;
    fault.sizes[ordinal] = bytes;
    if (ordinal == fault.selected) {
        ++fault.thrown;
        throw std::bad_alloc();
    }
}
[[gnu::noinline]] void* allocate(size_t bytes, size_t alignment = 0) {
    beforeAllocation(bytes);
    void* result = nullptr;
    if (alignment) {
        if (::posix_memalign(&result, alignment, std::max<size_t>(bytes, 1)) != 0) result = nullptr;
    } else {
        result = std::malloc(std::max<size_t>(bytes, 1));
    }
    if (!result) throw std::bad_alloc();
    return result;
}
[[gnu::noinline]] void deallocate(void* value) noexcept { std::free(value); }
int trackOpen(const char* path, int fd) {
    if (faultThread && fault.armed && fd >= 0) {
        if (std::strcmp(path, fault.path) == 0 && isTarget(fd)) {
            fault.liveFd = fault.watchedFd = fd;
        } else if (fd == fault.watchedFd) {
            fault.watchedFd = -1;
        }
    }
    return fd;
}
} // namespace

void* operator new(size_t bytes) { return allocate(bytes); }
void* operator new[](size_t bytes) { return allocate(bytes); }
void* operator new(size_t bytes, const std::nothrow_t&) noexcept { try { return allocate(bytes); } catch (...) { return nullptr; } }
void* operator new[](size_t bytes, const std::nothrow_t&) noexcept { try { return allocate(bytes); } catch (...) { return nullptr; } }
void* operator new(size_t bytes, std::align_val_t align) { return allocate(bytes, static_cast<size_t>(align)); }
void* operator new[](size_t bytes, std::align_val_t align) { return allocate(bytes, static_cast<size_t>(align)); }
void* operator new(size_t bytes, std::align_val_t align, const std::nothrow_t&) noexcept { try { return allocate(bytes, static_cast<size_t>(align)); } catch (...) { return nullptr; } }
void* operator new[](size_t bytes, std::align_val_t align, const std::nothrow_t&) noexcept { try { return allocate(bytes, static_cast<size_t>(align)); } catch (...) { return nullptr; } }
void operator delete(void* value) noexcept { deallocate(value); }
void operator delete[](void* value) noexcept { deallocate(value); }
void operator delete(void* value, size_t) noexcept { deallocate(value); }
void operator delete[](void* value, size_t) noexcept { deallocate(value); }
void operator delete(void* value, const std::nothrow_t&) noexcept { deallocate(value); }
void operator delete[](void* value, const std::nothrow_t&) noexcept { deallocate(value); }
void operator delete(void* value, std::align_val_t) noexcept { deallocate(value); }
void operator delete[](void* value, std::align_val_t) noexcept { deallocate(value); }
void operator delete(void* value, size_t, std::align_val_t) noexcept { deallocate(value); }
void operator delete[](void* value, size_t, std::align_val_t) noexcept { deallocate(value); }
void operator delete(void* value, std::align_val_t, const std::nothrow_t&) noexcept { deallocate(value); }
void operator delete[](void* value, std::align_val_t, const std::nothrow_t&) noexcept { deallocate(value); }

extern "C" int open(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open"));
    if (!original) ::_exit(87);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); permissions = va_arg(args, mode_t); va_end(args);
    }
    return trackOpen(path, original(path, flags, permissions));
}

extern "C" int open64(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open64"));
    if (!original) ::_exit(87);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); permissions = va_arg(args, mode_t); va_end(args);
    }
    return trackOpen(path, original(path, flags, permissions));
}

extern "C" int __open_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open_2"));
    if (!original) ::_exit(87);
    return trackOpen(path, original(path, flags));
}

extern "C" int __open64_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open64_2"));
    if (!original) ::_exit(87);
    return trackOpen(path, original(path, flags));
}

extern "C" int close(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "close"));
    if (!original) ::_exit(87);
    const bool watched = faultThread && fault.armed && fd == fault.watchedFd;
    if (watched) ++fault.closes;
    const int result = original(fd);
    if (faultThread && fault.armed && fd == fault.liveFd) fault.liveFd = -1;
    if (watched && fault.closeError && result == 0) {
        ++fault.closeErrors;
        errno = EIO;
        return -1;
    }
    return result;
}

namespace {
size_t descriptors() {
    size_t result = 0;
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        const std::string name = entry.path().filename().string();
        char* end = nullptr;
        const long fd = std::strtol(name.c_str(), &end, 10);
        if (end && !*end && fd >= 0 && isTarget(static_cast<int>(fd))) ++result;
    }
    return result;
}
std::string snapshot(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.good(), "snapshot open");
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    require(!in.bad(), "snapshot read");
    return data;
}
void put(const fs::path& path, const std::string& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    out.close();
    require(out.good(), "fixture write");
}
struct PrivateDirectory {
    fs::path path;
    PrivateDirectory() {
        char pattern[] = "/tmp/dbms-lo-fd-exception.XXXXXX";
        const char* created = ::mkdtemp(pattern);
        require(created, "mkdtemp");
        path = created;
    }
    ~PrivateDirectory() {
        fault.armed = false;
        faultThread = false;
        // Leak observations are made before this fixture-only cleanup. Never
        // let an old implementation leak private descriptors into later cases.
        if (fault.liveFd >= 0 && isTarget(fault.liveFd)) (void)::close(fault.liveFd);
        std::error_code error;
        fs::remove_all(path, error);
    }
};
struct Seed {
    PrivateDirectory directory;
    fs::path db, object, destination;
    std::string data;
    std::unique_ptr<dbms::LargeObjectManager> observer;
    Seed(size_t bytes, Flow flow) : db(directory.path / "db"), object(db / ".lobjects/lo_1.dat"),
        destination(directory.path / "exported"), data(bytes, '\0') {
        for (size_t i = 0; i < bytes; ++i) data[i] = static_cast<char>(i * 37 + 19);
        if (flow == Flow::CreateCollision) observer = std::make_unique<dbms::LargeObjectManager>(db.string());
        {
            dbms::LargeObjectManager writer(db.string());
            require(writer.create() == 1 && writer.write(1, 0, data), "seed create/write");
        }
        if (flow != Flow::CreateCollision) observer = std::make_unique<dbms::LargeObjectManager>(db.string());
        put(destination, "old export");
        fault = FaultState{};
        struct stat status {};
        require(::stat(object.c_str(), &status) == 0, "seed stat");
        fault.device = status.st_dev; fault.inode = status.st_ino;
        const std::string name = object.string();
        require(name.size() < sizeof(fault.path), "fixture path too long");
        std::memcpy(fault.path, name.c_str(), name.size() + 1);
        require(descriptors() == 0, "seed descriptors");
    }
    void healthyRetry(Flow flow, int expectedNext = 0) {
        require(observer->read(1) == data && observer->size(1) == data.size(), "healthy read/size retry");
        dbms::LargeObjectManager fresh(db.string());
        require(fresh.read(1) == data, "fresh read retry");
        if (flow == Flow::CreateCollision) {
            const int id = observer->create();
            require(id == expectedNext && observer->write(id, 0, "retry"), "create retry");
            require(observer->read(1) == data, "create retry overwrote peer");
        }
        require(observer->exportFile(1, destination.string()) && snapshot(destination) == data, "healthy export retry");
        require(descriptors() == 0, "retry descriptors");
    }
};

struct Outcome { bool success = false, caughtBadAlloc = false; size_t allocations = 0; };
Outcome invoke(Seed& seed, Flow flow, size_t selected, bool closeError) {
    fault.selected = selected; fault.closeError = closeError;
    fault.allocations = fault.thrown = fault.closes = fault.closeErrors = 0;
    fault.liveFd = fault.watchedFd = -1;
    fault.sizes.fill(0);
    Outcome result;
    faultThread = true;
    fault.armed = true;
    try {
        switch (flow) {
            case Flow::Read: result.success = seed.observer->read(1) == seed.data; break;
            case Flow::Construct: {
                dbms::LargeObjectManager constructed(seed.db.string());
                // Do not allocate or open a second object fd while armed.
                result.success = true;
                break;
            }
            case Flow::CreateCollision: result.success = seed.observer->create() == 2; break;
            case Flow::Export: result.success = seed.observer->exportFile(1, seed.destination.string()); break;
        }
    } catch (const std::bad_alloc&) {
        result.caughtBadAlloc = true;
    } catch (...) {
        fault.armed = false;
        faultThread = false;
        throw;
    }
    fault.armed = false;
    faultThread = false;
    result.allocations = fault.allocations;
    return result;
}

struct Discovery { size_t count = 0; std::array<size_t, 256> sizes {}; };
bool caseRun(size_t bytes, Flow flow, size_t selected, bool closeError,
             Discovery* discovery = nullptr, size_t expectedSize = 0) {
    Seed seed(bytes, flow);
    const Outcome result = invoke(seed, flow, selected, closeError);
    const size_t leaked = descriptors();
    if (discovery) {
        require(selected == noFailure && !closeError && (result.allocations > 0 || flow == Flow::CreateCollision), "allocation discovery");
        discovery->count = result.allocations;
        discovery->sizes = fault.sizes;
        std::cout << "ALLOCATION_SITES bytes=" << bytes << " flow=" << static_cast<int>(flow) << " count=" << discovery->count;
        for (size_t i = 0; i < discovery->count; ++i) std::cout << ' ' << discovery->sizes[i];
        std::cout << '\n';
    }
    bool good = leaked == 0 && fault.closes == 1 && fault.closeErrors == (closeError ? 1u : 0u);
    const bool allocationFaultExpected = selected != noFailure && expectedSize != 0;
    if (allocationFaultExpected) {
        require(fault.thrown == 1 && selected < result.allocations && fault.sizes[selected] == expectedSize, "exact allocation fault not reached");
        good = good && !result.success && (result.caughtBadAlloc || flow == Flow::Export);
        if (flow == Flow::CreateCollision) require(!fs::exists(seed.db / ".lobjects/lo_2.dat"), "failed collision created unexpected object");
    } else {
        if (selected != noFailure) {
            // A prepared cache node can eliminate this former fd-live site.
            // Keep its controls, but never pretend an allocation was failed.
            require(flow == Flow::CreateCollision && result.allocations == 0, "unexpected missing allocation site");
        }
        good = good && fault.thrown == 0 && !result.caughtBadAlloc &&
            result.success == !(closeError && flow == Flow::Export);
        if (flow == Flow::Export) require(snapshot(seed.destination) == seed.data, "normal export bytes");
    }
    require(snapshot(seed.object) == seed.data, "source changed on allocation/close failure");
    if (!good) std::cerr << "FD_OWNERSHIP_FAILURE bytes=" << bytes << " flow=" << static_cast<int>(flow)
                         << " ordinal=" << selected << " closeError=" << closeError << " leaked=" << leaked
                         << " closes=" << fault.closes << " caught=" << result.caughtBadAlloc << '\n';
    // This is fixture cleanup AFTER observing a failure, not product recovery.
    if (fault.liveFd >= 0 && isTarget(fault.liveFd)) require(::close(fault.liveFd) == 0, "cleanup observed private leak");
    fault.liveFd = fault.watchedFd = -1;
    require(descriptors() == 0, "private leak cleanup");
    seed.healthyRetry(flow, allocationFaultExpected ? 2 : 3);
    return good;
}

bool repeatedRead(size_t bytes) {
    Seed seed(bytes, Flow::Read);
    std::array<int, 32> leakedFds {};
    size_t leakCount = 0;
    bool good = true;
    for (size_t i = 0; i < leakedFds.size(); ++i) {
        const Outcome result = invoke(seed, Flow::Read, 0, false);
        require(fault.thrown == 1 && result.caughtBadAlloc && !result.success, "repeat read allocation fault");
        const size_t count = descriptors();
        good = good && count == 0 && fault.closes == 1;
        if (fault.liveFd >= 0 && isTarget(fault.liveFd)) leakedFds[leakCount++] = fault.liveFd;
    }
    const size_t leaked = descriptors();
    if (!good) std::cerr << "REPEATED_FD_LEAK bytes=" << bytes << " attempts=32 leaked=" << leaked << '\n';
    for (size_t i = 0; i < leakCount; ++i) {
        require(isTarget(leakedFds[i]) && ::close(leakedFds[i]) == 0, "cleanup repeated private leaks");
    }
    fault.liveFd = fault.watchedFd = -1;
    require(descriptors() == 0 && snapshot(seed.object) == seed.data, "repeat source/cleanup");
    seed.healthyRetry(Flow::Read);
    return good;
}
} // namespace

int main() {
    int controls = 0, failures = 0;
    size_t allocationSites = 0;
    for (size_t bytes : {size_t(257), size_t(4099), size_t(131101)}) {
        for (Flow flow : {Flow::Read, Flow::Construct, Flow::CreateCollision, Flow::Export}) {
            Discovery discovery;
            ++controls;
            try { if (!caseRun(bytes, flow, noFailure, false, &discovery)) ++failures; }
            catch (const std::exception& error) {
                fault.armed = false; ++failures;
                std::cerr << "CONTROL_ERROR discovery bytes=" << bytes << " flow=" << static_cast<int>(flow) << ' ' << error.what() << '\n';
                continue;
            }
            allocationSites += discovery.count;
            for (size_t ordinal = 0; ordinal < std::max<size_t>(discovery.count, 1); ++ordinal) {
                for (bool closeError : {false, true}) {
                    ++controls;
                    try { if (!caseRun(bytes, flow, ordinal, closeError, nullptr, discovery.sizes[ordinal])) ++failures; }
                    catch (const std::exception& error) {
                        fault.armed = false; ++failures;
                        std::cerr << "CONTROL_ERROR fault bytes=" << bytes << " flow=" << static_cast<int>(flow) << " ordinal=" << ordinal << ' ' << error.what() << '\n';
                    }
                }
            }
            ++controls;
            try { if (!caseRun(bytes, flow, noFailure, true)) ++failures; }
            catch (const std::exception& error) {
                fault.armed = false; ++failures;
                std::cerr << "CONTROL_ERROR close bytes=" << bytes << " flow=" << static_cast<int>(flow) << ' ' << error.what() << '\n';
            }
        }
        ++controls;
        try { if (!repeatedRead(bytes)) ++failures; }
        catch (const std::exception& error) { fault.armed = false; ++failures; std::cerr << "CONTROL_ERROR repeated " << error.what() << '\n'; }
    }
    std::cout << "LARGE_OBJECT_FD_EXCEPTION_COMPLETE " << controls << " FAILURES " << failures << " ALLOCATION_SITES " << allocationSites << '\n';
    return failures == 0 ? 0 : 1;
}
