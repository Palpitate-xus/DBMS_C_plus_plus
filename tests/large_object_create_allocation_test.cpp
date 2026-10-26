// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"
#include "access/IndexFileUtil.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
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
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
enum class Scenario { Empty, Peer, CachedGone, NearMax, Existing, CachedPeer, MaxPeer, Exhausted };
enum class Mode { Healthy, OpenError, OpenSpace, OpenInterrupt, SyncError, CloseError, DirectorySync };
constexpr size_t noFailure = std::numeric_limits<size_t>::max();
constexpr size_t sweepLimit = 64;
thread_local bool faultThread = false;
struct State {
    bool armed = false;
    Mode mode = Mode::Healthy;
    char prefix[1024] {};
    size_t selected = noFailure, allocations = 0, postAllocations = 0, thrown = 0, ioErrors = 0, created = 0;
    int createdFd = -1;
    dev_t device = 0;
    ino_t inode = 0;
    std::array<size_t, 128> sizes {};
} state;
size_t orphans = 0, badRetries = 0, injectedAllocations = 0, postCreationFaults = 0;

void require(bool good, const char* label) {
    if (!good) throw std::runtime_error(label);
}
bool active() { return faultThread && state.armed; }
void beforeAllocation(size_t bytes) {
    if (!active()) return;
    if (state.allocations == state.sizes.size()) ::_exit(86);
    const size_t ordinal = state.allocations++;
    state.sizes[ordinal] = bytes;
    if (state.created) ++state.postAllocations;
    if (ordinal == state.selected) {
        ++state.thrown;
        throw std::bad_alloc();
    }
}
[[gnu::noinline]] void* allocate(size_t bytes, size_t alignment = 0) {
    beforeAllocation(bytes);
    void* result = nullptr;
    if (alignment) {
        if (::posix_memalign(&result, alignment, std::max<size_t>(bytes, 1)) != 0) result = nullptr;
    } else result = std::malloc(std::max<size_t>(bytes, 1));
    if (!result) throw std::bad_alloc();
    return result;
}
[[gnu::noinline]] void deallocate(void* value) noexcept { std::free(value); }
bool matchingPath(const char* path) {
    return std::strncmp(path, state.prefix, std::strlen(state.prefix)) == 0;
}
bool failOpen(const char* path, int flags) {
    if (!active() || !matchingPath(path) || state.ioErrors || !(flags & O_EXCL)) return false;
    if (state.mode != Mode::OpenError && state.mode != Mode::OpenSpace && state.mode != Mode::OpenInterrupt) return false;
    ++state.ioErrors;
    errno = state.mode == Mode::OpenSpace ? ENOSPC : state.mode == Mode::OpenInterrupt ? EINTR : EIO;
    return true;
}
int trackOpen(const char* path, int flags, int fd) {
    if (active() && fd >= 0 && matchingPath(path) && (flags & O_CREAT) && (flags & O_EXCL)) {
        struct stat status {};
        if (::fstat(fd, &status) != 0) ::_exit(87);
        ++state.created; state.createdFd = fd; state.device = status.st_dev; state.inode = status.st_ino;
    }
    return fd;
}
bool isCreated(int fd) {
    if (!active() || fd != state.createdFd || fd < 0) return false;
    struct stat status {};
    return ::fstat(fd, &status) == 0 && status.st_dev == state.device && status.st_ino == state.inode;
}
} // namespace

// The replaceable allocation family is appended from the accepted fd fixture.

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
    if (failOpen(path, flags)) return -1;
    return trackOpen(path, flags, original(path, flags, permissions));
}
extern "C" int open64(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open64"));
    if (!original) ::_exit(87);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); permissions = va_arg(args, mode_t); va_end(args);
    }
    if (failOpen(path, flags)) return -1;
    return trackOpen(path, flags, original(path, flags, permissions));
}
extern "C" int __open_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open_2"));
    if (!original) ::_exit(87);
    if (failOpen(path, flags)) return -1;
    return trackOpen(path, flags, original(path, flags));
}
extern "C" int __open64_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open64_2"));
    if (!original) ::_exit(87);
    if (failOpen(path, flags)) return -1;
    return trackOpen(path, flags, original(path, flags));
}
extern "C" int fsync(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "fsync"));
    if (!original) ::_exit(87);
    if (isCreated(fd) && state.mode == Mode::SyncError && !state.ioErrors) {
        ++state.ioErrors; errno = EIO; return -1;
    }
    return original(fd);
}
extern "C" int close(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "close"));
    if (!original) ::_exit(87);
    const bool inject = isCreated(fd) && state.mode == Mode::CloseError && !state.ioErrors;
    const int result = original(fd);
    if (active() && fd == state.createdFd) state.createdFd = -1;
    if (inject && result == 0) { ++state.ioErrors; errno = EIO; return -1; }
    return result;
}

namespace {
struct PrivateDirectory {
    fs::path path;
    PrivateDirectory() {
        char pattern[] = "/tmp/dbms-lo-create-allocation.XXXXXX";
        const char* made = ::mkdtemp(pattern);
        require(made, "mkdtemp"); path = made;
    }
    ~PrivateDirectory() {
        faultThread = false; state.armed = false;
        dbms::index_file::directorySyncFailureCountForTesting.store(0);
        std::error_code error; fs::remove_all(path, error);
    }
};
void put(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); out.close();
    require(out.good(), "fixture write");
}
std::string bytesAt(const fs::path& path) {
    std::ifstream in(path, std::ios::binary); require(in.good(), "snapshot open");
    std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    require(!in.bad(), "snapshot read"); return bytes;
}
using Image = std::map<std::string, std::string>;
Image snapshot(const fs::path& directory) {
    Image result;
    for (const auto& entry : fs::directory_iterator(directory)) {
        require(entry.is_regular_file(), "unexpected non-file in private object directory");
        result.emplace(entry.path().filename().string(), bytesAt(entry.path()));
    }
    return result;
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        std::error_code error;
        const std::string target = fs::read_symlink(entry.path(), error).string();
        if (!error && target.rfind(state.prefix, 0) == 0) ++count;
    }
    return count;
}
std::string filename(int id) { return "lo_" + std::to_string(id) + ".dat"; }
struct Seed {
    PrivateDirectory directory;
    fs::path db, objectDirectory;
    std::unique_ptr<dbms::LargeObjectManager> observer;
    Image before;
    int expected = 1;
    std::string data;
    Seed(Scenario scenario, bool longPath) : db(longPath ? directory.path / std::string(150, 'L') / "db" : directory.path / "db"),
        objectDirectory(db / ".lobjects"), data(257, '\0') {
        for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 37 + 19);
        if (scenario == Scenario::NearMax || scenario == Scenario::MaxPeer || scenario == Scenario::Exhausted) {
            fs::create_directories(objectDirectory);
            put(objectDirectory / filename(scenario == Scenario::Exhausted ? INT_MAX : INT_MAX - 1), data);
        }
        if (scenario == Scenario::Existing) {
            dbms::LargeObjectManager writer(db.string());
            require(writer.create() == 1 && writer.write(1, 0, data), "existing seed");
        }
        observer = std::make_unique<dbms::LargeObjectManager>(db.string());
        if (scenario == Scenario::Peer || scenario == Scenario::CachedPeer || scenario == Scenario::CachedGone || scenario == Scenario::MaxPeer) {
            dbms::LargeObjectManager peer(db.string());
            const int peerId = scenario == Scenario::MaxPeer ? INT_MAX : 1;
            require(peer.create() == peerId && peer.write(peerId, 0, data), "peer seed");
            if (scenario == Scenario::CachedPeer || scenario == Scenario::CachedGone) require(observer->write(1, 0, data), "cache known peer");
            if (scenario == Scenario::CachedGone) require(peer.drop(1), "peer drop known object");
        }
        expected = scenario == Scenario::NearMax ? INT_MAX :
            (scenario == Scenario::MaxPeer || scenario == Scenario::Exhausted) ? 0 :
            (scenario == Scenario::Peer || scenario == Scenario::Existing || scenario == Scenario::CachedPeer) ? 2 : 1;
        before = snapshot(objectDirectory);
        state = State{};
        const std::string prefix = (objectDirectory / "lo_").string();
        require(prefix.size() < sizeof(state.prefix), "fixture path too long");
        std::strcpy(state.prefix, prefix.c_str());
        require(descriptors() == 0, "seed fd leak");
    }
    void verifyProtected() const {
        for (const auto& [name, bytes] : before) require(bytesAt(objectDirectory / name) == bytes, "preexisting/peer object changed");
    }
};
struct Profile { size_t count = 0, post = 0; };

bool check(Scenario scenario, bool longPath, Mode mode, size_t selected, Profile* profile = nullptr, size_t knownSites = 0) {
    Seed seed(scenario, longPath);
    state.mode = mode; state.selected = selected;
    if (mode == Mode::DirectorySync) dbms::index_file::failNextDirectorySyncForTesting();
    int result = 0; bool caught = false;
    faultThread = true; state.armed = true;
    try { result = seed.observer->create(); }
    catch (const std::bad_alloc&) { caught = true; }
    catch (...) { faultThread = false; state.armed = false; throw; }
    faultThread = false; state.armed = false;
    dbms::index_file::directorySyncFailureCountForTesting.store(0);
    if (profile) {
        profile->count = state.allocations; profile->post = state.postAllocations;
        require(profile->count <= sweepLimit, "allocation profile exceeds fixed sweep");
        std::cout << "CREATE_ALLOCATION_PROFILE scenario=" << static_cast<int>(scenario) << " long=" << longPath
                  << " sites=" << profile->count << " post=" << profile->post << '\n';
    } else if (selected != noFailure) {
        const bool expectedFault = selected < knownSites;
        require(caught == expectedFault && state.thrown == (expectedFault ? 1u : 0u), "allocation ordinal mismatch");
    }
    injectedAllocations += state.thrown;
    if (caught && state.created) ++postCreationFaults;
    if (mode != Mode::Healthy && mode != Mode::DirectorySync) require(state.ioErrors == 1, "I/O injection not reached");
    seed.verifyProtected();
    const Image after = snapshot(seed.objectDirectory);
    bool good = descriptors() == 0;
    if (caught) {
        good = good && after == seed.before;
        for (const auto& [name, bytes] : after) if (!seed.before.count(name)) {
            require(bytes.empty(), "unexpected payload in unreturned object"); ++orphans;
        }
    } else {
        const int expectedResult = mode == Mode::Healthy ? seed.expected : 0;
        good = good && result == expectedResult;
        Image expectedImage = seed.before;
        if (result > 0) {
            require(!seed.before.count(filename(result)), "returned an existing peer ID");
            expectedImage.emplace(filename(result), "");
        }
        good = good && after == expectedImage;
    }
    if (caught) {
        const int retry = seed.observer->create();
        if (retry != seed.expected) { ++badRetries; good = false; }
        if (retry > 0) {
            require(!seed.before.count(filename(retry)) && fs::file_size(seed.objectDirectory / filename(retry)) == 0, "retry returned invalid object");
            require(seed.observer->write(retry, 0, "retry payload"), "healthy retry write");
            dbms::LargeObjectManager fresh(seed.db.string());
            require(fresh.read(retry) == "retry payload", "fresh retry reader");
        }
    } else if (result > 0) {
        require(seed.observer->size(result) == 0 && seed.observer->write(result, 0, "returned payload"), "new object metadata/write");
        dbms::LargeObjectManager fresh(seed.db.string());
        require(fresh.read(result) == "returned payload", "fresh new-object reader");
    } else if (mode != Mode::Healthy) {
        const int retry = seed.observer->create();
        require(retry > 0 && seed.observer->write(retry, 0, "I/O retry"), "healthy I/O retry");
    }
    seed.verifyProtected();
    require(descriptors() == 0, "create retry fd leak");
    if (!good) std::cerr << "CREATE_ALLOCATION_FAILURE scenario=" << static_cast<int>(scenario) << " long=" << longPath
                         << " ordinal=" << selected << " caught=" << caught << " result=" << result << " created=" << state.created << '\n';
    return good;
}
} // namespace

int main() {
    int controls = 0, failures = 0;
    size_t profileSites = 0, profilePost = 0;
    for (bool longPath : {false, true}) {
        for (Scenario scenario : {Scenario::Empty, Scenario::Peer, Scenario::CachedGone, Scenario::NearMax}) {
            Profile profile;
            ++controls;
            try { if (!check(scenario, longPath, Mode::Healthy, noFailure, &profile)) ++failures; }
            catch (const std::exception& error) {
                faultThread = false; state.armed = false; ++failures;
                std::cerr << "CONTROL_ERROR profile " << error.what() << '\n'; continue;
            }
            profileSites += profile.count; profilePost += profile.post;
            for (size_t selected = 0; selected < sweepLimit; ++selected) {
                ++controls;
                try { if (!check(scenario, longPath, Mode::Healthy, selected, nullptr, profile.count)) ++failures; }
                catch (const std::exception& error) {
                    faultThread = false; state.armed = false; ++failures;
                    std::cerr << "CONTROL_ERROR allocation " << error.what() << '\n';
                }
            }
        }
        for (Scenario scenario : {Scenario::Existing, Scenario::CachedPeer, Scenario::MaxPeer, Scenario::Exhausted}) {
            ++controls;
            try { if (!check(scenario, longPath, Mode::Healthy, noFailure)) ++failures; }
            catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR semantic " << error.what() << '\n'; }
        }
        for (Scenario scenario : {Scenario::Empty, Scenario::Peer}) for (int mode = 1; mode <= 6; ++mode) {
            ++controls;
            try { if (!check(scenario, longPath, static_cast<Mode>(mode), noFailure)) ++failures; }
            catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR I/O " << error.what() << '\n'; }
        }
    }
    std::cout << "LARGE_OBJECT_CREATE_ALLOCATION_COMPLETE " << controls << " FAILURES " << failures << " PROFILE_SITES " << profileSites
              << " POST_SITES " << profilePost << " INJECTED " << injectedAllocations << " ORPHANS " << orphans
              << " BAD_RETRIES " << badRetries << " POST_CREATION_FAULTS " << postCreationFaults << '\n';
    return failures == 0 ? 0 : 1;
}
