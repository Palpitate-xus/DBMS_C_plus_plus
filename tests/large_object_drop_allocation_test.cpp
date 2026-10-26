// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"
#include "access/IndexFileUtil.h"

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
enum class Mode { Healthy, DirectorySync, Missing, BlockedDirectory, ReservationClose,
                  ObjectOpen, Rename, ObjectClose, CleanupSync };
constexpr size_t noFailure = std::numeric_limits<size_t>::max();
thread_local bool faultThread = false;
struct State {
    bool armed = false, reserved = false;
    Mode mode = Mode::Healthy;
    char object[1024] {}, prefix[1024] {}, temporary[1024] {};
    dev_t device = 0, temporaryDevice = 0;
    ino_t inode = 0, temporaryInode = 0;
    int reservationFd = -1;
    size_t selected = noFailure, allocations = 0, thrown = 0, ioErrors = 0, reservations = 0;
    std::array<size_t, 64> allocationSizes {};
} state;
size_t observedLeaks = 0, injectedAllocations = 0;

void require(bool good, const char* label) {
    if (!good) throw std::runtime_error(label);
}
void beforeAllocation(size_t bytes) {
    if (!faultThread || !state.armed || !state.reserved) return;
    if (state.allocations == state.allocationSizes.size()) ::_exit(86);
    const size_t ordinal = state.allocations++;
    state.allocationSizes[ordinal] = bytes;
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
bool active() { return faultThread && state.armed; }
int afterReservation(char* name, int fd) {
    if (active() && fd >= 0 && std::strncmp(name, state.prefix, std::strlen(state.prefix)) == 0) {
        if (std::strlen(name) >= sizeof(state.temporary)) ::_exit(87);
        std::strcpy(state.temporary, name);
        struct stat status {};
        if (::fstat(fd, &status) != 0) ::_exit(87);
        state.temporaryDevice = status.st_dev; state.temporaryInode = status.st_ino;
        state.reserved = true; state.reservationFd = fd; ++state.reservations;
    }
    return fd;
}
bool failOpen(const char* path) {
    if (active() && state.reserved && state.mode == Mode::ObjectOpen && !state.ioErrors &&
        std::strcmp(path, state.object) == 0) {
        ++state.ioErrors; errno = EIO; return true;
    }
    return false;
}
} // namespace

// Global allocation adapters use a thread-local gate before reading State.

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

extern "C" int mkstemp(char* name) {
    using Original = int(*)(char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "mkstemp"));
    if (!original) ::_exit(87);
    return afterReservation(name, original(name));
}
extern "C" int mkstemp64(char* name) {
    using Original = int(*)(char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "mkstemp64"));
    if (!original) ::_exit(87);
    return afterReservation(name, original(name));
}
extern "C" int open(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open"));
    if (!original) ::_exit(87);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); permissions = va_arg(args, mode_t); va_end(args);
    }
    if (failOpen(path)) return -1;
    return original(path, flags, permissions);
}
extern "C" int open64(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open64"));
    if (!original) ::_exit(87);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); permissions = va_arg(args, mode_t); va_end(args);
    }
    if (failOpen(path)) return -1;
    return original(path, flags, permissions);
}
extern "C" int __open_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open_2"));
    if (!original) ::_exit(87);
    if (failOpen(path)) return -1;
    return original(path, flags);
}
extern "C" int __open64_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open64_2"));
    if (!original) ::_exit(87);
    if (failOpen(path)) return -1;
    return original(path, flags);
}
extern "C" int close(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "close"));
    if (!original) ::_exit(87);
    bool inject = false;
    if (active() && state.reserved && !state.ioErrors) {
        if (state.mode == Mode::ReservationClose && fd == state.reservationFd) inject = true;
        if (state.mode == Mode::ObjectClose) {
            struct stat status {};
            inject = ::fstat(fd, &status) == 0 && status.st_dev == state.device && status.st_ino == state.inode;
        }
    }
    const int result = original(fd);
    if (active() && fd == state.reservationFd) state.reservationFd = -1;
    if (inject && result == 0) { ++state.ioErrors; errno = EIO; return -1; }
    return result;
}
extern "C" int rename(const char* from, const char* to) noexcept {
    using Original = int(*)(const char*, const char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "rename"));
    if (!original) ::_exit(87);
    if (active() && state.reserved && state.mode == Mode::Rename && !state.ioErrors &&
        std::strcmp(from, state.object) == 0 && std::strcmp(to, state.temporary) == 0) {
        ++state.ioErrors; errno = EIO; return -1;
    }
    return original(from, to);
}

namespace {
struct PrivateDirectory {
    fs::path path;
    PrivateDirectory() {
        char pattern[] = "/tmp/dbms-lo-drop-allocation.XXXXXX";
        const char* made = ::mkdtemp(pattern);
        require(made, "mkdtemp");
        path = made;
    }
    ~PrivateDirectory() {
        faultThread = false; state.armed = false;
        dbms::index_file::directorySyncFailureCountForTesting.store(0);
        std::error_code error;
        fs::remove_all(path, error);
    }
};
void put(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
    require(out.good(), "fixture write");
}
std::string snapshot(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.good(), "snapshot open");
    const std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    require(!in.bad(), "snapshot read");
    return data;
}
size_t hiddenFiles(const fs::path& directory) {
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.path().filename().string().rfind(".lo_drop_1_", 0) == 0) ++count;
    }
    return count;
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        const std::string name = entry.path().filename().string();
        char* end = nullptr;
        const long fd = std::strtol(name.c_str(), &end, 10);
        struct stat status {};
        if (end && !*end && fd >= 0 && ::fstat(static_cast<int>(fd), &status) == 0 &&
            ((status.st_dev == state.device && status.st_ino == state.inode) ||
             (status.st_dev == state.temporaryDevice && status.st_ino == state.temporaryInode))) ++count;
    }
    return count;
}
bool normalResult(Mode mode) {
    return mode == Mode::Healthy || mode == Mode::Missing || mode == Mode::CleanupSync;
}
bool ioMode(Mode mode) {
    return mode == Mode::ReservationClose || mode == Mode::ObjectOpen || mode == Mode::Rename || mode == Mode::ObjectClose;
}
struct Profile { size_t count = 0; std::array<size_t, 64> sizes {}; };

bool check(size_t length, bool longPath, Mode mode, size_t selected, Profile* profile = nullptr, size_t knownSites = 0) {
    PrivateDirectory directory;
    const auto db = longPath ? directory.path / std::string(150, 'L') / "db" : directory.path / "db";
    const auto object = db / ".lobjects/lo_1.dat";
    const auto saved = directory.path / "saved";
    std::string data(length, '\0');
    for (size_t i = 0; i < length; ++i) data[i] = static_cast<char>(i * 37 + 19);
    dbms::LargeObjectManager objects(db.string());
    require(objects.create() == 1 && objects.write(1, 0, data), "seed object");
    state = State{};
    state.mode = mode; state.selected = selected;
    const std::string objectName = object.string();
    const std::string prefix = (db / ".lobjects/.lo_drop_1_").string();
    require(objectName.size() < sizeof(state.object) && prefix.size() < sizeof(state.prefix), "fixture path length");
    std::strcpy(state.object, objectName.c_str()); std::strcpy(state.prefix, prefix.c_str());
    struct stat status {};
    require(::stat(object.c_str(), &status) == 0, "seed stat");
    state.device = status.st_dev; state.inode = status.st_ino;
    if (mode == Mode::Missing) require(::unlink(object.c_str()) == 0, "fixture absent object");
    if (mode == Mode::BlockedDirectory) {
        fs::rename(object, saved); fs::create_directory(object); put(object / "blocker", "occupied");
    }
    if (mode == Mode::DirectorySync) dbms::index_file::failNextDirectorySyncForTesting();
    if (mode == Mode::CleanupSync) dbms::index_file::failDirectorySyncAfterForTesting(1);
    bool result = false, caught = false;
    faultThread = true; state.armed = true;
    try { result = objects.drop(1); }
    catch (const std::bad_alloc&) { caught = true; }
    catch (...) { faultThread = false; state.armed = false; throw; }
    faultThread = false; state.armed = false;
    dbms::index_file::directorySyncFailureCountForTesting.store(0);
    require(state.reservations == 1, "reservation scope not reached");
    if (profile) {
        profile->count = state.allocations; profile->sizes = state.allocationSizes;
        require(profile->count <= 8, "profile exceeds fixed exhaustive fault sweep");
        std::cout << "DROP_ALLOCATION_PROFILE length=" << length << " long=" << longPath << " mode=" << static_cast<int>(mode) << " sites=" << profile->count;
        for (size_t i = 0; i < profile->count; ++i) std::cout << ' ' << profile->sizes[i];
        std::cout << '\n';
    } else {
        const bool expectedFault = selected < knownSites;
        require(caught == expectedFault && state.thrown == (expectedFault ? 1u : 0u), "allocation ordinal not exercised as discovered");
        injectedAllocations += state.thrown;
    }
    if (!caught && ioMode(mode)) require(state.ioErrors == 1, "I/O injection not reached");
    const size_t hidden = hiddenFiles(db / ".lobjects");
    observedLeaks += hidden;
    bool good = hidden == 0 && descriptors() == 0 && (caught || result == normalResult(mode));
    const bool absent = mode == Mode::Missing || (!caught && result);
    if (mode == Mode::BlockedDirectory) {
        require(fs::is_directory(object) && snapshot(object / "blocker") == "occupied" && snapshot(saved) == data, "blocked object changed");
        fs::remove(object / "blocker"); fs::remove(object); fs::rename(saved, object);
    } else if (absent) require(!fs::exists(object), "successful DROP kept object");
    else require(snapshot(object) == data, "failed DROP lost original object");
    if (!good) std::cerr << "DROP_ALLOCATION_FAILURE length=" << length << " long=" << longPath << " mode=" << static_cast<int>(mode)
                         << " ordinal=" << selected << " hidden=" << hidden << " caught=" << caught << '\n';
    if (hidden) {
        // Observe first, then clean ONLY the fixture's own empty reservation.
        struct stat leftover {};
        require(hidden == 1 && ::lstat(state.temporary, &leftover) == 0 && S_ISREG(leftover.st_mode) && leftover.st_size == 0 &&
            leftover.st_dev == state.temporaryDevice && leftover.st_ino == state.temporaryInode, "unexpected leftover identity");
        require(::unlink(state.temporary) == 0, "fixture reservation cleanup");
    }
    require(hiddenFiles(db / ".lobjects") == 0, "reservation cleanup incomplete");
    if (!fs::exists(object)) {
        dbms::LargeObjectManager fresh(db.string());
        require(fresh.create() == 1 && fresh.write(1, 0, data), "healthy public recreate");
    }
    require(objects.read(1) == data, "healthy read retry");
    dbms::LargeObjectManager reopened(db.string());
    require(reopened.read(1) == data && reopened.size(1) == data.size(), "fresh reader after failure");
    require(objects.drop(1) && objects.drop(1) && !fs::exists(object) && hiddenFiles(db / ".lobjects") == 0, "healthy/idempotent DROP retry");
    require(descriptors() == 0, "DROP descriptor leak");
    return good;
}
} // namespace

int main() {
    int controls = 0, failures = 0;
    size_t sites = 0;
    for (size_t length : {size_t(257), size_t(4099)}) for (bool longPath : {false, true}) {
        for (int mode = 0; mode < 9; ++mode) {
            Profile profile;
            ++controls;
            try { if (!check(length, longPath, static_cast<Mode>(mode), noFailure, &profile)) ++failures; }
            catch (const std::exception& error) {
                faultThread = false; state.armed = false; ++failures;
                std::cerr << "CONTROL_ERROR profile mode=" << mode << ' ' << error.what() << '\n';
                continue;
            }
            sites += profile.count;
            for (size_t ordinal = 0; ordinal < 8; ++ordinal) {
                ++controls;
                try { if (!check(length, longPath, static_cast<Mode>(mode), ordinal, nullptr, profile.count)) ++failures; }
                catch (const std::exception& error) {
                    faultThread = false; state.armed = false; ++failures;
                    std::cerr << "CONTROL_ERROR fault mode=" << mode << " ordinal=" << ordinal << ' ' << error.what() << '\n';
                }
            }
        }
    }
    std::cout << "LARGE_OBJECT_DROP_ALLOCATION_COMPLETE " << controls << " FAILURES " << failures << " PROFILE_SITES " << sites
              << " INJECTED " << injectedAllocations << " LEAKED_RESERVATIONS " << observedLeaks << '\n';
    return failures == 0 ? 0 : 1;
}
