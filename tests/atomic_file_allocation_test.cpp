// test_sources:
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
#include <new>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
using dbms::index_file::WriteResult;
enum class Mode { Healthy, WriteError, FileSyncError, FileCloseError, PublishError, DirectorySyncError, LateUnlinkError };
constexpr size_t noFailure = std::numeric_limits<size_t>::max();
thread_local bool faultThread = false;
struct State {
    bool armed = false, created = false;
    Mode mode = Mode::Healthy;
    char parent[1024] {}, target[1024] {}, temporary[1024] {};
    dev_t device = 0, parentDevice = 0;
    ino_t inode = 0, parentInode = 0;
    int fd = -1;
    size_t selected = noFailure, allocations = 0, thrown = 0, ioErrors = 0, received = 0;
    const char* expectedBytes = nullptr;
    size_t expectedSize = 0;
    bool transfersCorrect = true;
} state;
size_t injected = 0, unexpectedNames = 0, leakedDescriptors = 0;

void require(bool good, const char* label) { if (!good) throw std::runtime_error(label); }
bool active() { return faultThread && state.armed; }
void beforeAllocation() {
    if (!active() || !state.created) return;
    const size_t ordinal = state.allocations++;
    if (ordinal == state.selected) { ++state.thrown; throw std::bad_alloc(); }
}
[[gnu::noinline]] void* allocate(size_t bytes, size_t alignment = 0) {
    beforeAllocation();
    void* result = nullptr;
    if (alignment) {
        if (::posix_memalign(&result, alignment, std::max<size_t>(bytes, 1)) != 0) result = nullptr;
    } else result = std::malloc(std::max<size_t>(bytes, 1));
    if (!result) throw std::bad_alloc();
    return result;
}
[[gnu::noinline]] void deallocate(void* value) noexcept { std::free(value); }
bool matchesTemporary(int fd) {
    struct stat status {};
    return state.created && ::fstat(fd, &status) == 0 && status.st_dev == state.device && status.st_ino == state.inode;
}
int recordTemporary(const char* path, int fd) {
    if (active() && fd >= 0) {
        const size_t parentSize = std::strlen(state.parent);
        if (std::strncmp(path, state.parent, parentSize) == 0 && path[parentSize] == '/') {
            struct stat status {};
            if (::fstat(fd, &status) != 0 || std::strlen(path) >= sizeof(state.temporary)) ::_exit(87);
            state.created = true; state.fd = fd; state.device = status.st_dev; state.inode = status.st_ino;
            std::strcpy(state.temporary, path);
        }
    }
    return fd;
}
} // namespace

// Standard replaceable allocation family is appended from the accepted fixture.

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
    const int fd = original(path, flags, permissions);
    return (flags & O_CREAT) && (flags & O_EXCL) ? recordTemporary(path, fd) : fd;
}
extern "C" int open64(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open64"));
    if (!original) ::_exit(87);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); permissions = va_arg(args, mode_t); va_end(args);
    }
    const int fd = original(path, flags, permissions);
    return (flags & O_CREAT) && (flags & O_EXCL) ? recordTemporary(path, fd) : fd;
}
extern "C" int __open_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open_2"));
    if (!original) ::_exit(87);
    const int fd = original(path, flags);
    return (flags & O_CREAT) && (flags & O_EXCL) ? recordTemporary(path, fd) : fd;
}
extern "C" int __open64_2(const char* path, int flags) {
    using Original = int(*)(const char*, int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "__open64_2"));
    if (!original) ::_exit(87);
    const int fd = original(path, flags);
    return (flags & O_CREAT) && (flags & O_EXCL) ? recordTemporary(path, fd) : fd;
}
extern "C" int mkstemp(char* name) {
    using Original = int(*)(char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "mkstemp"));
    if (!original) ::_exit(87);
    return recordTemporary(name, original(name));
}
extern "C" int mkstemp64(char* name) {
    using Original = int(*)(char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "mkstemp64"));
    if (!original) ::_exit(87);
    return recordTemporary(name, original(name));
}
extern "C" ssize_t write(int fd, const void* data, size_t length) {
    using Original = ssize_t(*)(int, const void*, size_t);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "write"));
    if (!original) ::_exit(87);
    if (!active() || !matchesTemporary(fd)) return original(fd, data, length);
    if (state.mode == Mode::WriteError && !state.ioErrors) { ++state.ioErrors; errno = EIO; return -1; }
    const off_t position = ::lseek(fd, 0, SEEK_CUR);
    state.transfersCorrect = state.transfersCorrect && position >= 0 && static_cast<size_t>(position) == state.received &&
        length == state.expectedSize - state.received;
    const ssize_t count = original(fd, data, length);
    if (count > 0) {
        state.transfersCorrect = state.transfersCorrect && static_cast<size_t>(count) <= state.expectedSize - state.received &&
            std::memcmp(data, state.expectedBytes + state.received, static_cast<size_t>(count)) == 0 && ::lseek(fd, 0, SEEK_CUR) == position + count;
        state.received += static_cast<size_t>(count);
    }
    return count;
}
extern "C" int fsync(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "fsync"));
    if (!original) ::_exit(87);
    if (active() && !state.ioErrors) {
        if (state.mode == Mode::FileSyncError && matchesTemporary(fd)) { ++state.ioErrors; errno = EIO; return -1; }
        struct stat status {};
        if (state.mode == Mode::DirectorySyncError && ::fstat(fd, &status) == 0 &&
            status.st_dev == state.parentDevice && status.st_ino == state.parentInode) {
            ++state.ioErrors; errno = EIO; return -1;
        }
    }
    return original(fd);
}
extern "C" int close(int fd) {
    using Original = int(*)(int);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "close"));
    if (!original) ::_exit(87);
    const bool inject = active() && state.mode == Mode::FileCloseError && !state.ioErrors && matchesTemporary(fd);
    const int result = original(fd);
    if (active() && fd == state.fd) state.fd = -1;
    if (inject && result == 0) { ++state.ioErrors; errno = EIO; return -1; }
    return result;
}
extern "C" int rename(const char* from, const char* to) noexcept {
    using Original = int(*)(const char*, const char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "rename"));
    if (!original) ::_exit(87);
    if (active() && state.mode == Mode::PublishError && !state.ioErrors &&
        std::strcmp(from, state.temporary) == 0 && std::strcmp(to, state.target) == 0) {
        ++state.ioErrors; errno = EIO; return -1;
    }
    return original(from, to);
}
extern "C" int link(const char* from, const char* to) noexcept {
    using Original = int(*)(const char*, const char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "link"));
    if (!original) ::_exit(87);
    if (active() && state.mode == Mode::PublishError && !state.ioErrors &&
        std::strcmp(from, state.temporary) == 0 && std::strcmp(to, state.target) == 0) {
        ++state.ioErrors; errno = EIO; return -1;
    }
    return original(from, to);
}
extern "C" int unlink(const char* path) noexcept {
    using Original = int(*)(const char*);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "unlink"));
    if (!original) ::_exit(87);
    if (active() && state.mode == Mode::LateUnlinkError && !state.ioErrors && std::strcmp(path, state.temporary) == 0) {
        ++state.ioErrors; errno = EIO; return -1;
    }
    return original(path);
}

namespace {
struct PrivateDirectory {
    fs::path path;
    PrivateDirectory() {
        char pattern[] = "/tmp/dbms-atomic-file-allocation.XXXXXX";
        const char* made = ::mkdtemp(pattern); require(made, "mkdtemp"); path = made;
    }
    ~PrivateDirectory() {
        faultThread = false; state.armed = false;
        if (state.fd >= 0 && matchesTemporary(state.fd)) (void)::close(state.fd);
        std::error_code error; fs::remove_all(path, error);
    }
};
void put(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); out.close(); require(out.good(), "fixture write");
}
std::string bytesAt(const fs::path& path) {
    std::ifstream in(path, std::ios::binary); require(in.good(), "snapshot open");
    std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    require(!in.bad(), "snapshot read"); return bytes;
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        const auto name = entry.path().filename().string(); char* end = nullptr;
        const long fd = std::strtol(name.c_str(), &end, 10);
        if (end && !*end && fd >= 0 && matchesTemporary(static_cast<int>(fd))) ++count;
    }
    return count;
}
size_t temporaries(const fs::path& parent, const fs::path& target) {
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator(parent)) if (entry.path() != target) ++count;
    return count;
}
bool expectedIo(Mode mode, bool noReplace, bool occupied, size_t bytes) {
    if (mode == Mode::Healthy) return false;
    if (mode == Mode::WriteError && !bytes) return false;
    if (mode == Mode::DirectorySyncError && noReplace && occupied) return false;
    return true;
}
struct Profile { size_t sites = 0; };

bool check(bool noReplace, bool occupied, bool longPath, size_t length, Mode mode,
           size_t selected, Profile* profile = nullptr, size_t knownSites = 0) {
    PrivateDirectory directory;
    const auto parent = longPath ? directory.path / std::string(150, 'L') : directory.path;
    fs::create_directories(parent);
    const auto target = parent / "index.idx";
    const std::string original("original\0payload", 16);
    if (occupied) put(target, original);
    std::string data(length, '\0');
    for (size_t i = 0; i < length; ++i) data[i] = static_cast<char>(i * 37 + 19);
    state = State{}; state.mode = mode; state.selected = selected;
    const auto parentName = parent.string(); const auto targetName = target.string();
    require(parentName.size() < sizeof(state.parent) && targetName.size() < sizeof(state.target), "fixture path length");
    std::strcpy(state.parent, parentName.c_str()); std::strcpy(state.target, targetName.c_str());
    struct stat parentStatus {}; require(::stat(parent.c_str(), &parentStatus) == 0, "parent stat");
    state.parentDevice = parentStatus.st_dev; state.parentInode = parentStatus.st_ino;
    state.expectedBytes = data.data(); state.expectedSize = data.size();
    WriteResult result = WriteResult::IO_ERROR; bool caught = false;
    faultThread = true; state.armed = true;
    try { result = dbms::index_file::writeAtomicallyImpl(target, data, noReplace); }
    catch (const std::bad_alloc&) { caught = true; }
    catch (...) { faultThread = false; state.armed = false; throw; }
    faultThread = false; state.armed = false;
    require(state.created && state.transfersCorrect, "actual temporary or transfer scope not exercised");
    if (profile) {
        profile->sites = state.allocations; require(profile->sites <= 8, "profile exceeds fixed sweep");
        std::cout << "ATOMIC_ALLOCATION_PROFILE noreplace=" << noReplace << " occupied=" << occupied << " long=" << longPath
                  << " bytes=" << length << " sites=" << profile->sites << '\n';
    } else if (selected != noFailure) {
        const bool expectedFault = selected < knownSites;
        require(caught == expectedFault && state.thrown == (expectedFault ? 1u : 0u), "allocation ordinal mismatch");
    }
    injected += state.thrown;
    const bool io = expectedIo(mode, noReplace, occupied, length);
    if (!caught) require(state.ioErrors == (io ? 1u : 0u), "I/O fault branch mismatch");
    const bool allowedAlias = !caught && mode == Mode::LateUnlinkError;
    const size_t tempCount = temporaries(parent, target), fdCount = descriptors();
    if (tempCount > (allowedAlias ? 1u : 0u)) unexpectedNames += tempCount - (allowedAlias ? 1u : 0u);
    leakedDescriptors += fdCount;
    bool good = fdCount == 0 && tempCount == (allowedAlias ? 1u : 0u);
    if (caught) {
        good = good && fs::exists(target) == occupied;
        if (occupied) good = good && bytesAt(target) == original;
    } else {
        const bool ioFailure = io && mode != Mode::LateUnlinkError;
        const WriteResult expectedResult = ioFailure ? WriteResult::IO_ERROR : noReplace && occupied ? WriteResult::ALREADY_EXISTS : WriteResult::OK;
        good = good && result == expectedResult;
        const bool changed = (!ioFailure && !(noReplace && occupied)) || (mode == Mode::DirectorySyncError && !noReplace);
        good = good && fs::exists(target) == (changed || occupied);
        if (changed) good = good && bytesAt(target) == data;
        else if (occupied) good = good && bytesAt(target) == original;
        if (allowedAlias) {
            struct stat targetStatus {}; require(::stat(target.c_str(), &targetStatus) == 0, "alias target stat");
            good = good && targetStatus.st_dev == state.device && targetStatus.st_ino == state.inode;
        }
    }
    if (!good) std::cerr << "ATOMIC_ALLOCATION_FAILURE noreplace=" << noReplace << " occupied=" << occupied << " long=" << longPath
                         << " bytes=" << length << " mode=" << static_cast<int>(mode) << " ordinal=" << selected
                         << " caught=" << caught << " temporaries=" << tempCount << " fds=" << fdCount << '\n';
    // Cleanup occurs after observations, only for the witnessed private inode.
    if (state.fd >= 0 && matchesTemporary(state.fd)) require(::close(state.fd) == 0, "cleanup observed private fd");
    state.fd = -1;
    if (fs::exists(state.temporary)) {
        struct stat leftover {}; require(::lstat(state.temporary, &leftover) == 0 &&
            leftover.st_dev == state.device && leftover.st_ino == state.inode && S_ISREG(leftover.st_mode), "cleanup temporary identity");
        require(::unlink(state.temporary) == 0, "cleanup witnessed temporary");
    }
    require(temporaries(parent, target) == 0 && descriptors() == 0, "fixture cleanup incomplete");
    const bool retryOccupied = fs::exists(target);
    const std::string beforeRetry = retryOccupied ? bytesAt(target) : std::string();
    const auto retry = dbms::index_file::writeAtomicallyImpl(target, data, noReplace);
    require(retry == (noReplace && retryOccupied ? WriteResult::ALREADY_EXISTS : WriteResult::OK), "healthy retry result");
    require(bytesAt(target) == (noReplace && retryOccupied ? beforeRetry : data), "healthy retry bytes");
    require(temporaries(parent, target) == 0 && descriptors() == 0, "healthy retry residue");
    return good;
}
} // namespace

int main() {
    int controls = 0, failures = 0; size_t sites = 0;
    for (bool noReplace : {false, true}) for (bool occupied : {false, true}) for (bool longPath : {false, true}) {
        for (size_t length : {size_t(0), size_t(257), size_t(131101)}) {
            Profile profile;
            ++controls;
            try { if (!check(noReplace, occupied, longPath, length, Mode::Healthy, noFailure, &profile)) ++failures; }
            catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR profile " << error.what() << '\n'; continue; }
            sites += profile.sites;
            for (size_t ordinal = 0; ordinal < 8; ++ordinal) {
                ++controls;
                try { if (!check(noReplace, occupied, longPath, length, Mode::Healthy, ordinal, nullptr, profile.sites)) ++failures; }
                catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR allocation " << error.what() << '\n'; }
            }
            for (int mode = 1; mode <= 5; ++mode) {
                ++controls;
                try { if (!check(noReplace, occupied, longPath, length, static_cast<Mode>(mode), noFailure)) ++failures; }
                catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR I/O " << error.what() << '\n'; }
            }
            if (noReplace && !occupied) {
                ++controls;
                try { if (!check(noReplace, occupied, longPath, length, Mode::LateUnlinkError, noFailure)) ++failures; }
                catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR alias " << error.what() << '\n'; }
            }
        }
    }
    std::cout << "ATOMIC_FILE_ALLOCATION_COMPLETE " << controls << " FAILURES " << failures << " PROFILE_SITES " << sites
              << " INJECTED " << injected << " UNEXPECTED_TEMPORARIES " << unexpectedNames << " LEAKED_FDS " << leakedDescriptors << '\n';
    return failures == 0 ? 0 : 1;
}
