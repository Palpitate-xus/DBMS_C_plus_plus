// test_sources: src/storage/LargeObject.cpp
// Hard pread errors must not expose a plausible partial blob as a valid read.
#include "storage/LargeObject.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
enum class Mode { Normal, Eintr, TwiceEintr, ShortOnce, AllShort, Mixed,
                  FirstEio, ShortEio, TwoShortEio, ShortEintrEio,
                  TruncateBeforeFirst, TruncateAfterShort, ShortEintr, ShortInvalid };
struct State {
    bool armed = false;
    Mode mode = Mode::Normal;
    dev_t device = 0;
    ino_t inode = 0;
    std::string path;
    std::string source;
    size_t offset = 0, requested = 0, received = 0;
    size_t calls = 0, shorts = 0, interrupts = 0, errors = 0, eof = 0, truncations = 0;
    bool bytesCorrect = true, positionsCorrect = true;
} state;

void require(bool good, const char* message) {
    if (!good) throw std::runtime_error(message);
}

bool matches(int fd) {
    struct stat status {};
    return ::fstat(fd, &status) == 0 && status.st_dev == state.device && status.st_ino == state.inode;
}

size_t objectDescriptors() {
    size_t result = 0;
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        const std::string name = entry.path().filename().string();
        char* end = nullptr;
        const long fd = std::strtol(name.c_str(), &end, 10);
        if (end && !*end && fd >= 0 && matches(static_cast<int>(fd))) ++result;
    }
    return result;
}

bool hardError(Mode mode) {
    return mode == Mode::FirstEio || mode == Mode::ShortEio || mode == Mode::TwoShortEio ||
           mode == Mode::ShortEintrEio || mode == Mode::ShortInvalid;
}

bool completeInjection() {
    switch (state.mode) {
        case Mode::Normal: return !state.shorts && !state.interrupts && !state.errors && !state.eof;
        case Mode::Eintr: return state.interrupts == 1;
        case Mode::TwiceEintr: return state.interrupts == 2;
        case Mode::ShortOnce: return state.shorts == 1;
        case Mode::AllShort: return state.shorts > 1;
        case Mode::Mixed: return state.shorts > 1 && state.interrupts == 2;
        case Mode::FirstEio: return state.errors == 1 && state.received == 0;
        case Mode::ShortEio: return state.errors == 1 && state.shorts == 1 && state.received == 7;
        case Mode::TwoShortEio: return state.errors == 1 && state.shorts == 2 && state.received == 10;
        case Mode::ShortEintrEio: return state.errors == 1 && state.interrupts == 1 && state.received == 7;
        case Mode::TruncateBeforeFirst: return state.truncations == 1 && state.eof == 1 && state.received == 0;
        case Mode::TruncateAfterShort: return state.truncations == 1 && state.eof == 1 && state.received == 7;
        case Mode::ShortEintr: return state.interrupts == 1 && state.shorts == 1;
        case Mode::ShortInvalid: return state.errors == 1 && state.shorts == 1 && state.received == 7;
    }
    return false;
}

struct PrivateDirectory {
    fs::path path;
    PrivateDirectory() {
        char pattern[] = "/tmp/dbms-lo-read-failure.XXXXXX";
        const char* created = ::mkdtemp(pattern);
        require(created != nullptr, "mkdtemp");
        path = created;
    }
    ~PrivateDirectory() {
        state.armed = false;
        std::error_code error;
        fs::remove_all(path, error);
    }
};

std::string bytesAt(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.good(), "fixture snapshot open");
    const std::string value((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    require(!in.bad(), "fixture snapshot read");
    return value;
}

std::string payload(size_t length) {
    std::string value(length, '\0');
    for (size_t i = 0; i < length; ++i) value[i] = static_cast<char>((i * 37 + 19) & 255);
    return value;
}
} // namespace

extern "C" ssize_t pread(int fd, void* bytes, size_t length, off_t offset) {
    using Original = ssize_t(*)(int, void*, size_t, off_t);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "pread"));
    if (!original) ::_exit(86);
    if (!state.armed || !matches(fd)) return original(fd, bytes, length, offset);
    ++state.calls;
    if (state.calls > 65536) ::_exit(87);
    const off_t position = ::lseek(fd, 0, SEEK_CUR);
    state.positionsCorrect = state.positionsCorrect && offset >= 0 &&
        static_cast<size_t>(offset) == state.offset + state.received &&
        length == state.requested - state.received;
    const size_t initialInterrupts = state.mode == Mode::TwiceEintr || state.mode == Mode::Mixed ? 2 :
        (state.mode == Mode::Eintr ? 1 : 0);
    if (state.interrupts < initialInterrupts) {
        ++state.interrupts; errno = EINTR; return -1;
    }
    if ((state.mode == Mode::ShortEintr || state.mode == Mode::ShortEintrEio) &&
        state.received > 0 && state.interrupts == 0) {
        ++state.interrupts; errno = EINTR; return -1;
    }
    if ((state.mode == Mode::FirstEio && !state.errors) ||
        ((state.mode == Mode::ShortEio || state.mode == Mode::ShortEintrEio || state.mode == Mode::ShortInvalid) && state.received >= 7) ||
        (state.mode == Mode::TwoShortEio && state.received >= 10)) {
        ++state.errors;
        errno = state.mode == Mode::ShortInvalid ? EINVAL : EIO;
        return -1;
    }
    if (!state.truncations && (state.mode == Mode::TruncateBeforeFirst ||
        (state.mode == Mode::TruncateAfterShort && state.received >= 7))) {
        const int writer = ::open(state.path.c_str(), O_WRONLY | O_CLOEXEC);
        const off_t newLength = state.mode == Mode::TruncateBeforeFirst ? 0 : offset;
        if (writer < 0 || ::ftruncate(writer, newLength) != 0 || ::close(writer) != 0) ::_exit(88);
        ++state.truncations;
    }
    size_t transfer = length;
    if ((state.mode == Mode::ShortOnce || state.mode == Mode::ShortEio || state.mode == Mode::ShortEintrEio ||
         state.mode == Mode::TruncateAfterShort || state.mode == Mode::ShortEintr || state.mode == Mode::ShortInvalid) &&
        !state.received) transfer = std::min<size_t>(transfer, 7);
    if (state.mode == Mode::AllShort || state.mode == Mode::Mixed) transfer = std::min<size_t>(transfer, 13);
    if (state.mode == Mode::TwoShortEio) transfer = std::min<size_t>(transfer, 5);
    const ssize_t count = original(fd, bytes, transfer, offset);
    if (count == 0) ++state.eof;
    if (count > 0) {
        if (static_cast<size_t>(count) < length) ++state.shorts;
        const size_t begin = static_cast<size_t>(offset);
        state.bytesCorrect = state.bytesCorrect && begin <= state.source.size() &&
            static_cast<size_t>(count) <= state.source.size() - begin &&
            std::memcmp(bytes, state.source.data() + begin, static_cast<size_t>(count)) == 0;
        state.received += static_cast<size_t>(count);
    }
    state.positionsCorrect = state.positionsCorrect && ::lseek(fd, 0, SEEK_CUR) == position;
    return count;
}

namespace {
std::pair<size_t, size_t> range(size_t total, int flow) {
    switch (flow) {
        case 0: return {0, 0};
        case 1: return {17, 0};
        case 2: return {31, 137};
        case 3: return {total - 37, std::numeric_limits<size_t>::max()};
        case 4: return {0, 173};
        case 5: return {total - 113, 109};
        default: throw std::runtime_error("unknown range");
    }
}

bool check(size_t length, int flow, Mode mode) {
    PrivateDirectory directory;
    dbms::LargeObjectManager objects(directory.path.string());
    const int id = objects.create();
    const int unrelated = objects.create();
    const std::string source = payload(length);
    require(id == 1 && unrelated == 2, "seed ids");
    require(objects.write(id, 0, source) && objects.write(unrelated, 0, "unrelated"), "seed write");
    const auto object = directory.path / ".lobjects/lo_1.dat";
    const auto other = directory.path / ".lobjects/lo_2.dat";
    struct stat status {};
    require(::stat(object.c_str(), &status) == 0, "seed stat");
    const auto [offset, requestedLength] = range(length, flow);
    const size_t wanted = requestedLength == 0 ? length - offset : std::min(requestedLength, length - offset);
    state = State{};
    state.device = status.st_dev; state.inode = status.st_ino;
    state.path = object.string(); state.source = source; state.offset = offset; state.requested = wanted; state.mode = mode;
    require(objectDescriptors() == 0, "seed fd count");
    state.armed = true;
    require(objects.read(unrelated) == "unrelated" && state.calls == 0, "unrelated scope");
    const std::string actual = objects.read(id, offset, requestedLength);
    state.armed = false;
    require(completeInjection() && state.bytesCorrect && state.positionsCorrect, "injection did not exercise exact intended reads");
    require(objectDescriptors() == 0, "read descriptor leaked");
    std::string expected = source.substr(offset, wanted);
    std::string expectedFile = source;
    if (hardError(mode)) expected.clear();
    if (mode == Mode::TruncateBeforeFirst) { expected.clear(); expectedFile.clear(); }
    if (mode == Mode::TruncateAfterShort) { expected = source.substr(offset, 7); expectedFile = source.substr(0, offset + 7); }
    require(bytesAt(object) == expectedFile && bytesAt(other) == "unrelated", "read changed file bytes outside explicit fixture truncation");
    require(objects.size(id) == expectedFile.size(), "read changed object size");
    const bool good = actual == expected;
    if (!good) std::cerr << "READ_MISMATCH length=" << length << " flow=" << flow << " mode=" << static_cast<int>(mode)
                         << " actualBytes=" << actual.size() << " expectedBytes=" << expected.size() << '\n';
    if (state.truncations) require(objects.write(id, 0, source), "restore own truncated fixture");
    require(objects.read(id, offset, requestedLength) == source.substr(offset, wanted), "healthy retry");
    dbms::LargeObjectManager reopened(directory.path.string());
    require(reopened.read(id, offset, requestedLength) == source.substr(offset, wanted), "fresh reader retry");
    require(objectDescriptors() == 0, "retry descriptor leaked");
    return good;
}

bool guard(int scenario) {
    PrivateDirectory directory;
    dbms::LargeObjectManager objects(directory.path.string());
    const int id = objects.create();
    const int empty = objects.create();
    const std::string source = payload(257);
    require(id == 1 && empty == 2 && objects.write(id, 0, source), "guard setup");
    switch (scenario) {
        case 0: return objects.read(0).empty();
        case 1: return objects.read(-1).empty();
        case 2: return objects.read(42).empty();
        case 3: return objects.read(empty).empty();
        case 4: return objects.read(id, source.size()).empty();
        case 5: return objects.read(id, source.size() + 1).empty();
        case 6: return objects.read(id, std::numeric_limits<size_t>::max()).empty();
        case 7: return objects.read(id, 19, 1) == source.substr(19, 1);
        case 8: return objects.read(id) == source && source.find('\0') != std::string::npos;
        case 9: return objects.read(id, 19, std::numeric_limits<size_t>::max()) == source.substr(19);
        default: throw std::runtime_error("unknown guard");
    }
}
} // namespace

int main() {
    int controls = 0, failures = 0;
    for (size_t length : {size_t(257), size_t(131101)}) {
        for (int flow = 0; flow < 6; ++flow) {
            for (int selected = 0; selected < 14; ++selected) {
                ++controls;
                try { if (!check(length, flow, static_cast<Mode>(selected))) ++failures; }
                catch (const std::exception& error) {
                    state.armed = false;
                    ++failures;
                    std::cerr << "CONTROL_ERROR length=" << length << " flow=" << flow << " mode=" << selected << ' ' << error.what() << '\n';
                }
            }
        }
    }
    for (int scenario = 0; scenario < 10; ++scenario) {
        ++controls;
        try { if (!guard(scenario)) ++failures; }
        catch (const std::exception& error) { ++failures; std::cerr << "CONTROL_ERROR guard=" << scenario << ' ' << error.what() << '\n'; }
    }
    std::cout << "LARGE_OBJECT_READ_FAILURE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures == 0 ? 0 : 1;
}
