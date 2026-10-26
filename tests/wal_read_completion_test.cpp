// test_sources: src/storage/WAL.cpp
// Complete actual WAL transfers; reject genuine EOF, hard errors and corruption.
#include "storage/WAL.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
using dbms::Lsn;
using dbms::WALManager;
enum class Mode { Normal, Eintr, TwiceEintr, HeaderShort, PayloadShort,
                  AllShort, Mixed, HeaderError, PayloadError, ShortError,
                  OpenEintr, OpenError };
enum class Flow { Record, Next, Checkpoint, FindCheckpoint, Reopen };
struct Identity { dev_t device{}; ino_t inode{}; uint32_t segment = 0; };
std::array<Identity, 2> identities{};
std::array<std::string, 2> segmentPaths{};
size_t identityCount = 0;
Lsn targetLsn = 0, targetEnd = 0;
std::vector<char> targetBytes;
Mode mode = Mode::Normal;
bool armed = false, bytesCorrect = true, positionsCorrect = true;
size_t reads = 0, shortReads = 0, interrupts = 0, hardErrors = 0, openFaults = 0, opens = 0;

void require(bool good, const char* label) {
    if (!good) throw std::runtime_error(label);
}
const Identity* identity(int fd) {
    struct stat status{};
    if (::fstat(fd, &status)) return nullptr;
    for (size_t i = 0; i < identityCount; ++i)
        if (status.st_dev == identities[i].device && status.st_ino == identities[i].inode)
            return &identities[i];
    return nullptr;
}
bool matchingPath(const char* path) {
    for (size_t i = 0; i < identityCount; ++i)
        if (segmentPaths[i] == path) return true;
    return false;
}
void arm(Mode selected) {
    mode = selected;
    reads = shortReads = interrupts = hardErrors = openFaults = opens = 0;
    bytesCorrect = positionsCorrect = true;
    armed = true;
}
bool positive(Mode selected) {
    return selected != Mode::HeaderError && selected != Mode::PayloadError &&
           selected != Mode::ShortError && selected != Mode::OpenError;
}
size_t segmentDescriptors() {
    size_t result = 0;
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        const auto name = entry.path().filename().string();
        char* end = nullptr;
        const long descriptor = std::strtol(name.c_str(), &end, 10);
        if (end && !*end && descriptor >= 0 && identity(static_cast<int>(descriptor))) ++result;
    }
    return result;
}
bool injectionComplete(Mode selected) {
    switch (selected) {
        case Mode::Normal: return !shortReads && !interrupts && !hardErrors && !openFaults;
        case Mode::Eintr: return interrupts == 1;
        case Mode::TwiceEintr: return interrupts == 2;
        case Mode::HeaderShort: case Mode::PayloadShort: return shortReads == 1;
        case Mode::AllShort: return shortReads > 1;
        case Mode::Mixed: return interrupts == 1 && shortReads > 1;
        case Mode::HeaderError: case Mode::PayloadError: return hardErrors == 1;
        case Mode::ShortError: return shortReads == 1 && hardErrors == 1;
        case Mode::OpenEintr: case Mode::OpenError: return openFaults == 1;
    }
    return false;
}
uint64_t hashFiles(const fs::path& directory) {
    uint64_t result = 14695981039346656037ULL;
    for (uint32_t segment = 0; segment < 2; ++segment) {
        const auto path = directory / (segment ? "000000010000000000000001" : "000000010000000000000000");
        if (!fs::exists(path)) continue;
        const int fd = ::open(path.c_str(), O_RDONLY);
        require(fd >= 0, "hash open");
        std::array<char, 65536> bytes{};
        while (true) {
            const auto n = ::read(fd, bytes.data(), bytes.size());
            require(n >= 0, "hash read");
            if (!n) break;
            for (ssize_t i = 0; i < n; ++i) { result ^= static_cast<unsigned char>(bytes[i]); result *= 1099511628211ULL; }
        }
        require(::close(fd) == 0, "hash close");
    }
    return result;
}
}

extern "C" int open(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "open"));
    if (!original) ::_exit(86);
    mode_t permissions = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list arguments; va_start(arguments, flags); permissions = va_arg(arguments, mode_t); va_end(arguments);
    }
    if (armed && matchingPath(path)) {
        ++opens;
        if (!openFaults && (mode == Mode::OpenEintr || mode == Mode::OpenError)) {
            ++openFaults; errno = mode == Mode::OpenEintr ? EINTR : EIO; return -1;
        }
    }
    return original(path, flags, permissions);
}

extern "C" ssize_t read(int fd, void* bytes, size_t length) {
    using Original = ssize_t(*)(int, void*, size_t);
    static const auto original = reinterpret_cast<Original>(::dlsym(RTLD_NEXT, "read"));
    if (!original) ::_exit(86);
    const auto* file = armed ? identity(fd) : nullptr;
    const off_t physical = file ? ::lseek(fd, 0, SEEK_CUR) : -1;
    const Lsn logical = file && physical >= 0 ? file->segment * WALManager::kSegmentSize + physical : 0;
    if (!file || physical < 0 || logical < targetLsn || logical >= targetEnd)
        return original(fd, bytes, length);
    ++reads;
    if (reads > 4096) ::_exit(87);
    positionsCorrect = positionsCorrect &&
        length <= WALManager::kSegmentSize - static_cast<uint64_t>(physical) &&
        length <= targetEnd - logical;
    const bool header = logical < targetLsn + sizeof(dbms::XLogRecHeader);
    const size_t requiredInterrupts = mode == Mode::TwiceEintr ? 2 :
        (mode == Mode::Eintr || mode == Mode::Mixed ? 1 : 0);
    if (interrupts < requiredInterrupts) { ++interrupts; errno = EINTR; return -1; }
    if (!hardErrors && ((mode == Mode::HeaderError && header) ||
            (mode == Mode::PayloadError && !header) ||
            (mode == Mode::ShortError && shortReads))) {
        ++hardErrors; errno = EIO; return -1;
    }
    size_t transfer = length;
    if ((mode == Mode::HeaderShort && header && !shortReads) ||
        (mode == Mode::PayloadShort && !header && !shortReads) ||
        (mode == Mode::ShortError && !shortReads))
        transfer = std::min<size_t>(length, 7);
    if (mode == Mode::AllShort || mode == Mode::Mixed) transfer = std::min<size_t>(transfer, 13);
    const ssize_t n = original(fd, bytes, transfer);
    if (n > 0) {
        if (static_cast<size_t>(n) < length) ++shortReads;
        const size_t offset = static_cast<size_t>(logical - targetLsn);
        bytesCorrect = bytesCorrect && static_cast<size_t>(n) <= targetBytes.size() - offset &&
            !std::memcmp(bytes, targetBytes.data() + offset, static_cast<size_t>(n));
        positionsCorrect = positionsCorrect && ::lseek(fd, 0, SEEK_CUR) == physical + n;
    }
    return n;
}

namespace {
struct Seed {
    fs::path directory;
    Lsn target, anchor, end;
    dbms::XLogRecord expected;
    std::unique_ptr<WALManager> reader;
    uint64_t originalHash;
    Seed(const fs::path& path, Lsn start) : directory(path), target(start), anchor(start - 32) {
        {
            WALManager writer(directory);
            require(writer.ensureOpen(), "seed open");
            std::vector<char> prefix(static_cast<size_t>(anchor - sizeof(dbms::XLogRecHeader)), 'P');
            require(writer.XLogInsert(dbms::RM_HEAP_ID, dbms::XLOG_HEAP_INSERT, 17, prefix) == 0, "seed prefix");
            require(writer.XLogInsert(dbms::RM_HEAP_ID, dbms::XLOG_HEAP_DELETE, 18, {}) == anchor, "seed anchor");
            std::vector<char> payload(89);
            for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<char>(i * 37);
            require(writer.XLogInsert(dbms::RM_CHECKPOINT_ID, dbms::XLOG_CHECKPOINT_ONLINE, 73, payload) == target, "seed target");
            require(writer.XLogInsert(dbms::RM_HEAP_ID, dbms::XLOG_HEAP_CLEAN, 74, {'z', '\0', 'w'}) != dbms::INVALID_LSN, "seed tail");
            end = writer.currentWriteLsn();
            require(writer.XLogFlush(end), "seed flush");
        }
        reader = std::make_unique<WALManager>(directory);
        require(reader->ensureOpen(), "seed reader");
        auto record = reader->ReadRecord(target);
        require(record.has_value(), "seed record"); expected = *record;
        require(expected.header.xl_prev == anchor && expected.data.size() == 92, "seed layout");
        originalHash = hashFiles(directory);
    }
    void select(const fs::path& path) const {
        require(!armed, "select outside injection");
        targetLsn = target; targetEnd = target + expected.header.xl_tot_len;
        targetBytes.resize(expected.header.xl_tot_len);
        std::memcpy(targetBytes.data(), &expected.header, sizeof(expected.header));
        std::memcpy(targetBytes.data() + sizeof(expected.header), expected.data.data(), expected.data.size());
        identityCount = 0;
        WALManager names(path);
        for (uint32_t segment = 0; segment < 2; ++segment) {
            const auto file = names.segmentPath(segment);
            struct stat status{};
            if (::stat(file.c_str(), &status)) continue;
            segmentPaths[identityCount] = file.string();
            identities[identityCount++] = {status.st_dev, status.st_ino, segment};
        }
        require(identityCount, "selected segments");
    }
    bool equal(const std::optional<dbms::XLogRecord>& record) const {
        return record && !std::memcmp(&record->header, &expected.header, sizeof(expected.header)) &&
               record->data == expected.data;
    }
};

bool invoke(Flow flow, WALManager& reader, const Seed& seed) {
    switch (flow) {
        case Flow::Record: return seed.equal(reader.ReadRecord(seed.target));
        case Flow::Next: return seed.equal(reader.ReadNextRecord(seed.anchor));
        case Flow::Checkpoint: return seed.equal(reader.readCheckpoint(seed.target));
        case Flow::FindCheckpoint: return reader.findLastCheckpointLsn() == seed.target;
        case Flow::Reopen: return reader.ensureOpen(false);
    }
    return false;
}

bool runCase(const Seed& seed, Flow flow, Mode selected, size_t layout) {
    seed.select(seed.directory);
    std::unique_ptr<WALManager> fresh;
    WALManager* reader = seed.reader.get();
    if (flow == Flow::Reopen) { fresh = std::make_unique<WALManager>(seed.directory); reader = fresh.get(); }
    const size_t descriptors = segmentDescriptors();
    arm(selected);
    const bool result = invoke(flow, *reader, seed);
    armed = false;
    bool good = result == positive(selected) && injectionComplete(selected) && positionsCorrect && bytesCorrect &&
                segmentDescriptors() == descriptors && reads <= 4096;
    if (positive(selected)) good = good && reader->currentWriteLsn() == seed.end;
    const size_t observedReads = reads, observedShorts = shortReads, observedIntr = interrupts,
                 observedHard = hardErrors, observedOpenFaults = openFaults;
    // No cleanup or rewritten bytes assist this retry: only disarm the adapter.
    const bool retry = invoke(flow, *reader, seed) && seed.equal(reader->ReadRecord(seed.target));
    good = good && retry && reader->currentWriteLsn() == seed.end && segmentDescriptors() == descriptors;
    std::cout << "WAL_READ_CASE layout=" << layout << " flow=" << static_cast<int>(flow)
              << " mode=" << static_cast<int>(selected) << " result=" << result
              << " reads=" << observedReads << " short=" << observedShorts << " intr=" << observedIntr
              << " hard=" << observedHard << " openFaults=" << observedOpenFaults
              << " retry=" << retry << " good=" << good << '\n';
    return good;
}

void corrupt(const Seed& seed, const fs::path& path, int kind) {
    WALManager names(path);
    const auto at = [&](Lsn location, const void* bytes, size_t size) {
        const int fd = ::open(names.segmentPath(static_cast<uint32_t>(location / WALManager::kSegmentSize)).c_str(), O_WRONLY);
        require(fd >= 0, "corrupt open");
        require(::pwrite(fd, bytes, size, location % WALManager::kSegmentSize) == static_cast<ssize_t>(size), "corrupt write");
        require(::close(fd) == 0, "corrupt close");
    };
    if (kind <= 1) {
        const Lsn stop = seed.target + (kind == 0 ? 10 : sizeof(dbms::XLogRecHeader) + 7);
        const auto segment = static_cast<uint32_t>(stop / WALManager::kSegmentSize);
        require(::truncate(names.segmentPath(segment).c_str(), stop % WALManager::kSegmentSize) == 0, "physical truncate");
        for (uint32_t later = segment + 1; later < 2; ++later) fs::remove(names.segmentPath(later));
    } else if (kind == 2) {
        const char changed = static_cast<char>(seed.expected.data[3] ^ 0x5a);
        at(seed.target + sizeof(dbms::XLogRecHeader) + 3, &changed, 1);
    } else {
        const uint32_t length = kind == 3 ? 31 :
            static_cast<uint32_t>(sizeof(dbms::XLogRecHeader) + WALManager::kMaxRecordPayload + 8);
        at(seed.target + offsetof(dbms::XLogRecHeader, xl_tot_len), &length, sizeof(length));
    }
}

bool guard(const Seed& seed, const fs::path& path, bool reopen, int kind) {
    fs::copy(seed.directory, path, fs::copy_options::recursive);
    std::unique_ptr<WALManager> reader = std::make_unique<WALManager>(path);
    if (!reopen) require(reader->ensureOpen(false), "guard warm reader");
    corrupt(seed, path, kind);
    const bool rejected = reopen ? !reader->ensureOpen(false) : !reader->ReadRecord(seed.target);
    reader.reset();
    // Repair only this deliberately corrupted private copy, not the seed.
    WALManager names(path);
    for (uint32_t segment = 0; segment < 2; ++segment) {
        const auto source = seed.reader->segmentPath(segment);
        if (fs::exists(source)) fs::copy_file(source, names.segmentPath(segment), fs::copy_options::overwrite_existing);
    }
    reader = std::make_unique<WALManager>(path);
    const bool healthy = reader->ensureOpen(false) && seed.equal(reader->ReadRecord(seed.target));
    reader.reset();
    fs::remove_all(path);
    return rejected && healthy;
}
}

int main() {
    char pattern[] = "/tmp/dbms-wal-read-completion.XXXXXX";
    const char* made = ::mkdtemp(pattern);
    if (!made) return 120;
    const fs::path directory(made);
    try {
        size_t controls = 0, failures = 0;
        const auto check = [&](bool good, const std::string& label) {
            ++controls;
            if (!good) { ++failures; std::cerr << "WAL_READ_FAIL " << label << '\n'; }
        };
        const std::array<Lsn, 4> starts{64, WALManager::kSegmentSize - 16,
                                      WALManager::kSegmentSize - 64, WALManager::kSegmentSize};
        for (size_t layout = 0; layout < starts.size(); ++layout) {
            Seed seed(directory / ("seed-" + std::to_string(layout)), starts[layout]);
            for (Flow flow : {Flow::Record, Flow::Next, Flow::Checkpoint, Flow::FindCheckpoint, Flow::Reopen})
                for (Mode selected : {Mode::Normal, Mode::Eintr, Mode::TwiceEintr, Mode::HeaderShort,
                        Mode::PayloadShort, Mode::AllShort, Mode::Mixed, Mode::HeaderError,
                        Mode::PayloadError, Mode::ShortError, Mode::OpenEintr, Mode::OpenError})
                    check(runCase(seed, flow, selected, layout), "public flow transfer");
            for (bool reopen : {false, true}) for (int kind = 0; kind < 5; ++kind)
                check(guard(seed, directory / ("guard-" + std::to_string(controls)), reopen, kind), "genuine EOF/CRC/length guard");
            seed.select(seed.directory);
            for (Lsn invalid : {seed.target + 1, seed.end, seed.end + 8, dbms::INVALID_LSN}) {
                arm(Mode::Normal); const bool rejected = !seed.reader->ReadRecord(invalid); armed = false;
                check(rejected && !reads && !opens, "invalid position without IO");
            }
            require(hashFiles(seed.directory) == seed.originalHash, "seed disk bytes unchanged");
        }
        require(controls == 296, "complete WAL read inventory");
        fs::remove_all(directory);
        std::cout << "WAL_READ_COMPLETION_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        armed = false;
        std::cerr << "WAL_READ_DRIVER_EXCEPTION " << error.what() << '\n';
        fs::remove_all(directory);
        return 120;
    }
}
