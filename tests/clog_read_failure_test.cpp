// test_sources: src/storage/CommitLog.cpp
#include "storage/CommitLog.h"
#include "common/DbError.h"
#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {
using Log = dbms::CommitLog;
using Status = Log::Status;
constexpr dbms::TxnId nearXid = 12, farXid = 50000, localXid = 40000, peerXid = 60000;
enum class Fault { None, ReadError, PartialError, Eintr, Short, RepeatedEintr,
                   ShortEintr, OpenError, OpenEintr, Missing, PartialEOF, BlockedParent };
bool armed = false;
Fault selected = Fault::None;
std::string target;
dev_t targetDevice = 0;
ino_t targetInode = 0;
size_t readCalls = 0, openCalls = 0, injections = 0;
bool replaceOnRead = false, replacementDone = false;
std::string replacementPath;

void require(bool good, const char* message) {
    if (!good) throw std::runtime_error(message);
}
bool targetsPath(const char* path) { return armed && std::strcmp(path, target.c_str()) == 0; }
bool failOpen(const char* path) {
    if (!targetsPath(path)) return false;
    ++openCalls;
    if ((selected == Fault::OpenError || selected == Fault::OpenEintr) && openCalls == 1) {
        ++injections;
        errno = selected == Fault::OpenError ? EIO : EINTR;
        return true;
    }
    return false;
}
bool targetFd(int fd) {
    struct stat status {};
    return armed && ::fstat(fd, &status) == 0 && status.st_dev == targetDevice && status.st_ino == targetInode;
}
void arm(Fault fault, const std::string& path) {
    selected = fault;
    target = path;
    struct stat status {};
    if (::stat(path.c_str(), &status) == 0) {
        targetDevice = status.st_dev;
        targetInode = status.st_ino;
    } else { targetDevice = 0; targetInode = 0; }
    readCalls = openCalls = injections = 0;
    armed = true;
}
std::string bytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "read fixture snapshot");
    return std::string(std::istreambuf_iterator<char>(input), {});
}
void writeFile(const std::string& path, const std::string& data) {
    const int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
    require(fd >= 0, "fixture file open");
    size_t done = 0;
    while (done < data.size()) {
        const auto n = ::write(fd, data.data() + done, data.size() - done);
        require(n > 0, "fixture file write");
        done += static_cast<size_t>(n);
    }
    require(::close(fd) == 0, "fixture file close");
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        (void)entry;
        ++count;
    }
    return count;
}
bool fails(Fault mode) {
    return mode == Fault::ReadError || mode == Fault::PartialError ||
           mode == Fault::OpenError || mode == Fault::BlockedParent;
}
bool special(Fault mode) {
    return mode == Fault::Missing || mode == Fault::PartialEOF || mode == Fault::BlockedParent;
}
}

extern "C" int open(const char* path, int flags, ...) {
    using Original = int(*)(const char*, int, ...);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "open"));
    if (!original) ::_exit(86);
    if (failOpen(path)) return -1;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); const auto mode = va_arg(args, unsigned int); va_end(args);
        return original(path, flags, mode);
    }
    return original(path, flags);
}
extern "C" FILE* fopen64(const char* path, const char* mode) {
    using Original = FILE*(*)(const char*, const char*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "fopen64"));
    if (!original) ::_exit(86);
    if (failOpen(path)) return nullptr;
    return original(path, mode);
}
extern "C" FILE* fopen(const char* path, const char* mode) {
    using Original = FILE*(*)(const char*, const char*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "fopen"));
    if (!original) ::_exit(86);
    if (failOpen(path)) return nullptr;
    return original(path, mode);
}
extern "C" ssize_t read(int fd, void* data, size_t size) {
    using Original = ssize_t(*)(int, void*, size_t);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "read"));
    if (!original) ::_exit(86);
    if (!targetFd(fd)) return original(fd, data, size);
    ++readCalls;
    if ((selected == Fault::ReadError && readCalls == 1) ||
        (selected == Fault::PartialError && readCalls == 2)) {
        ++injections; errno = EIO; return -1;
    }
    if ((selected == Fault::Eintr && readCalls == 1) ||
        (selected == Fault::RepeatedEintr && readCalls <= 3) ||
        (selected == Fault::ShortEintr && readCalls == 2)) {
        ++injections; errno = EINTR; return -1;
    }
    if (readCalls == 1 && (selected == Fault::PartialError || selected == Fault::Short || selected == Fault::ShortEintr)) {
        ++injections;
        return original(fd, data, std::min<size_t>(size, 64));
    }
    const auto result = original(fd, data, size);
    if (replaceOnRead && result > 0) {
        replaceOnRead = false;
        replacementDone = ::rename(replacementPath.c_str(), target.c_str()) == 0;
    }
    return result;
}

static bool scenario(int flow, Fault fault, const char* name) {
    const auto directory = "clog-" + std::to_string(flow) + "-" + name;
    const auto segment = directory + "/pg_xact/0";
    {
        Log seed(directory);
        seed.setStatus(nearXid, Status::Committed);
        seed.setStatus(farXid, Status::Aborted);
        require(seed.flush(), "seed flush");
    }
    Log backend(directory);
    if (flow) require(backend.getStatus(farXid) == Status::Aborted, "warm backend");
    if (flow >= 2) {
        backend.setStatus(localXid, Status::Committed);
        if (!special(fault)) {
            Log peer(directory);
            peer.setStatus(peerXid, Status::Aborted);
            require(peer.flush(), "peer flush");
        }
    } else if (flow == 1 && !special(fault)) {
        Log peer(directory);
        peer.setStatus(peerXid, Status::Aborted);
        require(peer.flush(), "refresh replacement");
    }
    if (fault == Fault::Missing) require(::unlink(segment.c_str()) == 0, "remove fixture segment");
    if (fault == Fault::PartialEOF) {
        // A real legacy short segment keeps its available bytes and zero tail.
        writeFile(segment + ".replacement", bytes(segment).substr(0, 64));
        require(::rename((segment + ".replacement").c_str(), segment.c_str()) == 0, "replace short fixture");
    }
    const std::string before = fault == Fault::Missing ? std::string{} : bytes(segment);
    if (fault == Fault::BlockedParent) {
        require(::rename((directory + "/pg_xact").c_str(), (directory + "/saved-xact").c_str()) == 0, "retain blocked directory");
        writeFile(directory + "/pg_xact", "not a directory");
    }
    const auto fdBefore = descriptors();
    bool threw = false, wrongException = false, flushed = false;
    Status observed = Status::SubCommitted;
    arm(fault, segment);
    try {
        if (flow == 3) backend.truncate(Log::kXidsPerSegment + 1);
        else if (flow == 2) flushed = backend.flush();
        else observed = backend.getStatus(farXid);
    } catch (const dbms::DbError& error) { threw = error.sqlState() == "58030"; wrongException = !threw; }
      catch (...) { wrongException = true; }
    armed = false;
    const auto actualReads = readCalls, actualOpens = openCalls, injected = injections;
    if (fault == Fault::BlockedParent) {
        require(::unlink((directory + "/pg_xact").c_str()) == 0, "remove fixture blocker");
        require(::rename((directory + "/saved-xact").c_str(), (directory + "/pg_xact").c_str()) == 0, "restore fixture directory");
    }
    const bool fdGood = fdBefore == descriptors();
    const Status farExpected = fault == Fault::Missing || fault == Fault::PartialEOF ? Status::InProgress : Status::Aborted;
    const Status nearExpected = fault == Fault::Missing ? Status::InProgress : Status::Committed;
    bool firstGood = !wrongException;
    bool diskGood = true, retryGood = true, pendingGood = true;
    if (flow == 3) {
        const bool exists = std::filesystem::exists(segment);
        firstGood = firstGood && !threw && exists == fails(fault);
        if (fails(fault)) {
            diskGood = exists && bytes(segment) == before;
            pendingGood = backend.getStatus(localXid) == Status::Committed;
            retryGood = backend.flush();
            Log verifier(directory);
            retryGood = retryGood && verifier.getStatus(nearXid) == nearExpected &&
                verifier.getStatus(farXid) == farExpected && verifier.getStatus(localXid) == Status::Committed &&
                verifier.getStatus(peerXid) == (!special(fault) ? Status::Aborted : Status::InProgress);
            backend.truncate(Log::kXidsPerSegment + 1);
        }
        retryGood = retryGood && !std::filesystem::exists(segment) &&
            backend.getStatus(localXid) == Status::InProgress;
    } else if (flow == 2) {
        firstGood = firstGood && !threw && flushed == !fails(fault);
        if (fails(fault)) diskGood = bytes(segment) == before;
        pendingGood = backend.getStatus(localXid) == Status::Committed;
        retryGood = backend.flush();
        Log verifier(directory);
        retryGood = retryGood && verifier.getStatus(nearXid) == nearExpected &&
            verifier.getStatus(farXid) == farExpected && verifier.getStatus(localXid) == Status::Committed &&
            verifier.getStatus(peerXid) == (!special(fault) ? Status::Aborted : Status::InProgress);
    } else {
        firstGood = firstGood && (fails(fault) ? threw : !threw && observed == farExpected);
        retryGood = backend.getStatus(farXid) == farExpected && backend.getStatus(nearXid) == nearExpected;
        if (flow == 1 && !special(fault)) retryGood = retryGood && backend.getStatus(peerXid) == Status::Aborted;
        diskGood = fault == Fault::Missing ? !std::filesystem::exists(segment) : bytes(segment) == before;
    }
    const bool faultReached = special(fault) || fault == Fault::None || injected > 0;
    const bool good = firstGood && diskGood && retryGood && pendingGood && fdGood && faultReached;
    std::cout << "CLOG_READ_FAILURE_CASE flow=" << flow << ' ' << name << " good=" << good
        << " first=" << firstGood << " disk=" << diskGood << " retry=" << retryGood
        << " read=" << actualReads << " open=" << actualOpens << " injected=" << injected
        << " fd=" << fdGood << '\n';
    return good;
}

static bool replacedAfterRead() {
    const std::string directory = "clog-read-replaced-owner";
    const std::string segment = directory + "/pg_xact/0";
    {
        Log seed(directory);
        seed.setStatus(nearXid, Status::Committed);
        require(seed.flush(), "identity seed");
    }
    auto replacement = bytes(segment);
    const auto offset = static_cast<size_t>(peerXid / 4);
    const auto shift = static_cast<unsigned>((peerXid % 4) * 2);
    replacement[offset] = static_cast<char>((static_cast<unsigned char>(replacement[offset]) & ~(3U << shift)) | (1U << shift));
    replacementPath = segment + ".replacement";
    writeFile(replacementPath, replacement);
    Log observer(directory);
    const auto fdBefore = descriptors();
    arm(Fault::None, segment);
    replaceOnRead = true;
    replacementDone = false;
    const auto first = observer.getStatus(peerXid);
    armed = replaceOnRead = false;
    const auto second = observer.getStatus(peerXid);
    const bool good = replacementDone && first == Status::Committed && second == Status::Committed &&
        observer.getStatus(nearXid) == Status::Committed && fdBefore == descriptors() && bytes(segment) == replacement;
    std::cout << "CLOG_READ_FAILURE_CASE replaced-after-read good=" << good
        << " replacement=" << replacementDone << " first=" << static_cast<int>(first)
        << " second=" << static_cast<int>(second) << '\n';
    return good;
}

int main() {
    try {
        const std::pair<Fault, const char*> modes[] = {
            {Fault::None, "ordinary"}, {Fault::ReadError, "read-eio"},
            {Fault::PartialError, "partial-eio"}, {Fault::Eintr, "eintr"},
            {Fault::Short, "short"}, {Fault::RepeatedEintr, "repeated-eintr"},
            {Fault::ShortEintr, "short-eintr"}, {Fault::OpenError, "open-eio"},
            {Fault::OpenEintr, "open-eintr"}, {Fault::Missing, "missing"},
            {Fault::PartialEOF, "partial-eof"}, {Fault::BlockedParent, "blocked-parent"},
        };
        size_t controls = 0, failures = 0;
        for (int flow = 0; flow < 4; ++flow) for (const auto& mode : modes) {
            ++controls;
            if (!scenario(flow, mode.first, mode.second)) ++failures;
        }
        ++controls;
        if (!replacedAfterRead()) ++failures;
        std::cout << "CLOG_READ_FAILURE_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        armed = false;
        std::cerr << "CLOG_READ_FAILURE_DRIVER_EXCEPTION " << error.what() << '\n';
        return 120;
    }
}
