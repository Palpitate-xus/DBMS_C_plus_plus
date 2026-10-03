// test_sources: src/transaction/TxnIdGenerator.cpp

#include "transaction/TxnIdGenerator.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

constexpr uint32_t kMagic = 0x31444958u;
constexpr uint32_t kVersion = 1;

uint64_t checksum(const char* data, size_t size) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<unsigned char>(data[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

template <typename T>
void append(std::string& bytes, const T& value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void writeFile(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
    assert(out);
}

void writeLegacyState(const fs::path& path, uint64_t next, uint64_t highWater,
                      bool trailingByte = false) {
    std::string bytes;
    append(bytes, next);
    append(bytes, highWater);
    if (trailingByte) bytes.push_back('!');
    writeFile(path, bytes);
}

void writeCurrentState(const fs::path& path, uint64_t next, uint64_t highWater,
                       bool corruptChecksum) {
    std::string bytes;
    append(bytes, kMagic);
    append(bytes, kVersion);
    append(bytes, next);
    append(bytes, highWater);
    uint64_t sum = checksum(bytes.data(), bytes.size());
    if (corruptChecksum) sum ^= 1;
    append(bytes, sum);
    writeFile(path, bytes);
}

bool readCurrentState(uint64_t& next, uint64_t& highWater) {
    std::ifstream in(".txnid", std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
    if (bytes.size() != 32) return false;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t storedChecksum = 0;
    std::memcpy(&magic, bytes.data(), sizeof(magic));
    std::memcpy(&version, bytes.data() + 4, sizeof(version));
    std::memcpy(&next, bytes.data() + 8, sizeof(next));
    std::memcpy(&highWater, bytes.data() + 16, sizeof(highWater));
    std::memcpy(&storedChecksum, bytes.data() + 24, sizeof(storedChecksum));
    return magic == kMagic && version == kVersion &&
           storedChecksum == checksum(bytes.data(), 24);
}

int childMode(const std::string& mode) {
    auto& generator = dbms::TxnIdGenerator::instance();
    if (mode == "legacy") {
        if (generator.maxCommittedTxId() != 41 || generator.nextTxId() != 42 ||
            generator.maxCommittedTxId() != 42) return 1;
        uint64_t next = 0;
        uint64_t highWater = 0;
        return readCurrentState(next, highWater) && next == 43 && highWater == 42
            ? 0 : 1;
    }
    if (mode == "invalid") {
        return generator.nextTxId() == 0 ? 0 : 1;
    }
    if (mode == "exhausted") {
        return generator.nextTxId() == 0 ? 0 : 1;
    }
    if (mode == "heap_xid_boundary") {
        const uint64_t heapXidMax =
            std::numeric_limits<uint32_t>::max();
        if (generator.nextTxId() != heapXidMax ||
            generator.maxCommittedTxId() != heapXidMax ||
            generator.nextTxId() != 0) {
            return 1;
        }
        uint64_t next = 0;
        uint64_t highWater = 0;
        return readCurrentState(next, highWater) &&
                       next == heapXidMax + 1 && highWater == heapXidMax
                   ? 0 : 1;
    }
    if (mode == "save_failure") {
        if (generator.nextTxId() != 1 || generator.maxCommittedTxId() != 1 ||
            ::chmod(".", 0500) != 0) return 1;
        const uint64_t failed = generator.nextTxId();
        if (::chmod(".", 0700) != 0 || failed != 0 ||
            generator.maxCommittedTxId() != 1) return 1;
        return generator.nextTxId() == 2 && generator.maxCommittedTxId() == 2
            ? 0 : 1;
    }
    return 2;
}

bool runChild(const fs::path& executable, const fs::path& directory,
              const std::string& mode) {
    const pid_t pid = ::fork();
    assert(pid >= 0);
    if (pid == 0) {
        if (::chdir(directory.c_str()) != 0) _exit(125);
        ::execl(executable.c_str(), executable.c_str(), mode.c_str(), nullptr);
        _exit(126);
    }
    int status = 0;
    assert(::waitpid(pid, &status, 0) == pid);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

fs::path makeCase(const fs::path& base, const std::string& name) {
    fs::path path = base / name;
    fs::create_directories(path);
    return path;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 2) return childMode(argv[1]);

    const fs::path executable = fs::read_symlink("/proc/self/exe");
    const fs::path base = fs::temp_directory_path() /
        ("txnid_generator_test_" + std::to_string(::getpid()));
    fs::remove_all(base);
    fs::create_directories(base);

    const fs::path legacy = makeCase(base, "legacy");
    writeLegacyState(legacy / ".txnid", 42, 40);
    assert(runChild(executable, legacy, "legacy"));

    const fs::path truncated = makeCase(base, "truncated");
    writeFile(truncated / ".txnid", std::string(8, '\0'));
    assert(runChild(executable, truncated, "invalid"));

    const fs::path trailing = makeCase(base, "trailing");
    writeLegacyState(trailing / ".txnid", 5, 4, true);
    assert(runChild(executable, trailing, "invalid"));

    const fs::path inconsistent = makeCase(base, "inconsistent");
    writeLegacyState(inconsistent / ".txnid", 5, 5);
    assert(runChild(executable, inconsistent, "invalid"));

    const fs::path badChecksum = makeCase(base, "bad_checksum");
    writeCurrentState(badChecksum / ".txnid", 5, 4, true);
    assert(runChild(executable, badChecksum, "invalid"));

    const fs::path exhausted = makeCase(base, "exhausted");
    writeCurrentState(exhausted / ".txnid",
                      std::numeric_limits<uint64_t>::max(),
                      std::numeric_limits<uint64_t>::max() - 1, false);
    assert(runChild(executable, exhausted, "exhausted"));

    const fs::path heapXidBoundary = makeCase(base, "heap_xid_boundary");
    const uint64_t heapXidMax = std::numeric_limits<uint32_t>::max();
    writeCurrentState(heapXidBoundary / ".txnid", heapXidMax,
                      heapXidMax - 1, false);
    assert(runChild(executable, heapXidBoundary, "heap_xid_boundary"));
    assert(runChild(executable, heapXidBoundary, "exhausted"));

    const fs::path failedSave = makeCase(base, "save_failure");
    assert(runChild(executable, failedSave, "save_failure"));

    fs::permissions(failedSave, fs::perms::owner_all, fs::perm_options::replace);
    fs::remove_all(base);
    std::cout << "[TXNID] strict load, atomic allocation, tuple-XID boundary, and exhaustion OK\n";
    return 0;
}
