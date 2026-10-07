// test_sources: src/transaction/TxnIdGenerator.cpp
#include "transaction/TxnIdGenerator.h"
#include "common/DbError.h"

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <set>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

static void readExact(int fd, void* data, size_t length) {
    auto* bytes = static_cast<char*>(data);
    while (length != 0) {
        const auto count = read(fd, bytes, length);
        if (count < 0 && errno == EINTR) continue;
        assert(count > 0);
        bytes += count;
        length -= count;
    }
}

static pid_t writer(const char* executable, int writeFd, size_t count) {
    const auto child = fork();
    assert(child >= 0);
    if (child == 0) {
        const std::string fd = std::to_string(writeFd);
        const std::string iterations = std::to_string(count);
        execl(executable, executable, "--writer", fd.c_str(), iterations.c_str(),
              static_cast<char*>(nullptr));
        _exit(127);
    }
    return child;
}

static void finished(pid_t child) {
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--writer") {
        const int fd = std::stoi(argv[2]);
        const size_t iterations = std::stoul(argv[3]);
        for (size_t i = 0; i < iterations; ++i) {
            const uint64_t xid = dbms::TxnIdGenerator::instance().nextTxId();
            assert(xid != 0);
            assert(write(fd, &xid, sizeof(xid)) == sizeof(xid));
        }
        return 0;
    }
    auto& generator = dbms::TxnIdGenerator::instance();
    assert(generator.maxCommittedTxId() == 0);
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    int pipeFds[2];
    assert(pipe(pipeFds) == 0);
    const auto firstChild = writer(executable, pipeFds[1], 1);
    uint64_t firstXid = 0;
    readExact(pipeFds[0], &firstXid, sizeof(firstXid));
    finished(firstChild);
    const auto refreshedHorizon = generator.maxCommittedTxId();
    const auto parentXid = generator.nextTxId();
    std::cerr << "[TXNID CROSS PROCESS] child=" << firstXid
              << " parent_horizon=" << refreshedHorizon << " parent=" << parentXid << '\n';
    assert(firstXid == 1 && refreshedHorizon == firstXid && parentXid == 2);

    std::vector<pid_t> children;
    constexpr size_t processes = 6;
    constexpr size_t allocations = 12;
    for (size_t process = 0; process < processes; ++process)
        children.push_back(writer(executable, pipeFds[1], allocations));
    std::set<uint64_t> ids;
    for (size_t i = 0; i < processes * allocations; ++i) {
        uint64_t xid = 0;
        readExact(pipeFds[0], &xid, sizeof(xid));
        assert(xid > parentXid && ids.insert(xid).second);
    }
    for (pid_t child : children) finished(child);
    assert(ids.size() == processes * allocations);
    assert(*ids.begin() == parentXid + 1 && *ids.rbegin() == parentXid + ids.size());
    assert(generator.maxCommittedTxId() == *ids.rbegin());
    assert(generator.nextTxId() == *ids.rbegin() + 1);
    close(pipeFds[0]);
    close(pipeFds[1]);
    // A warmed singleton must not conceal later durable corruption behind
    // an old snapshot horizon, nor overwrite it with another allocation.
    std::ifstream input(".txnid", std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(input)), {});
    assert(bytes.size() == 32);
    bytes.back() ^= 1;
    {
        std::ofstream output(".txnid", std::ios::binary | std::ios::trunc);
        output.write(bytes.data(), bytes.size());
        output.flush();
        assert(output);
    }
    bool failedClosed = false;
    try {
        (void)generator.maxCommittedTxId();
    } catch (const dbms::DbError& error) {
        failedClosed = error.sqlState() == "58030";
    }
    assert(failedClosed && generator.nextTxId() == 0);
    std::cout << "[TXNID CROSS PROCESS] external horizon and concurrent durable allocations passed\n";
}
