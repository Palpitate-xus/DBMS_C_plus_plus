#include "transaction/LockManager.h"
#include "common/DbError.h"

#include <cerrno>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
bool sendByte(int fd, char value) noexcept {
    ssize_t n;
    do { n = ::write(fd, &value, 1); } while (n < 0 && errno == EINTR);
    return n == 1;
}
char readByte(int fd) noexcept {
    char value = '?'; ssize_t n;
    do { n = ::read(fd, &value, 1); } while (n < 0 && errno == EINTR);
    return n == 1 ? value : '?';
}
struct Observation {
    bool armed = false, gapFault = false, gateFault = false, fired = false;
    dev_t device = 0; ino_t inode = 0;
    unsigned actualResourceAttempts = 0;
    int request = -1, reply = -1;
    char gapWriterResult = '?';
};
thread_local Observation observation;
}

// All normal calls execute the real Linux syscall. A selected conversion
// reproduces flock's documented remove-old-lock/failed-new-lock window;
// an actual child process probes it, rather than a mocked lock registry.
extern "C" int flock(int fd, int operation) noexcept {
    if (observation.armed && (operation & LOCK_EX) != 0) {
        struct stat status {};
        const bool resource = ::fstat(fd, &status) == 0 && status.st_dev == observation.device && status.st_ino == observation.inode;
        if (resource) {
            ++observation.actualResourceAttempts;
            if (observation.gapFault && !observation.fired) {
                observation.fired = true;
                (void)::syscall(SYS_flock, fd, LOCK_UN);
                if (sendByte(observation.request, 'G')) observation.gapWriterResult = readByte(observation.reply);
                errno = EWOULDBLOCK; return -1;
            }
        } else if (observation.gateFault && !observation.fired) {
            observation.fired = true;
            errno = EIO; return -1;
        }
    }
    return static_cast<int>(::syscall(SYS_flock, fd, operation));
}

namespace {
using dbms::LockManager;
enum class Kind { Table, Row, Page };
bool acquire(LockManager& manager, Kind kind, const std::string& database, bool exclusive) {
    if (kind == Kind::Table) return exclusive ? manager.lockExclusive("resource") : manager.lockShared("resource");
    if (kind == Kind::Row) return exclusive ? manager.rowLockExclusive("resource", 7) : manager.rowLockShared("resource", 7);
    return exclusive ? manager.pageLockExclusive(database, "resource", 3) : manager.pageLockShared(database, "resource", 3);
}
void release(LockManager& manager, Kind kind, const std::string& database, size_t depth) {
    if (kind == Kind::Table) for (size_t n = 0; n < depth; ++n) manager.unlock("resource");
    else if (kind == Kind::Row) manager.rowUnlock("resource", 7);
    else manager.pageUnlock(database, "resource", 3);
}
bool same(const LockManager::LockCheckpoint& a, const LockManager::LockCheckpoint& b) {
    return a.tableCounts == b.tableCounts && a.tableModes == b.tableModes &&
        a.rowLocks == b.rowLocks && a.rowModes == b.rowModes && a.pageLocks == b.pageLocks && a.pageModes == b.pageModes;
}
}

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    size_t controls = 0, failures = 0, cases = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "LOCK_UPGRADE_PROCESS_FAIL " << label << '\n'; }
    };
    for (auto kind : {Kind::Table, Kind::Row, Kind::Page})
        for (const std::string policy : {"timeout", "cancel", "success", "conversion_gap", "gate_error"}) {
            ++cases;
            const std::string database = "upgrade_process_private_" + std::to_string(cases);
            const bool owned = std::filesystem::create_directory(database);
            check(owned, "create exact private namespace");
            if (!owned) return 2;
            int requests[2], replies[2];
            const bool pipes = ::pipe(requests) == 0 && ::pipe(replies) == 0;
            check(pipes, "create real child coordination pipes");
            if (!pipes) return 2;
            const pid_t child = ::fork();
            check(child >= 0, "fork separate advisory-lock participant before parent acquires");
            if (child < 0) return 2;
            if (child == 0) {
                ::alarm(10); ::close(requests[1]); ::close(replies[0]); observation.armed = false;
                LockManager peer; peer.setResourceNamespace(database); peer.setLockTimeout(30);
                bool held = false;
                for (;;) {
                    const char command = readByte(requests[0]);
                    if (command == 'S') { held = acquire(peer, kind, database, false); if (!sendByte(replies[1], held ? '1' : '0')) ::_exit(3); }
                    else if (command == 'D') { if (held) release(peer, kind, database, 1); held = false; if (!sendByte(replies[1], '1')) ::_exit(3); }
                    else if (command == 'G' || command == 'X') {
                        const bool got = acquire(peer, kind, database, true);
                        if (got) release(peer, kind, database, 1);
                        if (!sendByte(replies[1], got ? '1' : '0')) ::_exit(3);
                    } else if (command == 'Z') {
                        if (held) release(peer, kind, database, 1);
                        ::_exit(0);
                    } else ::_exit(4);
                }
            }
            ::close(requests[0]); ::close(replies[1]);
            {
                LockManager manager; manager.setResourceNamespace(database);
                manager.setLockTimeout(policy == "timeout" ? 50 : 1000);
                const size_t depth = kind == Kind::Table ? 3 : 1;
                bool good = true;
                for (size_t n = 0; n < depth; ++n) good &= acquire(manager, kind, database, false);
                check(good, "parent owns actual shared kernel token");
                const auto original = manager.captureCheckpoint();
                const std::string prefix = kind == Kind::Table ? "table-" : kind == Kind::Row ? "row-" : "page-";
                std::filesystem::path resource;
                for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::path(database)/".lockmgr"))
                    if (entry.path().filename().string().rfind(prefix, 0) == 0) resource = entry.path();
                struct stat identity {};
                check(!resource.empty() && ::stat(resource.c_str(), &identity) == 0, "observe real resource inode, not transition gate");
                const bool sharedPeer = policy != "conversion_gap";
                if (sharedPeer) check(sendByte(requests[1], 'S') && readByte(replies[0]) == '1', "other process holds genuine compatible shared token");
                observation = Observation{};
                observation.armed = true; observation.device = identity.st_dev; observation.inode = identity.st_ino;
                observation.gapFault = policy == "conversion_gap"; observation.gateFault = policy == "gate_error";
                observation.request = requests[1]; observation.reply = replies[0];
                bool peerReleased = !sharedPeer, during = true;
                manager.setInterruptHandler([&] {
                    if (observation.actualResourceAttempts != 0) {
                        during &= same(original, manager.captureCheckpoint());
                        if (policy == "cancel" || policy == "conversion_gap")
                            throw dbms::DbError("57014", "process upgrade cancellation");
                        if (policy == "success" && !peerReleased) {
                            peerReleased = sendByte(requests[1], 'D') && readByte(replies[0]) == '1';
                        }
                    }
                });
                bool upgraded = false; std::string state;
                try { upgraded = acquire(manager, kind, database, true); }
                catch (const dbms::DbError& error) { state = error.sqlState(); }
                observation.armed = false; manager.clearInterruptHandler();
                check(policy == "gate_error" ? observation.fired : observation.actualResourceAttempts != 0,
                      "selected real kernel/transition-failure path reached");
                check(policy == "success" ? upgraded && state.empty() : (policy == "cancel" || policy == "conversion_gap") ?
                      !upgraded && state == "57014" : !upgraded && state.empty(), "exact outcome after real external contention/fault");
                check(during, "logical old checkpoint remains during external kernel wait");
                if (policy == "conversion_gap") check(observation.fired && observation.gapWriterResult == '0', "cooperating real child writer excluded inside forced conversion gap");
                if (policy != "success") check(same(original, manager.captureCheckpoint()), "failure retains exact original logical weak owner");
                manager.rollbackToCheckpoint(original);
                check(same(original, manager.captureCheckpoint()), "checkpoint restores original mode/depth after conversion");
                if (!peerReleased) check(sendByte(requests[1], 'D') && readByte(replies[0]) == '1', "release competing process before probing parent ownership");
                check(sendByte(requests[1], 'X') && readByte(replies[0]) == '0', "parent alone still excludes actual cross-process writer");
                release(manager, kind, database, depth);
                check(sendByte(requests[1], 'X') && readByte(replies[0]) == '1', "cross-process writer enters only after explicit original owner release");
                check(manager.getLockHolds().empty() && manager.getLockWaits().empty(), "local state fully cleaned");
            }
            (void)sendByte(requests[1], 'Z'); ::close(requests[1]); ::close(replies[0]);
            int status = 0;
            check(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "child completes all original coordination commands");
            std::filesystem::remove_all(database);
        }
    check(cases == 15, "all table/row/page external and transition fault schedules reached");
    if (controls != 232) { ++failures; std::cerr << "LOCK_UPGRADE_PROCESS_BAD_REACH " << controls << '\n'; }
    std::cout << "LOCK_UPGRADE_PROCESS_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
