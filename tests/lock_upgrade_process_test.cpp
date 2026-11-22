#include "transaction/LockManager.h"
#include "common/DbError.h"

#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace {
bool sendByte(int fd, char value) noexcept {
    ssize_t n;
    do { n = ::write(fd, &value, 1); } while (n < 0 && errno == EINTR);
    return n == 1;
}
char readByte(int fd) noexcept {
    char value = '?';
    ssize_t n;
    do { n = ::read(fd, &value, 1); } while (n < 0 && errno == EINTR);
    return n == 1 ? value : '?';
}
using dbms::LockManager;
enum class Kind { Table, Row, Page };
const char* label(Kind kind) {
    return kind == Kind::Table ? "table" : kind == Kind::Row ? "row" : "page";
}
bool acquire(LockManager& manager, Kind kind, const std::string& db, bool exclusive) {
    if (kind == Kind::Table) return exclusive ? manager.lockExclusive("resource") : manager.lockShared("resource");
    if (kind == Kind::Row) return exclusive ? manager.rowLockExclusive("resource", 7) : manager.rowLockShared("resource", 7);
    return exclusive ? manager.pageLockExclusive(db, "resource", 3) : manager.pageLockShared(db, "resource", 3);
}
// The former donor only exposes the opportunistic fresh-token API; retain
// its baseline while exercising the explicit SQL NOWAIT API when present.
template <class Manager>
auto acquireTableExclusiveNoWait(Manager& manager, int)
    -> decltype(manager.lockTable("resource", LockManager::LockMode::Exclusive, true)) {
    return manager.lockTable("resource", LockManager::LockMode::Exclusive, true);
}
template <class Manager>
bool acquireTableExclusiveNoWait(Manager& manager, long) {
    return manager.lockExclusiveNoWait("resource");
}
template <class Manager>
auto supportsTableUpgradeNoWait(Manager& manager, int)
    -> decltype(manager.lockTable("resource", LockManager::LockMode::Exclusive, true), bool{}) {
    return true;
}
template <class Manager>
bool supportsTableUpgradeNoWait(Manager&, long) { return false; }
bool acquireExclusiveNoWait(LockManager& manager, Kind kind, const std::string& db) {
    if (kind == Kind::Table) return acquireTableExclusiveNoWait(manager, 0);
    if (kind == Kind::Row) return manager.rowLockExclusiveNoWait("resource", 7);
    return manager.pageLockExclusiveNoWait(db, "resource", 3);
}
void release(LockManager& manager, Kind kind, const std::string& db, size_t depth) {
    if (kind == Kind::Table) for (size_t n = 0; n < depth; ++n) manager.unlock("resource");
    else if (kind == Kind::Row) manager.rowUnlock("resource", 7);
    else manager.pageUnlock(db, "resource", 3);
}
bool same(const LockManager::LockCheckpoint& a, const LockManager::LockCheckpoint& b) {
    return a.tableCounts == b.tableCounts && a.tableModes == b.tableModes &&
        a.rowLocks == b.rowLocks && a.rowModes == b.rowModes && a.pageLocks == b.pageLocks && a.pageModes == b.pageModes;
}
bool setOfdLock(int fd, short type) {
    struct flock lock{};
    lock.l_type = type;
    lock.l_whence = SEEK_SET;
    lock.l_len = 1;
    return ::fcntl(fd, F_OFD_SETLK, &lock) == 0;
}
}

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    size_t controls = 0, failures = 0, cases = 0;
    const auto check = [&](bool good, const std::string& description) {
        ++controls;
        if (!good) { ++failures; std::cerr << "LOCK_UPGRADE_PROCESS_FAIL " << description << '\n'; }
    };

    // Previous versions interposed flock to force a conversion gap or gate
    // error. Those injected faults cannot target OFD locks; the preserved
    // legacy diagnostic is separate. Here every participant uses a real
    // process lock and failed conversion, without interposing variadic fcntl.
    for (const auto kind : {Kind::Table, Kind::Row, Kind::Page}) {
        const size_t kindControls = controls, kindFailures = failures;
        for (const std::string policy : {"timeout", "cancel", "success", "retain", "nowait"}) {
            ++cases;
            const std::string description = std::string(label(kind)) + " " + policy;
            const std::string db = "upgrade_process_private_" + std::to_string(cases);
            const bool owned = std::filesystem::create_directory(db);
            check(owned, description + " create private namespace");
            if (!owned) return 2;
            int requests[2], replies[2];
            const bool pipes = ::pipe(requests) == 0 && ::pipe(replies) == 0;
            check(pipes, description + " create coordination pipes");
            if (!pipes) return 2;
            const pid_t child = ::fork();
            check(child >= 0, description + " fork real peer");
            if (child < 0) return 2;
            if (child == 0) {
                ::alarm(10); ::close(requests[1]); ::close(replies[0]);
                LockManager peer; peer.setResourceNamespace(db); peer.setLockTimeout(30);
                bool held = false;
                for (;;) {
                    const char command = readByte(requests[0]);
                    if (command == 'S') {
                        held = acquire(peer, kind, db, false);
                        if (!sendByte(replies[1], held ? '1' : '0')) ::_exit(3);
                    } else if (command == 'D') {
                        if (held) release(peer, kind, db, 1);
                        held = false;
                        if (!sendByte(replies[1], '1')) ::_exit(3);
                    } else if (command == 'X') {
                        const bool got = acquire(peer, kind, db, true);
                        if (got) release(peer, kind, db, 1);
                        if (!sendByte(replies[1], got ? '1' : '0')) ::_exit(3);
                    } else if (command == 'N') {
                        // An independent manager has no local token to
                        // upgrade, so this probes the actual remote lock.
                        LockManager observer; observer.setResourceNamespace(db);
                        const bool got = acquireExclusiveNoWait(observer, kind, db);
                        if (got) release(observer, kind, db, 1);
                        if (!sendByte(replies[1], got ? '1' : '0')) ::_exit(3);
                    } else if (command == 'Z') {
                        if (held) release(peer, kind, db, 1);
                        ::_exit(0);
                    } else ::_exit(4);
                }
            }
            ::close(requests[0]); ::close(replies[1]);
            {
                LockManager manager; manager.setResourceNamespace(db);
                manager.setLockTimeout(policy == "success" ? 1000 : 60);
                const size_t depth = kind == Kind::Table ? 3 : 1;
                bool held = true;
                for (size_t n = 0; n < depth; ++n) held &= acquire(manager, kind, db, false);
                check(held, description + " parent holds SHARE");
                const auto original = manager.captureCheckpoint();
                check(sendByte(requests[1], 'S') && readByte(replies[0]) == '1',
                      description + " peer holds actual SHARE");
                bool peerReleased = false, during = true;
                unsigned ticks = 0;
                manager.setInterruptHandler([&] {
                    ++ticks;
                    during &= same(original, manager.captureCheckpoint());
                    // The second loop tick follows a real attempt against
                    // the peer's kernel-held shared lock.
                    if (ticks >= 2 && policy == "cancel")
                        throw dbms::DbError("57014", "upgrade cancellation");
                    if (ticks >= 2 && policy == "success" && !peerReleased)
                        peerReleased = sendByte(requests[1], 'D') && readByte(replies[0]) == '1';
                });
                bool upgraded = false;
                std::string state;
                try {
                    upgraded = policy == "nowait"
                        ? acquireExclusiveNoWait(manager, kind, db)
                        : acquire(manager, kind, db, true);
                } catch (const dbms::DbError& error) { state = error.sqlState(); }
                manager.clearInterruptHandler();
                check(policy == "nowait" || ticks >= 2, description + " actual conflict reached");
                check(policy == "success" ? upgraded && state.empty() : policy == "cancel"
                          ? !upgraded && state == "57014" : !upgraded && state.empty(),
                      description + " exact outcome");
                if (policy == "nowait") {
                    check(sendByte(requests[1], 'N') && readByte(replies[0]) == '0',
                          description + " independent NOWAIT respects remote SHARE");
                } else {
                    check(during, description + " original checkpoint retained while waiting");
                }
                if (policy != "success")
                    check(same(original, manager.captureCheckpoint()), description + " failure retains SHARE");
                if (policy == "retain") {
                    check(sendByte(requests[1], 'X') && readByte(replies[0]) == '0',
                          description + " peer cannot promote between failed calls");
                    check(!acquire(manager, kind, db, true), description + " second attempt remains blocked");
                    check(same(original, manager.captureCheckpoint()), description + " second failure retains SHARE");
                }
                manager.rollbackToCheckpoint(original);
                bool checkpointRestored = same(original, manager.captureCheckpoint());
                if (kind == Kind::Table && policy == "success" && supportsTableUpgradeNoWait(manager, 0)) {
                    // The SQL-oriented API can upgrade a sole holder NOWAIT;
                    // the older opportunistic placement API intentionally cannot.
                    const bool soleUpgrade = acquireTableExclusiveNoWait(manager, 0);
                    if (soleUpgrade) manager.rollbackToCheckpoint(original);
                    checkpointRestored = checkpointRestored && soleUpgrade &&
                        same(original, manager.captureCheckpoint());
                }
                check(checkpointRestored, description + " checkpoint restores SHARE and depth");
                if (!peerReleased)
                    check(sendByte(requests[1], 'D') && readByte(replies[0]) == '1', description + " release peer");
                check(sendByte(requests[1], 'X') && readByte(replies[0]) == '0', description + " parent blocks remote writer");
                release(manager, kind, db, depth);
                check(sendByte(requests[1], 'X') && readByte(replies[0]) == '1', description + " writer enters after release");
                check(manager.getLockHolds().empty() && manager.getLockWaits().empty(), description + " local state cleaned");
            }
            (void)sendByte(requests[1], 'Z'); ::close(requests[1]); ::close(replies[0]);
            int status = 0;
            check(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
                  description + " peer exits cleanly");
            std::filesystem::remove_all(db);
        }
        std::cout << "LOCK_UPGRADE_" << label(kind) << "_COMPLETE " << controls - kindControls
                  << " FAILURES " << failures - kindFailures << '\n';
    }

    // Directly witness the kernel property used by the production path:
    // a failed OFD read-to-write conversion does not release the read lock.
    // The peer is forked before either independent description is opened.
    const std::string kernelDb = "upgrade_kernel_private";
    const bool owned = std::filesystem::create_directory(kernelDb);
    check(owned, "kernel create private namespace");
    if (!owned) return 2;
    const auto path = std::filesystem::path(kernelDb) / "resource.lock";
    int requests[2], replies[2];
    const bool pipes = ::pipe(requests) == 0 && ::pipe(replies) == 0;
    check(pipes, "kernel create coordination pipes");
    if (!pipes) return 2;
    const pid_t child = ::fork();
    check(child >= 0, "kernel fork independent description");
    if (child < 0) return 2;
    if (child == 0) {
        ::alarm(10); ::close(requests[1]); ::close(replies[0]);
        const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
        if (fd < 0) ::_exit(3);
        for (;;) {
            const char command = readByte(requests[0]);
            if (command == 'S') {
                if (!sendByte(replies[1], setOfdLock(fd, F_RDLCK) ? '1' : '0')) ::_exit(3);
            } else if (command == 'D') {
                if (!sendByte(replies[1], setOfdLock(fd, F_UNLCK) ? '1' : '0')) ::_exit(3);
            } else if (command == 'X') {
                const bool acquired = setOfdLock(fd, F_WRLCK);
                if (acquired) (void)setOfdLock(fd, F_UNLCK);
                if (!sendByte(replies[1], acquired ? '1' : '0')) ::_exit(3);
            } else if (command == 'Z') {
                ::close(fd); ::_exit(0);
            } else ::_exit(4);
        }
    }
    ::close(requests[0]); ::close(replies[1]);
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    check(fd >= 0, "kernel open parent description");
    if (fd < 0) return 2;
    check(setOfdLock(fd, F_RDLCK), "kernel parent SHARE");
    check(sendByte(requests[1], 'S') && readByte(replies[0]) == '1', "kernel peer SHARE");
    errno = 0;
    const bool promoted = setOfdLock(fd, F_WRLCK);
    const int conversionError = errno;
    check(!promoted && (conversionError == EAGAIN || conversionError == EACCES),
          "kernel conflicting OFD conversion fails");
    check(sendByte(requests[1], 'D') && readByte(replies[0]) == '1', "kernel release peer SHARE");
    check(sendByte(requests[1], 'X') && readByte(replies[0]) == '0',
          "kernel failed conversion retains parent SHARE");
    check(setOfdLock(fd, F_UNLCK), "kernel release parent SHARE");
    check(sendByte(requests[1], 'X') && readByte(replies[0]) == '1', "kernel peer enters after release");
    ::close(fd);
    (void)sendByte(requests[1], 'Z'); ::close(requests[1]); ::close(replies[0]);
    int childStatus = 0;
    check(::waitpid(child, &childStatus, 0) == child && WIFEXITED(childStatus) && WEXITSTATUS(childStatus) == 0,
          "kernel peer exits cleanly");
    std::filesystem::remove_all(kernelDb);

    check(cases == 15, "all real table/row/page external schedules reached");
    if (controls != 241) { ++failures; std::cerr << "LOCK_UPGRADE_PROCESS_BAD_REACH " << controls << '\n'; }
    std::cout << "LOCK_UPGRADE_PROCESS_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
