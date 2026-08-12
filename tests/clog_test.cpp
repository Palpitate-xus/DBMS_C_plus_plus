// test_sources: src/storage/CommitLog.cpp
#include "CommitLog.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace dbms;

int main() {
    std::string testDir = "clog_test_dir";
    std::filesystem::remove_all(testDir);
    std::filesystem::create_directories(testDir);

    {
        CommitLog clog(testDir);
        clog.setStatus(1, CommitLog::Status::Committed);
        clog.setStatus(2, CommitLog::Status::Aborted);
        clog.setStatus(3, CommitLog::Status::InProgress);
        assert(clog.flush());

        assert(clog.getStatus(1) == CommitLog::Status::Committed);
        assert(clog.getStatus(2) == CommitLog::Status::Aborted);
        assert(clog.getStatus(3) == CommitLog::Status::InProgress);
        assert(clog.getStatus(100) == CommitLog::Status::InProgress);

        std::cout << "[CLOG TEST] basic set/get OK\n";
    }

    {
        CommitLog clog(testDir);
        assert(clog.getStatus(1) == CommitLog::Status::Committed);
        assert(clog.getStatus(2) == CommitLog::Status::Aborted);
        std::cout << "[CLOG TEST] persistence OK\n";
    }

    {
        CommitLog clog(testDir);
        std::vector<std::pair<TxnId, CommitLog::Status>> entries;
        for (TxnId i = 10; i < 20; ++i) {
            entries.emplace_back(i, (i % 2 == 0) ? CommitLog::Status::Committed
                                                 : CommitLog::Status::Aborted);
        }
        assert(clog.setStatuses(entries));
        for (TxnId i = 10; i < 20; ++i) {
            auto expected = (i % 2 == 0) ? CommitLog::Status::Committed
                                         : CommitLog::Status::Aborted;
            assert(clog.getStatus(i) == expected);
        }
        std::cout << "[CLOG TEST] batch set OK\n";
    }

    // Separate backend objects may cache the same pg_xact segment. Their
    // updates must be merged rather than allowing the later full-segment
    // flush to erase the earlier transaction state.
    {
        CommitLog first(testDir);
        CommitLog second(testDir);
        assert(first.getStatus(100) == CommitLog::Status::InProgress);
        assert(second.getStatus(101) == CommitLog::Status::InProgress);
        first.setStatus(100, CommitLog::Status::Committed);
        second.setStatus(101, CommitLog::Status::Aborted);
        assert(first.flush());
        assert(second.flush());

        CommitLog verifier(testDir);
        assert(verifier.getStatus(100) == CommitLog::Status::Committed);
        assert(verifier.getStatus(101) == CommitLog::Status::Aborted);
        std::cout << "[CLOG TEST] cross-backend merge OK\n";
    }

    // An already-open observer must notice an atomic segment replacement from
    // another backend instead of serving a stale cached status forever.
    {
        CommitLog observer(testDir);
        CommitLog writer(testDir);
        assert(observer.getStatus(200) == CommitLog::Status::InProgress);
        writer.setStatus(200, CommitLog::Status::Committed);
        assert(writer.flush());
        assert(observer.getStatus(200) == CommitLog::Status::Committed);
        writer.setStatus(201, CommitLog::Status::Aborted);
        assert(writer.flush());
        assert(observer.getStatus(201) == CommitLog::Status::Aborted);
        std::cout << "[CLOG TEST] cross-backend refresh OK\n";
    }

    // Truncation removes only segments that are safely persisted, while
    // retaining a segment if its durable save cannot acquire the file lock.
    {
        CommitLog clog(testDir);
        const TxnId oldXid = 300;
        const TxnId retainedXid = CommitLog::kXidsPerSegment + 300;
        clog.setStatus(oldXid, CommitLog::Status::Committed);
        assert(clog.flush());
        clog.setStatus(retainedXid, CommitLog::Status::Aborted);
        assert(clog.flush());
        clog.truncate(CommitLog::kXidsPerSegment + 1);

        CommitLog verifier(testDir);
        assert(verifier.getStatus(oldXid) == CommitLog::Status::InProgress);
        assert(verifier.getStatus(retainedXid) == CommitLog::Status::Aborted);
        assert(!std::filesystem::exists(std::filesystem::path(testDir) / "pg_xact" / "0"));
        std::cout << "[CLOG TEST] truncate OK\n";
    }

    {
        const std::string failureDir = "clog_truncate_failure_dir";
        std::filesystem::remove_all(failureDir);
        std::filesystem::create_directories(failureDir);
        CommitLog clog(failureDir);
        clog.setStatus(400, CommitLog::Status::Committed);
        assert(clog.flush());
        clog.setStatus(401, CommitLog::Status::Aborted);

        const auto lockPath = std::filesystem::path(failureDir) / "pg_xact" / ".clog.lock";
        std::filesystem::remove(lockPath);
        std::filesystem::create_directory(lockPath);
        clog.truncate(CommitLog::kXidsPerSegment + 1);

        // The failed save must not make truncate delete the last durable
        // image of the segment.
        CommitLog verifier(failureDir);
        assert(verifier.getStatus(400) == CommitLog::Status::Committed);
        assert(verifier.getStatus(401) == CommitLog::Status::InProgress);
        std::cout << "[CLOG TEST] truncate failure retention OK\n";

        std::filesystem::remove_all(failureDir);
    }

    std::filesystem::remove_all(testDir);
    std::cout << "[CLOG TEST] all passed\n";
    return 0;
}
