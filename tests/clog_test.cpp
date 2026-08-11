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
        clog.flush();

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
        clog.setStatuses(entries);
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
        first.flush();
        second.flush();

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
        writer.flush();
        assert(observer.getStatus(200) == CommitLog::Status::Committed);
        writer.setStatus(201, CommitLog::Status::Aborted);
        writer.flush();
        assert(observer.getStatus(201) == CommitLog::Status::Aborted);
        std::cout << "[CLOG TEST] cross-backend refresh OK\n";
    }

    std::filesystem::remove_all(testDir);
    std::cout << "[CLOG TEST] all passed\n";
    return 0;
}
