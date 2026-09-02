#include "storage/CommitLog.h"
#include "TableManage.h"
#include "HeapTupleHeader.h"
#include "Config.h"
#include <cassert>
#include <cstring>
#include <filesystem>
#include <iostream>

dbms::Config g_config;
using namespace dbms;

int main() {
    // Test 1: subTxnIds in ReadView are treated as active (invisible)
    {
        StorageEngine::ReadView rv;
        rv.creatorTxnId = 1;
        rv.upLimitId = 10;
        rv.lowLimitId = 100;
        rv.activeTxnIds = {};
        rv.subTxnIds = {50, 51};
        rv.commitLog = nullptr;

        // rowTxnId 50/51 are subtransactions in progress -> invisible
        assert(!rv.isVisible(50));
        assert(!rv.isVisible(51));

        // rowTxnId 20 is in [upLimitId, lowLimitId), not active, not subxip -> fallback visible
        assert(rv.isVisible(20));

        // rowTxnId 5 < upLimitId -> visible
        assert(rv.isVisible(5));

        // rowTxnId 200 >= lowLimitId -> invisible
        assert(!rv.isVisible(200));

        // creator's own rows are visible
        assert(rv.isVisible(1));

        std::cout << "[SUBXIP] basic subTxnIds visibility OK\n";
    }

    // Test 2: Empty subTxnIds does not affect normal visibility
    {
        StorageEngine::ReadView rv;
        rv.creatorTxnId = 1;
        rv.upLimitId = 10;
        rv.lowLimitId = 100;
        rv.activeTxnIds = {20, 21};
        rv.commitLog = nullptr;

        assert(rv.isVisible(5));
        assert(!rv.isVisible(20));
        assert(!rv.isVisible(21));
        assert(rv.isVisible(30));
        assert(!rv.isVisible(200));

        std::cout << "[SUBXIP] empty subTxnIds fallback OK\n";
    }

    // Test 3: HeapTupleHeader visibility respects subTxnIds
    {
        alignas(8) char buf[128] = {};
        auto* htup = castHeapHeader(buf);
        initHeapTupleHeader(htup, 50, 2, false, false);
        htup->t_fields.t_xmax = 0;

        StorageEngine::ReadView rv;
        rv.creatorTxnId = 1;
        rv.upLimitId = 10;
        rv.lowLimitId = 100;
        rv.activeTxnIds = {};
        rv.subTxnIds = {50};
        rv.commitLog = nullptr;

        // xmin=50 is in subxip -> invisible
        assert(!rv.isVisible(buf, sizeof(buf), 2));

        // Remove from subxip and set xmin committed via hint bit -> visible
        rv.subTxnIds.clear();
        setXminCommitted(htup);
        assert(rv.isVisible(buf, sizeof(buf), 2));

        std::cout << "[SUBXIP] heap header subTxnIds visibility OK\n";
    }

    // Test 4: CLOG status is authoritative for an xid inside the snapshot.
    {
        const std::string clogDir = "__t_subxip_clog";
        std::filesystem::remove_all(clogDir);
        CommitLog clog(clogDir);

        StorageEngine::ReadView rv;
        rv.creatorTxnId = 1;
        rv.upLimitId = 10;
        rv.lowLimitId = 100;
        rv.commitLog = &clog;

        // A missing CLOG entry is IN_PROGRESS and must remain invisible.
        assert(!rv.isVisible(20));
        clog.setStatus(20, CommitLog::Status::Committed);
        assert(rv.isVisible(20));
        clog.setStatus(21, CommitLog::Status::Aborted);
        assert(!rv.isVisible(21));

        // Being older than snapshot xmin means "finished", not "committed".
        clog.setStatus(5, CommitLog::Status::Aborted);
        clog.setStatus(6, CommitLog::Status::Committed);
        assert(!rv.isVisible(5));
        assert(rv.isVisible(6));

        std::filesystem::remove_all(clogDir);
        std::cout << "[SUBXIP] CLOG visibility is fail-closed OK\n";
    }

    // Test 5: tuple visibility combines CLOG outcome with snapshot timing.
    {
        const std::string clogDir = "__t_tuple_visibility_clog";
        std::filesystem::remove_all(clogDir);
        CommitLog clog(clogDir);
        clog.setStatus(5, CommitLog::Status::Aborted);
        clog.setStatus(6, CommitLog::Status::Committed);
        clog.setStatus(7, CommitLog::Status::Committed);
        clog.setStatus(20, CommitLog::Status::Committed);
        clog.setStatus(120, CommitLog::Status::Committed);

        StorageEngine::ReadView rv;
        rv.creatorTxnId = 1;
        rv.upLimitId = 10;
        rv.lowLimitId = 100;
        rv.commitLog = &clog;

        alignas(8) char buf[128] = {};
        auto resetTuple = [&](uint32_t xmin, uint32_t xmax = 0) {
            std::memset(buf, 0, sizeof(buf));
            auto* header = castHeapHeader(buf);
            initHeapTupleHeader(header, xmin, 2, false, false);
            header->t_fields.t_xmax = xmax;
            return header;
        };

        // Old aborted inserts stay invisible; old committed inserts remain.
        resetTuple(5);
        assert(!rv.isVisible(buf, sizeof(buf), 2));
        setXminCommitted(castHeapHeader(buf));
        assert(!rv.isVisible(buf, sizeof(buf), 2));
        resetTuple(6);
        assert(rv.isVisible(buf, sizeof(buf), 2));

        // Outcome hints cannot make transactions that were active or had not
        // started at snapshot creation visible retroactively.
        auto* header = resetTuple(20);
        setXminCommitted(header);
        rv.activeTxnIds = {20};
        assert(!rv.isVisible(buf, sizeof(buf), 2));
        rv.activeTxnIds.clear();
        header = resetTuple(120);
        setXminCommitted(header);
        assert(!rv.isVisible(buf, sizeof(buf), 2));

        // An aborted old DELETE leaves its row visible; a committed old
        // DELETE hides it. A delete active/new at snapshot time does not hide
        // the row even if it commits later and gains a hint.
        resetTuple(6, 5);
        assert(rv.isVisible(buf, sizeof(buf), 2));
        setXmaxCommitted(castHeapHeader(buf));
        assert(rv.isVisible(buf, sizeof(buf), 2));
        resetTuple(6, 7);
        assert(!rv.isVisible(buf, sizeof(buf), 2));
        header = resetTuple(6, 20);
        setXmaxCommitted(header);
        rv.activeTxnIds = {20};
        assert(rv.isVisible(buf, sizeof(buf), 2));
        rv.activeTxnIds.clear();
        header = resetTuple(6, 120);
        setXmaxCommitted(header);
        assert(rv.isVisible(buf, sizeof(buf), 2));

        std::filesystem::remove_all(clogDir);
        std::cout << "[SUBXIP] tuple outcome and snapshot timing OK\n";
    }

    std::cout << "[SUBXIP] all passed\n";
    return 0;
}
