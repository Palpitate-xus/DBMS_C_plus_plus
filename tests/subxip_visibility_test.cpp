#include "TableManage.h"
#include "HeapTupleHeader.h"
#include "Config.h"
#include <cassert>
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

    std::cout << "[SUBXIP] all passed\n";
    return 0;
}
