// test_sources:
#include "HeapTupleHeader.h"
#include <cassert>
#include <iostream>

using namespace dbms;

int main() {
    // Test 1: Basic header initialization
    {
        alignas(8) char buf[64] = {};
        auto* htup = castHeapHeader(buf);
        initHeapTupleHeader(htup, 42, 3, false, false);

        assert(htup->t_fields.t_xmin == 42);
        assert(htup->t_fields.t_xmax == 0);
        assert(htup->t_fields.t_cid == 0);
        assert(htup->t_infomask2 == 3);
        assert((htup->t_infomask & HEAP_HASNULL) == 0);
        assert((htup->t_infomask & HEAP_HASVARWIDTH) == 0);
        assert(htup->t_hoff == computeHeapHeaderSize(3));
        std::cout << "[HEAP TUPLE HEADER TEST] basic init OK\n";
    }

    // Test 2: Header with null bitmap and varwidth flag
    {
        alignas(8) char buf[64] = {};
        auto* htup = castHeapHeader(buf);
        initHeapTupleHeader(htup, 100, 5, true, true);

        assert((htup->t_infomask & HEAP_HASNULL) != 0);
        assert((htup->t_infomask & HEAP_HASVARWIDTH) != 0);
        assert(htup->t_hoff == computeHeapHeaderSize(5));
        assert(htup->t_hoff >= sizeof(HeapTupleHeaderData) + (5 + 7) / 8);
        std::cout << "[HEAP TUPLE HEADER TEST] null/varwidth flags OK\n";
    }

    // Test 3: ctid get/set
    {
        alignas(8) char buf[64] = {};
        auto* htup = castHeapHeader(buf);
        initHeapTupleHeader(htup, 1, 1, false, false);

        ItemPointer ctid{123, 7};
        setCtid(htup, ctid);
        ItemPointer got = getCtid(htup);
        assert(got.pageId == 123);
        assert(got.offset == 7);
        std::cout << "[HEAP TUPLE HEADER TEST] ctid get/set OK\n";
    }

    // Test 4: Hint bits for xmin/xmax
    {
        alignas(8) char buf[64] = {};
        auto* htup = castHeapHeader(buf);
        initHeapTupleHeader(htup, 1, 1, false, false);

        assert(!xminCommitted(htup));
        assert(!xmaxCommitted(htup));

        setXminCommitted(htup);
        assert(xminCommitted(htup));
        assert(!xminInvalid(htup));

        setXmaxCommitted(htup);
        assert(xmaxCommitted(htup));
        assert(!xmaxInvalid(htup));

        setXminInvalid(htup);
        assert(xminInvalid(htup));
        assert(!xminCommitted(htup));

        setXmaxInvalid(htup);
        assert(xmaxInvalid(htup));
        assert(!xmaxCommitted(htup));

        std::cout << "[HEAP TUPLE HEADER TEST] hint bits OK\n";
    }

    // Test 5: Null bitmap manipulation
    {
        alignas(8) char buf[64] = {};
        auto* htup = castHeapHeader(buf);
        initHeapTupleHeader(htup, 1, 4, true, false);

        // Default init sets all bits to 0 (all nulls in PG convention)
        assert(isNull(htup, 0));
        assert(isNull(htup, 3));

        setNotNull(htup, 0);
        setNotNull(htup, 2);
        assert(!isNull(htup, 0));
        assert(isNull(htup, 1));
        assert(!isNull(htup, 2));
        assert(isNull(htup, 3));

        setNull(htup, 0);
        assert(isNull(htup, 0));

        std::cout << "[HEAP TUPLE HEADER TEST] null bitmap OK\n";
    }

    // Test 6: Tuple data offset (t_hoff alignment)
    {
        for (int natts : {1, 2, 3, 8, 11, 16}) {
            alignas(8) char buf[128] = {};
            auto* htup = castHeapHeader(buf);
            initHeapTupleHeader(htup, 1, static_cast<uint16_t>(natts), true, false);
            assert(htup->t_hoff % 8 == 0);
            assert(getTupleData(htup) == buf + htup->t_hoff);
        }
        std::cout << "[HEAP TUPLE HEADER TEST] t_hoff alignment OK\n";
    }

    std::cout << "[HEAP TUPLE HEADER TEST] all passed\n";
    return 0;
}
