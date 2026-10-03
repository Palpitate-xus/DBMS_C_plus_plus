# Issue 974: stop transaction IDs before 32-bit tuple-XID overflow

## Finding

The persistent transaction-ID generator uses `uint64_t`, but `HeapTupleFields::t_xmin` and `t_xmax` in `src/storage/HeapTupleHeader.h` are `uint32_t`. Tuple creation and xmax update paths narrow transaction IDs to those fields. There is no XID epoch, wraparound, or freezing scheme that could make a value beyond the 32-bit range unambiguous. Before this change, the allocator continued past `UINT32_MAX`, so tuple headers could silently store a different transaction ID and produce incorrect visibility decisions.

## Change

Commit `c18bd1bd` stops allocation once the next ID exceeds `UINT32_MAX`. The maximum representable tuple XID can still be allocated once; subsequent requests return the existing invalid-ID sentinel (`0`) and the persisted allocator state remains exhausted across restart. The heap on-disk format is unchanged.

This is deliberately fail-closed: databases approaching the boundary will stop obtaining transaction IDs. It prevents silent truncation but is not a complete XID lifecycle implementation. Epoch-aware tuple IDs, freezing, all-frozen state, MultiXact, oldest-xmin computation, and vacuum horizon rules remain unresolved; P0-08 is therefore still partial.

## Regression and verification

`tests/txnid_generator_test.cpp` seeds the persistent state at `UINT32_MAX`, verifies the last representable value is returned exactly once, checks the next ID and high-water mark on disk, and confirms a restarted generator continues to reject allocation. Against the old implementation, this boundary assertion failed because it allocated `UINT32_MAX + 1`.

The focused test passed:

```text
bash scripts/build_one_test.sh txnid_generator_test src/transaction/TxnIdGenerator.cpp
```

The final registered test suite also passed, including the production build, all native C++ tests, protocol tests, and E2E tests:

```text
DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120 DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh
All tests passed
```

No PostgreSQL differential result is claimed for this internal allocator boundary change. No push was performed; GitHub Actions remain disabled as requested.
