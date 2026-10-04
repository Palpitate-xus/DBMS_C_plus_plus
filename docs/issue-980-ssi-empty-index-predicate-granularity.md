# Issue 980 — SSI granularity for empty indexed predicates

## Finding

`StorageEngine::filterRows` already recorded logical predicates for simple
binary-collated primary/secondary index keys, but every empty result also added
a relation-wide SIREAD marker. Two serializable transactions probing disjoint,
missing primary-key values and inserting their own distinct values therefore
appeared to conflict at relation granularity and one was rejected with a
serialization failure. Before the fix, the new regression reproducibly failed
at the first transaction's commit assertion.

## Change

`recordSsiIndexPredicate` now reports whether it recorded a usable predicate.
For an empty result, `filterRows` keeps the relation-wide fallback only when no
usable index predicate covers a query conjunct. A matching future row must
satisfy that indexed conjunct, so the recorded predicate remains a sound
superset; scans without a supported index predicate retain the old conservative
coverage. Non-empty scans continue to register page SIREAD coverage.

Source/test commit: `e808d50d` (`fix(txn): avoid false SSI conflicts on disjoint
empty index probes`).

## Verification

- Old behavior reproduced: `bash scripts/build_one_test.sh
  phase5_remaining_test` failed because the first disjoint-index transaction
  was rejected at commit.
- Final focused test: `bash scripts/build_one_test.sh phase5_remaining_test`
  passed. It covers conservative conflict detection for an unindexed empty
  predicate, both commits for disjoint empty primary-key probes, an overlapping
  predicate cycle that still aborts one transaction, disjoint-page concurrency,
  and a cross-page dangerous structure.
- Production build: `bash scripts/build.sh` passed and linked `dbms_main`.
- Full registered suite:
  `DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120
  DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh` exited 0 with
  `All tests passed` (registered C++, complete protocol test, and E2E tests).
- No PostgreSQL 18.6 direct oracle or differential was run for this issue.

## Remaining work

TXN-09 remains partial: this is a granularity improvement for simple indexed
conjuncts and a conservative fallback for other empty scans. It does not
implement PostgreSQL's complete relation/page/tuple/index-range predicate-lock
promotion across all access methods or the full SSI conflict graph.
