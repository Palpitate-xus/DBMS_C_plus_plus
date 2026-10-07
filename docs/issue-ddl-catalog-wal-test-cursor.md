# DDL catalog-WAL fixture used the wrong record cursor

## Actual failure and cause

The unchanged complete `ddl_transaction_skeleton_test.cpp` aborts at its
`assert(found)` on current production/test source `84e16a74`, not merely on
an older full-suite snapshot. The actual SQL remains
`CREATE TABLE wal_tbl (id INT)` through the genuine DDL executor.

`WALManager::ReadNextRecord(lsn)` is documented to skip the record at a
nonzero LSN. The fixture instead advanced that LSN by the returned record's
length, as if the API read exactly at the supplied position. That traversal
skips the real catalog record. The production WAL already contains it;
this is a false-negative test, not a repaired recovery implementation.

## Change

Use `earliestAvailableLsn()` and exact-position `ReadRecord(lsn)`, advancing
once by the actual current record length. Retain the original `found`
assertion, SQL, DDL calls and all nineteen complete fixture sections.
Strengthen the catalog assertion to check the genuine CREATE operation,
the length-prefixed `table` / `wal_tbl` payload, its nonzero transaction XID
and that same XID's later COMMIT record. Length/forward-progress assertions
guard the test traversal; there is no fabricated catalog record.

No production source, public header, storage format, WAL guard, original
DDL rollback control or protocol deadline changes.

## Verified current-source evidence

Artifacts: `/tmp/dbms-ddl-wal-cursor.ZMu3A0ka/`.

- `baseline-current84e-full.log`, session 92825: wrapper actually exits 1;
  unchanged full native actually aborts 134 at `assert(found)`.
- `candidate-current84e-full.log`, session 30243: complete nineteen-section
  fixture actually exits 0. The real WAL has three scanned records, the
  catalog CREATE for `wal_tbl` uses XID 14 in this run, and its matching
  COMMIT is present. XID 14 is observed evidence, not a hardcoded expectation.
- `adjacent-current84e-full.log`, session 17482: all ten complete unchanged
  native drivers actually exit 0: DDL bridge routing, DDL AST bridge,
  DDL/cache lock order, ADD-column clean rollback, WAL basic, full-page WAL,
  physical-identity codec/guard/generation and sequence-DDL rollback generation.
- The immutable external `verify-current-native.sh` verifies every one of
  the donor's 58 original normal-object receipts, production stamp, source/
  header/manifest/actual compiler flags and frozen binary before linking.
  All 58 current CPP inputs and relative header hashes are identical to
  exact `84e16a74` in
  `/tmp/dbms-canonical-enum-empty-final.1VJXdIIT/repo`.
  Drivers and shared stubs are freshly compiled under those normal O2 flags
  and linked to its matching 57 non-main production objects. Each native
  runs in a separately owned temporary CWD on default disk.
- The unchanged production frozen SHA-256 is
  `8b5debc0afdd93417922916fad51907f4ce4d770c04220e805f5f3fdb0d500b1`.
  `git diff 2cf7e790 -- src scripts cmake` is empty in this repair tree.
  This test-only change does not require, and does not claim, a new fresh-58
  production compilation or a sanitizer build.

## Scope remains open

This proves an actual DDL catalog record and matching transaction commit,
not ordinary heap-backed catalog relations, catalog WAL/MVCC atomicity,
all recovery crash windows or the full original 273-item audit. CAT-01,
TXN-05 and the broader DDL/recovery requirements remain partial/open.
The current original full suite has not been rerun; existing genuine
stale-temp, pending-TRUNCATE and other failures are not hidden or reclassified
by this fixture correction. No push, Actions activation or deferred
security/TDE work.
