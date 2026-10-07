# Exported snapshots must retain the exporter's in-progress XID

## Actual failure and causal check

The complete original `snapshot_export_import_test.cpp` is unchanged. At line
182 its imported reader must still exclude the exporter's `alice` INSERT after
the writer commits. It aborted 134 on the matching unmodified current
`90feab5919e30982473c570b44e3fac1f707b3ec` production build.

`refreshReadView` intentionally removes the creator XID from the live active
set: the creator reads its own writes through the special own-XID/CID rules.
`exportSnapshot` copied that set unchanged. For another owner the creator is
foreign, not own. Before COMMIT its CLOG entry hides the row; after COMMIT the
missing XID in the imported active set incorrectly lets CLOG expose it.

An independent actual-API diagnostic used the unchanged 1d production objects:
exporter XID 1, xmin 1, xmax 2, exported own-in-xip 0, CLOG IN_PROGRESS.
Two real readers imported the original bytes versus a copy augmented with that
actual XID. Both excluded the INSERT before COMMIT. After COMMIT, with CLOG
Committed, the original reader returned one row and the augmented-copy reader
returned none. The writer's live view and original export remained unchanged.
This A/B diagnostic is evidence for the cause, not a replacement production
implementation or a copied visibility algorithm.

## Fix and contracts

Only `StorageEngine::exportSnapshot` changes. Its transfer copy gains the actual
exporter XID in sorted, duplicate-free `activeXids` when that XID is below the
retained xmax. An importer that re-exports an older snapshot has an own XID at
or beyond that xmax; the horizon already excludes it, so it is not added.

The live `ReadView`, transaction/command identity, CLOG, snapshot-acquisition
timing, import admission, serialized v2 format, subxip, SID, WAL and physical
storage are unchanged. Existing malformed-payload/version/truncation controls
remain intact. The original serialization fixture and actual new transfers
verify sorted/disjoint active/subxip lists and byte round trips.

## Permanent controls

`snapshot_exporter_visibility_test.cpp` is picked up by the existing canonical
standalone native-test glob. It uses real owners, heap values, typed bound plans
and NULL bitmaps, not fabricated tuple/snapshot algorithms. Its COMMIT and
ROLLBACK scenarios retain INSERT/DELETE/UPDATE visibility before and after the
exporter's terminal event, other active writers that later commit, two imports
of the same snapshot, re-export/re-import, released and rolled-back savepoint
writes, writer-own data, importer-own INSERT command-ID visibility, nonzero
serialized CID, unchanged live CID/combocid state, and fresh-snapshot controls.

`snapshot_export_import_reference18_test.py --reference18` is an explicit
reference-only oracle, using the existing PGREF environment/password contract.
Every connection verifies exactly `server_version_num=180006`. Four live
connections execute PostgreSQL `pg_export_snapshot()` and `SET TRANSACTION
SNAPSHOT`: two imports, a foreign active writer, and the exporter. Both COMMIT
and ROLLBACK cases preserve complete rows/NULLs, UPDATE/DELETE/INSERT, savepoint
release/rollback, writer-own data and importer-own INSERT controls. This oracle
does not claim a new frontend SQL snapshot command implementation.

The untouched original fixture now runs all seven sections, including its
post-failure import guards, database scoping, lazy REPEATABLE READ and statement
READ COMMITTED/READ UNCOMMITTED controls.

## Evidence

Local evidence directory: `/tmp/dbms-snapshot-exporter-xid.LwTYePFJ`.

- `old1d-exporter-xid-actual-observer.log` retains the first diagnostic compile
  error (missing explicit CommitLog include); the corrected actual A/B run is
  `old1d-exporter-xid-actual-observer-v2.log`, authoritative 0.
- `current90fe-matching58-normal-baseline-build.log`: 56 production units were
  migrated only after exact source/all-relative-header/flags/receipt/object-byte
  checks against the completed 1d normal epoch; the two array-changed CPPs were
  freshly built with official O2 flags. All 58 receipts and repeat build pass.
  Baseline frozen SHA256
  `af651bb8481396d2c098d6a5802b206c4bd6f174cca3ec64ee06747efcf59486`.
- `original-current90fe-native-full.log`: original whole fixture aborts 134 at
  line 182; new whole native aborts 134 with actual writer XID 5 missing from
  the transfer's active list (foreign XID 4 remains). Both failed full logs stay.
- `candidate-one-cpp-normal-build.log`: only TableManage freshly rebuilt,
  other 57 units matched to that current 58-unit epoch; all receipts, repeat
  and frozen checks pass. Candidate frozen SHA256
  `d1e0e3d442957f661d12414e589048ddd3d49fe8bc7ed8b0306db4515b63c5e1`.
- `candidate-v1-primary-native-full.log` and `final20-native-full.log`: original
  and new whole native fixtures pass; all 20 complete native tests pass, including
  external-XID snapshots, aborted recovery, deferred owners, map snapshots,
  savepoint images/stack, vacuum, subxip/CLOG/CID, FK and prepared transactions.
- `scoped1cpp-native-full.log`: four whole native fixtures pass with ASan+UBSan
  on TableManage plus drivers/stubs and 56 matching normal non-main units.
  Leak detection is disabled; this is not an all-58 sanitizer build.
  `final-cid-native-full.log` and `final-cid-scoped-native-full.log` separately
  verify the final added before/after-export CID/combocid assertions; the scoped
  rerun reuses the same frozen production SAN object and recompiles its driver.
- `permanent-reference18-exporter-xid-full.log`: the complete permanent strict
  PostgreSQL 18.6 reference oracle passes.
- `final12-whole-serial.log`: all 12 unchanged complete protocol scripts pass
  with their original SQL/assertions/deadlines, including isolation, query/read
  snapshot ownership, savepoint/locks, defaults, DELETE rollback, arrays, DML
  EXPLAIN, FK statement visibility and deferred constraints. Every owned server
  reaches fixture cleanup; there is no concurrent second candidate server.
- `audit-final-committed.log`: all production sources/relative headers, flags,
  58 receipts, unchanged object bytes, final test inputs and frozen bytes match.

The separately observed pre-existing view-trigger error-path timeout remains
OPEN and is not modified or declared fixed by this snapshot transfer change.
