# Unchanged catalog publication on disk-backed queries

Scope: repeated publication of an unchanged, already-durable catalog image.
This is not a declaration that all protocol timeouts, catalog concurrency, or
physical backup/restore performance are fixed.

## Actual failures and profiling

The diagnostic baseline is commit `0fdb53146836efbcc925f449061369583e169e16`,
using its frozen normal-O2 binary
`/tmp/dbms-canonical-pattern-owner.AR5UGRVA/dbms_main.pattern-owner.frozen`
(SHA-256 `c6d2ab03f0a4a34b4abe322e5f88d1e7b7251b42eb96b69529c2fba60ea7f4fb`).
All scripts retain their original SQL, row/type/counter assertions, disk-backed
`/tmp` directories, and default 15-second socket deadline. No `TMPDIR` or timeout
override is used. Other independently owned builds/tests were also active;
elapsed times therefore are not a controlled throughput comparison.

Artifacts are under `/tmp/dbms-default-disk-wire.tucbZgUx`:

- `catalog-clean-baseline.log`: the new permanent native control fails with
  status 134 because four unchanged `persistAll()` calls replace `pg_class.cat`.
- `baseline-unknown-v2.log`: the unchanged UNKNOWN-input protocol script
  completes; sampling nevertheless observes `D / jbd2_log_wait_commit` on
  ordinary non-literal SELECTs. This does not erase earlier ROOT timeouts.
- `trace-unknown.log` and `trace-unknown/server.strace`: the original UNKNOWN
  script completes under tracing; there are **350** `pg_catalog/pg_*.cat` file
  `fsync` calls. Each catalog replacement also synchronizes its directory.
- `trace-append.log` and `trace-append/server.strace`: the original typed-Append
  script actually times out after 15 seconds on `ROLLBACK TO app_case`, following
  `WITH h AS(SELECT 1) SELECT(SELECT 1 WHERE false UNION ALL SELECT 2 WHERE false)`.
  The owned worker remains in filesystem/journal waits while a complete
  `info.restore_staging...` image is synchronized. Individual recorded syncs
  include 1.646 seconds for `pg_wal/.timeline`, 1.595 seconds for the physical
  relation watermark, and over one second for function/directory images.
  The actual server and attached tracer both finish with status 0 during
  explicit cleanup. The runner owns/kills the server, not a tracer wrapper.

Two initial diagnostic include-path/argv setup failures occurred before DBMS
execution and are not counted as database reproductions.

## Root cause and implementation

`CatalogManager::persistAll()` previously rewrote all ten catalog files on every
call, even when nothing changed. Physical BEGIN/promotion, statement snapshots,
and catalog destruction call it. The old atomic protocol is file write, file
`fsync`, rename, and directory `fsync`; repeated unchanged calls unnecessarily
repeat all of that work.

The fix retains that complete durability protocol whenever an image needs
publication. It first serializes the **actual whole model**, using the unchanged
catalog row formats. It does not rely on mutation setters or a dirty boolean,
so edits through an existing row cannot disappear from the decision.

Successful file-and-directory synchronization establishes a receipt containing
the serialized bytes and physical file generation (device/inode, size,
mtime/ctime, and parent-directory device/inode). A shared, weakly registered
per-canonical-directory state serializes in-process publishers. Publication is
skipped only if both bytes and current physical generation match that receipt.
Cold or replaced images without such proof are published normally.

Each manager also retains its loaded/successfully published canonical baseline:

- A clean cache seeing a newer peer image reloads it rather than overwriting it.
- Unsaved local changes plus a changed peer image return failure without losing
  the local model or overwriting peer metadata. This is not an automatic merge.
- Missing images are reconstructed and synchronized; non-regular/removed
  database targets are not treated as successful clean publication.
- Partial success advances only the individual successful receipts/baselines.
  A rename followed by failed directory synchronization establishes **no**
  durable receipt; retry repeats publication.
- Reload validates the physical generations before/after reading and replaces
  rows and indexes atomically. Failed peer reload returns `false` from the bool
  persistence API and preserves the prior model.

The last point has an actual retained candidate failure:
`catalog-clean-reload-negative.log` (status 134) reports `XX001` escaping
`persistAll()` on an invalid peer `pg_type.cat`. It is fixed, not omitted.
`catalog-clean-fsync.log` retains an earlier fault-test placement error: the new
fault block changed the file before an earlier unchanged-file assertion. Only
the placement of that new block was corrected; all original assertions remain.

## Verification

Production inputs were frozen for `build-catalog-all58-O0.log`: all 58 production
objects were freshly compiled against the new private CatalogManager state.
After the atomic-reload correction, `build-catalog-reload-v2-O0.log` rebuilds the
one changed catalog source, links, repeats the build as up-to-date, and validates
all 58 per-object signatures plus the binary stamp. Final binary SHA-256:
`27d7c57008485bc41b654bbf9d1212ba891e4e33c012746879183984062e6b75`.
Flags use the ordinary shared build configuration with a final `-O0`; they are
not represented as a normal-O2 production build.

`catalog-adjacent-v2.log` completes all **12** matching native tests:
`catalog_clean_publication`, `catalog_persistence_failure`, `catalog_resolve`,
`catalog_service`, `catalog_snapshot`, `temp_alter_column_catalog`,
`json_xml_catalog_type_identity`, `domain_catalog_text`,
`shared_heap_engine_rows`, `shared_heap_engine_index_rows`, `phase5_remaining`,
and `foreign_key_action_dml`. The two-engine controls preserve both successful
commits, all rows/indexed rows, and restart; the SSI controls retain necessary
dangerous-structure aborts.

`catalog-clean-final-fault.log` completes the permanent native control with an
additional GNU `--wrap=fsync` build: one directory sync fails **after rename**,
and retry must replace/synchronize the image rather than accept a clean receipt.
`catalog-clean-final-sanitized.log` completes the same complete test with
ASan/UBSan instrumentation on all four test/catalog/OID/system-table translation
units, including this fault. Leak detection is disabled for the intentional
process-lifetime registries; this is not full-storage sanitizer coverage.

`candidate-trace-unknown.log` completes the unchanged disk/default-deadline
UNKNOWN matrix, with server/tracer terminal status 0. Its corresponding trace
has **32**, not 350, catalog-file syncs. This is evidence that repeated catalog
publication was removed, not a universal latency claim. Three additional
untraced UNKNOWN repeats in `candidate-unknown-repeat-{1,2,3}.log` also complete
with no failed assertions and explicitly terminated owned servers.

`candidate-append.log` completes the original typed-Append matrix with no failed
assertions on disk and the unchanged 15-second per-query deadline. Its complete
run is approximately 94 seconds and still samples journal waits during exact
snapshot backup/restore. A pass on this run does not invalidate the earlier
actual timeout under load.

`candidate-fromless.log` also completes the whole original FROM-less execution
demand matrix on disk with its original values, NULL/OIDs, sequence/effect and
15-second deadline assertions. `wire-final.log` records all three UNKNOWN
repeats and this final whole-script terminal success. No owned server or tracer
remains live after these runs.

## Remaining boundaries

- Whole-image savepoint backup/restore remains expensive. A sticky prior-DDL
  dirty flag currently causes a savepoint image to be restored and re-created
  even after an intervening read-only statement. Its demand/ownership needs a
  separate fix; the earlier 15-second failure remains a required regression.
- This does not implement multi-file catalog transactions, catalog MVCC,
  conflict merging, cross-process concurrent catalog publication, parser repair
  for every malformed legacy row, or transaction-wide durability of the OID
  allocator. Existing necessary syncs are not removed.
- Catalog-owned row pointers retain their existing lock/lifetime limitations.
  In-process refresh deliberately invalidates old model pointers; callers must
  use copied metadata rather than retain a pointer across a publication or
  concurrent catalog mutation.
- The newer ROOT retired-cache fix is not present in this exact `0fdb` donor.
  ROOT must independently rebuild/verify the combined public headers and source;
  these private results are not combined-production approval.
