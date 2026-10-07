# Back up current files without flushing an idle retired observer

The unchanged `alter_table_only_test` failed at its physical-backup assertion
after tablespace relocation and transaction snapshot restoration. A second
engine retained a clean old heap allocator and old WAL manager. Direct
observation showed `allocator_open=1 heap_current=0 dirty=0 wal_current=0`.
Fail-closed low-level flush correctly rejected those obsolete physical owners,
but database backup propagated that rejection despite having real current
catalog-resolved files and no old changes to publish.

BufferPool now distinguishes an open, regular main descriptor whose old
location/generation was replaced by an existing current regular file from a
closed descriptor, missing main file or sidecar-only mismatch. Under one pool
lock it checks dirty frames, ordinary/orphan pins and in-flight loads. Database
cache flush may skip only such an obsolete, proven idle owner.
It retains the actual old object for raw callers; it does not discard dirty
buffers or destroy an allocator while a reader might still hold its pointer.
Backup still copies real current files under the existing database-wide
exclusive ownership. The WAL background worker refreshes its valid current
namespace owner instead of repeatedly trying to flush an obsolete directory.

`clean_retired_heap_backup_test` checks current restored bytes through a real
backup/restore, preserves `oldAllocator.flush()==false`, and retains negative
dirty, active-pin, orphan-pin, closed-owner and missing-live-file cases. No implicit reopen
or file invention turns a physical fault into a success.

## Scoped evidence

Artifacts: `/tmp/dbms-clean-retired-cache.zcS6anO4/`.

- The unchanged original and read-only owner diagnostic are in the identity
  donor's `/tmp/dbms-heap-wal-identity.dcGDltHu/completion-baseline/`.
- Added control baseline 74484 exited 134 at observer backup. An exact archived
  `2e0545e0` source/header/flag donor repeat, `baseline-exact-v2.log` (57602),
  also exited 134 at that correct assertion. The earlier 82929 attempt failed
  to link because its harness omitted the cmake TLS-stub source manifest; it
  is retained but is not a database red.
- V1 all-58 rebuild (80054) exited 0. V1 native15 (51820) exited 1: the new
  control and 13 adjacent originals passed, and the unchanged tablespace test
  advanced past backup136 but exposed independent LOGGED-to-UNLOGGED DROP
  retirement failure. It was not labelled a passing group.
- V2 includes the completed-retirement dependency, plus the exact independent
  UNLOGGED lifecycle correction. Its new public BufferPool API was verified
  with a new full 58-unit `-O0` rebuild, 82047, exit 0; no old public-header
  objects were mixed. SHA-256:
  `5a8a692e5941da1cb2e5099bfb111146277842c028daf91c1e001a0810694913`.
  Source/header before/final receipts are under `build-v2/`.

- Final `native-v2.log`, 58276, exit 0: all 15 native executables passed,
  including the entire unchanged original tablespace test, the new fault
  control, FK rename recovery, shared heap owner/generation/rows/index rows,
  missing-live-file protection, B+Tree owner/generation/TOAST, and physical
  backup consistency/restore validation/manifest/replacement originals.
- Final `sanitized-v2.log`, 93965, exit 0: four scoped controls passed (new
  cache fault control, entire unchanged tablespace original, backup consistency
  and restore validation). TableManage, BufferPool, PageAllocator, stubs and
  drivers were freshly ASan/UBSan instrumented; the other 54 native dependency
  objects were matching uninstrumented `-O0`, with leak detection disabled.
  This is not full-engine sanitizer or production optimization evidence.
- Source/header, object and test before/final receipts are in `native-v2/`
  and `sanitized-v2/`; no original assertion or timeout changed.

- Before committing V2, an added real orphan-reader control (97754, exit 1)
  exposed `backup=1`: `getFrameInfo()` intentionally omits orphaned frames,
  so the two-check predicate could miss a retained pin after `invalidatePage`.
  The original ordinary-pin negative was preserved. V3 replaces that
  diagnostic-list check with one pool-locked idle/retired predicate including
  the actual orphan-pin and loading maps. V2 is not claimed as final completion.
- Final V3 full fresh 58-unit `-O0` rebuild, 48443, exit 0. Public headers were
  rebuilt again after the new atomic predicate API; no V2 old-header objects
  were borrowed. SHA-256:
  `e2891b42693488aa0e0fb31982594a333b80edc2f514eb9d060548319a83ea84`.
  Source/header before-final audits passed in `build-v3/`. Adding the verified
  retirement dependency after the build did not change any production byte;
  the extra dependency source/header receipts confirm this explicitly.
- Final `native-v3.log`, 80625, exit 0: all 18 executables passed. The original
  15 above, strengthened orphan-pin control, new six-phase UNLOGGED retirement
  control, original UNLOGGED recovery/zero-WAL control, and nine-phase
  retirement-completion fault control are all preserved.
- Final `sanitized-v3.log`, 3450, exit 0: all four scoped controls passed with
  freshly instrumented TableManage/BufferPool/PageAllocator, stubs and drivers,
  using the matching new-header full58 basis for the other 54 uninstrumented
  native dependency objects. Leak detection remained disabled. All before-final
  source/header/object/test audits passed; not full-engine sanitizer coverage.

Production O2 integration belongs to ROOT's later combined rebuild. The
completed-retirement and UNLOGGED lifecycle changes are distinct dependency
commits, not part of this cache root cause.

The atomic pool predicate does not hold a pool lock through a whole backup.
Actual engine DML is excluded by backup's existing exclusive database
ownership; low-level concurrent raw-allocator mutation is not a new supported
backup contract. Background skipping only defers a retained idle object; dirty
or pinned state on a later check remains an error. This is not whole storage,
partition relocation, TDE, SSI, or cross-process cache-family closure.
