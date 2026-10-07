# Proven no-effect savepoint rollback

The engine previously restored and recopied the whole database whenever a
savepoint owned a physical image, even if no image payload had changed.
That duplicated the physical restore tree's necessary syncs for reads and
input errors after earlier transactional DDL. The original disk-backed
default-15-second failures remain in their logs; deadlines and SQL assertions
are unchanged.

This change attaches a receipt only to an already-valid, full, owned physical
backup. It verifies the backup manifest and all payload hashes against the
actual active database, captures complete source/backup inode generations,
copies the entire five-vector catalog metadata model (including its currently
unserialized fields), and records the flushed WAL position. It does not infer
no effects from a dirty bit or mutation epoch alone.

Before skipping restore, the engine requires unchanged row/DDL/logical log
boundaries, persists actual cached catalog content, checks cache flushes,
flushes CLOG/WAL, proves clean/pin-free/current-owner heap buffers, verifies
real index generations and derived-map bytes, and compares both complete
physical generations twice. The existing `.oid_counter` writer may rewrite
identical bytes in place; only the same inode plus the entire saved byte hash
licenses that exception. All other payload changes, unknown files, image
damage, missing/closed owners, pending/in-flight/orphaned buffer pins and
uncertain cache states retain the original restore/error path. Cancellation
SQLSTATE 57014 is not swallowed as an optimization failure.

The `.lockmgr` directory is omitted exactly as in the original physical backup;
its contents are runtime coordination, and the original logical lock
checkpoint is still rolled back. The first collector incorrectly rejected
this directory and disabled the optimization. Actual byte/directory controls
identified that implementation error; the failed candidate logs are retained.
This is not a blanket directory/file exclusion.

## Actual native evidence

Private artifacts: `/tmp/dbms-savepoint-image-demand.9gzfRDln`.

- Baseline `baseline-native.log`, tool 85309, exit 134: the unchanged table's
  `.stc` inode was replaced by a full restore.
- Intermediate 33981/3280/52247/60607/43971/17012 failures are preserved. The
  FSM barrier was first conservative; later directory probes found the known
  `.lockmgr` omission. No unchanged-inode assertion was relaxed.
- New test harness corrections are explicitly distinct: 55941 initially
  assumed the raw `createTable` API registered a `pg_class` row; the fixture now
  creates that catalog row explicitly. 17355 initially omitted the documented
  native full-rollback backup opt-ins; `preserveTransactionBackupOnRollback`
  and `restoreTransactionBackupBeforeRowUndo` are now enabled before DDL,
  retaining the same full-abort absence assertion.
- `no-effect-final-native.log`, tool 13233, exit 0: unchanged inode; mutable
  cached catalog row restoration without a setter/epoch; direct ALTER layout;
  unknown physical sidecar removal; real row DML; repeated rollback; complete
  outer abort; and damaged-backup IO_ERROR plus transaction abort.
- `adjacent-final.log`, tool 33069, all 13 tests exit 0: buffer snapshot,
  derived-map owner/snapshot, savepoint stack/read-only/latest/namespace,
  insert-undo failure, index failure, clean DDL rollback, sequence-generation
  rollback, catalog publication, and original foreign-key action/restart.
- Buffer snapshot ASan/UBSan, 91480, exit 0: dirty/pinned/orphaned/retired and
  closed owners. Derived-map fsync-fault and sanitizer proof is recorded in
  `issue-derived-map-snapshot-ownership.md`.
- `no-effect-sanitized.log`, 63260, exit 0: the complete strong native test
  with TableManage, BufferPool, FSM/VM and test/stubs freshly ASan/UBSan
  instrumented; the other 54 matching production objects remain O0. This is
  explicitly a scoped sanitizer proof, not whole-source sanitization. Its
  initial helper include-path compilation failure is retained separately.

The complete 58-source O0 development rebuild (25857), then matching final
TableManage compilation/repeat/signatures/stamp (13233), exited 0. The final
binary SHA256 is
`0ec757238dc32f555640fa5c4f8b2c2c0fbba0f24da6b04ae7fab67f9c6cf609`.
This is not an optimized whole-workspace gate or a claim that old objects can
cross the new receipt/buffer/derived-map class layouts.

## Unchanged disk-backed protocol evidence

- Entire original Domain/FK six-case matrix, `no-effect-domain-wire.log`,
  5371, exit 0, original states/OIDs/rows/tags/deadline; server 3834261 exited 0.
- `no-effect-wire-final.log`, 92514, exit 0: original UNKNOWN input script
  repeated three times; entire original fromless demand script; read-only
  savepoint and DML lock-error scripts. Each has a separate matching log and
  explicit real-server terminal receipt.
- Entire original typed UNION ALL matrix, `no-effect-append-wire.log`, 38879,
  **exit 1**: values/types/states/effects were correct through the second
  writing UPDATE, then real-image `ROLLBACK TO app_case` exceeded 15 seconds
  at elapsed 53.705 seconds. Its finally ROLLBACK also timed out. Server
  3846526 eventually exited 0. The whole matrix was not counted passing.

There was concurrent independent native/compiler/filesystem load. These runs
prove scoped correctness and unchanged-default outcomes, not controlled
throughput, exclusive I/O causation, or resolution of every disk latency issue.
The prior ROOT Domain/FK timeout/misaligned-finally and original append/UNKNOWN
timeouts are not overwritten by these successful repetitions.

## Still separate

This patch does not yet reuse images across unchanged nested savepoints, so
creation still copies/syncs a fresh image. `alias-baseline.log`, 57147, exit 134,
retains the independent duplicate-image red and its subsequent strong alias,
duplicate-name, mutation, release, commit and abort controls. Restores after
real writes still use the complete original image. Loaded SPGiST or legacy
auto-increment caches without a complete durable-owner proof conservatively
keep that path. No physical-table, transaction, savepoint, or all-15-second
performance family is claimed closed.
