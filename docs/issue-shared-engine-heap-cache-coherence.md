# Independent StorageEngine heap owners silently overwrite committed rows

## Actual failure

On production source `a7d460c7`, two embedded engines run the original
SERIALIZABLE disjoint-key probe: both first read an empty indexed predicate,
then insert IDs 99 and 100 and both report successful COMMIT (`00000`). A
fresh third engine sees only ID 100. The permanent
`shared_heap_engine_rows_test.cpp` preserves the original operations and
adds the required two-row assertion. All three isolated baseline repetitions
failed that assertion; successful COMMIT alone was a false-positive oracle.

The baseline artifact is `/tmp/dbms-heap-shared-ownership.x89vCqkb/baseline`;
`baseline.log` retains all three failures. All 58 production source files,
all public headers, shared flags, manifest and stubs were compared before
copying the normal O2 production donor. The 58 original object signatures
are in `baseline/donor-signatures.audit.txt`. The copied objects are private,
not a dependency on the subsequently changing production cache.

The separately committed extent-publication fix `6c418739` serializes
physical marker publication and protects failed live owners. It does not
make two independent buffer caches coherent and does not close this issue.

## Cause and production change

Each engine retained its own PageAllocator, page-zero allocation state and
BufferPool for the same heap. Page locks serialized operations on different
cached copies. Both engines could choose the same physical page/slot; a
later writeback overwrote the preceding committed row.

StorageEngine now retains shared physical heap owners. A canonical-file
registry supplies one actual allocator and buffer pool, including page-zero
state and the existing allocation mutex. Per-file initializer slots prevent
duplicate owners without holding the global registry mutex during file I/O.
Main heaps, runtime partition forks and TOAST heaps use that owner. Partition
cache keys come from their real source metadata, not inferred SQL roles in a
filename. Expired idle registry entries are pruned.

The WAL manager is shared by canonical directory and physical directory
identity. A heap writeback barrier captures its shared WAL owner, not a raw
pointer owned by whichever engine first opened the heap. The first opener
or a temporary reader may exit without closing another engine's cache or
leaving its barrier dangling. Existing statement snapshots, tuple visibility,
page locks and SSI checks are retained; rows are not merged by displayed
value and valid commits are not replaced by blanket serialization failures.

Getters recognize changed physical file generations. A stale heap flush or
close cannot publish its old allocation header through a replacement path.
A runtime heap-cache miss must find an actual heap file: it is not CREATE
TABLE. The established empty-header locator of a partitioned parent remains
supported; it contains no tuples and real rows occupy separately owned forks.
A too-broad intermediate guard failed the original partition WAL-routing
positive at line 738; `verify-heap-final-v3.log` preserves that failure. The
guard is now tied to the actual non-partitioned physical-heap role, without
changing the original positive or weakening the displaced-file negative.
A displaced-file negative exposed the intermediate candidate creating
an empty replacement (`allocator=1, recreated=1`, exit 134); the runtime
getter now fails closed and preserves the recoverable original file.
`probe-missing.baseline.log` retains that failure. The old cached getter also
failed this negative, but retained the old descriptor without recreating the
path (`allocator=1, recreated=0`); that distinction is recorded in
`probe-missing-old.log` rather than relabeled as the same observed behavior.
The corresponding TOAST-generation negative also exposed an intermediate
getter inventing an empty replacement after the real chunk heap was displaced
(`recreated=1`, exit 134, `probe-toast-missing-v6.log`). The final runtime
TOAST getter requires its existing chunk heap too; after restoring the original
file, the original 10,000-byte value must still be readable.

An intermediate sanitizer generation run also exposed a real DROP/recreate
failure, not a sanitizer memory report: a second engine's cached/background
WAL getter recreated the deleted database directory. The deterministic
`shared_wal_dropped_database_test.cpp` reproduced `manager=1, resurrected=1`
and exit 134 (`probe-dropped-wal.baseline.log`). The getter now requires the
database still to exist and creates only its WAL child directory, never
the missing database parent. Background workers remain enabled. The original
intermediate failure is retained in `verify-heap-sanitized-v4.log`.

The V5 optimized run passed 17 native controls, five complete SSI repetitions
and nine generation repetitions, then the tenth generation run failed the
original CREATE-after-DROP assertion (exit 134). Its retained directory
contained only an empty `pg_wal`; no table list survived. A direct
`shared_wal_stale_directory_test.cpp` also proved that a retained manager
reported a successful flush after its WAL directory was renamed and recreated
that missing pathname (`flush=1, recreated=1`, exit 134,
`probe-wal-stale.baseline.log`). The unconditional recursive directory creation
was in `WALManager::acquireWalFileLock`, not merely the engine getter.

The final manager pins its actual directory descriptor. Open/flush/lock
operations reject a changed or removed directory generation, with a second
identity check after acquiring the file lock. The lock file is opened relative
to that descriptor, and a warm lock operation never creates directories.
Engine initialization uses `ensureOpen(false)` on its already-created WAL
directory; standalone explicit creation remains supported. This preserves
the original lock-failure and timeline tests without inventing an always-valid
opened pathname. The direct displaced-directory negative is green in
`probe-wal-stale-v6.log`.

## Scoped evidence

The public cache-owner types and WAL directory-owner layout changed: every one
of the 58 production objects and the stubs was freshly compiled after the final
header change. Exact source/header/flags receipts and the binary are in
`heap-v6`. No old-header object is used in its native or sanitizer consumers.
The final TOAST existing-file guard changes only TableManage.cpp: its matching
owned object is freshly replaced, while every other source and all 102 headers
are rechecked against that full fresh basis.
The final linked binary is `heap-optimized-v7/dbms_main`, SHA256
`d7d0204f0635330c0b08f1acf1535b7f46498a142c91ac54fc25ec5a24c4f109`;
`binary.sources.audit.txt` and `binary.headers.audit.txt` retain its matching
whole-source/header checks. Its three owned objects are O2 and the other 55
production objects are the matching fresh O0 basis.

Permanent stronger controls cover:

- The unchanged two-engine operations, both successful commits and both rows.
- The same real allocator/WAL owner, reads from both engines and a third
  observer, observer/first-opener destruction, rollback and failed-writeback
  retry, then reopen after all owners have exited.
- Distinct newly allocated pages with three wide attributes, both allocation
  headers agreeing on the three-page extent, and heap/database replacement
  generations without stale-owner publication.
- An exec-isolated writer process exiting without destructors, after two
  committed rows plus a stolen uncommitted third row. Actual WAL recovery
  must retain exactly 99 and 100 and undo 101.
- A missing runtime heap failing closed, with restoration of its original
  recoverable file and row.
- A stale WAL getter returning no owner after DROP, without recreating the
  database, followed by a legitimate CREATE obtaining one new shared owner.
- A displaced WAL directory rejecting the stale manager before and after a
  legitimate replacement manager opens; cold runtime opening cannot create
  a missing database parent, while standalone creation still works.

`verify-heap-final-v7.log` records optimized owned TableManage, PageAllocator
and WAL objects, fresh optimized drivers/stubs, the matching other 54 O0
objects, and 24 passing native controls, five passing repetitions of the
complete six-shape SSI fixture and ten passing generation repetitions
(terminal exit 0). Adjacent controls include checkpoint, bgwriter, MVCC,
WAL/page failures, rollback, partition routing and physical backup/restore.
The failed-writeback injection driver isolates its manual barrier by extending
that driver's background intervals; the original lost-row fixture and the
generation/DROP controls retain the normal background workers and intervals.
`verify-heap-sanitized-v7.log` records scoped ASan+UBSan instrumentation of
TableManage, PageAllocator, BufferPool, WAL, stubs and nine drivers, followed
by five generation repetitions, all passing with terminal exit 0; other 53
matching objects are uninstrumented, with leak detection disabled. This is
not a fully instrumented production build or the canonical full suite.

## Remaining independent boundaries

The strengthened `shared_heap_engine_index_rows_test.cpp` found a separate
real failure after heap rows were preserved: the primary B+Tree cache has
only one entry where two are required. The failure log is
`first/shared_heap_engine_index_rows_test.log`. That permanent strong test
belongs to the separate B+Tree physical-owner fix; it is not weakened and
heap-row success is not reported as complete storage/index coherence.
The matching new-header baseline in `index-baseline-v6` preserves this failure
and strengthened primary/secondary/composite and external-value controls.
The latter's seed read passes and both disjoint writers commit, then the
original three-row read returns zero rows rather than three. These negative
tests remain separate from this heap/WAL commit and require their own fix.

This change provides in-process ownership for StorageEngine users. It does
not claim coherent arbitrary standalone PageAllocators, hard-link aliases,
cross-process buffer caches, all access-method caches or universal SSI
correctness. This does not replace transactional database ownership for every
arbitrary concurrent standalone WAL/DDL producer.
The independently tested low-level failed-live extent-owner
contract remains in force. No git push or GitHub Actions change is included.
