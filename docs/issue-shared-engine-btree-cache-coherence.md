# Independent engines overwrite B+Tree cache state

## Actual failure preserved after the heap fix

The separate heap/WAL fix `fa765bc1` retains both committed physical rows. It
does not repair index node/header caches owned independently by each engine.
The original strengthened `shared_heap_engine_index_rows_test.cpp` therefore
still fails: both commits succeed, both heap rows and point reads exist, but
the first loaded primary tree's `allValues()` contains one entry instead of
two. The intermediate point-read success is not an index-integrity oracle.

`index-baseline-v6` retains source/header audits and three failing native
controls linked to the fresh 58-object heap/WAL basis. The first assertion of
the expanded primary/secondary/composite fixture also proves the primary
entry count is wrong. The external-value fixture first successfully reads its
seed, then both writers commit their distinct 10,000-byte values; reading the
three expected rows returns zero. This is a database usability/data-integrity
failure, not a display-only difference. The original and expanded assertions
are retained unchanged by the index implementation.

The first candidate exposed a mistake in the newly added direct-tree caller,
not an index resolution regression: with `pkColIndices=[0]`, the descriptor
encodes ID 99 as `99\x01`, whereas a legacy inline-only single-column key is
raw `99`. Its direct `search("99")` assertion failed after the original
entry-count failure was fixed (`btree-initial-v1` and both V1 candidate logs).
The caller now uses the actual `TableSchema::buildPKValue` and additionally
asserts that the unencoded raw key is absent. All SQL operations, entry counts,
payloads, rollback and generation expectations remain intact. This fixture
correction is not reported as a production fix or PostgreSQL differential.

## Independent production change

The primary, secondary/composite and TOAST B+Tree maps now retain one actual
shared physical tree per canonical file identity. They reuse the physical
owner initializer slots introduced by the heap fix. All operations therefore
use the same node cache, file header, buffer pool and existing reader/writer
tree mutex. Opening a cold tree serializes only that tree; the registry mutex
is not held across disk I/O. Cache hits and registry reuse require the current
physical file generation. A deliberately closed current owner is retained as
an I/O failure; a getter must not reopen it and turn failed-index mutations
into successful statements.

The canonical pathname is the ownership key only. The tree keeps its existing
`filePath()` spelling for index WAL images and unlogged-relation matching;
making only that path absolute would change the existing relation-role
comparison. Private REINDEX staging trees remain unique owners, and their
generation publication continues to make peer getters discard old trees.
Destroying an observer or the first opener releases its lease without closing
another engine's live shared tree. This does not turn standalone arbitrary
BPTrees into an in-process StorageEngine lease.

That distinction is covered by two independent candidate negatives. The V1/V2
loader reopened a deliberately closed cached owner and violated the original
`insert_index_failure_savepoint_test` and `index_build_failure_test` IO_ERROR
assertions. V3 retains the closed owner and both original negative contracts
pass. However, its new closed-owner DROP/CREATE control fails: the replacement
INSERT returns 58030 rather than succeeding. The former generation check only
checked an open buffer pool, so it could not distinguish a closed old file
from a closed current file. The complete failing probe is retained in
`probe-btree-closed-v3.log`.

The final candidate pins the identity of the successfully opened index file
with a read-only descriptor. `close()` releases its buffer pool but retains
that pin; DROP/CREATE and REINDEX can therefore invalidate the closed old
generation without reopening a same-generation failure. Explicit successful
reopen validates and replaces the pin, and destruction releases it. Keeping
the descriptor open also prevents inode reuse from making a replacement look
like the old generation. Existing open-pool sidecar identity checks remain.

## Scoped verification

All 58 production objects and stubs are rebuilt after the three public cache
value types change. The final pinned-descriptor BTree layout is rebuilt again
in `btree-v4` under `/tmp/dbms-heap-shared-ownership.x89vCqkb`; its
source/header/flag receipts are independent of the old-header heap and earlier
BTree objects.

Permanent stronger controls cover:

- Both original commits, both exact primary entries, indexed point reads and
  reopen after all engine owners exit.
- Primary/secondary/composite entries and shared owners, duplicate secondary
  keys, observer/first-opener destruction, rollback, REINDEX and peer reload.
- Three exact external values, both writers, rollback and all-owners-gone
  reopen. Values are checked byte-for-byte rather than through row count alone.
- An exec-isolated process ending without destructors after both commits and
  a stolen uncommitted heap/primary/secondary mutation. Recovery must retain
  exactly IDs 99 and 100 and exactly two entries in both indexes.
- Deliberately closed peer trees followed by DROP/CREATE and REINDEX. Each
  replacement must be open and contain only the expected generation's keys,
  while the existing same-generation closed-fault tests still require IO_ERROR.

The V3 scoped sanitizer group actually passed all 11 native controls and five
owner repeats, including both original closed-index negatives. Its optimized
group was stopped with exit 143 after the new generation probe failed; its
partial green controls are not reported as a passing complete group.
The V4 sanitizer driver was also stopped with exit 143 after the new owner
fixture gained RID-set assertions beyond its already-captured test manifest.
That superseded group is not a passing final proof. V5 rebuilds every driver
and stubs against the immutable, unchanged five instrumented components,
records current test and all linked object hashes, and audits them again.

Final scoped proofs under `/tmp/dbms-heap-shared-ownership.x89vCqkb`:

- `build-btree-v4.log`, tool session 79621: fresh all 58 production objects
  and fresh stubs with the final headers, exit 0. Four heap controls pass.
- `verify-btree-optimized-v4.log`, tool session 51710: 27 natives, five owner
  repeats and five full six-shape SSI repeats, exit 0. TableManage,
  PageAllocator, WAL and BPTree are compiled with the production O2 flags;
  the other 54 matching basis objects are O0. This is not an all-O2 build.
- `verify-btree-sanitized-v5.log`, tool session 4291: 12 natives plus five
  owner repeats, exit 0. Those four components and BufferPool, stubs and
  drivers use AddressSanitizer/UBSan; remaining matching components are not
  instrumented, and leak detection is disabled. This is a scoped sanitizer
  proof, not a fully instrumented production build.
- Both final groups audit all 58 source files, 102 headers and their exact
  test manifests successfully. V5 additionally audits all 57 linked native
  production objects. `btree-optimized-v4/dbms_main` has SHA-256
  `b269b6abf167bb45794c2e6e7fbf88d6d6cc232017aea2b975ba2901669da9b1`.

These controls use native storage APIs, not a PostgreSQL version differential.
The canonical full suite and production all-O2 combination remain ROOT-owned.

## Separate unchanged recovery failure remains open

An additional original `foreign_key_action_dml_test.cpp` is byte-identical to
ROOT's e6 formal-combination fixture. The fresh private current-BTree native
run (tool session 62228) exits 134 after its autocommit/index/TOAST, rollback
and table-rename assertions pass. Startup then rejects heap WAL LSN 8248 in
`foreign_key_rename_db`; the exact log is
`btree-optimized-v4/foreign_key_action_dml_test.log`. This is an additional
failing 28th control, not part of the passing 27-test group.

The operation is `alterTableRenameTable(parent, renamed_parent)`, not a database
rename. A CRC-validated WAL read records LSN 8248 as relation `parent`, page 1,
fork 0, xid 23, heap AFTER info 17 (`wal-renamed-parent-inspect-v2.log`, tool
session 72741 exit 0). The old-name physical heap/schema are absent while the
renamed files exist. Runtime existing-file rejection must remain intact;
neither recreating a missing old heap nor blindly skipping missing replay
targets is a recovery fix. Explicit durable table identity/name-generation
handling is a separate required follow-up. The initial inspection harness's
omitted WAL-open precondition also remains logged separately; that assertion
is not classified as a database failure.

## Remaining boundaries

This is a B+Tree ownership fix for in-process StorageEngine users, not a claim
that all access methods, standalone callers, hard-link aliases, cross-process
caches or all SSI/snapshot/lifecycle cases are correct. The original heap
lost-row, failed extent publication, stale WAL and displaced-heap negatives
remain registered independently. No push or GitHub Actions change is made.
