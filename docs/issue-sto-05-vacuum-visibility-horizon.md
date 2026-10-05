# STO-05: VACUUM must not claim all-visible without a visibility horizon

## Finding and reproduction

`StorageEngine::vacuum()` used `liveCount() == slotCount()` after compaction as
proof that a page was all-visible. Those counts describe line pointers, not
whether every tuple version is visible to every active MVCC snapshot. This
VACUUM path has no `OldestXmin`/visibility-horizon check.

The regression in `tests/parallel_vacuum_test.cpp` keeps an old
`REPEATABLE READ` snapshot open, updates a row, and rolls back another insert
to leave an unused line pointer. The old snapshot continues to see the old
tuple version across VACUUM, but the prior heuristic marked its page all-visible.
The assertion that the VM bit stay clear failed before the fix.

## Fix

After compaction, this simplified VACUUM now clears the all-visible bit rather
than inferring visibility from line-pointer counts. This is intentionally
conservative: it can forgo an optimization, but does not publish an unsafe
visibility claim. It must not set the bit again until VACUUM checks the active
transaction horizon. Source inspection also found no current query/index-only
consumer of `VisibilityMap::isAllVisible()`, so this corrects the metadata
contract without claiming a present query-result change.

## Verification

- The targeted regression failed on the old implementation at
  `!vm->isAllVisible(1)` and passed after the change.
- `bash scripts/build.sh` passed (TLS stub; zlib enabled).
- `bash scripts/build_one_test.sh parallel_vacuum_test` passed, including the
  old-snapshot visibility case and existing parallel VACUUM cases.
- The full registered suite was not rerun on this revision; no PostgreSQL 18.6
  runtime differential was run.

## Remaining STO-05 work

This only removes one unsafe all-visible claim. FSM/VM crash recovery and
rebuild semantics, durable/checksummed metadata, all-frozen state, a real
visibility horizon, and index-only scan integration remain unimplemented or
unverified. STO-05 remains partial.
