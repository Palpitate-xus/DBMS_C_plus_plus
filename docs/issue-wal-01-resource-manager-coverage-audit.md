# WAL-01: resource-manager coverage remains partial

## Findings

The WAL has real checksummed records and working page/index/transaction
recovery paths, but it is not yet the authoritative log for every mutable
database resource. `src/storage/WAL.h` currently defines resource IDs for
heap, transaction, storage-manager, checkpoint, catalog, and index records.
Heap changes are represented by before/after full-page images, and B-tree and
hash sidecars use whole-file before/after images. The index header explicitly
limits this scheme to those access methods.

The inspected recovery image applier handles `RM_HEAP_ID` and `RM_INDEX_ID`;
`walCatalogChange()` logs only operation kind plus object type/name, not the
catalog row or file state needed for redo. There are no corresponding resource
records/replay handlers for FSM/VM, TOAST chunks, sequence state, multixact,
standby state, or the remaining specialized index implementations. Some paths
have separate safety mechanisms: commit flushes TOAST heap/index state before
publishing the main heap tuple, and specialized sidecar indexes are rebuilt at
transaction boundaries. Those mechanisms do not make WAL a complete resource
manager log.

## Evidence and verification

Focused existing tests passed:

- `bash scripts/build_one_test.sh wal_basic_test` — verifies heap before/after
  images, B-tree index file images, and commit records.
- `bash scripts/build_one_test.sh redo_crash_recovery_test` — verifies
  uncommitted insert rollback and committed insert/delete recovery.
- `bash scripts/build_one_test.sh wal_full_page_write_test` — verifies the
  checkpoint record and a post-checkpoint page image.

These prove the covered subset only. No code changed in this audit; the full
registered suite was not rerun, and no PostgreSQL 18.6 runtime differential
was run.

## Status

WAL-01 remains partial. Resource-manager records and redo/undo for all mutable
heap, index, catalog, FSM/VM, TOAST, sequence, multixact, and standby state are
still required. This audit records current boundaries and does not claim full
WAL durability or PostgreSQL-compatible recovery.
