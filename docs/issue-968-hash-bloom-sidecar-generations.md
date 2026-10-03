# Issue 968: Hash/Bloom sidecar loading and restore generations

Date: 2026-10-03
Source/test commit: `e1ef1122`
Status: locally committed; not pushed. `IDX-05` and `IDX-14` remain partial.

Runtime access to a declared Hash or Bloom index previously treated a missing
sidecar as a valid empty map. An already cached map was also trusted after its
backing file was deleted, atomically replaced, or truncated in place. This
could hide matching rows and could let a stale cache write back over a changed
generation. Explicit index creation and maintenance need the opposite
behavior: they must be able to start with an empty map and rebuild it from the
heap.

Runtime loading now uses strict `openExisting()`; deliberate CREATE/REINDEX,
DDL rewrite, recovery reset/rebuild, and VACUUM paths use `openForBuild()`.
Hash/Bloom caches record device, inode, size, and nanosecond mtime/ctime. A
generation mismatch makes runtime lookup fail closed, and the stale mapping is
discarded without destructor writeback before a replacement cache is loaded.
Physical restore distinguishes public backups that omit mutable UNLOGGED heap
forks from transaction snapshots that preserve them: omitted forks are reset
from init storage and all derived indexes are rebuilt; preserved forks keep
their rows while indexes are reindexed. Partial fork sets abort and roll back.

Verification:

- `bash scripts/build.sh`: passed.
- Focused C++ tests passed: `unlogged_recovery_test`, `alter_table_only_test`,
  `ddl_transaction_skeleton_test`, `missing_memory_index_guard_test`, and
  `insert_bloom_cleanup_test`.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120 DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh` ran the complete suite but exited 1: all C++ tests and all other protocol/E2E checks passed; `drop_multiple_fk_group_protocol_e2e_test.py` hit `ConnectionAbortedError` while connecting during server startup, before its SQL assertions.
- The failed E2E then passed once in isolation and in 10 consecutive additional runs. This supports a transient startup/connection failure but does not erase the full-suite failure; no all-green full-suite claim is made.
- `git diff --check` passed before commit.

Remaining work includes the original corrupt-index DML rollback path that can
report `rollback incomplete`, and the broader persistence, WAL, crash-point,
and cross-process guarantees required to complete either index checklist
family. The security/TDE audit item the user asked to skip remains deferred.
