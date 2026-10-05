# WAL-08: stale temporary relation startup recovery

## Finding and fix

Startup runs WAL recovery before `cleanupStaleSessionTemporaryFiles()`, which
is necessary so persisted recovery work finishes before session objects are
discarded. If a backend crashed after a temporary relation name reached
`tlist.lst` but before its schema marker was complete, the recovery phase still
passed that name to `rebuildAllSpecializedIndexes()`. The generic rebuild then
treated the absent temporary relation as an error and aborted server startup,
so the later cleanup never got a chance to remove the stale name and files.

`rebuildAllSpecializedIndexes()` now skips the reserved session-temporary
physical-name namespace. Those relations cannot survive a backend restart;
the existing post-recovery cleanup removes their catalog names, physical forks,
and session catalog objects. Ordinary persistent relations still use the
existing fail-closed rebuild path.

## Verification

- Before the fix, the isolated stale-temp startup reproduction aborted with
  `[recovery] failed to rebuild specialized indexes` before temporary-file
  cleanup.
- `bash scripts/build_one_test.sh stale_temp_startup_recovery_test` — passed.
  The test leaves a temporary name and heap fork in a database without a
  completed temporary schema, constructs a new engine, and verifies startup
  succeeds, the ordinary table remains visible, and the stale name/fork are
  removed.
- `bash scripts/build_one_test.sh temp_table_ddl_test` — passed; session
  temporary CREATE/ALTER/DROP lifecycle remains unchanged.
- `bash scripts/build_one_test.sh temp_serial_owned_sequence_test` — passed;
  temporary owned-sequence setup and cleanup remain unchanged.
- `git diff --check` — passed before commit.

No full registered suite, standalone executable build, WAL-bearing temp crash
matrix, or PostgreSQL 18.6 differential was run for this fix.

## Remaining gaps

WAL-08 remains partial. This closes one startup ordering failure for stale
session-temp names, but does not establish crash semantics for temporary WAL,
2PC, sequence/DDL recovery, or logical slots. Those require separate recovery
state-machine and crash-window coverage.

Source/test commit: `327b693b` (not pushed).
