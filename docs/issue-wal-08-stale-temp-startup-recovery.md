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

At the initial isolated fix, no full registered suite, standalone executable
build, WAL-bearing temp crash matrix, or PostgreSQL 18.6 differential was run.

## Subsequent frozen full-suite observation

The original registered 1492-entry run on frozen
`2442e4c77030e3ba9fd9a7ff86ee270245fb1770` has now terminated. It recorded
`stale_temp_startup_recovery_test`, `temp_table_ddl_test`, and
`temp_serial_owned_sequence_test` as PASS in that same run. The native fixtures
were freshly compiled and linked against byte-verified matching production
objects. This supersedes treating an older startup abort as the latest result
for this fixture; it does not change or erase the older failure evidence.

The overall run returned 1: 1481 PASS and 11 FAIL, with all 1492 entries observed.
It is not a current-HEAD full-suite pass, a fresh compilation of all production
units, or evidence for the missing crash/recovery matrix below.

Terminal receipt:
`/tmp/dbms-accepted2442-full1492.9obmw81r/completed-canonical-receipt.json`,
SHA256 `4005e670928e604302ca4b9073b24bb87064d3d136feea03a7a962e5ac6650ca`.
Original log:
`/tmp/dbms-accepted2442-full1492.9obmw81r/canonical-full-registered.log`,
SHA256 `66d5b5ffd1e20f8c1cbce0c827079fc37804f0a951f6db7d2866bb5f313f15ba`.
These are local temporary evidence, not a portable release artifact.

## Remaining gaps

WAL-08 remains partial. This closes one startup ordering failure for stale
session-temp names, but does not establish crash semantics for temporary WAL,
2PC, sequence/DDL recovery, or logical slots. Those require separate recovery
state-machine and crash-window coverage.

Source/test commit: `327b693b` (not pushed).
