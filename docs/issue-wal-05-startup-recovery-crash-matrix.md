# WAL-05: startup recovery crash matrix

## Finding and fix

The existing process-level crash matrix was not exercising the intended data
directory reliably: it omitted the required `-D` argument, used PostgreSQL
compatibility mode while issuing the extended-only `USE DATABASE` command, and
could kill a wrapper shell instead of `dbms_main`. The harness now supplies
the data directory, selects extended mode, and `exec`s the database process
before recording its PID.

With the harness corrected, four mid-transaction cases reproducibly failed
their restart with exit code 139; all post-commit and post-checkpoint cases
passed. The database had already completed WAL recovery and crashed while
rebuilding indexes. `dbms_main` owns a global `StorageEngine` whose constructor
runs recovery during cross-translation-unit static initialization. During
recovery, `forEachRow` iterated the process-wide active-transaction `std::set`
before that set's dynamic initialization was guaranteed to have run. The
uninitialized tree header led to a null-pointer dereference. This was a
startup failure, not evidence of corrupt WAL records.

The active-transaction mutex, set, and database map are now function-local
statics, whose initialization is guaranteed on first access. This removes the
cross-translation-unit initialization-order dependency while preserving the
existing registry and locking semantics.

## Verification

- `bash scripts/build.sh` — production build passed.
- `bash scripts/build_one_test.sh redo_crash_recovery_test` — passed, including
  an actual `SIGKILL` with an open transaction; committed rows survived and
  uncommitted rows remained invisible.
- `bash tests/crash_matrix_test.sh` — `PASS=12 FAIL=0`: WAL insert, TDE-enabled
  insert, mixed DDL/DML, and connection-pool workloads, each killed
  mid-transaction, post-commit, and post-checkpoint.
- `git diff --check` — passed before the source/test commit.

The TDE-enabled case is only a crash/recovery integration test; no TDE
cryptographic or security review was performed. The full registered suite and
a PostgreSQL 18.6 runtime differential were not run for this change.

## Status

WAL-05 remains partial. This fixes a real startup crash and validates the
listed matrix, but does not prove the custom before-image undo/replay design
equivalent to PostgreSQL's redo-based recovery state machine across all
steal/no-force, concurrent-checkpoint, partial-write, torn-page, and power-loss
windows. Those broader crash-consistency claims remain open.

Source/test commit: `a7c15da5` (not pushed).
