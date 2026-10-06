# ADD COLUMN: keep pure declaration rejection out of physical restore

## Confirmed root cause and change

`DdlExecutor::executeAlterTable` marked its physical snapshot dirty before
converting an `ADD COLUMN` declaration. `columnDefToColumn` rejects an explicit
`NULL` constraint on a SERIAL type with `42601` without changing storage. The
dirty flag nevertheless sent the failed statement through whole-database
physical restore, replacing heap files and closing database caches.

For `ADD COLUMN`, the dirty mark now follows successful declaration conversion
and still precedes `alterTableAddColumn`. Other ALTER actions retain their
existing dirty mark. This is not a SERIAL-name special case. Earlier successful
actions in the same statement keep the snapshot dirty, so a later invalid
declaration still rolls back their changes. A pre-existing outer DDL backup is
not cleared or replaced by a clean rejection.

## Decisive native regression

`tests/ddl_add_column_clean_rollback_test.cpp` pins an open descriptor to the
target heap before the invalid ALTER. This prevents inode reuse from concealing
a physical replacement. It checks the path's device/inode and the pinned
file's link count, in addition to exact `42601`, unchanged rows/schema, backup
ownership, transaction state and lock release.

The test covers standalone rejection; rejection after prior transactional DML;
rejection after prior successful transactional DDL; a first successful ADD and
a later invalid ADD in one statement; full outer ROLLBACK; and a valid nullable
ADD. It is automatically discovered by the existing `tests/*_test.cpp` runner.

Baseline `fd183ec3` actually failed with exit 134: pinned heap
`64512:77491338` became `64512:77491619`, with the old inode's link count zero.
The candidate preserves inode/link count in all three pure-rejection controls
and passes the complete test, including the dirty multi-action rollback cases.

## Verification and retained evidence

Artifacts: `/tmp/dbms-serial-null-diagnosis.dyv6Aqac/`.

- Baseline: `candidate/tests/ddl_add_column_clean_rollback_test.baseline2.log`.
- Candidate: `candidate/tests/ddl_add_column_clean_rollback_test.log`.
- Five matching native tests passed: the new test,
  `ddl_transaction_skeleton`, `serial_explicit_null`, `alter_add_column_null`,
  and `alter_column_type`.
- `serial-ddl-focused-reference18.log`: unchanged original
  `serial_explicit_null_conflict`, `ddl_transaction`,
  `quoted_alter_add_column`, and `alter_rename_column_transaction` all match
  the strict PostgreSQL **18.6 / 180006** reference at port 15486. The reference
  database is unique to this run and was dropped in `finally`.
- The wire timeout remains the original **15 seconds**.
- Three existing protocol scripts passed unchanged: SERIAL explicit NULL,
  TEMP ALTER column/catalog, and transaction DDL upgrade timeout. Their logs
  are `serial-protocol.log`, `temp-alter-protocol.log`, and
  `ddl-upgrade-protocol.log`; each script stopped its owned server.
- All old-fd public headers and flags match the immutable donor; 56 unchanged
  source/object pairs match; fd's binder, the changed DDL translation unit,
  stubs and test objects are rebuilt. This is a matching **O0** diagnostic
  build, not a claim of a fresh formal all-O2 ROOT build.
- Binary SHA256:
  `67a46b50119c38cb4cb064cab78b6b55ddfa13ecf5dc03d6844f1ea4d3196e8b`.

## Original full-gate timeout remains unclosed

The original strict reference run's socket timeout at
`ALTER TABLE diff_serial_null_alter ADD COLUMN id SERIAL NULL` is retained in
`/tmp/dbms-canonical-input-cursor.4U3gkfUf/full-pg18-differential.log`. That run
completed 345 of 465 cases; later cases were not reached. Its cause has not
been proved by this scoped repair.

The unchanged fd binary passed the isolated original case and three bounded
repeats. The failing ALTER took 0.696, 0.777 and 0.751 seconds in those repeats,
while writing about 1 MB and making 55 write syscalls each. One live worker was
observed in `submit_bio_wait`; a different DDL worker was observed in
`jbd2_log_wait_commit`. `strace` attachment was denied, so no syscall-level
fsync counts are claimed. Logs are `serial-bounded-diagnostic.log`,
`strace-status.log`, and the earlier
`/tmp/dbms-with-source-runtime.zl5yD3MZ/serial-null-isolated-original.log`.

This change eliminates the demonstrated unnecessary physical restore. Backup
creation and unrelated DDL I/O remain outside this fix. It does not establish
that the full-run SERIAL timeout, a later valid CHECK ADD timeout, or the
complete DDL/performance family has been solved.
