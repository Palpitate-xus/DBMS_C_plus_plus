# Cold-start transaction-image recovery and lock-registry lifetime

Status: the source/test fix is committed locally as `615f2c54`, integrated
from the isolated fixing worktree's `78371580`. The isolated development and
optimized candidates passed the scoped checks below. The combined `615f2c54` /
`7bc76204` worktree also passed its fresh 55-object optimized build, repeat build,
all object-signature/binary-stamp checks and the four-case cold regression.
No complete protocol,
registered-suite or PostgreSQL 18.6 differential pass is claimed. The related
WAL/transaction families remain partial. No push or Actions enablement; the
user-deferred security/TDE work remains deferred.

## Reproduction and cause

While investigating retained complete-protocol timeouts, a separate cold
restart defect was found. With an open transaction and a durable physical
transaction image, the test-owned server was killed with `SIGKILL` (Python
process status `-9`). A new executable process using that same isolated
fixture exited with `SIGSEGV` (`-11`, or shell status 139) before accepting a
client. This is a startup crash, not a SQL timeout or an established corrupt
WAL-record finding. The old-binary regression fails at the cold-start
assertion; it is not counted as a successful recovery case.

`main.cpp` owns a global `StorageEngine`. Its constructor invokes
`recoverAllDatabases()` before `main()` runs. When recovery finds an
uncommitted `dbname.txn_backup.xid` image, it calls `physicalRestore()`, which
acquires the database lock through `databaseTxnLockFor()`.

The old lock registry was a namespace-scope mutex and `std::map` in
`TableManage.cpp`. The map's dynamic initialization was not guaranteed to
have run when the global engine in another translation unit entered recovery.
Accessing the uninitialized map/tree could therefore crash. The retained
old-executable trace includes `SIGSEGV`, `SEGV_MAPERR`, address `0x8`. Merely
constructing a native engine after entering `main()` does not exercise this
initialization-order window.

This is distinct from the earlier active-transaction `std::set` startup
defect fixed in `a7c15da5`; see
[the preceding WAL-05 crash-matrix report](issue-wal-05-startup-recovery-crash-matrix.md).
The earlier set fix did not initialize this separate database-lock map.

## Change

The mutex and map now belong to one `DatabaseTransactionLockRegistry`,
initialized together by a function-local static pointer on first access.
`databaseTxnLockFor()` still locks that mutex and returns a shared pointer to
the same per-database `shared_mutex`; lock modes and callers' locking rules
are unchanged.

The registry intentionally has process lifetime. A destructed function-local
registry first used after `main()` starts could otherwise be torn down before
the earlier-constructed global engine stops its background workers. Keeping
the registry alive removes that teardown-order dependency as well as the
cold-start initialization dependency. This is not an implementation of the
full PostgreSQL heavyweight-lock conflict matrix or queueing model.

The registered regression,
`tests/cold_start_transaction_backup_protocol_e2e_test.py`, creates separate
small test-owned clusters and kills the actual started executable, not a
wrapper shell. It closes the client only after the server is confirmed dead,
so a normal disconnect cannot roll back the transaction before the crash.
Before restart it requires a transaction image with the physical-backup
marker; after restart it checks data, schema, image cleanup and a new
successful transaction.

## Scoped verification

The isolated fixing worktree was
`/tmp/dbms-cold-recovery-fix.6qWb1f/repo`, at `78371580`. Its development and
optimized binaries were `/tmp/dbms-cold-recovery-fix.6qWb1f/dbms_main.dev` and
`dbms_main.optimized`. These are isolated candidates, not proof that the
current combined source tree's official optimized build or all tests pass.
The reported successful runs are retained tool/terminal results; complete
stdout logs for every run were not persisted alongside those binaries.

| Cold-restart case | Recovery assertion | Isolated dev / O2 |
|---|---|---|
| ALTER | Discard uncommitted added column; restore committed rows and SQL NULL | Passed / passed |
| CREATE following snapshot-backed ALTER | Discard both pending schema change and created table; missing table reports `42P01` | Passed / passed |
| DROP | Restore the transaction's dropped base relation and committed rows | Passed / passed |
| Nested savepoint and DML after DDL | Discard uncommitted update and schema change; remove nested statement images | Passed / passed |

All four cases also check that transaction/statement backup directories are
gone after recovery, headers are `id,payload`, and a subsequent
`BEGIN; INSERT; COMMIT` persists a new row. One protocol entry point covers
four independently created fixtures for each candidate; these are not four
different registered test scripts. They require no timeout extension or
copied user database.

Three adjacent protocol entry points passed on both isolated candidates:

- `tests/transaction_ddl_upgrade_timeout_protocol_e2e_test.py`
- `tests/commit_failure_recovery_protocol_e2e_test.py`
- `tests/alter_database_rename_connection_protocol_e2e_test.py`

Three native recovery tests, freshly linked for the respective development
and optimized candidates and run in isolated directories, also passed:

- `tests/aborted_snapshot_recovery_test.cpp`
- `tests/recovery_integrity_test.cpp`
- `tests/stale_temp_startup_recovery_test.cpp`

These native tests are adjacent recovery evidence; the new real-executable
test supplies the cold static-initialization coverage. Separately, the combined
`615f2c54` / `7bc76204` frozen development binary passed the cold regression as
part of a seven-script batch. The combined formal optimized binary passed it and
all three adjacent scripts above as part of a ten-script batch. It rebuilt all
55 production objects, linked normally, then passed repeat up-to-date build,
55/55 signature checks and the binary cache stamp check. All three freshly
relinked combined optimized native recovery retests above passed. The combined
complete default-protocol run instead exited 1 with a socket TimeoutError at
main line 3170, `INSERT INTO kw_joined VALUES (9)`; it is not counted as passing
and does not prove the prior timeout causes.
An unsuccessful combined development socket-start diagnostic is not included
among these passes; a later independent captured startup and query passed.

## Separate complete-protocol timeout investigation

The two preceding complete-protocol failures remain recorded in
[the JOIN range-identity report](issue-join-range-identity.md): the prepared
transaction ALTER boundary and the ON COMMIT DELETE ROWS INSERT boundary.
Small isolated controls passed, but did not explain those large-fixture
timeouts. This cold-start fix does not claim to resolve either failure.

A third instrumented run of the unchanged complete protocol fixture, using
the copied preceding `d3fcfc0b` binary and the original 10-second socket
timeout, also failed. It timed out on
`CREATE TABLE upd_t (id INT PRIMARY KEY, v TEXT)` (main line 2807, diagnostic
query index 357), after 10.010 seconds. The server remained alive at that
time. Snapshots captured the active worker in `D (disk sleep)`; the later
sample's wait channel was `jbd2_log_wait_commit`, and process I/O counters
continued to change. Those observations support following up on I/O and
snapshot durability costs; they are not proof of a mutex deadlock or a
complete explanation of all three timeouts.

The smaller traced follow-up used a recovered copy of that fixture and
completed the two earlier kinds of operation within the same timeout. Its
ON COMMIT DELETE ROWS INSERT took 3.809 seconds, with 855 completed
`fsync`/`fdatasync` calls in the query interval. The trace parser attributed
720 calls to two statement-image generations; their summed syscall duration
was 2.178 seconds, out of 2.428 seconds for all 855 calls. Its ALTER took
4.424 seconds and 929 such calls. These are measurements of that traced run,
not a general benchmark, a durability optimization, or evidence that the
large complete-protocol timeout is fixed.

Diagnostic scripts, event records, process snapshots and syscall traces were
retained under `/tmp/dbms-protocol-timeout-diagnosis.oQHLzp`. The reproducible
read-only statistics command is `python3 trace_stats.py` in that directory.
Temporary diagnostic artifacts are not a substitute for checked-in tests.

## Remaining scope and ledger mapping

The primary mapping is WAL-05 (cold recovery) and WAL-08 (DDL/transaction-image
startup rules), with TXN-05 as scoped snapshot/savepoint cleanup evidence.
TXN-07 can record the registry-lifetime fix only as supporting internal
locking evidence, not completed heavyweight-lock semantics. This change
does not implement a new STO on-disk format, relation layout or buffer
manager; it should not promote those families based on an adjacent startup
test. The fsync observations are a performance follow-up, not completion of
WAL-07 power-loss/durability coverage.

Before-image restore versus a complete redo-based recovery state machine,
concurrent checkpoint and backup/restore races, complete prepared-state
recovery, partial/torn writes, sustained I/O failure and real power-loss
windows remain open. The complete registered suite and a PostgreSQL 18.6
runtime differential were not run for this isolated fix. No deferred
security/TDE audit was performed.
