# Implicit read owner must not disable every index probe

Date: 2026-10-06. Narrow current-read access-path fix, not complete versioned
indexes, snapshot isolation or the 273-item audit. IDX-03/TXN-01/02 remain partial.

## Actual failure

The optimized atomicity/lexical combination `5411a7aa` made every ordinary query
own its calling-statement transaction. Its full-default protocol terminal 29960
failed at postgres_protocol_test.py:2374: table t had seq_scan=1/seq_tup_read=1
but idx_scan=0/idx_tup_fetch=0 after the indexed lookup. The exact row was
`t,1,1,0,0,1,0,0,1`; this was an assertion failure, not an I/O timeout.
Log: `/tmp/dbms-plpgsql-combination.D4U50YDA/full-default-protocol.log`.

The counters accurately described the selected heap path. filterRows rejected
index candidates whenever any transaction on the database was active, including
the new otherwise isolated read owner. Artificially incrementing counters or
weakening the full protocol assertion would not fix the lost index access.

Independent native baseline 7293 exited 134 at the new idxScan+1 assertion,
after its no-owner index positive control passed. Real wire baseline 9693 used
the frozen formal c911593e binary and exited 1: lookup changed counters from
`0,0,0,0,2,0,0,2` to `1,1,0,0,2,0,0,2`, not an index scan.

## Snapshot-completeness guard

- A current owner is eligible only with an acquired snapshot, no own writes,
  row undo/DDL undo or dirty backup, and no excluded active snapshot IDs.
- Ignore that eligible owner alone when checking active transactions; any other
  active transaction on the same database still forces the heap path.
- Require the snapshot upper allocation boundary to match the current allocation
  high-water plus one. An old fixed snapshot cannot use current-only indexes
  simply because the other writer has already committed.
- Capture allocation high-water before indexed lookup; after obtaining candidates,
  reacquire owner/snapshot/write state and verify the same high-water. Discard
  candidates and fall back to the original snapshot's heap scan if either changed.
  TxnIdGenerator's legacy maxCommittedTxId name actually denotes monotonic durable
  allocation high-water, including aborted allocations; a begin/write/rollback
  between the checks therefore cannot evade the allocation fence.
- Checked index corruption remains fail-closed, even if the allocation fence
  changed. Optional scan-error out-parameters are not relied on as the sole
  internal error signal. Existing SSI dependency recording remains conservative.

The preexisting nontransactional index path is not granted a new completeness
guarantee by this narrow owner exception. This does not implement MVCC index
entries or all concurrent/vacuum/index-AM behavior.

## Tests and matching artifacts

Worktree `/tmp/dbms-read-owner-index.vlzK7jrJ/repo`, baseline `5411a7aa`.
No API/header/layout/disk-format change. One final TableManage CPP was freshly
compiled with the shared formal optimized configuration. All other 54 production
sources and every header were compared byte-for-byte against the matching
formal ROOT 5411 baseline before reuse; fresh test stubs and test translation
units were compiled, and each native used its own fresh working directory.

Final CPP SHA256 `c46704cec282175b69132ec0756a1e7fde27798a83bad406655f98e54ac51a77`;
object `eaa52b7029b0561aa3433270770db9da664783996ddc86c08a3866d49075ddf7`;
candidate `715aafc8c38106a9cc593f1b34c026d44221928402dcd1ead5f432e6f2edc91e`.
Artifacts and verify-candidate.sh are retained in that parent directory.
An initial compiled candidate was superseded before validation by the final
owner-state/internal-error checks; no old candidate result is called final proof.

Native terminal 39698 exited 0 for eight fresh matching optimized entry points:
read_owner_index_snapshot, index_scan_full_value_recheck,
integer_index_full_value_recheck, legacy_index_prefix_recheck,
update_index_failure, mvcc_update, runtime_stats and planner_runtime_stats.
The new native verifies real index counters, old snapshot after committed update,
another active owner, same-command own old version, next-command new version
and rollback recovery. Existing tests preserve long-key rechecks, checked index
failure/repair and concurrent MVCC behavior.

New dual-connection protocol terminal 66722 exited 0: real autocommit and explicit
current-read index scans, committed-writer old snapshot, overlapping owner,
own UPDATE/rollback, correct rows and genuine heap/index counters. The 10 adjacent
protocol scripts in terminal 33437 also exited 0: stored_function_atomicity,
plpgsql_select_into, query_snapshot_characteristics, transaction_isolation,
index_scan_full_value, legacy_index_prefix_recheck, self_join_range_identity,
dml_cte, commit_failure_recovery and cold_start_transaction_backup (its four
scenarios count as one entry point). All use the same optimized candidate.
The new complete-default protocol 46761 exited 1 with a socket TimeoutError at
main3431 / prepared_transaction_error_boundaries1197, ALTER TABLE t ADD COLUMN
prepared_note TEXT. The earlier statistics assertion was passed; the timeout
does not refute that narrow fix or establish a complete-protocol pass.
Log: `/tmp/dbms-read-owner-index.vlzK7jrJ/full-default-protocol.log`.
No live owned candidate server remained after the runner's cleanup. This is
the same statement location as one earlier failed full gate, not proof of a
common cause. Statement-image I/O amplification remains independent open work.

The private source/test/doc commit `1e96cbe7` was integrated as ROOT `d184d8a1`,
after independent WHERE resolver commit `aa149ca8` (private `7353cd9d`). The new
shared resolver header was freshly built into all 55 optimized ROOT objects
in terminal 69618, exit 0. The subsequent JOIN combination 6460a246 passed
30 wire scripts and 23 of 24 fresh matching natives; constraint_expr failed
its retained SUM('abc') 42725 assertion. These actual combination results are
recorded in `docs/issue-where-index-null-safe-join-combination.md`.
The initial source review did not identify a defect, but a later independent
fresh formal native (16308, exit 134) actually reproduced duplicate residual
writes: one indexed input row caused two calls instead of one. A residual
writer invalidates owner state, so the post-expression heap fallback repeats
its effect. The unchanged expected-one assertion and failed artifact are
retained. Physical candidate materialization/fence validation must precede
expression execution; the independent follow-up is in progress. Earlier
simple physical-column passes do not establish safety for arbitrary residuals.
No full-suite/PG18.6/TLS
runtime or whole-engine sanitizer claim. No push or Actions enablement; skipped
security/TDE work remains deferred.

## Independent residual-effect follow-up

The actual repeated-effect defect was repaired separately in private `e1b03c1d`,
integrated as ROOT `86ddccf9`: decide physical candidate completeness before
evaluating residuals, and retain captured row bytes/NULL bitmaps. Its expected-one
native baseline exited 134; final matching formal-O2 candidate passed 18 distinct
native and nine wire entry points. The wire control already passed the old
frontend and is not claimed as a new wire red. Native full UPDATE host support
still explicitly reports 0A000; real frontend source UPDATE/NULL/rollback is
verified separately. See `docs/issue-index-residual-execution-once.md` for precise
scope and retained failures. The ROOT owner/receiver/numeric combination freshly
rebuilt all 55 objects in terminal 96406; repeat build and all-signature/binary
stamp audit passed. Matching 39 native and 36 wire entry points passed, including
the residual regression. Complete default protocol80673 then passed with its
original10s timeout on that same frozen binary. Seven unchanged clause failures
remain distinct, not a full-engine completion claim or a repaired I/O cause.
