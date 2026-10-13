# R17: retain an incomplete rollback until durable recovery succeeds

## Scope and reproduced cause

Private base: `066ee78542784ab47509fb6d49536201669413d8` (the separately
verified R16, optional-directory compatibility, R18 snapshots, and O01 owner
healthy-retry chain). Root inputs, caches, master, Actions, and remotes are not
changed by this private issue. Authentication, permissions, TDE, CTAS, and the
deferred checkpoint investigation are outside this repair.

The old `StorageEngine::rollbackTransaction` accumulated undo errors but still
published ABORT, removed the active xid, released locks, cleared undo actions,
and ended the transaction. `DdlTransaction::rollback` could then discard the
physical image even when restoration failed. Thus a later successful write
could be accepted while recovery still needed the old image; restoring it on
startup could erase that later write. A DDL destructor also called rollback
without containing secondary exceptions during a primary exception's unwind.

A normal registered undo callback returning false reproduces the first part
without altering heap/WAL bytes. The baseline's actual output was
`failed_status=5 active=0 retained=0 aborts=1 calls=1`; its strong pending/image
assertion failed with exit 1. That executable is retained as
`rollback_recovery_graph_test.baseline.frozen` (SHA-256
`ce33a40c76cf1179ce0b792ec80062604c2d1194cf66b01357d592287c949360`).
It ran the initial, smaller graph driver, not the subsequently expanded final
fixture. The later-write/restart data-loss consequence was a causal source
finding, not an executed baseline data-loss experiment.

## Repair contract

- A physical rollback attempt owns an explicit pending/running state. A
  failed image, row, DDL callback, index rebuild, writeback, or terminal-WAL
  boundary returns without destroying the transaction graph, active xid,
  database/table ownership, or recovery image. The caller repairs the ordinary
  storage condition and explicitly calls rollback again.
- The same backend rejects BEGIN, COMMIT, PREPARE, SQL-command admission,
  savepoint operations, and external image disposal while its rollback is
  pending. Actual database ownership checks also reject ordinary physical
  consumers in other backends for that database. A mutex-protected set of xids
  retains multiple already-admitted pending owners; one successful retry must
  not release another owner's quarantine.
- Dirty transaction snapshots are restored but retained until complete undo
  and a durable terminal ABORT. A saved ABORT LSN is reused on a durability
  retry rather than appending duplicate terminal records. CLOG is not advanced
  before physical undo/writeback succeeds. Successful dirty-image rollback
  removes the image; existing clean opt-in image/wrapper ownership is preserved.
- Without a snapshot, successful DDL callbacks are removed from the retry
  list, since ordinary DROP callbacks need not be idempotent. A retained image
  can reintroduce its prefix, so that prefix's callbacks remain available.
  The row undo log remains available for the healthy retry.
- A started-by-wrapper DDL transaction transfers its CREATE undo to the
  engine before commit/rollback can fail. Failed restoration of a nested
  statement falls back to whole-transaction rollback; an incomplete full
  rollback retains the graph. Destructor secondary errors are reported without
  replacing the primary error. Backend termination does not erase an
  unresolved context.
- Engine destruction leaves recovery images and nonterminal recovery work for
  real process restart. A new embedded engine in the same live process may not
  replay another failed owner's image. Teardown can flush already-owned state,
  but does not publish a clean-shutdown marker while an owner is pending.

The quarantine applies at normal Engine API ownership boundaries. An external
caller that already escaped a mutable allocator/index pointer still must obey
its caller-coordination contract; this is not a revocable handle redesign.
The patch does not make every existing DDL undo callback idempotent, redesign
the checkpoint algorithm, or claim complete exception safety of unrelated
member destructors.

## Ordinary buffer-writeback integration

The initial pending guard exposed a real normal-consumer integration error:
`backgroundBufferFlush` resolved a pending database's relation path, which
correctly threw SQLSTATE 58030 but escaped the worker. One complete 19-fixture
run exited 0, yet the identical graph driver repeated 20 times produced
13 failures and 7 passes. Raw terminate backtraces and all 20 logs are retained.
Before the diagnostic executable was relinked, `addr2line` resolved the chain
as `getTableSchema -> relationDir -> dataPath -> backgroundBufferFlush ->
backgroundWorkerLoop`. The old diagnostic executable was not frozen; the raw
logs and the prior symbol-resolution record are the evidence, not a claim that
those offsets describe the new executable.

The finite repair skips pending database allocators in ordinary buffer
writeback. A race during path/WAL resolution is contained only for structured
58030 while the same database is pending; unrelated healthy-database errors
are not swallowed. Other allocators retain their original writeback path.
No worker interval, checkpoint algorithm, or deadline is changed. The new
`rollback_pending_buffer_flush_test` marks an unchanged valid page dirty
through the normal pinned-page API in a healthy database and verifies that
the worker clears it while another database remains pending and rejects
ordinary access.

## Strong coverage and results

All native drivers use the existing isolated-cwd runner and normal optimization,
disk paths, flags, and deadlines. Existing row/index/recovery assertions remain.
The three original rollback durability fixtures formerly expected failure to
end the transaction and immediately permit a new write. That erroneous
contract is strengthened: after the original error and repair, assert the
transaction is still active, BEGIN is rejected, explicitly retry rollback,
then retain every original old-row/index/new-write assertion. Their original
local page/WAL durability scenarios are unchanged; no new attack input is added.

| Complete control | Actual final result |
| --- | --- |
| Original 19-fixture normal matrix below | exit 0, 19 passes |
| Same graph driver, same 20 isolated repetitions after buffer repair | exit 0, 20 passes |
| Healthy-database normal valid-page writeback control | exit 0 |
| Original INSERT rollback durability fixture, both sections | exit 0 |
| Original UPDATE rollback durability fixture, all three sections | exit 0 |
| Original DELETE rollback durability fixture, both sections | exit 0 |

The complete original 19-fixture list is:

```text
rollback_recovery_graph_test
ddl_rollback_image_retry_test
ddl_rollback_restart_test
table_owner_atomicity_test
table_owner_peer_retry_test
catalog_value_snapshot_test
catalog_complete_read_test
catalog_optional_directory_test
catalog_clean_publication_test
catalog_resolve_test
catalog_persistence_failure_test
type_catalog_descriptor_persistence_test
catalog_snapshot_test
ddl_transaction_skeleton_test
aborted_snapshot_recovery_test
database_snapshot_cleanup_guard_test
ddl_add_column_clean_rollback_test
prepared_transaction_test
alter_table_only_test
```

The new graph fixture covers exact nonterminal CLOG/WAL state after ordinary
undo failure, valid-image retention, rejected same/other backend consumers,
backend-end graph retention, explicit repair/retry, exactly one final ABORT,
no-image INSERT/UPDATE/DELETE with actual old-row/secondary-index restoration,
and two independently pending owners. The image fixture verifies restoration
after temporarily unavailable valid backup directories and preservation of a
primary exception when a later undo callback throws. The restart fixture uses
normal fork/exec and orderly Engine destruction: a fresh process restores the
original schema and row before accepting the next insert. It is not a forced
process crash or power-loss/fsync-failure proof.

## Build provenance, retained reds, and reproduction

All artifacts below are in `/tmp/dbms-review-ddl-rollback.rByuzcja/`.
`prove_baseline.sh` and `baseline-donor-proof.log` prove exact baseline
source/header/scripts/cmake/compiler/flags/manifest/TLS and all 58 donor
receipts/object bytes, with only private path-signature remapping. The public
Engine header then changes, so old-header objects are not candidate evidence.
`r17-final-header-fresh58-normal.log` is an actual exit-0 genuinely fresh normal
58-TU production build; its header SHA-256 is
`76b64842d9f004a06cbe24a7ccea78ffc8bc5c34bee1bc7c9dde1110e6547439`.
The earlier interrupted draft build exited 143 and is not counted as fresh58
success. Source-only corrections rebuilt TableManage on that proven header
epoch. Final `r17-buffer-pending-normal.log` is exit 0, exactly one fresh
TableManage TU plus 57 matching current-header receipts; `run_gates.sh` verifies
every current receipt and full build signature, then freshly compiles each
driver and the shared stubs before linking the 57 non-Main production objects.

Retained red logs are `recovery-graph-original-baseline.log`,
`r17-candidate-complete17.log` (CLOG ordering and original dirty-image cleanup
assertions), `r17-candidate-complete18.log` (ordinary worker exception), and
`graph-repeat-1.log` through `graph-repeat-20.log` (13 red/7 green). The
pre-buffer `r17-complete19-strict-diagnostic.log` exit 0 is not used to hide
the subsequent failed repetition control.

Final precommit complete logs are `r17-buffer-fixed-complete19.log` and
`r17-buffer-fixed-complete4.log`, both real exit 0. After-fix repetition logs
are `graph-fixed-repeat-1.log` through `graph-fixed-repeat-20.log`; their loop
terminated with exit 0. Production SHA-256:
`279fb9f78db6f92757d2f21c3f975fd187770ff0eb11c30824eb342ee062cada`.

Reproduce complete native controls with the private `run_gates.sh` and the
19 names above, then run the four additional names:

```sh
bash /tmp/dbms-review-ddl-rollback.rByuzcja/run_gates.sh \
  rollback_pending_buffer_flush_test rollback_insert_page_failure_test \
  rollback_update_page_failure_test rollback_delete_page_failure_test
```

The runner must receive objects built for its exact current headers; copying
old Root ABI objects is not an alternative to the fresh58 proof. This issue's
native results do not claim that all project SQL/protocol tests or the user's
entire review checklist have already completed.

## Root integrated verification

Root genuinely rebuilt all 58 production translation units after the public
header change (`root-r17-o04-fresh58-build.log`, actual exit 0, exactly 58 compile
lines). R34 then genuinely rebuilt ExprEvaluator.cpp. The final integrated
31-driver native batch, including all 23 R17 drivers, completed with exit 0;
every current object receipt and the unchanged final input signature was checked.
R32/R34/O04 protocol controls and the complete PostgreSQL protocol driver also
completed with exit 0. Logs are in `/tmp/dbms-review-fix-donor.N9sdas6f/`:
`root-r17-r34-o04-final31-native.log`, `root-r32-protocol.log`,
`root-r34-protocol.log`, `root-o04-protocol.log` and
`root-r17-r34-o04-postgres-protocol.log`. A preceding batch completed 27 drivers
but then failed because its requested NTILE filename omitted `window_`; that
exit-1 log remains as `root-r17-r34-o04-integrated-native.log`, not a passing
31-driver result. The final runner validates every requested filename first.
