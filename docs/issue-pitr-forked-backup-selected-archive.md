# PITR: recover the archive stream selected by the base image

## Reproduced loss and the repair

The unchanged `82bf3739` production baseline, linked from all 58 proved
normal-object inputs, loses a committed post-backup row when the base image
was taken after an earlier real PITR fork. `pitrRestore` imported only archive
filenames beginning with timeline 1, although the base image selected timeline
2. In the complete counterexample, restart returned `1 before_fork` and
`6 fork_base_image`, but not `3 fork_archive_keep`. The returned success and
"restored 1 archived segment" message referred to the unrelated old timeline.
The native assertion actually aborted with exit 134; it was not inferred from
source or replaced with a narrower test.

`pitrRestore` now opens the restored image's real `WALManager` to obtain its
durable selected timeline. Both the backup overlap boundary and imported
archive entries use that stream. Segment names must contain three complete
eight-digit hexadecimal fields and the zero middle log field of the current
WAL format. Foreign timeline files, partially parsed filenames and nonzero
middle log fields do not become recovery input. Directory/entry errors are
checked. No public header, schema layout, WAL record encoding, selector
encoding, recovery-target rule, privilege or session behavior changed.

## Complete controls and exact evidence scope

`tests/pitr_forked_backup_archive_test.cpp` retains the whole real lost-row
history and adds both an empty fork image and an ordinary timeline-1 image.
It uses actual `StorageEngine` transactions, a first inclusive PITR target,
durable numeric fork, physical base backup, post-backup commits, sealed/ready
WAL archive, `pitrRestore`, fresh engine startup and another write/restart.
Every case checks retained/excluded rows, primary and secondary index RIDs,
CLOG commit/abort decisions, consumed target and the next durable timeline.
Mixed archives contain the real old timeline-1 segment plus explicitly
unrelated/malformed filenames. Fork cases also reserve an unused high-number
abandoned-stream filename in the image to exercise the overlap boundary. This
reservation is not represented as valid historical WAL or a history-chain
implementation.

Private evidence directory: `/tmp/dbms-backup-timeline.eiNl8HmT`.

- Original lost-row reproductions: `baseline-counterexample-v2.log` and
  `baseline-counterexample-v3.log`, both actual exit 1 wrappers with the
  complete native driver exit 134. V3 reports missing actual `id=3` and rows.
- Baseline frozen executable SHA-256:
  `d1e0e3d442957f661d12414e589048ddd3d49fe8bc7ed8b0306db4515b63c5e1`.
- Candidate normal optimized build/repeat, all 58 current receipts, cache
  stamp and immutable executable agree. Only `TableManage.cpp` was freshly
  rebuilt; the other 57 normal objects are proved source/header/flags/receipt/
  byte-identical donors from the formal `82bf3739` epoch. This is not a fresh
  58-TU build and not an all-project sanitizer run.
- Candidate frozen executable SHA-256:
  `29f59b89b4ff3a0ea7de901285891da6d37a90e0fa667f3aa6da15348d989675`.
- `candidate-full-native-18.log`: all 18 complete drivers ran; 17 passed and
  the unchanged original `truncate_recovery_test` aborted during startup.
  This is not an 18-test PASS.
- `candidate-final-v2-full-native-23.log`: all 23 complete drivers ran; 21
  passed, including both populated/empty fork histories, original PITR,
  timeline archive, physical backup/restore controls, WAL archive/truncation,
  integrity, real crash redo, UNLOGGED SIGKILL and heap-generation crash.
  Original `truncate_recovery_test` and `stale_temp_startup_recovery_test`
  aborted. No original assertion, SQL input or timeout was changed.
- The later final three-history driver and all original ten backup/PITR
  controls passed in `candidate-final-v3-full-native-10.log` (actual session
  19147 exit 0, ten driver terminals 0). The final added reserved abandoned-
  stream boundary case passed with all three complete histories in
  `candidate-final-v4-full-native-1.log` (actual session 21620 exit 0).
- Complete original four-case `cold_start_transaction_backup_protocol_e2e_test`
  passed against the frozen candidate in
  `candidate-cold-backup-whole-wire.log`: actual executable SIGKILL/new exec,
  CREATE/ALTER/DROP/nested images, typed NULL and continued transactions.

The unchanged `truncate_recovery_test` also actually aborted with exit 134
against the original formal `82bf3739` objects in
`truncate-original-82bf-baseline.log`; it does not invoke `pitrRestore`.
The two original neighbors' additional line-buffered complete baseline runs
are retained in `original-82bf-neighbor-full-2.log` (actual session 67338 exit
1, both original driver terminals 134). TRUNCATE's completed-reset case passed
before its pending-reset case aborted. The original stale-temp fixture also
aborted against the unchanged baseline. Their causes and repairs are separate
work, not hidden by dropping them from the candidate group.

## Preserved fixture mistakes and remaining scope

The first scratch fixture forgot the archive API's explicit ready call after
`switchWal`; `baseline-counterexample.log` retains that real exit 134. The
corrected fixture calls `markSegmentsReadyBefore` and then reproduced the
actual lost row. A later two-case fixture unlinked the still-live cluster XID
counter between histories, causing `58030`; its full failed run remains in
`candidate-final-full-native-23.log`. The final driver removes that counter
only at program entry/exit and retains both complete histories. These are
fixture mistakes, not extra production bugs. The header preflight briefly
lost `rg` from the environment; the final recipe uses real `find`/SHA input
checks and the ordinary all-58 signatures, not empty search output as proof.

An independent public-WAL scratch counterexample also exists: after
`switchWal`, a record at LSN 16777216, close/reopen and a further real append,
the newly appended record's `xl_prev` was 0 instead of 16777216 (actual exit
134, `wal-padding-public-api-baseline.log`). Its fix is not part of this
selected-timeline repair and remains open. A first scratch compilation called
a private method and failed before running; the explicitly labeled tool
transcript is retained in `padding-probe-initial-compile.txt`.

This work fixes selected-stream archive import for the project's existing
offline time-target workflow. It does not implement PostgreSQL timeline
history chains, standby/promotion, `restore_command`, recovery signal files,
all target kinds/actions, incremental backup or the full crash state machine.
WAL-04/WAL-05/WAL-08/BACKUP-04 and the original total ledger remain partial;
no PG18 physical-format compatibility or full-suite PASS is claimed. User-
deferred security/TDE work is unchanged. No push or Actions enablement occurs.
