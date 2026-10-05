# WAL-04: recovery/standby lifecycle remains partial

## Findings

The repository has offline startup recovery and a bounded point-in-time
recovery path. PITR filters commits after the configured timestamp, applies
heap/index before-images to undo excluded transactions, rebuilds derived
indexes, and durably forks to a new numeric timeline. Recovery rejects
malformed record chains/images and unsafe index-image paths.

This does not implement PostgreSQL restartpoints or hot standby. There is no
standby snapshot/recovery-conflict machinery, no timeline history-file chain,
and no promotion/replay state machine for a streaming standby. Current PITR
timeline selection plus `.timeline`/PITR markers only cover this local subset.

## Evidence and verification

Focused tests passed:

- `bash scripts/build_one_test.sh pitr_recovery_test` — inclusive target,
  aborted post-target insert/update, durable timeline fork/restart, and
  malformed target fail-closed behavior.
- `bash scripts/build_one_test.sh recovery_integrity_test` — malformed
  transaction/heap/index WAL and unsafe index path fail closed.
- `bash scripts/build_one_test.sh wal_timeline_archive_test` — numeric
  timeline switch and archive workflow.

These tests validate offline recovery/PITR subsets only. No code changed in
this audit; the full registered suite and PostgreSQL 18.6 runtime differential
were not run.

## Status

WAL-04 remains partial. Restartpoint consistency, hot-standby snapshots,
recovery conflicts, timeline history files, and standby promotion are still
unimplemented or unverified.
