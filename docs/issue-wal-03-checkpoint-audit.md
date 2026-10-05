# WAL-03: checkpointing remains partial

## Findings

`StorageEngine::checkpoint()` takes the per-database transaction lock, refuses
to move the recovery start while a transaction is active, flushes loaded heap
and index caches through WAL-aware paths, writes and fsyncs a checkpoint WAL
record, writes a small atomic checkpoint sidecar, persists selected catalog
and statistics state, archives eligible segments, and only truncates complete
segments that are archived and strictly before the checkpoint LSN.

That is a useful local checkpoint protocol, but it is not PostgreSQL's complete
checkpointer/control-file model. The sidecar contains timestamp, max xid, and
checkpoint LSN, not a full control file or a checkpoint redo horizon. There
is no fuzzy checkpoint with a tracked dirty-page/redo horizon, checkpoint
completion record/state transition, or equivalent configurable dirty-buffer
throttling and scheduling protocol. Related recovery and retention work remains
open under WAL-01/02/04.

## Evidence and verification

`bash scripts/build_one_test.sh checkpoint_test` passed. It exercised buffer
writeback barriers, crash recovery of interrupted allocator-extent publication,
transaction-scoped cache writeback, refusal to checkpoint across active
transactions, and matching checkpoint sidecar/WAL record LSNs. This verifies
the implemented subset, not a PostgreSQL-compatible control file or redo
horizon. No code changed in this audit; the full registered suite and
PostgreSQL 18.6 runtime differential were not run.

## Status

WAL-03 remains partial. Redo horizon computation, full control-file state,
checkpoint completion/recovery protocol, and mature dirty-buffer scheduling,
throttling, and retention coordination remain unimplemented or unverified.
