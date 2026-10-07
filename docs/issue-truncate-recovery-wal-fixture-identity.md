# Pending-TRUNCATE fixture must use the current physical relation identity

## Current failure and cause

The unchanged complete `truncate_recovery_test.cpp` fails on actual current
production source `3ffbb516`, not just in a historical full-suite log.
Its completed-reset section passes, then startup during the pending-reset
section throws `WAL crash recovery failed; startup aborted to protect data`.
The native process actually aborts 134; the verification wrapper exits 1.

The fixture manually constructs a name-only TRUNCATE WAL payload for a newly
created table. Current schemas have a nonzero physical relation identity,
and the actual `walSmgrTruncate` producer appends that identity using
`heap_wal_identity::appendImage`. Recovery deliberately refuses to apply a
legacy name-only marker to a current physical generation. A matching name
cannot prove the record belongs to that generation.

This is a stale positive-test contract, not evidence that normal production
TRUNCATE records are missing the identity or that the guard should be relaxed.

## Change and retained controls

Read the actual table's nonzero `physicalRelationId` from its public schema
and append the existing versioned identity extension to the simulated durable
marker. Do not invent an ID or call the complete storage reset: the original
test still models a durable write-ahead record before reset/state publication.
Read that precise WAL LSN back and assert its real resource manager, operation,
nontransactional XID, table name and physical identity. Check that both startup
and the second restart preserve the same table generation.

All original three sections and row/index/TOAST/completion/corrupt-state
assertions remain. Add a fourth independent control: a genuine durable
name-only marker must still reject startup against a current generation,
without publishing a truncate-completion marker. The original guard and
legacy codec/identity controls are not weakened.

No production source, public header, WAL/schema format, SQL or protocol
deadline changes. The original three-section test is not reduced to a subset.

## Actual evidence

Artifacts: `/tmp/dbms-truncate-wal-fixture.8CdcMbRo/`.

- Original complete baseline, handle 12790: actual wrapper 1/native 134;
  `baseline-current80-complete.log`. Original test SHA-256 is
  `8328c5ffb6735be34decdfbbbe5f666b0e6c5993dcc65bc5938ffe621c0a9d54`.
- First complete candidate, handle 46591: actual 0; all original three
  sections and the new name-only control pass.
- Final complete candidate, handle 25481: actual 0;
  `candidate-final-current80-complete.log`. Final test SHA-256 is
  `07c5e810e54c16191fb86b825f8339ed286f9f7c3ea82be777d40947d0460361`.
- Eleven complete unchanged adjacent drivers, handle 20430: actual 0;
  `adjacent-current80-complete.log`: TRUNCATE, owned-sequence restart,
  physical-identity codec/guard/generation, WAL basic/full-page,
  checkpoint, PITR recovery, all nineteen DDL-transaction sections and
  sequence-DDL rollback generation. These are whole files, not selected
  statements or truncated successful prefixes.
- Immutable `verify-current-native.sh` proves all 58 current CPP/header inputs,
  manifest, actual normal compiler/linker flags, original donor receipts,
  cache stamp and frozen binary against exact `3ffbb516` in
  `/tmp/dbms-canonical-between-type.zwBrZgUc/repo` before every invocation.
  Fresh O2 test drivers and shared stubs link its matching 57 non-main
  normal production objects, in separate owned default-disk temporary CWDs.
  This is not a fresh-58 production build or sanitizer proof.
- The unchanged frozen production binary SHA-256 is
  `701db03fc9abbaad25136c8fd95d355dea08a8481c604d0745ea14e2bfb48a2d`.
  `git diff 0f88f496 -- src scripts cmake` is empty.

## Limits

This repairs the current positive fixture and retains the real generation
boundary; it does not implement redo-only recovery or prove every TRUNCATE,
checkpoint, DDL, temp, CLOG, ordinary live-owner or power-loss window. WAL-05,
WAL-08 and the full original 273-item audit remain open/partial. The ongoing
original full suite uses frozen `3ffbb516` test inputs and has not been replaced
or restarted to hide this original failure. No push, Actions activation or
deferred security/TDE work.
