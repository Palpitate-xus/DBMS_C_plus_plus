# Heap retirement intent is not completed deletion

The first fault control wrote durable retirement intent `0x21`, removed the
heap and schema, and crashed while `tlist.lst` still referenced the schema.
That control already failed closed on the old implementation. It is retained
as a positive, not reported as a reproduced defect.

The expanded control also durably published the empty table list, exactly the
last ordinary DROP metadata stage, then crashed without a completion record
or terminal transaction COMMIT. The old implementation incorrectly started
successfully (`failed_closed=0`): it treated the earlier intent as authority to
discard the committed row's absent-generation heap images. This is a real red,
not a filename or missing-row assumption.

DROP retains the write-ahead intent. Only after successful catalog publication
and relation/database directory sync does it insert and flush a distinct
`0x22` completion record. Recovery requires an earlier matching intent for the
same actual generation, name and transaction ID. Only a completed nontransactional
DROP or an actual committed transaction authorizes discarding older images.
Uncommitted completion is not a COMMIT. Missing or mismatched authority fails
closed without inventing the missing heap. UNLOGGED zero-WAL behavior is unchanged.

## Exact scoped evidence

All paths below are within `/tmp/dbms-heap-wal-identity.dcGDltHu/`.

- Old complete-catalog interruption: driver handle 18495, exit 134;
  `completion-baseline/expanded.log` retains both the already-correct first
  control and the real expanded failure. The unchanged table-generation
  baseline was `2e0545e0` with the immutable `build-v3` objects.
- New public constants/header inputs: all 58 translation units plus fresh
  stubs rebuilt under the same configured flags with appended `-O0`.
  `build-v4.log` handle 1124, exit 0; all source/header before-final audits
  passed. Binary SHA-256:
  `34cd2f75170f5d7cb7194dcc71141233ca64dcc1ef1c72df299fb88be193ccf4`.
- `native-v4.log` handle 83459, exit 0: all 19 scoped native executables passed.
  The original same-parent three-scenario crash test is unchanged and passes,
  with independent cross-process transaction-counter fix `02896e3b` already
  present. This does not substitute a fresh verifier for the original loop.
- The added completion executable covers nine independent crash phases:
  missing-schema intent; complete-catalog intent; completion without intent;
  mismatched name; mismatched transaction; actual successful public DROP with
  exactly one intent/completion pair; aborted owner before catalog update;
  aborted owner after catalog update; and aborted owner after completion.
  All aborted dirty-snapshot cases restore row 99. The five invalid-authority
  cases fail closed. Its exact log is
  `native-v4/heap_wal_retirement_completion_test.log`.
- `sanitized-v4.log` handle 42934, exit 0: eight scoped native executables
  passed with fresh ASan/UBSan TableManage/TxnIdGenerator, stubs and drivers.
  Other 55 native dependency objects are matching, uninstrumented `-O0`;
  leak detection was disabled. This is not an all-engine sanitizer claim.
- Normal and sanitizer groups retain object/test/source/header before-final
  hash audits under `native-v4/` and `sanitized-v4/`.

The other normal controls are generation/name reuse/TRUNCATE/rollback, exact
legacy codec and missing-live-heap guards, unchanged foreign-key table-rename
recovery, shared heap/B+Tree rows/crash/generation/TOAST, physical backup/restore,
UNLOGGED startup, checkpoints, six SSI cases and cross-process xid/snapshot
controls. These storage API tests are not PostgreSQL SQL differentials.

## Boundaries retained

The original `alter_table_only_test:136` backup and
`begin_transaction_database_drop_race_test:56` commit failures were separately
reproduced in `completion-baseline/`; neither assertion is changed by this
commit. They have distinct physical-cache and terminal-owner causes and remain
separate repairs. This retirement item does not claim whole-storage success.

Retained intent/completion history is required for this discard proof. General
WAL recycling/PITR history preservation, arbitrary legacy rename reconstruction,
heap-layout rewrite epochs and complete physical DROP atomicity remain broader
boundaries. An interruption with unprovable absence deliberately stops startup;
it is not silently declared a successful DROP.
