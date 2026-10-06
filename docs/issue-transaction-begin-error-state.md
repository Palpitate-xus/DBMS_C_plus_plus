# Statement-start SQLSTATE and transaction ownership

Status: structured-start mapping and its expanded regression are verified.
The independently observed pure-constant SELECT database-lock demand is a
separate root cause; mapping a status alone does not close its recovery test.

## Actual baseline

The original canonical production group is frozen at
`/tmp/dbms-prepared-query-namespace-combination.FS4egSBy/dbms_main.parsed.frozen`,
SHA256 `0204a334c1f7b8e7534f8dd07a5e4dffb951ae9441ae211a7e012470865f2728`.
It is an explicitly older runtime, not a binary for this private source parent
`912894e1266db803c716d83eafff4d276e08879f`.

Unmodified `table_lock_timeout_protocol_e2e_test.py` failed at its original
line 41: expected `55P03`, got `XX000`, message
`could not start statement transaction` (46801, exit 1;
`baseline-original.log`). The full canonical run had independently stopped
on the same assertion. A second isolated diagnostic confirmed explicit
BEGIN returned `XX000 / Begin transaction failed`; its following SELECT 1
also returned the implicit-start error. Both ReadyForQuery states were I
(42283, exit 1; `baseline-explicit-extended-v2.log`). The earlier diagnostic
mistakenly referenced runner key `proc` before its query loop; that fixture
failure is retained but is not SQL evidence.

`StorageEngine::beginTransaction` returns `LOCK_CONFLICT` when the database
shared/exclusive lock cannot be acquired within the session's lock_timeout.
It clears the attempted mutex ownership before returning, before assigning
a transaction ID or setting inTransaction. Both the calling-statement owner
and explicit BEGIN discarded this status and printed an unstructured error;
the protocol bridge therefore fell back to XX000.

The newer formal ROOT efc501b9/docs57d production group also failed the new
regression's first physical SELECT with exactly XX000 (20921, exit 1;
`baseline-root-preupdate.log`). Its explicit frozen binary is
`/tmp/dbms-native-explain-prepared-combination.64xbTwJu/dbms_main.preupdate.frozen`,
SHA256 `dd3ea2eed3e4b96f062f6018d0af86c263da8b54ffe721433dc6582f9aa1f7db`.
It includes ARRAY/RAISE/EXPLAIN, but is not relabelled as the private 912 parent.

## Repair contract

Both starts now throw `DbError(sqlstateForDBStatus(status), message)`. The
calling-statement entry restores executeDepth on a returned failure and on
an exception. No rollback, notification/advisory boundary or SQL command
visibility begins before successful ownership, so the failed reader cannot
undo the other session's transaction. Existing acquired-owner cleanup is
unchanged. Other known statuses use the same central mapping; their full
failure-injection family is not claimed verified here.

The registered independent protocol regression keeps lock_timeout=50 and
15-second socket bounds. It covers implicit SELECT/WHERE, a writing routine,
INSERT, EXPLAIN/ANALYZE, prepared EXECUTE, explicit BEGIN and the extended
query owner. It checks exact state, no result/CommandComplete, Sync/Ready I,
the holder's original transaction still T, and successful post-release
SELECT 1 plus a writing function whose committed row is visible to a second
connection. It does not weaken the original SELECT 1-while-locked test:
that remains a separate demand defect until its independent fix.

## Matching proof

Private V1 compiled all 57 production units, repeated up-to-date and matched
57 signatures/binary stamp (90130, exit 0). Frozen V1
`/tmp/dbms-query-begin-lock-state.TNJZ551R/dbms_main.lock-state.o2`, SHA256
`0375741794531be915fa08b8c0db7ca1932e6f63168555ef7e728d5c528728a9`, contains
only the implicit-start mapping. Its original recovery control still has a
real SELECT 1 failure (50804, exit 1; `v1-constant-red.log`): the original
physical SELECT/WHERE states now pass, then SELECT 1 returns 55P03.

V2 adds explicit-start mapping without new headers. Changed-source O2 build,
repeat up-to-date, all 57 object signatures and binary stamp succeeded
(46024, exit 0); immutable `dbms_main.lock-state.v2.o2` SHA256
`7d61d6d24e3455d534c05101bb5bc3d1c170e0c4a238bb5b0037318fc2967b49`.
The independent 9-shape protocol run passed (60282, exit 0;
`candidate-error-state.log`), including a subsequent actually committed
writer observed by the other connection. All processes belonged to isolated
test fixtures and were stopped/waited; PID 4019525 is gone. The stronger final
9-shape repeat, including the holder still T after extended failure, passed
(45800, exit 0; `candidate-error-state-final.log`, PID 4026288 stopped).
The serial adjacent batch passed 6/6 (2615, exit 0;
`adjacent-error-state-v2.log`): transaction SELECT relation lock, DDL upgrade
timeout, row timeout, DML lock-error/savepoint, writing-function atomicity,
and simple/extended query snapshot characteristics. Final PID 4024171 is
gone. Production inputs were frozen during these checks.
No full-suite or database-lock-family completion is claimed.
