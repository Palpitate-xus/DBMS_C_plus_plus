# VACUUM TOAST: establish the repeatable-read snapshot

The unchanged `vacuum_toast_test.cpp` failed at its original old-payload
assertion after shared physical heap ownership was corrected. The reader
began REPEATABLE READ, but its first data read occurred only after the writer
updated one row, deleted another, and committed. Its expectation of an older
snapshot was therefore invalid. No production storage change is made here.

PostgreSQL acquires a repeatable-read snapshot at the first non-transaction
control statement, not at BEGIN. See the official
[PostgreSQL 18 isolation documentation](https://www.postgresql.org/docs/18/transaction-iso.html).
The checked-in `tests/compat/vacuum_toast_snapshot_reference18.py` verifies
the distinction using two actual sessions and a strict `180006` version
gate. Both payload comparisons use the exact original 9000-byte strings.

The native fixture now first reads and checks both original rows before the
writer, establishing its old snapshot. Every existing old-payload,
deleted-row, vacuum deferral, orphan cleanup, and surviving-payload assertion
is preserved. A second reader begins before the same writer but first reads
after COMMIT; it must see only the updated row and its exact new payload.

## Evidence

- Original unchanged failure: `/tmp/dbms-heap-shared-ownership.x89vCqkb/btree-optimized-v4/vacuum_toast_test.log`, exit 134 at line 73.
- Diagnostic with original assertion retained: `/tmp/dbms-heap-shared-ownership.x89vCqkb/vacuum-toast-probe.log`, exit 134. The observed 9000-byte value was the new payload; both engines could still retrieve the old external payload. This was not evidence of a deleted TOAST object.
- Strict actual PostgreSQL 18.6 two-session proof: `/tmp/dbms-vacuum-toast-snapshot.W8qF2m/reference18.log`, exit 0. Lazy first read sees the new single row; an established snapshot sees both exact old values after writer COMMIT and VACUUM.
- Corrected native fixture and four adjacent controls: `/tmp/dbms-vacuum-toast-snapshot.W8qF2m/verify.log`, exit 0 (`vacuum_toast_test`, `toast_test`, `shared_btree_engine_toast_test`, `shared_heap_engine_rows_test`, `phase5_remaining_test`).
- Production source, all public headers, flags, linked native objects, and test-source before/after receipts are in `/tmp/dbms-vacuum-toast-snapshot.W8qF2m/proof/`. No production file changed. The reused immutable 58-TU basis has O2 owned storage objects and matching O0 remainder; this is not a new all-O2 or full-suite proof.

This closes the incorrect snapshot setup in this fixture, not the entire
VACUUM/TOAST or storage family. The independent heap WAL relation-identity
replay issue remains open.
