# Native DELETE exception owner

## Scope and cause

`StorageEngine::removeRows` already rolled back non-OK status returns at its
implicit transaction or internal statement-savepoint boundary. Exceptions from
predicate evaluation or a typed mutation observer bypassed that cleanup. This
left an implicit transaction open, leaked observer writes made through the same
owner, and left the caller's deleted-row output partially populated.

The fix catches exceptions around `removeInternal`, calls exactly the existing
status-error rollback boundary, restores the original output container, and
rethrows the original exception and SQLSTATE. It does not change row retirement,
heap identifiers, snapshot images, WAL, public headers, or the success path.

## Exact native evidence

The full `delete_exception_owner_test.cpp` is linked to actual production
objects and uses the public native APIs. Its old matching-runtime run reached
the final assertion with nine failures (exit 134), rather than stopping at the
first leaked owner. Diagnostic rollback is performed only after recording a
failure so subsequent independent controls still run.

Both old and candidate runs retain these controls:

- invalid and demand-dependent trailing LIKE ESCAPE: SQLSTATE 22025;
- an observer that writes audit rows through the same transaction, then throws
  SQLSTATE 22012 on its second observation;
- implicit transactions and existing parent transactions, both with and
  without a caller savepoint;
- original caller output, zero affected-row count, owner state, prior parent
  writes, and rollback of only the failed statement's audit effects;
- five exact rows distinguishing NULL, empty text and text `NULL`, a 20,000-byte
  TOAST value, primary-key and secondary-index lookup controls;
- subsequent transactions, committed prior writes, a cold reader and lock reuse.

The fixed full native matrix exits 0 with `DELETE_OWNER_FAILURE_COUNT=0`.
Fourteen unchanged adjacent native fixtures also run against the same matching
production objects. ASan/UBSan coverage is scoped to TableManage.cpp plus the
new native fixture and test stubs; the other 56 non-main production objects are
matching normal objects. This is not an all-translation-unit sanitizer claim.

## Separate, still-red protocol contract

`delete_exception_owner_protocol_e2e_test.py` retains an additional whole-wire
contract, including exact rows, NULLs, OIDs and ReadyForQuery transaction states.
All its SQL and assertions pass on the local PostgreSQL 18.6 reference after
checking `server_version_num = 180006`. The default wire deadline is unchanged.

On both the old and this native-owner candidate, the original command

```sql
DELETE FROM delete_owner_rows WHERE id<=2 RETURNING delete_owner_note(id);
```

does not evaluate its volatile RETURNING function and reports `DELETE 5`.
Six assertions fail, including subsequent row/state controls affected by that
deletion. This is a separate frontend consumer failure, not proof that the
native owner fix failed or that the whole DELETE family is complete. The full
failure logs are retained, and this still-red script is not added to the green
E2E registry. It is carried forward intact to a separate frontend fix.

## Matching build and retained artifacts

Private tree: `/tmp/dbms-native-delete-owner.lXFKajuO/worktree`, based on
`a7a53e3170a4ab0c1578656460159976ce30c5b6`.

The initial 58-object baseline is an explicitly audited migration of the
immutable matching pattern build, not 58 newly compiled objects: every source,
relative header and header byte, compile/link flag, original receipt and object
byte was compared, then new-path receipts were generated. Only TableManage.cpp
changed, so its normal object was newly compiled; the other 57 production
objects were re-audited before linking and freezing.

The candidate frozen binary SHA256 is
`231879c6ae7d9c806b310d49d463cc197cb9cba55fc4c7efc4168effd544e30a`.
Artifacts outside the repository include `migrate-baseline58.log`,
`native-exactbaseline-retry.log`, `native-owner-candidate-v1.log`,
`adjacent-native-final.log`, `scoped-san-v1-final.log`,
`reference18-owner-whole.log`, `ours-owner-whole-baseline.log`, and the complete
`final15-serial-whole.log` plus per-script logs. Compile-only failures and the
first sanitizer receipt-race attempt are also retained, not passed off as
runtime results.
