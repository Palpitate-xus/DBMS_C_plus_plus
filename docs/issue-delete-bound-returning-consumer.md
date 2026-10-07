# Retained ordinary DELETE consumers

## Runtime ownership

The frontend's genuine prepared DELETE entry was restricted to pattern WHERE
predicates. Ordinary DELETE could therefore reach a thin legacy adapter that
did not own arbitrary WHERE or RETURNING expressions. The original owner wire
fixture observed `DELETE 5` for a two-row predicate, no volatile RETURNING
execution, and six resulting assertion failures. A separate native exception
owner fix does not fix that consumer.

The frontend now gives ordinary DELETE its retained whole AST and the existing
prepared mutation runtime. It uses actual typed target rows, real USING source
plans and child cursors, evaluates qualification and RETURNING once at their
proper demand points, and keeps the atomic owner around all mutation effects.
View targets still belong to their existing INSTEAD OF trigger boundary.

The public native `tryDmlBridge` target-only DELETE path similarly prepares and
consumes the whole bound mutation rather than adapting it to condition and
projection strings. Its historical output spelling for SQL NULL remains
`"NULL"` alongside the authoritative NULL bitmap. This adaptation occurs only
at publication; ordinary text `NULL` has a false bitmap entry, empty text is a
non-NULL empty cell, and the internal prepared runtime retains typed cells.

Only main.cpp and DmlExecutor.cpp change. No production header, public layout,
transaction snapshot, row retirement, SID, WAL or storage generation changes.
The native `USING` legacy entry, ONLY and cursor-positioned mutation boundaries
are not claimed complete by this target-only native change. Nullable outer
DELETE sources through the frontend are exercised as actual successful plans;
the separate UPDATE outer-source and missing-cursor capabilities remain OPEN.

## Preserved strong evidence

The original owner protocol fixture remains byte-for-byte unchanged and now
passes. The new full DELETE matrix retains its original seeds and all original
queries/assertions, including the explicit `[0:1]={3,4}` array producer that
previously exposed the separate array-bounds failure. Additional nullable LEFT
JOIN source controls do not replace any original input or assertion.

The complete protocol matrix checks FALSE and zero-match demand, zero-row
descriptors, missing static functions, actual PL/pgSQL volatile WHERE and
RETURNING calls, exact sequence counts, late 22012 rollback, same-owner audit
writes, prior parent writes and savepoint/ReadyForQuery states. It distinguishes
SQL NULL, empty text, text `NULL`, raw BYTEA, padded Unicode CHAR and array bounds.
It checks exact rows/OIDs/tags, duplicate USING matches deleting a target once,
nullable outer source rows, and successful later statements after errors. Both
full protocol fixtures pass the local strictly checked PostgreSQL 18.6 oracle
(`server_version_num = 180006`), with the original 15-second wire deadline.

The direct native fixture calls the public bridge, not a frontend substitute.
It checks exact AST handling, ambient-session restoration, typed NULL bits,
zero-row metadata, pure errors before mutation, volatile projection counts,
parent transaction preservation and exact surviving rows. Its final corrected
setup creates the sequence through real CREATE SEQUENCE DDL: the native
file-only sequence API does not register a SQL regclass catalog identity.
The original authored setup's 42P01 and sequence value 1 are retained as test
setup failures, not attributed to the runtime. With the identical final fixture
and matching old array-bounds production objects, nine assertions still fail
and exit 134; with this consumer they pass. Earlier eleven-failure and subsequent
two-failure logs are also retained, not counted as independent production bugs.

## Matching build and evidence artifacts

Private base: `506daa804fe45194ee2fc0fa14a83d7f39d5699b` (array bounds plus the
native owner), not the later ROOT public-header epoch. Directory:
`/tmp/dbms-delete-bound-integration.fkakvwdN`. The initial 58 objects were migrated
from the immutable matching array-bounds build only after comparing each source,
every relative header filename and byte, all flags/manifests, old receipts and
object bytes. New-path receipts were generated after those comparisons. This
is an audited migration, not 58 newly compiled objects. Both changed production
CPPs were then compiled with the official O2 flags; the final native publication
adjustment rebuilt DmlExecutor.cpp and re-audited the matching other 57 objects.

The final normal frozen binary SHA256 is
`9d0a96216642f7296badfb8d87d5732fc4e9767b9732110bc8490e1e69f75ed6`.
Scoped ASan/UBSan instruments main.cpp and DmlExecutor.cpp; its other 56
production TUs are matching normal objects. The native test and stubs are also
instrumented. Leak detection is disabled. This is not all-58 sanitizer coverage.
The scoped frozen binary SHA256 is
`d7e9e8600df4ab6b1be50649633198e1f217394dfa8fc24d2b84f4d276fff8e3`.

Artifacts include migration/build receipts, strict-reference full logs,
`baseline2-arrayfixed-serial.log`, `native-arrayfixed-deletebaseline.log`,
`native-delete-corrected-oldmatching.log`, all V1/V2 native and sanitizer logs,
`whole33-delete-v1-serial.log`, `native25-delete-v2.log`, and final serial
normal/scoped-sanitizer per-fixture logs. The original V1 33-script group exits
1 at the old source-DML RETURNING oracle error. Its exact old frozen baseline
passes the erroneous fixture, while the exact PostgreSQL original fixture exits
1 with 42702. The independent oracle commit retains the ambiguous original SQL
as 42702/no-effect/Ready negative controls and adds qualified positive controls;
it does not hide that history or claim the unsupported boundaries are closed.
