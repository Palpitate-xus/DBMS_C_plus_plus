# Static arithmetic metadata and exact row-column identities

Date: 2026-10-06. Two independent source/test repairs are committed locally.
Their new ROOT combination is being built; earlier frozen-binary passes are
not evidence for this source revision. The overall 273-item goal remains open.

| Defect | ROOT commit | Repair and current evidence |
| --- | --- | --- |
| Arithmetic projection metadata guessed a casted operand's type rather than the complete operator result | `5de9c73d` | A shared internal operator-type rule is used by execution and AST metadata. REAL plus INT/NUMERIC returns DOUBLE, REAL plus REAL returns REAL, integer widths remain distinct, and comparison roots return Boolean. Dedicated native inference and wire controls include typed NULLs and empty table/JOIN results. Private changed-source O2 and adjacent regression results are retained, but are not an all-ROOT O2 result. |
| Case-insensitive RowContext keys merged quoted `"F"` with unquoted `f` | `666bf0d1` | Canonical value-reference AST nodes bind to private datum positions, retaining declared types and explicit NULL flags. CAST/COLLATE/EXTRACT grammar labels are not row columns. Actual baseline native exited 134 (`"F"+f` produced 2 rather than 3); actual baseline wire exited 1 (`CAST("F" AS BIGINT)+f` produced 2 rather than 3). Final private native, six adjacent natives and eight wire entry points passed. |

Quoted-row artifacts are retained under `/tmp/dbms-null-safe-join.0MiJvD`:
`native_expression_quoted_row.baseline`, `native_expression_quoted_row.green`,
`native_expression_quoted_row.asan`, and `dbms_main.quoted_row.green`.
The final helper source SHA-256 was
`4701b60829cf87bedef5131e4039d6b575f12e03e4d0b126a2683e87c6e54463`.
The private server used matching owner-development core objects and a fresh
O2 helper/evaluator with the old table adapter, so its result does not depend
on the uncommitted ordinary-arithmetic bridge. Partial ASan/UBSan instrumented
the helper and test, not the whole engine. An initial parallel fixture sharing
a working directory caused a WAL-cleanup recovery failure; the isolated rerun
passed. Both records are retained, not presented as a sanitizer finding.

Static arithmetic artifacts and final controls are also in that directory.
Actual pre-fix native `94323` exited 134: a Boolean comparison root was
inferred as REAL rather than Boolean. Retained pre-fix wire `654007` exited 1
on `(16777216::REAL+1::REAL) IS NOT DISTINCT FROM 16777216::REAL`: the value
was `t`, but its protocol OID was 700 instead of the required 16. This wire
test stopped at its first failure; its later mixed-numeric cases are not
presented as individually executed red controls. The original expectations
were not changed.
The private passing server SHA-256 was
`ccbc5935a142e328bff76af1b66d84aa02a9f5cb06f1a3e888b37c212a506119`.
Its eight wire scripts and five adjacent native tests passed; changed-source
sanitizer checks were partial. These development/partial-O2 results must not
be promoted to a freshly built integrated production result.

## Frozen ROOT verification checkpoint

ROOT source is frozen at `666bf0d1`. Build handle `31026` exited 0 after freshly
compiling all 55 production TUs from the normal shared O2 settings (the log
contains 55 compilation entries). A new internal header invalidated every
production object's shared signature. Normal repeat `7db521` reported
up-to-date and `12bc6a` verified 55/55 signatures and the binary stamp.
The build log is
`/tmp/dbms-static-type-quoted-combination.eMhU7n4T/production-build.log`.
Frozen binary SHA-256 is
`b418a16758bafa1de3d759cd81f4f020add8c537c3ca91339133ab672d91eb5e`.
That directory's `verify.sh` checks every production object signature and the
binary stamp, then freshly compiles/links 41 native entries or runs 38 focused
protocol entries. Native `83618` and protocol `91373` are live at this
checkpoint; their partial output is not presented as terminal success.
Unchanged known-gap diagnostic `17996` exited 1 with the same seven genuine
clause errors; it is not one of the supported green protocol entries.
The previous `86ddccf9` frozen binary's 39 native/36 focused wire/default-full
protocol passes remain evidence only for that previous frozen revision.

## Remaining work and boundaries

The ordinary-column table arithmetic bridge still bypasses the typed evaluator
and is independently being repaired. Plain table `EXTRACT(year FROM "D")`
returned an empty value instead of 2026 in an actual retained private wire
failure (`13549`, exit 1). A casted EXTRACT test exercises the helper's role
binding and passes, but it does not repair or replace this plain-dispatch red.
The original plain assertion remains scheduled for its independent repair.

Qualified flat row maps still have their existing prevalidated single-relation
fallback. This is not a complete SQL namespace/correlation binder. CASE/VALUES
common-type selection is not replaced by the arithmetic operator rules.
Broader operator overloads, ARRAY operators, numerical/storage/protocol
semantics and whole-family scope remain incomplete. ORDER/subquery/EXPLAIN,
typed UPDATE and procedural whole-query metadata binding have separate work.

An exploratory nested same-table NULL-count probe against the old frozen
`86ddccf9` binary passed all five controls; it is retained as a green probe,
not evidence of a reproduced bug or of general reentrant NULL correctness.

All mapped families stay partial. Totals remain 273 items: 22 complete,
166 partial, 70 unverified and 15 deferred_by_user. No push; Actions stay
disabled; user-deferred security/TDE work is not resumed.
