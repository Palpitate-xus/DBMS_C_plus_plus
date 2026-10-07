# Native DELETE USING actual source ownership

The frontend DELETE consumer did not close the separate direct public native
DELETE USING path. Its old projection/condition adapter accepted ambiguous
RETURNING names, could not own arbitrary function output or nullable sources,
and did not preserve the whole qualification/projection demand graph.

The native bridge now consumes the genuine prepared DELETE root through
BoundDmlExecution and the private actual source provider introduced by the
independent UPDATE source fix. Table/derived/view cursors, JOIN occurrences,
typed NULL extension and source provenance remain those of the original
prepared graph. Duplicate source matches modify each target RID once, and
RETURNING is evaluated once per affected target. Planning and execution are
owned once inside the existing atomic unit; no successful partial legacy
mutation is retried, and no row is rendered into replacement SQL.

ONLY, cursor-positioned and view-trigger boundaries are preserved. The public
NULL payload spelling remains `NULL` with authoritative per-cell bitmap; raw
text `NULL` still has a false bitmap. The independent writer-status API adapter
keeps historical boolean status failures while preserving real DbError
exceptions. No public header/layout, WAL, row identifiers, retirement, storage
generation, snapshot or ROOT DEFAULT callback changes are introduced.

The new direct-native fixture retains five exact rows with nondefault integer
array bounds, SQL NULL, empty text/array and ordinary text NULL. It checks pure
42702/42883 no-effects, zero-match typed output descriptors, dynamic 22012
projection rollback and sequence counts, implicit and existing parent owners,
prior NULL writes, duplicate source matches, actual function RETURNING, nullable
LEFT sources, exact cell/NULL-bit pairs and all excluded rows. The complete
matching old run has twelve failed assertions/exit134; the new run exits0.
Diagnostic reseeding occurs only after recording baseline no-effect failures.
No input or failed assertion is replaced by cleanup. Output ordering is
normalized only as complete paired rows/NULL bits for the unique-id comparisons.

The corrected original dml_semantics fixture now runs all its controls to a true
terminal, including retained original ambiguous SQL negatives, qualified
positives, actual LEFT SQL and all original count/value/atomic/parent controls.
Its strict PostgreSQL 18.6 oracle verifies the original SQL states and no-effect
images plus qualified positive results. The exact temporary reference schemas
isolate the SQL replay in its own connection. The original owner/new DELETE and
new UPDATE full protocol matrices also pass strict server_version_num 180006;
the wire deadline remains 15 seconds.

Private tree `/tmp/dbms-native-delete-using.7TWH4a7l/worktree` is based on
`ef6beeb603c95dbbb9dcb51c9d6a27f77eb6cd40`, not ROOT's subsequent public-header
epoch. Its 58-object foundation is an explicit per-source/all-relative-header/
flags/manifests/receipt/object-byte migration from the immutable source tree,
followed by official O2 recompilation of DmlExecutor.cpp and a matching-other57
audit. This is not 58 fresh production CPP compilations. Final normal frozen
SHA256 is
`0bc1ffdc5ddc0aa652731935d86374177cc4785e0e36c5697f0adebeeb4fadec`.

The final one-wrapper strictly serial group contains 40 unchanged/strengthened
complete normal fixtures and 8 complete scoped-sanitizer fixtures, all exit0;
all test-owned finally servers are gone. The normal native adjacent group has
34 whole fixtures, all exit0, plus the independent exact status-API fixture.
ASan/UBSan instruments main.cpp and DmlExecutor.cpp plus the native drivers and
stubs; other56 production TUs are matching normal objects and leak detection is
disabled. This is not all58 sanitizer coverage. Scoped frozen SHA256 is
`261b12f061a9b0af08720e23063e0a56a83815703b6d1fd4b13dacb1395e3656`.

Old exact failures and all compile/runtime/reference/final audit logs remain in
the private directory. Earlier DELETE/SOURCE whole groups with the original
oracle error remain exit1, not retroactively called green. Complete cursor
mutation, native function-range/writing-CTE source lowering, all collation/ARE
families and any untested source shape remain OPEN; successful tested carriers
are not a claim that every DML or PostgreSQL capability is complete.
