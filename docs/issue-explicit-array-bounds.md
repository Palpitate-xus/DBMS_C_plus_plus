# Explicit array bounds retain actual values

## Cause and implementation

Both the expression cast parser and native storage normalizer accepted only a
brace body. A valid PostgreSQL input such as `[0:1]={3,4}` therefore failed before
the containing INSERT could produce rows. Empty-table failures afterwards were
cascades of this one producer error, not independent DELETE failures.

`SqlArrayText.h` supplies a shared comma-delimited array text envelope parser.
It retains each dimension's actual lower bound and length, validates the
declared lengths against the full rectangular body, keeps SQL NULL distinct
from quoted text `NULL` and empty text, and checks signed int32 bounds and the
six-dimension limit without overflowing intermediate arithmetic. Default lower
bounds canonicalize to a brace body; non-default lower bounds remain in output.
No heap, WAL or schema-file format/layout changes are introduced.

Native INSERT/UPDATE normalization and actual ALTER element conversion use the
same envelope. Scalar codecs still own element conversion, overflow and typmods.
Expression casts preserve the envelope while converting its elements. Array
inspection, scalar fetch, 1D slicing, positions, concatenation, append/prepend,
nested constructors and comparison retain the tested dimension semantics.
Metadata preparation keeps array OIDs with element typmods; a multidimensional
postfix fetch is folded as one receiver plus all indexes, not an inner fetch
whose intermediate NULL would destroy the outer expression.

This is not a claim about every custom array element type/delimiter or every
dynamic/multidimensional slice grammar variant. The native ExprHelper's relaxed
constructor-postfix grammar is documented separately; it is not presented as
legal PostgreSQL SQL without parentheses.

## Exact controls and terminal evidence

The complete new protocol fixture passes against strict local PostgreSQL 18.6
after checking `server_version_num = 180006`, and against the candidate with the
same SQL, rows, NULLs, OIDs, SQLSTATEs, ReadyForQuery states and default deadline.
It includes 1D/2D negative and zero lower bounds; integer, VARCHAR/CHAR and NUMERIC
element conversions; NULL/empty values; full and insufficient scalar indexes;
1D slices; metadata/position functions; ANY/unnest; appending/prepending/nested
construction; value-plus-dimension comparison; bad shape and signed-bound
overflow; stored/indexed values; actual INT[] to BIGINT[] rewrite; savepoint
rollback; and failed narrowing with all original values and BIGINT[] metadata
retained.

The exact old matching 6320+owner binary fails the full new matrix (exit 1).
The first new-header candidate retains three genuine protocol failures:
array-modifier scalar OIDs, independently folded inner multidimensional indexes,
and a slice grammar envelope mistakenly treated as an opaque value. All three
are fixed without changing the fixture's SQL or assertions. The final full
15-script serial group exits 0, including all 14 prior pattern/array fixtures;
the ten additional descriptor/RETURNING/WITH/quantified scripts also run intact.

The full native matrix uses actual production objects, exact native typed input,
NULL bitmaps, rejected bounds/element overflow, indexed values, a real type
rewrite, and cold reopen. Nineteen native fixtures pass. Its first authored
fixture used makeIntColumn scale 4 (BIGINT, not INT) and assumed alphabetical
projection order instead of the actual physical-schema order. Those test errors
are corrected to actual INT scale 2 with a type assertion and exact schema-order
rows/NULLs; all array inputs and overflow/cold/index controls are retained. The
first eight assertion failures and the first scoped sanitizer assertion failures
are preserved and are not counted as eight production bugs. The original native
multidimensional subscript oracle correction is a separate test/docs commit,
including its original exit-134 evidence and equivalent standard PG operator
results.

## Build and sanitizer boundaries

Private tree: `/tmp/dbms-explicit-array-bounds.gYPJNr09/worktree`, base
`f7d64635` (ROOT 6320 plus the independent native DELETE owner fix).

The new public helper header is covered by an independently fresh normal O2
compile of all 58 production units. The V2 changes only three CPP files, freshly
recompiled with all other 55 source/header/flag receipts checked before relink.
The final frozen candidate SHA256 is
`0b4edc53d01faee2dcb104dbe3e0b70254654796e8b7111a803500ea9386d620`.
ASan/UBSan covers the five changed production CPP units, the complete new native
fixture and test stubs; the other 52 non-main production objects are matching
normal objects. This is not an all-58 sanitizer claim; leak detection is disabled.

Artifacts outside the repository retain `reference18-array-bounds-final.log`,
`ours-array-bounds-exactbaseline.log`, `build-fresh58-v1.log`,
`native19-v1.log`, `final15-serial-v1.log`, `scoped-san4-v1.log`,
`build-changed3-v2.log`, `native19-v2-final.log`, `final15-serial-v2.log`,
`adjacent10-serial-v2.log` and `scoped-san5-v2-final.log`, with per-script logs.
The initial Python syntax-error attempt is retained separately and is not a
runtime reference result. No failed fixture is removed or registered as green.
