# Preserve every UPDATE assignment before semantic validation

## Actual reproduction and reference

The old map-based UPDATE AST discards earlier duplicate target/RHS sites.
Original native parser session82218 exits134 and original frozen-ROOT wire
session99078 exits1 with twelve failed assertions, including hidden bad names,
accepted duplicates and volatile writer/sequence effects. This is not merely
an error-message difference. Canonical unquoted A/a and quoted lowercase a
collide; quoted uppercase A is a distinct column and remains legal.

Actual PostgreSQL17.2 diagnostic reference uses a unique schema, transaction,
per-statement savepoints and final ROLLBACK. The permanent matrix now contains
all original 24 cases plus eight nested FROM/window cases. The final reference
exits0. Bad relation/name/function and unknown input conversion errors precede
duplicate 42601; constant arithmetic 1/0 and typed numeric narrowing must not
be run first, and volatile RHS/RETURNING writers must not run at all.

## Repair

UPDATE setClauses is an ordered vector of original target/expression pairs,
not an overwriting map. The parser preserves each site and original source
span, including contextual DEFAULT. Existing prepared-scalar native test
uses its sole original expression through front rather than map::at.
All ordinary storage-resolver consumers still validate unique target columns.

Duplicate semantic preflight runs before ordinary/FROM/DEFAULT/RETURNING
fallback dispatch, after the original database and UPDATE privilege checks.
It prepares the original whole AST and actual target descriptor without
scanning rows or running volatile callbacks. It checks every target, names,
function namespace, WHERE boolean type, RETURNING and direct unknown literal
input casts. The input visitor includes FROM/joins/derived children, CTEs,
VALUES, DISTINCT/GROUP/window definitions and actual scalar children. It
does not fold arithmetic or execute scalar children to determine this error.
No writer/default/RETURNING value is evaluated before duplicate rejection.

## Evidence and honest remaining gate

Artifacts: `/tmp/dbms-update-duplicate-target.2pS2rLEP`.
The vector AST public layout required true fresh57 normal-O2 build5450/exit0;
its first ordinary26native/12wire gates passed but the stronger reference
exposed real DEFAULT/FROM/RETURNING/input-priority failures, retained in
duplicate-priority-candidate-v1.log and duplicate-strong-candidate-v1.log.
V2/V3 correct those priority failures and passed their 26-native/12-wire
gates without removing any original assertions. The eight new nested-source
controls then reproduced seven failures on unchanged V3.

V4 build recipe omitted dbms_main_sources, failed linking and copied an old
binary afterward. That artifact is not a successful candidate despite the
outer shell's last command status. The failed build log is retained.
Correct V5 initializes the complete source manifest, rebuilds the changed
DML CPP at normal O2, verifies all57 object signatures and binary stamp, and
uses the unique frozen server SHA256
`895cc80451a073e1309d9420efabe44e29091979bde33c218f4fddab31ca32a3`.
Other56 units remain source/header-identical to the original fresh57 basis.
Native46486 exits0: all26 freshly compiled/linked tests passed. Wire17149
exits1: eleven adjacent entry points passed and the strengthened dedicated
matrix retains exactly one failure, also independently retained in39904.
Six of the seven new failures are repaired. The remaining derived
row_number OVER(ORDER BY CAST('bad' AS INT)) is wrongly rejected by the
separate routine metadata callback with42883 instead of PG22P02. That
window binding root cause is still open and must not be dropped or called
green. The dedicated E2E is registered normally, with no skip or expected
failure conversion. Complete-family closure is not claimed.

No new combined ROOT/full-suite or all-sanitizer claim is made. PostgreSQL17
diagnostics are not a PostgreSQL18 oracle. Views, general assignment types,
versioned RETURNING pseudo namespaces and broader DML routes remain tracked
by the overall audit. No push or Actions enablement occurred.
