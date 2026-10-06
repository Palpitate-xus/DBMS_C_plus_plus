# Canonical single-source SQL range scope

## Actual baseline

The single-table dispatcher compared parser-decoded column qualifiers with
raw FROM/alias tokens, lowercasing both a second time. Consequently a valid
`AS "M" ORDER BY f("M".id)` raised 42P01, an invalid `AS m ORDER BY f("M".id)`
succeeded, and a valid `AS "m" ORDER BY f(m.id)` raised 42P01. The retained
three-case diagnostic `79860` used the immutable `3720350b` development
binary in `/tmp/dbms-function-metadata-namespace.QpsuV6` (SHA-256
`3f72db901fd77eb136a29c49d77a13b54d40108a469a7767492299f17060c9ec`).
Its terminal status was 0 because it printed evidence, not because the
candidate met the expected results. A real assertion regression exited 1
at the first valid uppercase alias (`7754`). PostgreSQL **17.2**, not 18.6,
confirmed the two valid results and the exact 42P01 negative result in
isolated diagnostic transactions which rolled back.

Additional controls actually found the same raw-token problem downstream:
`AS "m" WHERE m.id=1` and `WHERE public.table_name.id=1` returned zero rows;
`GROUP BY m.id HAVING m.id=1` returned both ids instead of id 1 (`46508`).
The expanded regression also failed on the first such predicate against
the initial range-only candidate (`75571`). A target function with a
wrong-case source qualifier reported 22003 instead of binding-time 42P01.

## Repair

The dispatcher now decodes raw FROM/alias tokens exactly once into a
single-source namespace with canonical schema, relation, SQL-visible range,
source occurrence, and physical column ordinals. Parser ColumnRef components
are already decoded; all comparisons use their exact canonical bytes.
An alias hides both physical relation and schema-qualified physical names.
Qualified columns come from the physical source descriptor, not the set of
output aliases. Missing ranges yield 42P01; missing columns within a valid
range yield 42703.

Every existing nonempty qualifier caller of
`validateFromlessColumnBindings` previously supplied raw `tnameOrig` or
raw `tableAlias`; none supplied an already-canonical qualifier. All these
callers now consume the namespace object. FROM-less/row-count callers retain
an explicit absent source. Projection namespace checking also occurs before
any target function executes, including unreachable CASE branches.

Quoted aliases containing spaces or dots use the existing identifier parser
instead of a delimiter ban. Bare ORDER/GROUP physical keys are lowered from
a bound ColumnRef, not by deleting a raw prefix. Simple column/literal
comparisons lower only their structurally validated source column; complex
qualified WHERE predicates retain the original expression for typed
evaluation. String data are not globally rewritten or dequoted. Existing
structured subquery handling keeps its separate legacy scope path.

## Validation and remaining boundaries

Final matching artifact:
`/tmp/dbms-function-quoted-range-complete.O4n6Hh/dbms_main.matched`, SHA-256
`53ad2671d0025b8a4eee6b0aac332ca56338359c66964ef4408e5fcbd1d396ac`.
Only `src/main.cpp` changed; it freshly compiled (`34496`, terminal 0).
The other 54 production objects use the immutable ORDER/API2 group, with the
independently rebuilt metadata-namespace helper. `link-matched.sh` verified
every translation unit's source hash against its originating object group
and all unchanged headers (`61243`, terminal 0). There is no new API/layout.
The expanded quoted-range regression passed (`87762`, terminal 0), retaining
case-sensitive aliases, schema/table qualification, alias hiding, output
alias negatives, empty-input errors, preparation zero-effects, quoted
space/dot names, WHERE, GROUP BY, and HAVING controls.

Three freshly linked matching native tests passed (`74860`, terminal 0):
metadata namespace, ORDER metadata, and scalar resolver. Seven focused wire
tests passed with the final matching binary (`70066`, terminal 0): ORDER
execution, WHERE execution, WHERE function scope, quoted alias, table CASE
AST, constant boolean predicate, and group DISTINCT/FILTER. The unchanged
full clause diagnostic still exited 1 with its original five failures
(`48658`); none was deleted or relabeled as a pass.

The first final test attempts `7689` and `71975` failed during the runner's
20-second server connection stage, before SQL assertions, with observed
startup I/O wait; they are not test passes. An earlier hand-written link
script used a wrong helper source-path condition. Its binaries/results are
retained but are not matching-HEAD proof; the final artifact above is the
corrected source/object-audited link, not that intermediate candidate.

The additional `typed_group_key_protocol_e2e_test.py` gate failed with
`ORDER BY count(*)` reporting 42883 (`15798`). The unchanged immutable
`3720350b` binary reproduced exactly the same failure (`67520`), so the
aggregate ORDER role is a separate unresolved problem, not a passed
adjacent gate. Full scalar-function preparation on empty CASE projections
also remains unresolved. This scope fix does not repair scalar-subquery
execution, EXPLAIN ANALYZE, typed UPDATE, every query scope, or the optimized
ROOT combination. Related broad review families remain partial.
