# Ordinary scalar subqueries in WHERE

Ordinary table SELECT predicates previously passed a scalar child through
the legacy expression/string path. A child function's P0002/22P02 exception
was swallowed as a false predicate; correlated columns, true SQL NULL and
scalar cardinality were not evaluated through a query context.

For a supported fromless or single physical source query, the new dispatch
prepares the whole original SQL AST before opening the source or evaluating
any function. It uses the shared prepared plan and scalar carrier, actual
engine/database, source occurrence plus physical ordinal, typed nullable
cells, ancestor scopes, and existing row demand. Errors propagate to the
existing outer statement owner; no successful SELECT tag is published for a
failed predicate. Genuine scalar sites retain separate per-execution memo
entries; no global raw-SQL or result-value memo is introduced.

## Evidence and limits

Private base efc501b9 was rebuilt from all 57 source TUs plus fresh test
stubs at O0 (72742, source/header audits 0). Frozen baseline SHA256:
`404b5d8879acf68e544257793f52339f2d79fb8e050663c50b16eb97d1b04f2f`.
The unchanged original clause gate 92872 exited 1 with exactly three real
failures (ordinary WHERE, ORDER and this pre-DML baseline's typed UPDATE).
The new matrix 91224 also exited 1 with correlated/NULL/cardinality/error
failures and an explicit transaction that did not become failed.

Main-only rebuild 26358 exited 0 against the matching immutable other 56
objects and unchanged headers. Its real wire matrix 79050 exited 0:
P0002/22P02 plus body-write rollback; explicit BEGIN/SAVEPOINT recovery;
quoted BIGINT/OIDs; one/two-depth correlation and a nested WITH child;
NULL versus empty text and text `null`; zero-row UNKNOWN qualification;
DISTINCT/OFFSET/LIMIT; lazy CASE; unknown callee/column/width pre-effect
errors; 21000 cardinality; per-execution uncorrelated memo versus genuine
two sites; correlated writer counts; existing multirow IN/EXISTS controls.
Unchanged gate 44199 exited 1 with exactly two failures (ORDER and pre-DML
UPDATE). This commit does not claim to fix UPDATE; ROOT has that separate
commit. Artifacts and logs remain under
`/tmp/dbms-ordinary-scalar-consumer.I1bn6m0R`.

Isolated reference 17.2 ran 17 controls using only temporary objects,
BEGIN/savepoints and final ROLLBACK, all passed. This is not PostgreSQL 18.6
parity. Semantics agree with the official [PostgreSQL 18 scalar-subquery
documentation](https://www.postgresql.org/docs/18/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES):
scalar children are single-column/single-row expressions, zero rows produce
NULL, and ancestor columns retain the surrounding query's binding context.

The matching native P0001 control required the independent no-host routine
fix documented in `issue-native-scalar-query-routines.md`; its original
assertion was preserved, not weakened. Final matching native session 47467
exited 0 for seven tests, including the new typed ordinary-plan test and the
independent no-host routine regression. Final serial protocol group 63956
exited 0: the WHERE-plus-native binary reran the new WHERE matrix and three
adjacent stored-function WHERE/atomicity/ORDER scripts successfully. The
same group first ran three native-only binary adjacent scripts. The final
WHERE-plus-native SHA256 is
`28238dd82ec024e7377f4661dbfd5f34f8e7433bc1d8d8a27b9c74777fb09188`.
All production source/header hashes were rechecked after those runs; no
public API/header/layout is added by the WHERE consumer.

This staged ordinary consumer does not replace the established top-level
CTE/materialized-view/catalog/JOIN/group/window routes or mixed multirow
subquery roles. ORDER-only scalar children and canonical SubLink sort-slot
reuse remain separate actual open issues. No blanket ordinary-query,
optimizer/performance, type/signature-family or full-checklist completion
is implied by these focused results.
