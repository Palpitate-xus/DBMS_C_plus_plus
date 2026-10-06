# Typed, demand-driven prepared child cursor

This is a foundation for quantified-query execution, not a claim that the
existing ANY/ALL/SOME parser or EXPLAIN failures are fixed. No production
dispatcher is switched to a quantified consumer by this change.

## Contract

`PreparedQueryCursor` owns one execution's actual operator graph and a copied
declared output descriptor. Construction, `descriptor()` and `plan()` do not
open or execute it. `next()` opens lazily and returns ordinal `ExprValue` cells,
not display strings. SQL NULL and empty text stay distinct; complete prepared
cells retain type and explicit collation. The cursor borrows its caller's
transaction, command ID, parameters and snapshot; it does not invoke PL SPI.

Explicit `close()` is idempotent and preserves a normal close error. Destruction
never throws. Cleanup is attempted only once, including after a failed open.
The original SQL/C++ exception, including `StatementCommitError`, takes
precedence over a secondary close error. Failed reads clear partial cells.
Interrupts are checked before open and after a returned row.

`QueryPlanner::makePreparedCursor(plan, descriptor)` consumes that same graph.
Its `plan()` pointer is the actual graph, allowing subsequent quantified plan
instrumentation without running a different SELECT for display statistics.

The optional `PreparedQueryExecution::setChildCursorFactory` receives the
original prepared child `Stmt*` and the actual typed caller `RowContext`. When
installed, it takes priority over the existing full-row compatibility callback.
Removing it restores that callback and changing either callback clears cached
initplan results. Separate original child sites remain distinct; uncorrelated
scalar results are memoized only within this execution, while correlated
children consume the current caller row on every invocation.

The scalar consumer checks the prepared width/type before the first read,
returns a declared typed NULL for no rows, and requests at most two rows:
the second row reports `21000` without evaluating a third projection. This is
not the future quantified receiver, which must stream an arbitrary number of
rows and implement its own ANY/ALL demand and three-valued semantics.

## Retained evidence

Private source parent: `587e4b20631432991b63a229e66ce29d22912271`.
Artifact directory: `/tmp/dbms-quantified-query-demand.upTaG4c0`.

The first fresh 58-TU O0 build, session 74618, exited 0. Its repeat, every
source/header/flag object signature, and production binary stamp matched.
Its initial binary is retained as `dbms_main.cursor.initial.o0`, SHA-256
`fd0d7169abee2eaf0b8ec4494297e10b07ac846bdf23f297ca1fd65b16265e81`.

The first native invocation, session 18101, exited 1 at a test-only C++ `auto`
declaration combining differently sized xid/CID types. Those declarations
were separated; session 89093 then exited 0. The stronger DISTINCT control
exited 134 in sessions 80843 and 23029: the output value was `actual`, but its
explicit `c` collation had become empty. The original expectation was kept.
`PreparedDistinctOp` now buffers the original complete typed cell, alongside
its existing legacy row, instead of reconstructing it after child EOF.

Final changed-CPP build/relink session 20776 exited 0. Headers were unchanged
from the initial fresh 58-TU group; the final repeat and all 58 signatures and
binary stamp again matched. Final binary SHA-256:
`da79d755520a51249600f5dcc92a50fcf1afdb07a195f1970aa13012a5517678`.

`tests/prepared_query_cursor_test.cpp` covers pure construction, actual graph
identity, structured fallback without display parsing, typed NULL/whitespace,
collation through LIMIT/OFFSET and buffered DISTINCT, early demand, invalid
width/type, primary/secondary error priority, C++ and commit-phase subtype,
close-once, early `57014`/`57P01`, two original SELECT sites, correlated caller
cells, callback compatibility, and real caller xid/CID/snapshot preservation.
It also drives a real prepared projection over a typed source: closing after
its safe first row avoids the later invalid CAST; requesting that later row
reports the original `22P02`.

Final native group 71288 exited 0: the new cursor test plus prepared carrier,
checked SQLSTATE, typed EXPLAIN, logical source, child sort identity, engine
owner, function atomicity, binding, routine resolver and ORDER metadata (11
separate native executables). Final serial protocol group 97274 exited 0 with
no failures: typed EXPLAIN (46 cases, TEXT/JSON), PL binding, SELECT INTO demand,
function atomicity, ordinary scalar WHERE/ORDER and child sort-slot controls
(7 scripts). Every owned server was stopped in `finally`; no old server was
touched. Frozen objects, source/header/flag manifests and final binary reside
in `cursor-foundation-frozen`; its separate audit exited 0.

The explicitly version-checked 30-control oracle repeat on container `pgref`
also exited 0 at server version `170002`, retained independently in
`reference-demand-checked-pg17.log`. No full-suite pass, PostgreSQL 18 parity
or quantified-family completion is asserted here.

## Next independent quantified work

The retained 30-case reference logs are explicitly PostgreSQL 17.2 diagnostics.
They show that scan-mode ANY/ALL can stop early, evaluates the left expression
per demanded right-hand row, and does not evaluate it for an empty SQL child.
Hashable uncorrelated equality ANY instead reads its RHS before evaluating its
left operand. Ordered keys may require all input rows even when a non-key
projection only needs one row. A future receiver must retain those differences,
NULL/empty truth tables and whole metadata preparation before writing CTEs.
The corresponding semantics are documented in the primary
[PostgreSQL 17 subquery documentation](https://www.postgresql.org/docs/17/functions-subquery.html)
and implemented by the tagged
[planner](https://raw.githubusercontent.com/postgres/postgres/REL_17_2/src/backend/optimizer/plan/subselect.c)
and [executor](https://raw.githubusercontent.com/postgres/postgres/REL_17_2/src/backend/executor/nodeSubplan.c).
These references do not turn the diagnostics into a PostgreSQL 18 comparison.
