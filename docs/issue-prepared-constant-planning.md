# Execution-owned pure constant planning

Status: independently verified foundation. This does not close materialized-view
target DML or the complete ordinary-query/planner families.

## Defect and phase boundary

Whole preparation correctly checks SQL names/types and unknown input conversions,
but the existing `prepareExpression()` only implements CASE-specific constant
planning. It cannot by itself give `SELECT 1/0 LIMIT 0`, a reached scalar child
containing `1/0`, or `UPDATE ... SET id=1/0 WHERE false` the PostgreSQL planning
error without opening a source or executing an expression at runtime.

Conversely, eagerly walking every query target is wrong. PostgreSQL 18.6 permits
an unused derived/default-inline CTE target containing `1/0`; explicit
`MATERIALIZED`, DISTINCT, sort-key targets, volatile/SRF targets, implicit JOIN
USING keys and genuinely referenced/correlated output cells retain their demand.
CASE, boolean and COALESCE dead arms must not acquire runtime/planning demand.
Whole binding still precedes this phase, including errors in dead-branch names
and analysis-phase unknown string input.

## Interfaces and ownership

`PreparedQueryExecution` exposes explicit `planExpressionConstants(Expr*)` and
`planStatementConstants(const Stmt*)`. Default `prepareExpression()` remains
unchanged unless the caller has explicitly planned that expression first. The
planning visitor operates on execution-owned copies; actual consumers still
perform their usual runtime callback binding before evaluating those copies.

The supported statement carrier covers retained SELECT/VALUES, WITH, INSERT,
UPDATE, DELETE, EXPLAIN children and structured CTAS/default expression roles.
Unsupported statement shapes fail with `0A000`; this is not a claim of MERGE or
all DDL planning support. The visitor never obtains a planning datum from a
column, parameter value, routine invocation or child/source cursor. Declared
IMMUTABLE stored routines are not executed either. Existing pure typed
operators/casts and SQL CASE/boolean/COALESCE demand are handled structurally;
this is not complete PostgreSQL builtin/catalog optimizer constant folding.

`PreparedQuery::projectionBindings` records every expanded SELECT output ordinal
as `{original expression site, optional QueryColumnBinding}` during the actual
binder namespace traversal. A star retains its original site for every output
and each exact occurrence/column ordinal, including duplicate labels, quoted
names and USING merged ranges. No row values, SQL reconstruction or name lookup
are used to discover star provenance. Correlation permission/declared type are
validated against real retained owners. Each carrier unions demands for repeated
NOT MATERIALIZED occurrences without collapsing genuine expression sites.

The AST now preserves `CTE::materializationSpecified` separately from the legacy
default-true `materialized` flag. Sort identity includes that distinction. All
original SQL spans, parameter cells, routine/quantified/SRF metadata, array
concatenation bindings, implicit cast flags and simple CASE comparison metadata
remain in the owned copies.

These public metadata/private carrier layout changes require all translation
units to be rebuilt together. The prior object set is not an ABI donor.

## Retained evidence

The permanent strict PostgreSQL oracle is
`tests/compat/prepared_constant_planning_reference18.py`. It verifies
`server_version_num=180006`, runs EXPLAIN without ANALYZE inside an isolated
transaction, and uses a monotonically increasing sequence sentinel to prove
planning did not invoke writers. It rolls the transaction back afterwards.
The permanent native `prepared_constant_planning_test.cpp` uses an independent
engine/database, installs child/cursor callbacks that fail if called, and checks
the original AST, genuine typed parameters, array/quantified/SRF metadata,
projection ownership and no-effect sequence state.

Artifacts are retained under `/tmp/dbms-materialized-target.fBH1T6Yr`:

- The original MV protocol baseline and native baseline remain failed. The
  protocol has actual successful illegal writes, incorrect SQLSTATEs and an
  unwanted writer call; native 17 controls have 15 SQLSTATE failures. Those
  failures are not attributed to the independent sequence-generation repair.
- Strict 18.6 oracle v4 passes the first 46 controls. V3's draft LIMIT-unused
  expectation was disproved by real PostgreSQL and retained separately.
- The first fresh 58-object candidate passes those 46 native controls, but its
  unchanged expanded controls genuinely fail COALESCE, correlation and JOIN
  input-demand checks. The next candidate's incorrect COALESCE role reference
  fails with 42883; both failed logs remain retained.
- The expanded star/NOT MATERIALIZED candidate also fails true output-ordinal
  controls. The final strict 18.6 oracle v6 passes all 65 controls without
  resetting the sequence or relaxing deadlines.

Final fresh 58-source O0 build, all 101 public/private header signatures, source
signatures and native fixture signatures pass. Its immutable candidate SHA256
is `7c713fad51732fdb188129e40f254f87d6ecbecde286eed6c081a3e3c92c789a`.
All 65 native controls and the extra owner/parameter/original-AST/default-timing,
typed Q/SRF/array, wide COALESCE and duplicate-star metadata guards pass. Nine
matching adjacent native tests pass: prepared-query execution/cursor, query
binding, simple CASE constant/equality, quantified execution, independent engine
owner, WITH primary DML binding and genuine ArrayExpr. Their retained terminal
results are 86069=0 and 92917=0, not unfinished output observations.

Scoped ASan/UBSan recompiles parser, binder, carrier, helper, stubs and the test
against the matching other native objects; all 65 controls/extra guards pass
(34191=0). Leak detection is disabled for the process-lifetime registries/caches;
this is not a full sanitized build or a leak audit. Strict reference 18.6 v6
likewise passes all 65 controls. No deadlines or failed expectations were
weakened. The normal candidate does not activate the new explicit APIs in main,
so this is not a protocol-consumer integration claim or a ROOT formal O2 proof.

No claim is made that the MV target defect or any complete planner/query family
has been repaired by this foundation alone.

ROOT integration also updates retained VIEW projection sites when its UNKNOWN
output is wrapped in an implicit TEXT cast. The new root, not the still-live
operand, is the output expression identity. Genuine source-context assertions
cover both NULL and empty-string VIEW outputs; matching ROOT runtime verification
is still required after a complete fresh public-layout build.
