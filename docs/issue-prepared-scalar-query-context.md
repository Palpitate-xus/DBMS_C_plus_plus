# Prepared scalar query execution context

This is a shared metadata/runtime prerequisite, not a claim that ordinary
WHERE/ORDER scalar-subquery consumers or the five outstanding clause
diagnostics are repaired.

## Contract

`StorageEngine::prepareBoundQuery(db, sql, bindings={})` extracts the existing
PL/pgSQL binder's copied catalog/schema metadata builder. Preparation returns
the owned `PreparedQuery` AST, raw byte provenance, typed parameters, output
descriptor, and source descriptors without executing SQL, beginning a
transaction, advancing its command ID, or refreshing its ReadView. Routine
types use the existing canonical `scalarFunctionResultType` resolver with
the actual engine, not a second raw-name `getUDF` lookup. Existing incomplete
aggregate overload/type analysis is not declared complete.

`ColumnRefExpr::binding` is an optional `QueryColumnBinding` containing
`scopeDepth`, `sourceOrdinal`, `columnOrdinal`, `declaredType`, and
`mergedUsing`. The pure binder attaches it at the actual namespace match.
Source ordinals identify unique range occurrences in that prepared query,
not raw SQL strings or vector positions which shift when JOIN USING inserts
a merged range. Column ordinals belong to the copied source descriptor
(physical for a base table). Qualified left/right inputs and the virtual
USING output remain distinct. Output aliases are not mislabeled as source
datums. `PreparedQuery::sourceRanges` retains occurrence/owner/source-pointer,
canonical namespace, copied columns, and merged-range status. Those pointers
belong to the owned AST and survive its ownership move.

Opaque lexical frames retain the depth of WITH definitions and non-LATERAL
derived sources without exposing sibling FROM ranges. Ordinary parent
correlation therefore records depth 1; the tested scalar/derived/WITH/CTE
ancestor records depth 3. Identically named ranges in different levels,
quoted case-distinct aliases, JOIN predicates, and DML target/source
occurrences retain separate identities.

`ExprEvaluator::setScalarSubqueryExecutor(callback)` accepts
`ExprValue(const Expr*, const RowContext&)`. Only evaluating the particular
prepared-child node invokes it; unreachable CASE branches do not execute a
query. Without a callback the existing explicit 0A000 guard remains. A
prepared scalar child records its static type on its actual Literal node.
Two identical SQL children retain two different AST/site identities; no
raw-SQL result cache or callback-side type probing is introduced.

`StorageEngine::executeScalarSubquery(db, sql)` accepts an already-bound,
typed legacy child SQL adapter. It uses the existing embedding query host
directly with `PlPgsqlQueryOptions::Purpose::OrdinarySubquery` and a receiver
demand of two rows. It does not enter PL/SPI command-counter/snapshot
refresh logic. Main's host executes such children through
`executeQueryChild`, inheriting CTE/calling transaction state and without
adding a stored-function host frame. The caller owns the outer statement.
Zero rows return typed SQL NULL; width errors are 42601, multiple rows 21000,
and execution SQLSTATEs propagate unchanged. Values are structured cells,
not CLI display tokens. Native callers keep the checked native SELECT subset.

This helper does not re-prepare raw child names without their parent scope.
Consumers must clip/rebase original spans and typed parameter uses after
whole-statement binding. Correlated SQL columns need their true bound
source/ordinal runtime cells, not fake PL variable datums. Ordinary clause
and EXPLAIN consumers are separate follow-up work.

## Evidence boundary

Private artifacts: `/tmp/dbms-prepared-scalar-query-context.5aunU8`.
The new ExprEvaluator callback field, query-purpose options, AST binding
fields, and PreparedQuery source descriptors change public layouts/APIs.
All **56** production objects and stubs freshly rebuilt with these headers
(`52734`, terminal 0). After the derived lexical-frame strengthening,
only the binder translation unit rebuilt and the binary relinked
(`54681`, terminal 0); unchanged headers and final source hashes were audited.
This is not a claim that the earlier full-build source was the final binder.
Final matching development binary SHA-256:
`a4263b12198d8e367ec82bcb58e0577889f3cc4637e3631b9e1d0a2cff01977b`.

Seven fresh matching native tests passed in final `17144` (terminal 0): the
new prepared scalar context regression, query binding, metadata namespace,
ORDER metadata, scalar resolver, constraint expression, and statement
atomicity. The new regression uses an independent engine, verifies zero
preparation writes, unchanged XID/CID and snapshot boundaries, lazy callback
execution, missing-context 0A000, actual native multi-row 21000 and width
42601, zero-row typed NULL, exact empty/text-null/spaces/quotes, and typed
host error/purpose/demand propagation. It also checks parent/CTE provenance,
USING output identities, quoted multi-range aliases, DML target/source
identity, and output-alias separation.

Six focused protocol scripts passed (`49213`, terminal 0): PL query binding
(43 cases plus two sequence pre-effect controls), function atomicity,
stored ORDER, aggregate ORDER role, quoted range scope, and SELECT INTO.
The unchanged full clause diagnostic exited 1 with exactly its original
five failures (`25193`). No ordinary clause gate is relabeled green here.

Initial native harness runs retained actual failures: a bool/DBStatus
assertion mismatch, invalid insert initializer, and map/vector accessor
mistakes were corrected without weakening expectations. The first CTE
fixture also exposed the parser's unsupported direct parenthesized WITH
scalar syntax (`45927`/`32182`, 42601 at AS). The provenance regression uses
the already-supported derived-WITH syntax; direct WITH scalar parsing is
still a separate open issue. This prerequisite does not complete correlation
runtime, generic query support, aggregate typing, or the ROOT optimized
combination. Broad review families remain partial.
