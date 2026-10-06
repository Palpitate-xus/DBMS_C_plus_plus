# Stored scalar functions in WHERE

This independent repair fixes execution and preparation of stored scalar
functions in single-table WHERE predicates. It does **not** complete the
stored-routine/query compatibility families or the overall review checklist.

## Cause and repair

`NetworkServer::whereUnknownFunctionError` previously classified every
non-builtin call as undefined, even when the stored function existed. The
typed expression evaluator also had no database routine resolver. The legacy
function-left predicate path split arguments manually and treated every
NULL column argument as a strict call, disregarding the routine's own NULL
contract.

The shared `ExprEvaluator` now resolves canonical function metadata without
executing a routine. Builtins are resolved in the builtin/pg_catalog namespace;
database stored functions are resolved in the currently implemented public
namespace. Explicit unrelated schemas do not fall back to public. Quoted
mixed-case routine names remain distinct. Each stored AST call gets a private
evaluator registry key, avoiding its registry's lowercase-name collision.

`hasScalarFunction` and `scalarFunctionVolatility` are metadata-only. The
recursive `bindScalarFunctions` prepares every branch before evaluation,
including unused CASE/COALESCE arms. Its callback invokes `callUDF` only during
actual expression evaluation, preserving separate SQL NULL flags, the declared
result type and volatility. Syntax wrappers themselves have immutable
volatility; a query planner must also analyze their operand expressions.

WHERE analysis reuses the resolver while retaining the existing unknown-call
argument-type diagnostic. Function-containing predicates and legacy compact
function-left conditions use the complete typed predicate. Routine failures
retain their original SQLSTATE and flow through the existing outer statement
transaction owner, so their writes roll back.
The normalization adapter preserves real CASE syntax in WHERE rather than
lowering it to the legacy evaluator-only `case_when` pseudo-function.

## Retained evidence

Independent worktree: `/tmp/dbms-stored-function-atomicity.D0JmfO/repo`.
Candidate artifacts: `/tmp/dbms-function-where.8RONb5`.

- Real combination baseline: HEAD `5411a7aa`, frozen formal O2 binary
  `/tmp/dbms-plpgsql-combination.D4U50YDA/dbms_main.frozen`, SHA-256
  `c911593edbec82aaf96e5b67879b74bfe5c0ea955d605166b770f17a019c21f5`.
  The checked-in full diagnostic exited 1 with all nine real expectation
  mismatches (retained terminal session 68103). The new WHERE-only regression
  failed with actual 42883 instead of expected P0002 (52754, exit 1).
- Development candidate rebuilt all 55 production objects from this worktree
  with the new expression header (82749, exit 0). `build.sh`, object-specific
  compiler logs, source/header SHA-256 audits and final binary SHA-256 are kept
  in the artifact directory. Shared build configuration selected the TLS stub
  because OpenSSL was not detected in this environment; zlib and ICU were
  enabled. No old production/development objects were reused.
- A metadata volatility-only adjustment subsequently rebuilt the one changed
  expression source and relinked (46497, exit 0); unchanged headers and all
  final source hashes were audited. An added known-CASE short-circuit positive
  control exposed the old pseudo-function normalization (88785, exit 1);
  preserving the predicate AST then required rebuilding main and relinking
  (33680, exit 0). These failures and compiler artifacts are retained.
  This is development/O0 evidence, not a
  claim that the integrated ROOT formal/O2 build has passed this repair.
- WHERE-specific wire regression passed (38677, exit 0; final expanded suite
  63744, exit 0).
  It checks real P0002/22P02 with no surviving body writes; per-row volatile
  writer success; canonical quoted/public names; wrong-schema and arity
  rejection; NULL versus empty/`null`/space-containing text; strict versus
  non-strict calls; known lazy CASE/COALESCE branches; empty-input preparation;
  and actual UPDATE/DELETE predicate calls with command tags and full rollback.
- Five freshly linked native regressions passed (16148, exit 0): scalar
  resolver, stored-function atomicity, PL/pgSQL query host, quoted scalar
  binding, and function/procedure metadata. The scalar resolver test also
  checks the new metadata volatility accessor and actual NULL result. Its
  final expansion passed (19878, exit 0): wide BIGINT, quoted builtin-like
  routine names, repeated binding of the same AST without private-slot
  collisions, and unknown-call preparation before a real writing function.
- Nine final focused wire scripts passed (63744, exit 0): the new WHERE regression,
  existing WHERE function scope, stored-function atomicity, PL/pgSQL SELECT
  INTO, quoted scalar binding, table CASE AST, DISTINCT source query, lexical
  table predicates, and constant boolean predicates.
- The unchanged full clause diagnostic still exited 1 with **seven** remaining
  mismatches (43620 and final 58271). No expected SQLSTATE, row or write
  assertion was weakened.
  The original nine-red baseline and earlier atomicity failures are retained.

An isolated PostgreSQL reference transaction confirmed the positive WHERE,
quoted-name, NULL and no-preexecution controls (reference session 907aa6).
The actual reference version was PostgreSQL **17.2**, `server_version_num`
170002, not PostgreSQL 18.6; temporary relations/functions were rolled back.
The applicable primary PostgreSQL 18 semantics are described in
[value expressions](https://www.postgresql.org/docs/18/sql-expressions.html)
and [SQL functions](https://www.postgresql.org/docs/18/xfunc-sql.html).

## Remaining boundaries

The preserved seven failures are: direct ORDER BY routine error; scalar
subquery WHERE error; scalar-subquery ORDER BY error; actual ORDER BY writer
execution/order; FROM-less EXPLAIN ANALYZE; table-backed EXPLAIN ANALYZE; and
the CAST predicate with arithmetic UPDATE. They require separate root-cause
repairs and commits. Native query-host SELECT binding remains its existing
limited subset; the new explicit evaluator binding API can use an actual
non-global engine, but this commit does not expand every native query shape.
Routine overloads, named/default arguments and arbitrary stored-function
schemas are not newly implemented. This repair does not address the retained
full protocol-suite socket timeout/performance gate or ROOT's subsequently
observed combination-baseline zero-index-counter gate.
