# Open PL/pgSQL SQL-statement binding and preparation gaps

Date: 2026-10-06. Status: the seven recorded namespace/pre-effect defects now
have a scoped prepared-query implementation and matching focused proof below.
The broader preparation/type/record family and SQL-04/FUNC-06 remain partial.
The original recorded local candidate includes the native
SQLSTATE and quoted scalar fixes; its SQL-statement path is separate from the
scalar-expression binder. This report is not a completion claim.

Subsequent independent commits `711f9030` (COLLATE labels), `0888873f`
(typed AT TIME ZONE operands), `d2b7b566` (DISTINCT comparison operands) and
`b2c4d983` (ordinary source-query normalization/typed comparison) address the
last three lexical-role observations below. Their isolated tests passed; ROOT
combined formal verification is recorded in
[`issue-plpgsql-lexical-and-source-comparisons.md`](issue-plpgsql-lexical-and-source-comparisons.md):
14 matching optimized natives and 26 focused/adjacent protocol entry points
passed, while the fresh full-default protocol failed its index-scan statistics
assertion. The seven namespace/pre-effect observations were still open at that
earlier stage; the independent implementation below supersedes that status. Atomicity
commit `5411a7aa` owns supported function writes with the caller, but rollback
does not establish rejection before nontransactional sequence effects.

## Actual observations

One isolated candidate server was started for the probes and cleaned up after
completion. The diagnostic source is retained at
`/tmp/dbms-plpgsql-binding-diag.kv8gDi/probe.py`. Reference objects were temporary
tables/functions/sequences, with per-case savepoints in a transaction ultimately
rolled back. The observed reference version was 170002 (PostgreSQL 17.2), with
`plpgsql.variable_conflict=error`, not PostgreSQL 18.6.

| Reached SQL statement inside a stored function | Reference | Candidate |
|---|---|---|
| Local id=99 and source column id in bare projection | `42702` | Returns 99 |
| Same collision in WHERE id=1 | `42702` | Successful SQL NULL |
| Ambiguous projection with WHERE FALSE | `42702` | Successful SQL NULL |
| Parameter id and same-named source column | `42702` | Returns parameter 99 |
| Function-name-qualified parameter | Returns 99 | `42P01` |
| Quoted local and source column both named ID | `42702` | Returns local 99 |
| Writing CTE followed by ambiguous projection | `42702` before CTE runs | Returns 99 and inserts 10 |
| Local `"C"` and `COLLATE "C"` identifier | Returns a | `42704`; collation became cast |
| AT TIME ZONE with local tz=UTC | Correct timestamp | `22023` |
| IS DISTINCT FROM with local wanted=2 | True | `42703` |

These are observed failures, not merely inferred from code. Controls passed for
qualified table columns, nonambiguous locals, quoted case distinctions, qualified
quoted columns, DOUBLE PRECISION and a simple implicit alias. The corresponding
ordinary SQL literal COLLATE/time-zone/DISTINCT expressions also passed, isolating
the three procedural substitution/parser defects. No earlier-commit comparison
was made, so the report does not label every defect a newly introduced regression.

The reference writing CTE uses nextval on its private sequence. After the
ambiguous statement reports 42702 and its savepoint is rolled back, the sequence
still has is_called=false and the target is empty. Sequence increments are not
undone by transaction rollback; this establishes analysis before execution, not
just execution followed by removal of inserted rows. A fix must preserve that
observable ordering even after the separate function-transaction fix lands.

## Cause

`src/utils/plpgsql.cpp` substitutes variable values before handing the completed
SQL statement to `host.query`. A lexical role heuristic can recognize relation,
alias and type labels, but cannot determine whether a value ColumnRef also names
a visible source column. Once it has replaced id with a literal, the downstream
SQL binder cannot recover or reject the lost ambiguity. Looking at result rows
would also miss the error for an empty input or unreachable result row.

The qualifier path preserves table references but lacks the implicit function
label for parameters. Scalar positional binding fixes case collisions in scalar
expressions only; it does not provide that statement namespace.

Three more specific roles were wrong in the recorded baseline: FROM in IS DISTINCT FROM is treated as a
relation introducer; the time-zone operand is mistaken for an implicit alias;
COLLATE's identifier is substituted as a data value. Additionally, the SQL parser
stores AT TIME ZONE's zone in a unary operator string instead of retaining it as
an independently bound value expression. Correcting only one lexical skip rule
cannot provide arbitrary zone-expression semantics.

## Required preparation contract

Data positions accept variable parameters, but relation/column/function labels
are not data parameters. A default variable/source-column collision is an error;
explicit table, function and block qualification resolves the applicable scope.
Reached statements need analysis before execution, while unreachable procedural
branches must not be eagerly executed or prepared as commands. These contracts
are described in the [PostgreSQL 18 variable substitution and preparation
documentation](https://www.postgresql.org/docs/18/plpgsql-implementation.html).

The intended implementation preserves raw SQL after removing procedural INTO,
supplies canonical procedural scopes/types/NULLs to preparation, and binds value
AST nodes against independently constructed source and procedural namespaces.
Both namespaces matching a value reference yields 42702; only a procedural match
becomes a typed parameter. Function/block labels and quoted components must be
represented explicitly rather than reconstructed from a dotted string.

Preparation must derive CTE/derived/subquery/view output metadata without running
their SQL, including all branches and writing-CTE RETURNING shapes. It must finish
name/type/function analysis before writes or volatile expression evaluation.
Catalog/temp/search-path visibility and correlated query levels need explicit
contracts. If prepared plans are cached, values remain per execution and DDL,
database, search_path, temporary namespace and type changes must invalidate the
appropriate plan. A result-width check or broad keyword whitelist is not this
binder.

## Original implementation sequence

Preparation must be a distinct side-effect-free operation returning a bound
query/descriptor, followed by execution with current typed datum values. The
following dependency order defined the required work at the original diagnostic
stage. The implementation below covers a useful subset, not every item here.

1. Add copied, lock-consistent metadata snapshots for relations, attributes,
   types, procedures and namespace visibility. Existing CatalogService::get
   invokes migrateLegacyPublicDottedSequences on first load, so calling it during
   preparation is not automatically a read-only operation. Runtime table-name
   resolution and CTE materialization are also not metadata-only binders.
2. Preserve complete raw SQL/token positions and canonical quoted identifier
   components. Construct procedural parameter/local/implicit FOUND and function/
   block-label scopes separately from query range scopes. A quoted `"a.b"` is
   one identifier, not the same name path as `a.b`.
3. Derive all source shapes without executing SELECTs, views, table functions or
   writing CTEs. Validate generic CTE command bodies and their complete parses,
   output names/types and lexical dependencies. Keep SQL query-level visibility,
   correlation, JOIN USING/NATURAL merged columns and output-alias precedence.
4. Walk data AST positions in every reached statement clause, including ON,
   WHERE, GROUP/HAVING, ORDER, FILTER/window/page expressions and RETURNING.
   Resolve column/procedural collisions before evaluating any expression;
   function/block-qualified parameters use their own scope. Return stable typed
   parameter slots rather than render AST.toString placeholders back into SQL.
5. Validate all names/types/function signatures and output shapes before writing
   or volatile execution, including empty sources and short-circuited branches.
   Execution must preserve the independent caller/body snapshot, transaction
   owner and explicit output-demand contracts, not perform name analysis from
   already produced rows. Existing describePreparedResult is a limited wire
   descriptor path, not this complete preparation contract.
6. Initially prepare every reached statement without a plan cache. A later
   cache needs dependency/version invalidation for DDL and rollback, database/
   search_path/temp scope, record/polymorphic/trigger types and physical restore,
   plus revalidation or a schema lock against prepare/execute races.

Each stage needs zero-query/zero-volatile callback assertions and the real
sequence is_called negative control, in addition to matching native and wire
tests. Broader type/binder/record/CTE work must remain visible if a narrow first
implementation cannot support a shape. SQL literal substitution plus a broad
keyword skip list cannot satisfy these contracts.

## Work and verification still required

- Integrate and verify the four independent lexical/source-query commits above.
  Their regressions retain ordinary SQL, quoted labels, invalid collation/zone,
  NULL/type and genuine zone-expression controls, not just literal spellings.
- Implement the preparation-stage namespace/type binder and function/block label
  parameter binding. Cover the seven ambiguity/qualification observations above,
  CTE/derived/correlation scopes, empty rows and the nontransactional sequence
  negative control.
- Recheck the final combination with the independent SPI transaction/CID changes.
  Rollback of failed function writes is necessary but not a substitute for
  rejecting an ambiguous query before any CTE or volatile execution.
- Preserve neighboring quoted-scalar, SELECT INTO, ordinary query and type tests;
  fresh formal builds must match any changed public API/header layout.

An independent output-demand bug was also actually reproduced: INTO executes
unneeded VOLATILE projections and changes error precedence. It is distinct from
metadata preparation and function rollback; see
[`issue-plpgsql-select-into-execution-demand.md`](issue-plpgsql-select-into-execution-demand.md).

No new full registered suite or PostgreSQL 18.6 differential was run for this
diagnostic. Earlier complete-protocol timeout failures and I/O amplification
remain open. No git push, Actions enablement or user-deferred security/TDE work.

## Reached-statement prepared-query implementation

The independent source parent is `c78ea073229ed91a008a2a33c2c30e7f005d0bb1`.
`src/parser/query_binding.h/.cpp` introduces `prepareQuery`, a shared operation
that accepts raw SQL, canonical procedural datums and copied metadata callbacks.
It returns an owned AST, output descriptor, stable typed/null parameter cells
and original-byte provenance. It has no query-execution callback. Names and
declared types are resolved without consulting datum values or produced rows.

The implementation separates parameter/function-label and nested block-label
frames from SQL range namespaces. Quoted identifier components remain distinct:
`"scope.dot".id` is not `scope.dot.id`. A value matching both namespaces reports
`42702`; absent qualified ranges report `42P01`. Source aliases hide original
names. Explicit positional aliases use the original function parameter order,
not the first occupied internal slot: `SELECT local_value,$1` cannot bind `$1`
to `local_value` accidentally.

Source descriptors are obtained without running ordinary/temp tables, views,
CTEs, derived queries or scalar/EXISTS children. Generic CTE bodies include DML
RETURNING metadata. The walker prepares their expressions before the outer query
can run, including empty sources and WHERE FALSE. Correlated SQL ancestors pass
through nested queries and their WITH terms; non-LATERAL derived siblings are
not made visible. JOIN USING/NATURAL expose merged unqualified keys while
retaining qualified physical columns. The supported SELECT/DML trees also walk
JOIN ON, WHERE, GROUP/HAVING, ORDER, DISTINCT, function FILTER/windows and
RETURNING. Reached statements alone enter this path: an unreached procedural
branch is not prepared as an executable command.

Variable values are represented by genuine `ParameterExpr(slot, declaredType)`
nodes and `ExprValue` cells with a separate NULL flag. `RowContext::setParameters`
and the evaluator consume those bound nodes directly, including typed NULLs.
Raw SQL `$n` nodes are distinguishable from internally bound nodes and resolve
stable datum identity before receiving an internal slot. Projection labels are
preserved when an unaliased variable becomes a parameter, so nested
`WITH q AS(SELECT wanted) SELECT q.wanted FROM q` retains its descriptor.

The current production SQL dispatcher still receives a transitional
`PreparedQuery::legacySql()` adapter, not the bound AST itself. This adapter runs
only after whole-tree namespace preparation succeeds; it uses immutable original
byte spans and declared-type CAST encoding, never AST rendering or identifier
text matching. It does not expose unresolved PL names to the legacy dispatcher.
Original relation/alias/type/collation labels, literals and comments are retained.
Direct query execution of an attached prepared scalar child without a real query
evaluation context reports `0A000`, rather than treating SQL text as a datum.
The ordinary scalar-subquery runtime bridge is a separate, still-open task.

`CatalogManager::metadataSnapshot` copies namespace/relation/attribute/type/
procedure rows under one mutex. `CatalogService::metadataSnapshot` does not call
the catalog bootstrap/migration/persistence path. A cold read-only manager does
not create directories, allocate OIDs or persist in its destructor. Engine schema
and view descriptor fallback only reads files. The engine releases its metadata
cache mutex before executing the query; nested SQL/DDL can then use the existing
transaction owner, command IDs, snapshots and SELECT INTO output-demand options.
There is no prepared-plan cache: each reached statement takes fresh metadata.

## Actual red/green evidence for this implementation

Artifacts are retained under `/tmp/dbms-plpgsql-query-binder.e0u6jPqT`; these
paths describe local evidence, not files needed by installed executables.

- The actual red baseline is ROOT `86ddccf9`, not an allegedly matched `c78`
  executable. Its official fresh-55 optimized binary was
  `/tmp/dbms-owner-receiver-numeric-combination.SSCXIETb/dbms_main.frozen`, SHA256
  `a38da2abc3fb62270eedd988d67e66135a5a5f3a446f535c7e7ef92cb4a02b9c`.
  It differs from the source parent by subsequent numeric/SUM/index CPP/test
  fixes, not the new binder headers. `baseline-86dd-red.log` records terminal
  exit 1 and the seven original wrong results/errors. Its writing CTE inserted
  one row, currval returned 1 and the following nextval returned 2: the defect
  occurred before an irreversible sequence effect, not just before rollback.
- The first optimized candidate passed the initial 23 cases and sequence
  negative control (`candidate-v1.log`, terminal exit 0). Its expanded test
  subsequently exposed eight more failures (`candidate-v1-expanded-red.log`,
  terminal exit 1), including positional parameters, output labels, scalar CTE
  visibility and USING keys. Those failures were retained and fixed; the
  occupied-slot concern was a source-level risk, not an observed return of 99
  in that expanded runtime test, which actually reported `42P02`.
- The second candidate's all-56 fresh optimized build and signature-compatible
  native objects passed 38 cases plus the sequence control and nine natives
  (`candidate-v2.log`, `native-v2.log`, both terminal exit 0).
- The third optimized candidate passed all 40 cases plus sequence pre-effect,
  12 native entry points and 11 serial adjacent protocol entry points
  (`candidate-v3.log`, `native-v3.log`, `adjacent-v3.log`, terminal exit 0).
  Adjacent coverage includes the 42 SELECT INTO demand controls, ordinary INTO,
  quoted scalar, COLLATE, AT TIME ZONE, DISTINCT, stored-function atomicity,
  stored-function WHERE, writing CTE and two extended-protocol error/alias tests.
- Final pure-preparation review additionally produced a real legal-ancestor
  CTE failure in the third candidate (`cte-ancestor-v3-red.log`, terminal exit
  1). PostgreSQL 17.2 returned 1 for the ancestor case and `42P01` for the
  non-LATERAL sibling (`pg17-cte-ancestor-reference.log`, final rollback
  `00000`). WITH definitions now preserve ancestors, with both native controls.
- Peer review then exposed a schema-qualified EXTRACT role mistake. The frozen
  fourth candidate's expanded 42-case run actually returned NULL for the valid
  ordinary function call and for its unknown-field variant, inserting row 2
  and advancing currval to 2 / following nextval to 3
  (`candidate-v4-expanded42-red.log`, terminal exit 1). Only unqualified grammar
  EXTRACT skips its field identifier now; ordinary schema-qualified arguments
  are value nodes. The fifth optimized candidate passed all 43 cases, including
  INSERT SELECT returning 44, qualified EXTRACT returning 2026, and unknown field
  `42703` before either the target or sequence changed (`candidate-v5.log`,
  terminal exit 0). Its 12 native entry points also passed (`native-v5.log`).
- JOIN output-order review produced an actual fifth-candidate metadata failure:
  USING and NATURAL descriptors were sorted `ID,id`, and repeated USING keys
  were silently accepted (`using-order-v5-red.log`, terminal exit 1).
  PostgreSQL 17.2 gdesc returned `id integer, ID text`, and duplicate USING
  reported `42701` (`pg17-using-order-reference.log`). The sixth candidate's
  corresponding pure metadata probe passed both ordered descriptors and the
  duplicate rejection (`using-order-v6-green.log`, terminal exit 0), alongside
  the final native regression controls.
- Final sixth-candidate verification: official optimized production compilation
  succeeded; repeating `scripts/build.sh` reported up to date. All 56 object
  source/header/compiler signatures and the production stamp matched. The
  all-56 fresh header-compatible build was performed in the second round;
  subsequent CPP-only rounds rebuilt every changed TU and relinked through the
  official build, without using ROOT or another agent's objects. The frozen final
  executable is `dbms_main.binder-v6.o2`, SHA256
  `fd4b423f7b188abd830bd8c668250a9fe5e66590a76c9f9eaeb6d97594882000`.
  `candidate-v6.log` passed all 43 cases and both nontransactional sequence
  controls, `native-v6.log` passed 12 native entry points, and `adjacent-v6.log`
  passed all 11 adjacent protocol entry points; each process reached terminal
  exit 0. No full protocol suite was run in this independent worktree.
  The final committed test source was rerun in `candidate-v6-final.log`
  (registered status 0). Its optional 44-case open-namespace diagnostic was
  separately rerun in `candidate-v6-open44.log`: all 43 closed cases and both
  sequence controls still passed, and only schema-qualified routine CREATE
  failed, giving the deliberately retained diagnostic status 1.
- Reference `pg17-reference-expanded40.log` records server version 170002,
  `variable_conflict=error`, all 40 expected outcomes and final rollback
  `00000`. Private temporary objects and per-case savepoints were used. Its
  writing-CTE rejection left the target empty and sequence `is_called=false`.
  The candidate independently checks empty target, currval `55000`, then
  nextval 1; rollback cannot manufacture that result.
  `pg17-reference-expanded43.log` additionally verifies the INSERT SELECT and
  qualified EXTRACT controls, with a second empty-target/is_called=false
  negative control and final rollback `00000`.

## Explicit remaining boundaries

This closes the seven recorded namespace/pre-effect defects, not the complete
SQL-04/FUNC-06 family or every item in the original implementation sequence.
The prepared AST is not yet the dispatcher contract for every query path.
Utility commands outside SELECT/VALUES/INSERT/UPDATE/DELETE/EXPLAIN/CREATE TABLE
still use the existing engine path, and dynamic EXECUTE needs its own parameter
contract. General table-function/LATERAL/MERGE, record/polymorphic/trigger
descriptor semantics and additional catalog virtual-table descriptors are not
claimed complete. The current explicitly typed virtual descriptors cover
pg_class, pg_settings and pg_stat_activity only.

Static type analysis is still partial: general operator/function coercion,
CASE/set/recursive common types, full aggregate signature resolution and
constant/type/collation validation before effects need further work. Unsupported
opaque expressions, including some aggregate-order/slice shapes, fail explicitly
with `0A000` rather than silently bypassing preparation. Strict SELECT child
parsing is improved, but the whole grammar is not certified strict. Catalog
type dependency invalidation and an eventual plan cache remain future work.
In particular, full INSERT target-column/duplicate/width validation and UPDATE
target-name preparation are not supplied by the current expression walker;
LIMIT/OFFSET AST fields currently represent constant integers, not general bound
parameter expressions. Those broader metadata/pre-effect boundaries remain
explicitly open, rather than being inferred from the seven namespace greens.

A separate schema-qualified routine creation smoke test was actually red before
it could reach this binder: `CREATE FUNCTION binding_private_schema...` returned
`42601` (`candidate-v6-schema44.log`, terminal exit 1), while the PostgreSQL 17.2
implicit-function-label reference returned 99
(`pg17-reference-expanded44.log`). Routine CREATE/parser and non-public routine
resolution need independent work. The original 43 closed regression cases are
preserved; `OPEN_NAMESPACE_CASES` in the same test retains this unclosed diagnostic
and can be run with `--include-open-namespace`. It is not a final-candidate
44-case pass. Quoted public routine names containing a dot must
not be mistaken for schema paths when that later support is implemented.

The normal stored-body caller supplies the existing database transaction/DDL
fence; these focused checks do not establish a new general public metadata
prepare/execute race guarantee outside that ownership contract. No full suite,
PostgreSQL 18.6 differential, concurrency-DDL proof, push, Actions enablement or
deferred security/TDE work is claimed by this commit.
