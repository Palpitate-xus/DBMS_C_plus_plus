# CTE relation scope, lexical boundaries and stored-view isolation

Status: partial for SQL-01/04, CAT-16 and QRY-01/02/03/05. This closes
specific reproduced bugs, not the complete binder, CTE or view requirements.
No push; GitHub Actions remain disabled; user-deferred items stay deferred.

## Independent source/test commits

| Reproduced problem | Correction | Commit |
| --- | --- | --- |
| A CTE named `id` rewrote column `id` into `__cte_0`; matching output aliases and function names were also rewritten | Bind logical relation names in session/database-local nested CTE frames, preserve column/function/alias tokens, separate JOIN logical names from physical storage keys | `8dd62c09` |
| Compact `FROM(SELECT ...)d` fused FROM with the internal table name and failed with 42P01; CTE `AS(` was not recognized | Preserve spaces at replaced relation-factor boundaries; find CTE AS outside protected SQL units | `b0487280` |
| `WHERE id=1 /* comment */` returned every stored row; dollar-quoted RHS text also lost filtering/value identity | Remove actual comments without merging tokens and translate protected dollar literals into escaped ordinary literals for the legacy parser | `77849804` |
| Newly scoped CTEs leaked into stored view bodies: a base row 99 became the caller CTE's 1 | Independent execution namespace for stored statements; materialize the stored view first, preserve typed cells/NULLs, then execute the already-processed outer query with its lexical bindings | `5163f0b5` |

## Mechanisms and regression coverage

CTE frames are scoped by statement lifetime and unwound by RAII. Same-query
CTE bodies, derived queries, parenthesized queries and set operands explicitly
inherit their lexical parent. A nested WITH can shadow a parent without
changing sibling or subsequent statements. Recursive iteration overlays bind
the work table as a relation, without replacing similarly named columns.
Schema-qualified ordinary tables are not replaced by an unqualified CTE.
JOIN lookup keeps logical aliases and physical storage names distinct, including
aggregate type metadata and predicate keys. Missing nested relations report
42P01 rather than a rendered diagnostic degrading into XX000.

The public independent execution entry point blocks caller CTE frames. Stored
view execution cannot merely wrap the original WITH SQL in a derived query:
that would reintroduce the CTE scope and rerun writing CTEs. The replacement
uses the already-processed SQL, preserves the view's visible alias and applies
the outer projection, WHERE, ordering and LIMIT to the materialized result.
The protocol test verifies a writing CTE executes once, a stored view still
reads 99, a lexical CTE can shadow a view in the caller, chained views and
views within CTE bodies remain independent, and text spaces/empty text/text
NULL/SQL NULL plus typed empty output remain distinct.

The lexical adapter is private to the legacy dispatcher. Original SQL remains
available to AST parsing and diagnostics. Protected ordinary, double-quoted
and E-string units remain unchanged; actual nested/line comments become
separators; dollar-literal body case, whitespace, newline, quote and comment
markers remain values. Dollar-bearing ordinary identifiers are not mistaken
for quoted bodies. UPDATE/DELETE controls verify filtering remains scoped.

Four focused registered protocol entry points:

- `tests/cte_relation_scope_protocol_e2e_test.py`: relation/column/alias/function
  name collisions, schema-qualified controls, nested shadowing, set operands,
  recursive work tables, quoted names, JOIN values/aggregates, LATERAL, errors
  and statement cleanup. Its one-row repeated-CTE cross join is a control,
  not proof of general multirow self-join identity.
- `tests/materialized_factor_boundary_protocol_e2e_test.py`: compact factors,
  WHERE/GROUP/JOIN/LATERAL, CTE AS and column-list boundaries, sibling and
  recursive forms and quoted names containing AS.
- `tests/table_lexical_predicate_protocol_e2e_test.py`: comment and dollar RHS
  filtering, nested CTE/derived materialization, literal projection and DML
  controls, including text NULL versus SQL NULL and empty text.
- `tests/cte_stored_query_namespace_protocol_e2e_test.py`: stored-view namespace,
  typed outer-query composition, single execution of writing CTEs, prepared
  execution and supported constant-function/name-collision controls.

## Actual verification and retained failures

The previous optimized binary failed the initial CTE-name test with 42703,
compact-factor test with 42P01, and comment-filter test by returning all seven
rows. The initial CTE candidate also incorrectly returned 1/1 instead of 1/99
for a CTE cross joined with an independent ordinary table. Its first negative
missing-relation control returned XX000 rather than 42P01. Both candidate
failures were corrected and the complete focused script passed.

Four concurrently launched adjacent scripts timed out during CREATE/INSERT
setup at their 15-second socket deadline. They are failed runs, not passes or
proof of a particular production cause. A subsequent diagnostic setup and
sequential seven-script combination passed. A diagnostic probe measured time
before issuing queries; those numbers are not performance evidence.

The stored-view test reproduced the introduced namespace regression on the
three-commit optimized binary. An initial execution-boundary-only candidate
still returned 1 instead of 99, because raw view expansion reprocessed WITH.
Independent view materialization corrected it. A proposed PL/pgSQL SELECT INTO
fixture then failed with XX000 both with and without a caller CTE; this is a
separate existing capability defect, not evidence of CTE namespace leakage.
The stored-query test uses a supported constant-return function and does not
claim that PL/pgSQL table reads are fixed. Preserve SELECT INTO as follow-up.

The final development binary passed all four focused scripts and complete
CTE/derived/LATERAL composition, alias scope, DML CTE and review-SQL checks.
The final optimized TLS-stub/plain-TCP, zlib and ICU build passed; a repeat
build reported up to date. All 22 focused/adjacent protocol entry points
passed on that binary: the four new scripts; materialized composition, alias
and WHERE-function scope; CTE boundaries and subquery SQLSTATE; nested and
unterminated comments and unterminated literals; DML CTE and SQL literals;
review-SQL and function results; complete JOIN type, derived type and LATERAL
scope; materialized-view refresh; UPDATE/DELETE sources; extended-protocol
error-abort. The default full protocol script is recorded separately below.
Six gap-ledger checker unit tests and documentation-status, version-consistency
and compatibility-contract checks passed. The require-complete checker exits
1 with 22 complete / 166 partial / 70 unverified / 15 deferred_by_user: this
is explicitly evidence that the 273-item goal is not complete.
No full registered-suite, native C++ suite or PG18.6 runtime result is claimed.

A separate complete default-protocol run on the final development binary
failed with a socket TimeoutError at `INSERT INTO delete_rows VALUES (8)`
after creating a temporary table with ON COMMIT DELETE ROWS (line 3458).
This run overlaps optimized adjacent
checks and is retained as a failed run, without attributing it to a proven
load, timeout-fixture or production defect. The optimized default-protocol
run subsequently exited 0, verifying SSLRequest/plaintext negotiation,
startup/auth/simple/extended queries, error recovery and ReadyForQuery. This
is default TLS-stub/plain-TCP evidence, not TLS verification. Thus the final
optimized combination has 23 passing protocol/E2E entry points, not the full
registered suite. The 13 passing development entry points above do
not include this failed complete protocol run.

## Remaining work

General nested/correlated relation identity,
views as arbitrary JOIN sources, complete function/type/operator resolution,
late analysis, all DML CTE target/visibility combinations, parameterized plans,
SEARCH/CYCLE, MATERIALIZED/NOT MATERIALIZED, full recursive evaluation and
complete PostgreSQL view rewrite/dependencies remain unproven or unimplemented.
PL/pgSQL SELECT INTO table reads require independent diagnosis and repair.
An isolated native callback probe confirms the interpreter forwards
`SELECT id` and target `n from stored_cte_base` for
`SELECT id INTO n FROM stored_cte_base`, rather than projection/source
`id FROM stored_cte_base` and target `n`. The defect precedes CTE scope changes.

Two additional probes on the final optimized binary prove remaining defects:
duplicate sibling `WITH c AS(...), c AS(...)` silently selects the second
binding instead of rejecting it (local PostgreSQL 17.2 reports 42712); a
two-row CTE crossed with itself returns `(1,1),(1,1),(2,2),(2,2)` instead of
`(1,1),(1,2),(2,1),(2,2)`. The one-row control cannot prove multirow identity.
Neither bug is closed by the four commits in this record. Duplicate-name
analysis must precede writing CTE execution; self joins require independent
range identities rather than storage-table names as shared result keys.
Duplicate-name preflight is now addressed separately by `b6856ad4`; see
`issue-cte-duplicate-name-preflight.md` for actual native/protocol verification.
The multirow repeated-source defect is subsequently addressed by the separate
source/test commit `d3fcfc0b`; `issue-join-range-identity.md` records its actual
verification scope. General binding and complete JOIN semantics remain open.
The installed PostgreSQL reference is 17.2, not an 18.6 runtime oracle.
