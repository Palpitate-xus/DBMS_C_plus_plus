# Open PL/pgSQL SQL-statement binding and preparation gaps

Date: 2026-10-06. Status: the general preparation/namespace bugs remain open.
SQL-04/FUNC-06 remain partial. The recorded local candidate includes the native
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
assertion. The seven general namespace/pre-effect observations remain open. Atomicity
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

## Implementation sequence still required

Preparation must be a distinct side-effect-free operation returning a bound
query/descriptor, followed by execution with current typed datum values. The
following dependency order is still unimplemented; it is not a passed checklist.

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
