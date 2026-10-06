# Prepared primitive unknown-input conversion

This independent change repairs analysis-phase validation for actual unknown
SQL string constants converted to SMALLINT, INTEGER, BIGINT, BOOLEAN, NUMERIC,
REAL, DOUBLE PRECISION and UUID. It does not claim that the complete coercion,
CAST, type-resolution, WITH, expression or transaction families are finished.

## Cause and repair

Whole-query binding already visits unused CTE definitions and explicit CAST,
`::` and declared-literal nodes. Its engine-supplied input callback, however,
only checked scalar INTERVAL input. Thus an unused `CAST('bad' AS INT)` was
accepted and a primary INSERT, including unrelated writing CTEs, could run.

The engine callback now validates one actual unknown SQL string through the
existing primitive input implementation. It never evaluates rows, parameters,
functions, scalar children, arithmetic, or casts of already typed operands.
Canonical builtin type identities retain quoted case and only recognize an
explicit `pg_catalog` namespace, not a similarly named custom schema/type.
The existing INTERVAL assignment callback is unchanged.

Ordinary input validation omits typmods: e.g. an unused
`CAST('9999' AS NUMERIC(2,0))` must not fail at this phase. PostgreSQL's
[analysis-time unknown-constant coercion](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/parser/parse_coerce.c)
distinguishes type input from typed constant conversion and ordinarily passes
no typmod to the input function. The reference measurements below are from a
real isolated PostgreSQL **18.6**, checked by `SHOW server_version_num=180006`;
the source link is the PostgreSQL 18 stable source, not runtime evidence.

## Retained evidence

All artifacts are under `/tmp/dbms-prepared-unknown-input.qA51Hg3R`.

| Evidence | Result and boundary |
| --- | --- |
| `baseline-wire-v4-isolated.log` | Actual red on immutable `ed5ac648` O0 binary: illegal input accepted, real writer sequence effect, wrong duplicate/failed-block priority. Each primitive case resets its own input rows. |
| `baseline-native-actual.log` | Original pure engine preparation assertion aborts with exit 134; no literal SQL/input expectation changed. |
| `reference-final-lexical.log` | Complete protocol matrix passes against isolated PostgreSQL 18.6, including 16 input negatives, dollar/E strings, no premature folding, no writer effects, duplicate-target priority and user SAVEPOINT recovery. |
| `unused-original-reference18.log` | The original two-case unused-input diagnostic also passes against strict 180006; its CAST/division SQL and expected states/rows remain unchanged. |
| `build-v1.log`, `candidate/*audit` | Effective O0 candidate: fresh TableManage object plus 57 immutable, individually source/header/flag-matched objects from the complete 58-object `ed5ac648` build; matching stubs. No public header/layout/new TU change. Not a ROOT formal O2 proof. |
| `wire-v1.log`, `wire-adjacent-v1.log` | New protocol matrix and ten serial scripts pass, including the original unused-input diagnostic, original nine routine clauses, WITH primary DML, interval WITH, duplicate UPDATE, binder, EXPLAIN and ordinary WHERE/ORDER. |
| `wire-final-lexical.log`, `native-final-lexical.log` | Final expanded dollar/E-string controls pass against the identical frozen candidate; no source/header changes since compilation. |
| `native-final.log` | Ten distinct matching native tests pass, including pure input preparation and the original WITH/bound-command/carrier/scalar/ordinary-WHERE/ORDER/EXPLAIN/native PL host tests. Two phase-sensitive fixtures are corrected as described below. |

The first two setup runs (`baseline-wire-v4.log` and
`baseline-wire-v4-final.log`) failed at the candidate's existing CREATE
FUNCTION grammar boundary; canonical existing CREATE FUNCTION spelling was
used for the definitive baseline. Those failures remain retained and are not
counted as additional engine bugs.

Two old adjacent native fixtures constructed invalid unknown-string CASTs
outside their error-catching scope, assuming conversion happened only while
executing a plan. Their actual exit-134 logs (`native-adjacent-v1.log` and
`native-adjacent-v2.log`) remain retained. The original SQL and exact `22P02`
checks now explicitly assert preparation failure; separate already-typed TEXT
parameter controls retain runtime child/operator error propagation, NULL
binding restoration and rollback checks. No SQLSTATE was relaxed.

## Still open

This validates supported primitive unknown input; it does not replace CAST
nodes with frozen typed constants or supply general catalog/domain/user-cast,
array, temporal, XML, money or all contextual assignment semantics. Unsupported
type identities retain their existing path instead of being guessed as
primitive builtin types. Full ROOT fresh optimized combination and canonical
suite results must be recorded separately.
