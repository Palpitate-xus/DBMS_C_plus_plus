# Duplicate CTE names must fail before execution

Status: specific bug fixed by source/test commit `b6856ad4`; SQL-01/04,
QRY-05 and PROTO-02/03 remain partial. No push; Actions disabled;
user-deferred security/TDE work remains deferred.

## Reproduction

On the preceding optimized binary, `WITH c AS(SELECT 1 AS id),
c AS(SELECT 2 AS id) SELECT id FROM c` succeeded with row 2 and SELECT 1
instead of duplicate-alias SQLSTATE 42712. The new protocol regression failed
on this first case. The independently linked native parser regression also
failed: parsing the duplicate list reported success and no error.

An isolated writing probe showed both same-name INSERT CTEs executed: the
query returned the second CTE's row 2, while a subsequent ordinary SELECT
read rows 1 and 2. Merely rejecting a name at the second binding would be
too late. A duplicate inside a later nested query must also be found before
an earlier writing CTE or outer DML can execute.

A read-only diagnostic against the installed PostgreSQL 17.2 reference
reported 42712 for duplicate c; unquoted c and quoted uppercase C remained
distinct and succeeded. This is not a PostgreSQL 18.6 differential result.

## Change

`SQLParser::duplicateCteName` uses the shared lexer, preserves quoted-name
case, decodes doubled identifier quotes and folds ordinary names. Actual
WITH declaration lists get independent name sets; nested WITH lists can
shadow parent names. Quoted/string/dollar/comment contents are not keywords.
Matching parentheses are indexed once, so each enclosing list can skip a
body without walking it again. The outer scan also inspects nested lists,
including declarations in subqueries or later CTE bodies.

The common check is used by the native parser, before the main execution
dispatcher can perform writes, and during Extended Query Parse before
publishing a prepared statement. Parse errors use the existing structured
abort/recovery boundary; an already-failed block retains 25P02 rather than
being restarted by analysis. In Simple Query the existing failed-block gate
also retains normal transaction-error precedence.

This is targeted name analysis, not a replacement for PostgreSQL grammar
or a complete binder. MATERIALIZED/NOT MATERIALIZED duplicate-name controls
do not prove execution/planning of those modifiers is implemented.

## Verification

The matching development combination passed:

- New `cte_duplicate_name_parser_test`: eight rejected parser cases,
  independent nested scope, quoted-case separation and protected-literal
  controls; a 300-level helper-only nesting control is not a benchmark.
- Complete existing `parser_phase1_test` with the changed parser/network
  development objects and reusable production objects.
- New `cte_duplicate_name_protocol_e2e_test`: Simple and Parse-only errors,
  nested/quoted/recursive/materialization/comment forms, no writing-CTE or
  outer-DML side effects, exact 42712, ReadyForQuery, explicit transaction
  rollback, savepoint recovery, implicit multi-statement rollback, protected
  text and legitimate nested shadowing. The batch may have a successful
  prefix command tag; stored changes are nevertheless rolled back.
- CTE relation scope, stored-view isolation, materialized factor boundaries,
  lexical predicates, DML CTE, CTE clause boundaries and extended-error-abort
  E2E: seven adjacent entry points, eight development protocols total.

All production objects were rebuilt under the shared official optimized
configuration after the static parser API addition. Independent compiler
workers reused the official source/object signature functions and atomically
published generated objects; the normal `scripts/build.sh` then validated
the cache and linked successfully. A repeat normal build reported up to date.
This is TLS-stub/plain-TCP with zlib and ICU, not TLS verification.

On the optimized binary, all eight focused/adjacent protocol scripts passed.
Both native tests were freshly linked against the matching optimized
production objects and fresh test stubs, and passed. The complete default
protocol script subsequently exited 0, verifying SSLRequest/plaintext
negotiation, startup/auth/simple/extended-query and error recovery/ReadyForQuery.
The final scope is nine distinct protocol/E2E entry points and two native
tests, not TLS validation or the full registered suite. The expanded focused
script also passed duplicate-query 25P02 controls in an already-failed block
for both Simple Query and Parse-only. All 55 production object signatures
were checked against the current source/configuration. Six ledger unit tests
and the three documentation/version/compatibility-contract checks passed.
No full registered-suite or
PostgreSQL 18.6 runtime differential result is claimed.

## Remaining requirements

The full audit remains incomplete: 22 complete / 166 partial / 70 unverified /
15 deferred_by_user. A follow-up probe showed multirow same-source JOIN identity was broken;
an ordinary two-row table crossed with itself repeats diagonal pairs, and
`WHERE a.id=1 AND b.id=2` returns no row rather than `(1,2)`. The main
normalizer lowers both aliases to the same physical key; the storage JOIN
column map also overwrites equal physical qualifiers. Fixing projection
offsets alone would leave filtering wrong. This separate defect is now addressed
by source/test commit `d3fcfc0b`; see `issue-join-range-identity.md` for the
verification scope, without treating the entire JOIN/CTE families as complete.
An independent writing-CTE/later
missing-column probe reported 42703 and rolled back the new row, confirming
that bounded atomicity control, not all CTE snapshot/visibility requirements.
PL/pgSQL SELECT INTO partitioning and typed NULL assignment still need repair.
General CTE analysis, SEARCH/CYCLE, materialization planning, recursive
semantics and writing-CTE snapshot/visibility combinations remain open.
Full grammar, general namespaces/binding and complete protocol analysis are
not closed by this preflight check.
