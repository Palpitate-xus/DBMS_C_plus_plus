# PL/pgSQL SELECT INTO: complete queries and typed nullable assignments

Date: 2026-10-06. Source/test commit: `7bc76204`; native-fixture correction:
`acaa0bea`.
Status: this defect is fixed within the verified scalar-target scope; FUNC-06
and SQL-04 remain partial. This is not full PL/pgSQL compatibility.

## Reproduction and contract

The original interpreter split `SELECT id INTO n FROM ranges` into projection
`SELECT id` and an INTO target containing `n from ranges`. The old function host
also rejected WHERE and extracted values from formatted CLI output. The old
optimized protocol binary failed the first ordered-table function call with
XX000. The initial new native interpreter regression failed against the old
implementation.

Separate baseline callback probes showed both incorrect NULL transitions:
assigning 99 to `n` initialized with SQL NULL left `returnIsNull=true`; a no-row
query overwrote a previous 7 with the text `null` and left `returnIsNull=false`.
Whitespace, empty text, quotes and text `null` could not be represented reliably
by splitting rendered rows.

Without STRICT, a scalar target receives the first row, or SQL NULL when there
is no row. STRICT requires exactly one row. The command remains an ordinary SQL
query after removing the procedural INTO targets, and FOUND reports whether a
row was assigned. See the [PostgreSQL 18 basic-statement
contract](https://www.postgresql.org/docs/18/plpgsql-statements.html#PLPGSQL-STATEMENTS-SQL-ONEROW).

The default scalar-list width behavior is deliberately relaxed: surplus output
columns are ignored and missing target values become NULL. Enforcing equal
width would require the separate `strict_multi_assignment` extra-check setting;
STRICT itself checks row count, not that setting. See the [PostgreSQL 18 extra
checks](https://www.postgresql.org/docs/18/plpgsql-development-tips.html#PLPGSQL-EXTRA-CHECKS).
Local PostgreSQL reference probes used server_version_num 170002 and rolled-back
temporary objects; they are diagnostic evidence, not PostgreSQL 18.6 runtime
differential results.

## Change

- Use the shared SQL lexical protection for quotes, E/dollar literals and nested
  comments. Find only top-level SELECT/INTO tokens, remove the scalar target list
  and optional STRICT, and retain the complete query including CTEs, WHERE,
  ORDER BY, LIMIT/OFFSET and alternate INTO placement.
- Introduce `PlPgsqlQueryResult`: column count/types, row count and nullable
  first-row cells, plus an error message and SQLSTATE. Validate result shape
  before indexing. Keep the older host callbacks available for native embedders.
- Install the server query callback before backend threads start. Stored queries
  execute with the active session and database through the normal dispatcher,
  with independent stored-query CTE scope. Consume structured values/NULL bits,
  never formatted CLI table output.
- Supply a checked native fallback for FROM-less and simple single-table SELECT.
  Bind canonical AST column identities to private positional keys, preserving
  quoted case, spaces, dots, schema/table names and aliases. Reject unsupported
  CTE/JOIN/group/window/ROW shapes rather than silently scanning all rows.
- Preserve declaration order and types, initialize declarations without defaults
  to SQL NULL, and evaluate defaults, assignment and return expressions using
  typed bindings. INTO and assignment use the source SQL type for coercion.
  This distinguishes numeric 1.9 converted to integer 2 from text `1.9` rejected
  with 22P02, and preserves overflow 22003.
- Synchronize NULL bits in both assignment directions and keep NULL, empty text,
  text `null`, embedded whitespace/quotes and numeric-looking TEXT distinct.
  Return coercion uses the null-aware expression API and retains failure states.
- Preserve SQL identifier roles during statement-variable substitution, including
  qualified columns, function names, FROM relations, aliases, cast type names and
  CTE labels. Integer FOR iterators get local value/type/NULL scope and restore
  the outer binding on all exits.
- Bound recursive UDF calls by depth and a per-thread stack watermark, restoring
  the budget with RAII. The targeted infinite SQL query recursion now reports
  54001 and permits a subsequent finite call. This does not prove every recursion
  or stack-limit scenario.

## Retained candidate failures

1. The first complete development compile failed on a pointer member-access typo
   in the native column binder; the subsequent complete compile succeeded.
2. A candidate full-protocol regression failed on empty TEXT. Legacy return-cast
   evaluation conflated it with NULL; null-aware return evaluation corrected it.
3. Native quoted column cases failed because already-canonical AST identifiers
   were decoded again. Direct canonical binding and correctly quoted generated
   star aliases corrected case, whitespace, dotted and escaped identifiers.
4. Native ROW shape initially returned unknown-function 42883, not the intended
   unsupported-shape 0A000. The fallback now rejects this shape explicitly.
5. A relinked development protocol batch failed during socket setup before its
   first query. Its handle exited 1 and its test-owned process was cleaned up.
   A captured independent startup and SELECT 1 subsequently passed. The failure
   is retained; no specific production cause has been established.
6. The old `plpgsql_test.cpp` host echoed unsupported expression text rather than
   evaluating it, while the runaway case incremented an undeclared variable.
   Repeated substitution/quoting could expand expression text instead of counting
   integers. The development run ended with signal 9 / status 137; its kill cause
   could not be established from inaccessible kernel logs. The optimized process
   was observed at 56,086,640 KiB RSS (about 53.5 GiB) and was deliberately stopped
   with TERM / status 143. Neither run passed. `acaa0bea` supplies an actual scalar
   evaluator, removes the always-true concatenation assertion, declares the loop
   counter and requires the precise step-budget error. No budget was reduced.

## Verification

The interpreter-only typed/legacy-host regression passed in optimized standalone
and ASan/UBSan configurations, including NULL state, protected lexical units,
STRICT/FOUND, malformed host shape, typed assignment and loop binding controls.

Matching development objects passed `plpgsql_select_into_test.cpp` and
`plpgsql_query_host_test.cpp`; the latter invokes the actual storage engine,
expression evaluator, UDF path and native fallback, including quoted references,
filtering/sorting, CASE/IN/coalesce, NULL/empty values and unsupported shapes.
The corrected existing `plpgsql_test.cpp` passed with both matching development
and optimized objects, including arithmetic/control flow, exact concatenation,
the 200,000-step runaway guard and stored-function metadata round trips. The
development run finished in 1.39 seconds with 15,420 KiB peak RSS. This verifies
the corrected fixture; it does not convert the two stopped old-fixture runs into
passes. The final development native scope is these three test entry points.

The real `plpgsql_select_into_protocol_e2e_test.py` passed against the development
server. It checks ordered first-row selection, WHERE/parameters, multiple targets,
NULL transitions, empty and quoted TEXT, CTEs, same-source JOIN, stored CTE scope,
identifier roles, relaxed target widths, typed coercion/return SQLSTATE, STRICT,
failed explicit-transaction state and recovery, finite/infinite recursion.

The final frozen development batch passed seven protocol entry points: the new
SELECT INTO script, function result, stored-query CTE namespace, CTE relation
scope, DML CTE, self-JOIN range identity and cold-start transaction-image recovery.
The preceding failed startup batch is not counted among those passes.

The combined `615f2c54` / `7bc76204` formal optimized build recompiled all 55
production objects. Normal link, repeat up-to-date build, all 55 object signatures
and the final binary build stamp passed. The public StorageEngine layout changed;
old ABI objects were not reused for this verification. The same optimized binary
passed those seven scripts plus DDL-upgrade timeout, commit-failure recovery and
database-rename connection: ten distinct focused/adjacent protocol entry points.
Freshly relinked optimized native interpreter/query-host tests and the corrected
existing PL/pgSQL suite passed. Three combined optimized recovery native tests
also passed (aborted snapshot, recovery integrity and stale-temp startup), for
six distinct matching optimized native entry points in the combined scope.

The fresh complete default-protocol run exited 1 with a socket TimeoutError at
`postgres_protocol_test.py:3170`, `INSERT INTO kw_joined VALUES (9)`. It is a
failed full gate, not part of the ten passing protocol scripts. The earlier
prepared-boundary, temporary-table and instrumented CREATE TABLE timeouts remain
retained; this run does not establish their common cause. No complete registered
suite, full default-protocol pass, TLS runtime validation or PostgreSQL 18.6
differential is claimed. See the JOIN and cold-start reports for earlier failures
and the separate statement-image I/O amplification follow-up.

## Remaining work, not hidden by this fix

- A read-only protocol follow-up reproduced a quoted scalar-expression collision:
  `"X"=1` and `x=2` produced 4 for `"X"+x` instead of 3. With `"X"` NULL,
  `coalesce("X",9)+x` also produced 4 instead of 11. Direct quoted-variable
  returns and the native SELECT column binder do not prove compound variable
  binding. This needs an independent canonical positional-variable fix.
- Another real protocol follow-up reproduced failed autocommit function writes
  surviving P0002/22P02: a writing CTE executed inside SELECT INTO committed its
  INSERT before the outer `SELECT function()` reported an assignment/STRICT
  error. Outer stored-function statement atomicity needs an independent fix.
- Native fallback generic exception handling still needs independent verification
  of expression-error SQLSTATE propagation, beyond explicit DbError paths.
- General bare-variable versus source-column ambiguity, CREATE-time procedural
  compilation, arbitrary multiword SQL identifier roles, complete records/rowtype,
  exceptions/diagnostics, dynamic SQL, cursors, trigger variables, subtransactions,
  plan cache and dependency invalidation remain incomplete or unverified.
- Native standalone execution intentionally rejects broader SQL forms; server
  callback support inherits existing dispatcher/binder/CTE limitations and is not
  proof of all SQL grammar or transaction semantics.

The 273-item ledger is not complete. User-deferred security/TDE work remains
deferred, GitHub Actions remain disabled, and no git push was performed.
