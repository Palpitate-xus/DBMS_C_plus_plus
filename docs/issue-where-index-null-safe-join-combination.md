# WHERE routines, current-read indexes and typed JOIN ON: combined verification

Date: 2026-10-06. This records the actual ROOT `6460a246` combination, not
completion of the 273-item PostgreSQL audit.

## Independent changes

- `aa149ca8` (private `7353cd9d`) resolves canonical scalar calls for typed
  WHERE evaluation using metadata-only preparation and actual SQL NULL/type
  results. CASE predicates remain AST expressions. Its API requires matching
  rebuilt production objects. See `issue-stored-function-where-execution.md`.
- `d184d8a1` (private `1e96cbe7`) permits otherwise isolated, unmodified
  current-read owners to use real indexes. Old snapshots, overlapping owners
  and own writes retain heap access; allocation high-water and owner checks
  protect the new exception. This fixes the real full-protocol statistics
  failure without changing counters or expectations. It is not MVCC indexes.
- `6460a246` (private `c20cc0c2`) evaluates complete null-safe JOIN ON ASTs
  with resolved positional range bindings and typed nullable row pairs before
  outer extension. FULL's keyed residual matcher visits each pair once and
  records matched right input occurrences before WHERE filtering; it no longer
  reruns LEFT/RIGHT/INNER and triples a volatile function's sequence effect.

JOIN's real baseline protocol returned `0A000` for the first null-safe INNER
control, and its native assertion exited 134. A pre-final FULL candidate
returned the right pair but called a volatile routine three times instead of
once. The permanent FULL regression retained the expected count of one.
An isolated PG17.2/170002 temporary-object reference also counted one and was
rolled back; it is not a PostgreSQL 18.6 differential. FULL with only a null-safe
key and no merge/hash equality remains a tracked `0A000` negative control.

The private JOIN evidence under `/tmp/dbms-null-safe-join.0MiJvD` includes the
full handoff, baseline failures, intermediate errors, final nine wire scripts
and three native entry points. Its final backend was O2 but other core/main
objects were development O0, and its sanitizer instrumented only the backend,
test and stubs. Neither is misrepresented as a full optimized ROOT/sanitizer
build. Legal CREATE FUNCTION attribute ordering remains an independent gap.

## Actual optimized ROOT combination

The new resolver header was freshly compiled into all 55 formal O2 production
translation units in terminal `69618` (exit 0). JOIN then freshly rebuilt main
and TableManage in terminal `80975` (exit 0), with the other 53 signatures
matching. The normal entry point reported up-to-date in `847d2f`, and the
object/binary audit in `88d4f7` verified 55/55 signatures and the binary stamp.

Frozen binary:
`/tmp/dbms-join-where-index-combination.53yTUCXm/dbms_main.frozen`, SHA256
`d6efa285dc82a5b06de534cdd6a1978e59532abb5fa6351a05403c45d42ce7b0`.
The retained `verify.sh` selects exact sources/flags, checks signatures, freshly
compiles stubs and native entry points, and runs natives from separate fresh
directories. Subsequent source changes do not turn this frozen evidence into
proof for a different combination.

Native terminal `42031` exited **1**: 24 entry points executed, 23 passed.
The passing entry points are stored_function_atomicity,
plpgsql_native_query_sqlstate, plpgsql_quoted_scalar_binding,
plpgsql_query_host, plpgsql_select_into, corrected plpgsql,
plpgsql_collation_label, plpgsql_timezone_operand, plpgsql_distinct_operand,
function_procedure, interval_arith, parser_phase1,
query_snapshot_characteristics, stored_function_scalar_resolver,
read_owner_index_snapshot, null_safe_join, index_scan_full_value_recheck,
integer_index_full_value_recheck, legacy_index_prefix_recheck,
update_index_failure, mvcc_update, runtime_stats and planner_runtime_stats.

`constraint_expr` failed its existing line-70 `SUM('abc')` SQLSTATE `42725`
assertion. This passed in the earlier formal `5411a7aa` combination. The shared
scalar binder now rejects the evaluator's aggregate-role special case too
early. The independent matching WHERE baseline and owner candidate reproduce
the failure too; it is not dismissed as a fixture or silently removed.
Exact log: `native.log` in the frozen artifact directory.

Protocol terminal `62662` exited 0 for **30 distinct** focused/adjacent entry
points. The exact 30-script list is in `verify.sh`; it contains the earlier 25
lexical/function/CTE/transaction/literal scripts plus boolean_literal_boundary,
stored_function_where_execution, where_function_scope,
read_owner_index_snapshot and null_safe_join. All used the frozen combination;
the cold-start script's four scenarios count as one script. Exact log:
`protocol.log`. This is not the full registered suite.

Unmodified clause diagnostic terminal `34904` exited 1 with seven real failures:
ordinary UDF ORDER execution, scalar-subquery WHERE/ORDER error propagation,
writer ORDER effects, two EXPLAIN ANALYZE paths, and typed arithmetic UPDATE.
Direct stored-function WHERE controls now pass, reducing the earlier nine
failures to seven. The unchanged diagnostic remains a failing gate, not one
of the 30 passing scripts. Exact log: `known-gap.log`.

## Newly reproduced indexed residual defect, not yet closed here

Fresh formal matching native terminal `16308` exited 134. A caller-owned indexed
lookup with `id=1` and a typed volatile residual returned one input row, but the
residual inserted two sink rows, not one. The first indexed residual changed
`hasWrite`; the post-expression owner check discarded its candidates and the
heap fallback evaluated that same function again. The log prints
`[INDEX RECHECK EFFECT] actual calls=2` before the unchanged expected-one
assertion. `index-recheck-effect-actual.log` and its executable/source are
retained in the combination directory. Earlier probe-only compile and swapped
UDF-name/type fixture failures are retained separately and are not this red.

Independent follow-up under `/tmp/dbms-index-recheck-effects.mkD1UgDL` separates
physical candidate materialization/fence validation from expression execution,
and retains one-call assertions for true, false and NULL residual results.
It is in progress, not verified by the 30 wire passes or earlier simple-index
controls. The current-read exception must not be called universally safe until
this actual repeated-effect defect is fixed and independently verified.

## Remaining boundaries

The last complete default protocol was the index-only candidate, terminal
`46761`, which passed the statistics control then timed out at prepared ALTER
(main3431/prepared_transaction_error_boundaries1197). No complete default
protocol was rerun for this 646 combination. Earlier timeouts/statistics failure
and statement-image I/O amplification remain recorded, without inventing a
shared cause. Independent native engine-owner leakage, complete pre-effect
binding, procedural output demand, JOIN COLLATE/EXTRACT roles, static zero-row
typing and all broader routine/query/index semantics remain unfinished here.

The eight mapped families stay partial. Totals remain 273: 22 complete,
166 partial, 70 unverified, 15 deferred_by_user. No full registered suite,
TLS runtime, whole-engine sanitizer or PG18.6 differential claim. No push;
Actions remain disabled; user-deferred security/TDE work stays deferred.
