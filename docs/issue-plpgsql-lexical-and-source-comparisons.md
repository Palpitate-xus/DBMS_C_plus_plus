# Procedural lexical operands and source-query null-safe comparisons

Date: 2026-10-06. Four independent source/test commits, followed by combined
verification. These are narrow fixes, not completion of SQL-03/04, FUNC-06 or
TYPE-06/07. The complete statement namespace/preparation contract remains open.

| ROOT commit | Reproduced defect | Fix and permanent regression |
| --- | --- | --- |
| `711f9030` | Local `"C"` changed COLLATE `"C"` to CAST, producing 42704 | Preserve a complete qualified/delimited collation-label unit; `plpgsql_collation_label_test.cpp` and matching protocol test |
| `0888873f` | AT TIME ZONE variable/function/CASE/parenthesized/NULL operands failed; NULL/type and interval-offset handling were wrong | Parse timezone(zone,value) as an actual function AST at AT precedence; typed NULL/zone/source validation and second-resolution interval offsets; `plpgsql_timezone_operand_test.cpp` and matching protocol test |
| `d2b7b566` | IS DISTINCT FROM wanted treated its FROM as a relation introducer, so wanted stayed unbound/42703 | Preserve the complete comparison grammar unit while still protecting actual relation/alias labels; `plpgsql_distinct_operand_test.cpp` and matching protocol test |
| `b2c4d983` | Ordinary CAST comparison projection returned 42883, computed WHERE returned all rows, parenthesized BOOL returned f as TEXT/OID25 | Remove operand-truncating text rewrite, distinguish actual relation FROM, keep CASE/comparison ASTs, and use typed projection/predicate paths; `distinct_source_query_protocol_e2e_test.py` |

## Independent red-to-green evidence

Private source worktree `/tmp/dbms-plpgsql-lexical.tGdnrR` started at `8cd860e7`.
Its commits `51a00919`, `7b91e90f`, `9aca5e67`, `ca7825fe` map to the four ROOT
commits above. Source headers/API/layout were unchanged. No ROOT source was
edited during these isolated checks. Exact source/object/binary hashes and
terminal records are retained in `lexical-followups-evidence.md` there.

- COLLATE native baseline e8912e and wire b8eaff failed; optimized standalone
  utility 7d5e55 and wire 40221 passed. Quoted/qualified/comment labels, a
  same-named data variable, NULL, unknown collation and recovery are retained.
  Utility label preservation is not full catalog-qualified collation support.
- Time-zone native baseline 6f6773 and wire 94b3c3 failed. A separate ordinary
  parenthesized-zone probe 8fbeb9 also returned 22023. Actual ExprHelper native
  568558 and wire 83242 passed: genuine zone expressions, quoted cases, named
  DST-at-input-date, NULL/OIDs, invalid source/zone types, chaining, cast/concat/
  arithmetic precedence, minute/second offsets and month/day rejection.
- DISTINCT native baseline 037a48 and wire 1b232d failed. Optimized utility
  fa22c1, ASan/UBSan 597499 and FROM-less wire 21060 passed. The expanded table
  CAST wire 28779 failed with 42883 and was retained as the separate fourth bug.
- Source-query diagnostics 76661/61305 and baseline gate 80527 failed with the
  wrong value/rows/type above. Simply deleting the rewrite failed with 42P01
  (21467): the comparison's FROM still looked like a relation clause. The
  completed typed/source bridge passed dedicated gate 1126, including BOOL
  alias ordering, full CAST/CASE/function operands, SQL NULL, invalid cast22P02,
  repeated JOIN-range projection, PL INTO/refill/empty, literal UPDATE and
  DELETE with typed null-safe predicates, and recovery.

The final private server combined 50 matching-layout old development objects
with five fresh TableManage/plpgsql/parser/ExprEvaluator/main objects; the core
objects were O0, not a formal optimized build. Its SHA256 was
`5a29851b67efa65f6edc75c3eb7f87705a131b2347c0a7571d2b47bb4876c9df`.
Actual ExprHelper timezone/interval/constraint natives passed with matching
development core; separate utility units passed optimized and sanitizer checks.
This does not establish whole-engine sanitizer coverage.

The final private unchanged server passed 12 distinct adjacent protocol scripts:
boolean_literal_boundary, arithmetic_predicate, sql_literal_preservation,
table_case_ast, self_join_range_identity, constant_boolean_predicate,
plpgsql_select_into, plpgsql_quoted_scalar_binding, plpgsql_collation_label,
plpgsql_timezone_operand, plpgsql_distinct_operand and timezone_expression_header.
These are the actual checked-in `tests/*_e2e_test.py` entry points; the dedicated
fourth source-query test passed in addition. All owned servers were cleaned up.

## Failed attempts and remaining boundaries

The initial constraint-control compilation lacked -Isrc/common (51994) and its
nonexistent executable attempt returned 127; corrected build/runtime 17934
passed. Typed-main first compilation 96005 failed on a Stmt*/SelectStmt* visitor
mismatch; corrected compile 14653 succeeded. Runner startup 29381 and 52468
failed with ConnectionAbortedError, then unchanged reruns passed; the cause of
those startup failures was not established. None is recorded as a passing run.

Null-safe JOIN ON remains 0A000 on both baseline and fourth candidate. It is
independent work, not a passing projection test. UPDATE expression assignment
combined with a computed/typed predicate returned XX000 in old candidate probe
76507; ordinary WHERE plus arithmetic assignment succeeded (UPDATE 1) in the
same probe. Literal assignment in the fourth regression tests a genuine supported
predicate but does not fix that separate composition defect.

General PL variable/source ambiguity, function/block labels and preparation
before volatile/writing-CTE effects remain open; see
[`issue-plpgsql-query-binding-preflight.md`](issue-plpgsql-query-binding-preflight.md).
AT LOCAL, timetz and the complete timezone/collation/operator families are not
completed by these commits. PostgreSQL runtime controls used 17.2, not 18.6.

## ROOT combined verification

ROOT combined the four fixes with independent stored-function atomicity
`5411a7aa`, preserving each local source/test commit. Formal build terminal
70445 exited 0: five changed CPPs (main, TableManage, parser, ExprEvaluator,
plpgsql) were freshly compiled with the shared optimized configuration. The
remaining 50 were reused only after current source/header/config signatures
matched; no header/API/layout changed. Normal linker, repeat up-to-date build
and independent 55/55 object-signature/binary-stamp audit all exited 0.
This is an optimized matching combination, not a new all-55 cold compilation.

Frozen artifact `/tmp/dbms-plpgsql-combination.D4U50YDA/dbms_main.frozen` has
SHA256 `c911593edbec82aaf96e5b67879b74bfe5c0ea955d605166b770f17a019c21f5`.
The verifier source is retained as verify-combination.sh there. Terminal 75177
exited 0 after fresh optimized test compilation, fresh matching test stubs,
production-object links and separate fresh working directories for 14 natives:

`stored_function_atomicity_test.cpp`, `plpgsql_native_query_sqlstate_test.cpp`,
`plpgsql_quoted_scalar_binding_test.cpp`, `plpgsql_query_host_test.cpp`,
`plpgsql_select_into_test.cpp`, corrected `plpgsql_test.cpp`,
`plpgsql_collation_label_test.cpp`, `plpgsql_timezone_operand_test.cpp`,
`plpgsql_distinct_operand_test.cpp`, `function_procedure_test.cpp`,
`interval_arith_test.cpp`, `constraint_expr_test.cpp`, `parser_phase1_test.cpp`
and `query_snapshot_characteristics_test.cpp`, all under tests/.

Terminal 71868 exited 0 after 25 distinct frozen-binary protocol entry points:

- The five dedicated COLLATE/time-zone/DISTINCT-operand/source-query/atomicity
  scripts introduced by these five source/test commits.
- PL select_into, quoted_scalar_binding, function_result, stored_query_namespace,
  cte_relation_scope, dml_cte, self_join_range_identity and cold_start_transaction_backup.
- query_snapshot_characteristics, commit_failure_recovery,
  autocommit_deferred_constraint, autocommit_returning_deferred,
  transaction_select_table_lock and transaction_ddl_upgrade_timeout.
- constant_boolean_predicate, arithmetic_predicate, table_literal_boundary,
  table_case_ast, sql_literal_preservation and timezone_expression_header.

Names abbreviate actual checked-in tests/ protocol/e2e entry points; exact paths
are in the retained verifier. The cold-start script's four scenarios count as
one entry point. These tests are plain TCP with the configured TLS stub,
zlib and ICU, not TLS runtime verification or PostgreSQL 18.6 differential.

An additional matching `tests/boolean_literal_boundary_e2e_test.py` terminal
18039 exited 0, bringing the passing focused/adjacent entry points to 26. It
ran concurrently with the beginning of the full protocol attempt.

Fresh matching known-gap terminal 43009 exited 1 with all nine real function
WHERE/ORDER/subquery/EXPLAIN/typed-DML failures; none is counted among the 26
passing scripts. The fresh full default-protocol terminal 29960 exited 1 at
postgres_protocol_test.py:2374, not with a timeout: pg_stat_tables for t returned
`t,1,1,0,0,1,0,0,1`, failing the idx_scan/idx_tup_fetch assertion. Its exact log
is retained at `/tmp/dbms-plpgsql-combination.D4U50YDA/full-default-protocol.log`.
Source inspection shows filterRows disables index candidates whenever any
transaction on that database is active; the new implicit query owner activates
that guard even for an otherwise isolated read. This access-path regression is
independent open work. Counters reflect the heap path; they must not be
artificially incremented or the assertion weakened to claim a pass.

Earlier four full-default-protocol timeouts remain retained independently;
statement-image I/O amplification is not repaired. No full registered native/
protocol suite or PostgreSQL 18.6 differential was newly run.

Six ledger unit tests and documentation-status/version/compatibility checks all
exited 0 after these evidence updates. require-complete exited 1 honestly:
273 total, 22 complete, 166 partial, 70 unverified, 15 deferred_by_user. No family
was promoted merely because a narrow regression passed.

No push or Actions enablement. User-deferred security/TDE work remains deferred;
the 273-item goal is not complete.
