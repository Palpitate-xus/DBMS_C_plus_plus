# Simple CASE constant-NULL planning demand

## Scope

`PreparedQueryExecution` previously evaluated every simple CASE WHEN operand
even when the switch was a structural constant NULL. It also evaluated a
volatile switch when all equality conditions were constant NULL. PostgreSQL
18.6's planner discards those strict equality alternatives before execution.
This is not the runtime rule for a nullable source column: a row containing
NULL still demands the WHEN operands. Ordinary SQL's legacy CASE consumer
activation remains a separate issue; this change does not claim that every
CASE query shape or the general optimizer is complete.

The independent regression retains the original SQL and the required
`currval` SQLSTATE `55000`, rather than changing its expected effects to two.
The earlier mistaken extra expectation is retained in
`/tmp/dbms-simple-case-binding.KtyWx31T/prepared.reference18.final.log`.

## Change

The execution-owned expression copy now plans structural CASE constants after
whole-query binding. It folds only literal/cast/operator planning values, never
a routine, parameter, column or query child. It simplifies a WHEN condition
before deciding whether to discard the alternative, then leaves a discarded
THEN untouched. A known match drops subsequent alternatives and the old ELSE.
An all-discarded CASE reduces to ELSE, without evaluating a discarded switch.
Shared prepared ASTs, typed output descriptors and source bindings are unchanged.

This preserves the distinction documented for [CASE evaluation](https://www.postgresql.org/docs/18/functions-conditional.html):
lazy execution does not suppress a planning-time error in a constant expression.
Local official PostgreSQL 18.6 source `optimizer/util/clauses.c`, the CASE branch
at lines 3147–3277, was also inspected for condition/result traversal order.

## Actual evidence

All private artifacts are under `/tmp/dbms-simple-case-binding.KtyWx31T`.
The reference uses the root-owned isolated PostgreSQL 18.6 instance and a
strict `server_version_num = 180006` check, not the historical PostgreSQL 17
diagnostic instance. Fresh sequences are used per case: RESTART does not clear
session `currval`, so a first exploratory restart-based log was superseded by
`planning.reference18.freshseq.log`.

| Check | Original matching candidate | Fixed candidate / reference |
| --- | --- | --- |
| Original constant NULL, two volatile WHENs | sequence advanced twice | result 3, `55000` |
| Constant NULL, arithmetic-wrapped volatile WHEN | advanced once | result 3, `55000` |
| Volatile switch, all NULL WHENs | advanced once | result 3, `55000` |
| Same shape with discarded volatile THEN | advanced once | result 3, `55000` |
| Runtime NULL source column | two WHEN calls | two WHEN calls |
| Constant NULL with pure WHEN division/overflow | `22012` / `22003` | errors retained, including zero rows |
| Discarded THEN division/overflow | no error | no error |

The matching baseline native terminated with four failed effect assertions
(`demand/native.baseline.log`, tool 28968, exit 134); the full retained protocol
baseline also reported four failures (`demand.wire.baseline.log`, tool 46902,
exit 1). The final fixture adds child-query, boolean demand, zero-row and
whole-binding-before-pruning controls without dropping the original SQL.

Reference `constant-demand.reference18.expanded.log` exited 0. Candidate
`demand.wire.candidate.log` (tool 10440) exited 0. The production change has no
header/layout change: the core's 58-object source/header/flag basis is retained,
the sole changed execution TU is freshly compiled, and stubs are fresh.
`build.demand.log`, `demand/{sources,headers}.audit.txt`,
`demand/other57.sources.audit.txt` and `demand/binary.sha256` record this proof.

Four native controls (`simple_case_constant_demand_test`,
`simple_case_equality_binding_test`, `case_common_type_test`,
`prepared_query_execution_test`) pass with native `-O2` and also under scoped
ASan/UBSan, as recorded in `demand.asan.log`. The instrumented region comprises
parser, binder, evaluator, helper, prepared execution and DML plus fresh stubs;
the other 51 non-main objects are matching, uninstrumented development objects.
This is a scoped sanitizer proof, not a whole-program sanitizer run.

## Still open

This is structural CASE planning, not a complete immutable-function constant
folder, operator catalog, custom-type planner or all-query consumer closure.
Function calls themselves are deliberately not executed here. Ordinary raw SQL
CASE activation and other unsupported query-plan shapes are separately tracked.
