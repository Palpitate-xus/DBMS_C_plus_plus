# PL/pgSQL RAISE severity and SQLSTATE

The prior parser defaulted an unqualified RAISE to NOTICE, ignored USING,
and the fatal runtime path called `fail` without a code. Thus an explicit
`RAISE EXCEPTION 'late failure'` aborted and rolled back correctly but
reported XX000 instead of P0001. A bare RAISE incorrectly continued.

This independent fix covers default EXCEPTION/P0001, explicit SQLSTATE and
named conditions, typed USING ERRCODE/MESSAGE expressions (`=` or `:=`),
case-sensitive condition-name data, NULL option rejection, and duplicate
option errors. It preserves the legacy ERROR severity alias. Fatal RAISE
does not also call the nonfatal notice sink. Format `%`/`%%`, doubled quotes,
escape/dollar strings, NULL arguments (`<NULL>`), and format argument-count
checks are retained as structured grammar; invalid counts reject the entire
body before preceding host SQL or sequence effects execute. Argument/option
evaluation errors retain their host-provided structured SQLSTATE.

Semantics are grounded in the official [PostgreSQL 18 RAISE
documentation](https://www.postgresql.org/docs/18/plpgsql-errors-and-messages.html)
and [runtime implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/pl/plpgsql/src/pl_exec.c).
The condition-name table uses error entries with PL labels from the official
[error-code definitions](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/errcodes.txt).
Repeated condition names retain their first definition. Explicit 00000
behaves like the zero/unset code: fatal RAISE subsequently defaults to P0001;
this matches the runtime implementation and the isolated reference controls.

## Evidence and boundaries

Original baseline HEAD `14d9cb2e`: native session 5013 exited 134 and wire
session 1406 recorded exit 1 on default EXCEPTION, actual XX000. First RAISE
wire candidate 71385 exited 0. Its native session 93369 exposed a separate
arithmetic structured-error loss; that real failure remains retained and is
fixed independently by `b91e3d06` with constant-diagnostic follow-up
`5ddeca6a`. The independent CAST fix `a95e1076` is
also required for the native invalid-cast evaluation control; its private
mapping here is `39366713`. RAISE does not parse diagnostic strings for codes.

The final matching source-only O0 candidate is retained under
`/tmp/dbms-plpgsql-raise-state.0N05Nq7X`, binary SHA256
`4e75c97385e8726deff148b8f4ddfa143ba2714cbc62b5996066c0e09eabe2f9`.
Only the PL runtime and evaluator objects were rebuilt; the other 55
objects match immutable source/header origins checked by `build.sh`.
No public API/header/layout was changed by this RAISE fix, and this evidence
does not stand for a fresh full optimized build or ROOT combined validation.

Native sessions 28921 and final-diagnostic repeat 78177 exited 0: the new RAISE test (31 exact error controls,
static pre-effect rejection, and nonfatal formatting) plus six adjacent
native tests passed. Initial matching 5211c969 binary session 89154 exited 0
for six protocol scripts (RAISE, INTO, 43 binding cases plus two sequence
controls, atomicity, ORDER, WITH). The final diagnostic-refined binary's
protocol session 27850 also exited 0 for the same six scripts. ASan session 74385 exited
0 with the new test and PL runtime TU instrumented; other engine TUs were
not instrumented and leak checking was disabled, so this is a local runtime
check, not a full-engine sanitizer claim. The unchanged full clause diagnostic
session 39868 exited 1 with exactly five remaining failures (ordinary scalar
WHERE/ORDER error propagation, two EXPLAIN paths, typed UPDATE predicate).
This RAISE commit does not claim to repair those separate consumers.
The independent reference log has 26 actual controls on **PostgreSQL 17.2**,
using BEGIN/savepoints/final ROLLBACK; this is not PostgreSQL 18.6 parity.

Full EXCEPTION handlers, handler subtransactions, inside-handler rethrow,
and stacked diagnostics remain open. Outside-handler bare RAISE now returns
0Z002. The current host notice contract cannot publish a custom notice
SQLSTATE or DETAIL/HINT/object diagnostic fields; diagnostic options fail
closed with 0A000 rather than being silently discarded. Existing fatal
message prefixes remain for compatibility; exact PostgreSQL message text
and frontend NoticeResponse publication are not claimed by this change.
