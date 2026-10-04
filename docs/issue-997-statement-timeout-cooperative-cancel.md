# Issue 997: enforce `statement_timeout` on cooperative execution paths

## Reproduction

The PostgreSQL wire path accepted and reported `SET statement_timeout`, but did
not arrange for that timeout to interrupt execution. A bounded lock-wait test
set `statement_timeout = 100`, held a row lock on another connection, and sent
an `UPDATE` from the waiter. Before the fix, the client waited until its
four-second socket timeout and raised `TimeoutError` instead of receiving a
database error.

The interactive CLI had a parallel defect: it used `future.wait_for()` to
print a timeout message but never signaled the query worker, so the task kept
running and the next statement could not be processed until it finished.

PostgreSQL 18 documents that `statement_timeout` aborts statements exceeding
the configured duration; Simple Query applies it per statement, while Extended
Query has protocol-message timing semantics. `ReadyForQuery` must report `E`
for a failed explicit transaction until rollback. See the [PostgreSQL 18
`statement_timeout` documentation](https://www.postgresql.org/docs/18/runtime-config-client.html),
[protocol transaction-status documentation](https://www.postgresql.org/docs/18/protocol-message-formats.html),
and [transaction tutorial](https://www.postgresql.org/docs/18/tutorial-transactions.html).

## Change

Simple Query statements use a bounded watchdog per statement. Extended Query
protocol now records one deadline at the first Parse, Bind, Execute, or Describe
message, carries the remaining budget through protocol setup and execution,
and keeps it active until Execute or Sync completes. If it expires while the
backend is waiting for the next message, the server returns SQLSTATE `57014`
and requires Sync recovery; during execution, the remaining budget drives a
watchdog that the existing cooperative executor and lock-wait checks convert
to the same timeout error. The guard stops and joins the watchdog before
clearing shared backend interrupt state. The CLI timeout waiter now signals
that cooperative cancellation state to its worker instead of merely printing
an error while execution continues.

The `timeoutRequested` flag distinguishes timeout errors from explicit
CancelRequest errors while retaining the same SQLSTATE. Tests cover Simple
Query and Parse/Bind/Execute lock waits, plus a Parse-only cycle that remains
open past the timeout and is canceled before Execute; Sync then restores
`ReadyForQuery=I`. For an explicit transaction, the regression verifies
`ReadyForQuery=E`, a subsequent `25P02`, then successful `ROLLBACK` and
`ReadyForQuery=I`.

## Verification

- Baseline before the fix: `python3 tests/statement_timeout_protocol_e2e_test.py`
  failed because the lock-wait query reached the bounded socket timeout.
- `bash scripts/build.sh` — passed.
- `python3 tests/statement_timeout_protocol_e2e_test.py` — passed for Simple
  and Extended Query, autocommit and failed explicit-transaction recovery.
- `python3 tests/statement_timeout_cli_e2e_test.py` — passed; after a long
  generated-series query times out, the next `SELECT 1` executes.
- `python3 tests/postgres_protocol_test.py` — passed with its default bounded
  socket timeout on a serial run, including forged/valid CancelRequest,
  lock-wait cancellation, and connection reuse. An earlier parallel run hit
  that 10-second client bound under concurrent test load; the serial rerun
  passed.
- `python3 tests/copy_protocol_e2e_test.py` — passed for existing COPY cancel
  behavior.
- `python3 tests/cli_error_recovery_e2e_test.py` and
  `python3 tests/pg_stat_activity_protocol_e2e_test.py` — passed.
- Python compilation, shell syntax check, and `git diff --check` — passed.

The initial implementation is local commit `9d13355b`; first-message Extended
Query timing and its regression are local commit `aa904b22`. No full registered
suite or PostgreSQL 18.6 runtime differential is claimed.

## Remaining scope

OPT-17 remains partial. First-message Extended Query timing is now covered, but
the implementation is still cooperative: executor loops and lock waits must
poll for interrupts. COPY's blocking wire I/O, blocked/partial socket reads and
writes, other non-cooperative waits/loops, complete interrupt propagation
through every operator/worker/I/O path, and resource-owner cleanup are not
proven complete. The CLI and protocol tests verify only the paths listed above.
