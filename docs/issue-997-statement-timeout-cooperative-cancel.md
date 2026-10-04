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

Each protocol statement execution now starts a bounded watchdog. On expiry it
sets a timeout-specific interrupt flag that the existing cooperative executor
and lock-wait checks convert to SQLSTATE `57014` with a statement-timeout
message. The guard stops and joins the watchdog before clearing the shared
backend interrupt state. The CLI timeout waiter now signals the same
cooperative cancellation state to its worker instead of merely printing an
error while execution continues.

The `timeoutRequested` flag distinguishes timeout errors from explicit
CancelRequest errors while retaining the same SQLSTATE. The timeout is tested
for Simple Query and Parse/Bind/Execute while waiting on row locks. For an
explicit transaction, the regression verifies `ReadyForQuery=E`, a subsequent
`25P02`, then successful `ROLLBACK` and `ReadyForQuery=I`.

## Verification

- Baseline before the fix: `python3 tests/statement_timeout_protocol_e2e_test.py`
  failed because the lock-wait query reached the bounded socket timeout.
- `bash scripts/build.sh` — passed.
- `python3 tests/statement_timeout_protocol_e2e_test.py` — passed for Simple
  and Extended Query, autocommit and failed explicit-transaction recovery.
- `python3 tests/statement_timeout_cli_e2e_test.py` — passed; after a long
  generated-series query times out, the next `SELECT 1` executes.
- `python3 tests/postgres_protocol_test.py` — passed, including forged/valid
  CancelRequest, lock-wait cancellation, and connection reuse.
- `python3 tests/copy_protocol_e2e_test.py` — passed for existing COPY cancel
  behavior.
- `python3 tests/cli_error_recovery_e2e_test.py` and
  `python3 tests/pg_stat_activity_protocol_e2e_test.py` — passed.
- Python compilation, shell syntax check, and `git diff --check` — passed.

Implementation and regression tests are local commit `9d13355b`. No full
registered suite or PostgreSQL 18.6 runtime differential is claimed.

## Remaining scope

OPT-17 remains partial. The implementation is cooperative: executor loops and
lock waits must poll for interrupts. COPY's blocking wire I/O, other
non-cooperative waits/loops, complete interrupt propagation through every
operator/worker/I/O path, and resource-owner cleanup are not proven complete.
The current Extended Query watchdog is installed at statement execution, not
at the first Parse/Bind/Describe message as PostgreSQL 18 documents; setup-time
timeout semantics therefore still differ. CLI and Simple/Extended execution
tests verify the implemented scope only.
