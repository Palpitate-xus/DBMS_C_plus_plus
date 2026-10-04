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
`ReadyForQuery=I`. COPY FROM now uses the statement deadline while waiting for
frontend data; an idle COPY and a stalled partial CopyData frame both time out
with `57014`. The partial-frame case closes the connection after reporting the
error, because the remaining frame bytes cannot safely be parsed as a new
message, and verifies the inserted row was rolled back. CancelRequest also
interrupts COPY FROM while it is waiting for more input. COPY TO now writes
CopyData and CopyDone with deadline-aware nonblocking socket polling and checks
interrupts while waiting for writability. If a timeout occurs after output has
started, its ErrorResponse gets a bounded flush attempt; a still-backpressured
connection is closed instead of blocking indefinitely.
The TLS path uses nonblocking `SSL_read`/`SSL_write` retries as well. During
TLS COPY FROM, an idle timeout remains recoverable at `ReadyForQuery=I`; if a
partial encrypted record is received, timeout reports `57014` and closes the
connection because the TLS/protocol stream cannot be safely resumed.

## Verification

- Baseline before the fix: `python3 tests/statement_timeout_protocol_e2e_test.py`
  failed because the lock-wait query reached the bounded socket timeout.
- `bash scripts/build.sh` — passed.
- `python3 tests/statement_timeout_protocol_e2e_test.py` — passed for Simple
  and Extended Query, autocommit and failed explicit-transaction recovery,
  idle COPY FROM, extended-protocol COPY timeout, and partial-frame timeout
  with rollback.
- `python3 tests/statement_timeout_cli_e2e_test.py` — passed; after a long
  generated-series query times out, the next `SELECT 1` executes.
- `python3 tests/postgres_protocol_test.py` — passed with its default bounded
  socket timeout on a serial run, including forged/valid CancelRequest,
  lock-wait cancellation, and connection reuse. An earlier parallel run hit
  that 10-second client bound under concurrent test load; the serial rerun
  passed.
- `python3 tests/copy_protocol_e2e_test.py` — passed, including CancelRequest
  while COPY FROM is waiting for the next frontend message, plus a COPY TO
  client that does not read a multi-megabyte result and whose backend exits
  active state at the configured timeout.
- An OpenSSL 3.5.5-enabled `bash scripts/build.sh` — passed.
- `python3 tests/statement_timeout_tls_protocol_e2e_test.py` — passed repeatedly
  against a real TLS server: idle TLS COPY returns `57014` and ReadyForQuery I;
  sending only a TLS record header then stalling returns `57014` and closes.
- `python3 tests/cli_error_recovery_e2e_test.py` and
  `python3 tests/pg_stat_activity_protocol_e2e_test.py` — passed.
- Python compilation and `git diff --check` — passed.

- The complete registered `bash scripts/build_tests.sh` was run on the
  integrated local branch: all 460 C++ tests passed; 202 E2E tests passed and
  two older statistics E2E fixtures failed because they still queried the
  intentionally fail-closed `pg_stats` view (issue 989). Those two tests have
  since been updated and each passes in a targeted rerun; the full registered
  script has not been rerun after that test-only update. The default suite
  binary uses the TLS stub, so its TLS E2E was skipped; the OpenSSL-enabled TLS
  build and dedicated runtime test above passed.

The initial implementation is local commit `9d13355b`; first-message Extended
Query timing and its regression are local commit `aa904b22`; COPY input wait
timeout/cancellation and its regressions are local commit `78e0a0d7`; bounded
COPY output writes and backpressure regression are local commit `57422dbf`;
TLS COPY input deadlines and their live regression are local commit
`a9b662e2`. No PostgreSQL 18.6 runtime differential is claimed. The full
registered suite result and the
two stale statistics fixtures are recorded above; a clean full-suite rerun
after those fixture updates remains pending.

## Remaining scope

OPT-17 remains partial. First-message Extended Query timing, COPY FROM idle and
partial plaintext-frame waits, and COPY TO TCP output backpressure are now
covered, but the implementation is still cooperative: executor loops and lock
waits must poll for interrupts. TLS partial-record timeout has a live regression
for COPY FROM, but ordinary frontend reads outside COPY still use the legacy
blocking path; TLS output backpressure outside COPY, other blocking socket I/O,
non-cooperative waits/loops, complete interrupt propagation through every
operator/worker/I/O path, and resource-owner cleanup are not proven complete.
The CLI and protocol tests verify only the paths listed above.
