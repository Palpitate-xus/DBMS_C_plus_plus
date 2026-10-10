# Restore the active Session across recursive SQL execution

## Confirmed failure

The unchanged `view_trigger_typed_values_protocol_e2e_test.py` returned the
expected CHECK violation 23514, then lost its connection on the next SELECT.
Capturing the actual server process before test cleanup proved exit -11
(SIGSEGV), not a timeout or a test teardown signal. A diagnostic backtrace
identified `StorageEngine::beginTransaction` iterating
`currentSession()->tempTableOnCommit` through an expired Session.

The frontend trigger executor copies the active Session into a stack-local
Session. `executeInternal` changes the thread-local current pointer, but an
exception skips the trigger executor's manual restoration. The outer statement
therefore rolls back with a stale child pointer, and the next transaction
dereferences freed stack state. Recursive JOIN dispatcher copies had the same
unscoped entry contract.

## Repair and regression

The shared `executeWithCteInheritance` entry now binds its actual Session before
statement BEGIN, retains it through statement cleanup, and restores the previous
pointer through a local RAII scope on every return or exception. The change is
Main-only; no public layout, catalog format, transaction boundary or SQLSTATE
was changed.

A registered real-Main frontend driver (no test stubs) performs 14 checks:
empty input, lexical exception, caller-owned setting, SELECT success, exact
22012 native bool/diagnostic error, actual view-trigger success and CHECK throw,
outer statement rollback, continued execution and restoration of a null owner.
Its trigger callback uses a genuine copied stack-local Session. The initial
fixture incorrectly expected the native division branch to throw; its bool
return plus exact SQLSTATE diagnostic is now checked explicitly. Original
failure logs remain; the trigger 23514 assertion still requires a real throw.

## Actual evidence

- Own normal Main compilation session 92069: terminal 0; the other 57 own
  production units still match the previously rebuilt headers and receipts.
- Original full strict PostgreSQL 18.6 (`180006`) view-trigger fixture: terminal
  0, including failure rollback, nontransactional sequence demand, caller
  savepoint ownership, subsequent queries and typed DELETE.
- The same complete project fixture on the repaired frozen binary: terminal 0;
  no SQL, assertion or timeout was removed or relaxed.
- New real-Main lifetime driver session 62436: terminal 0, 14 checks.
- Existing real-Main EXPLAIN cleanup driver session 15558: terminal 0, zero
  failures. Only the test Main TU uses its documented -O0; production is -O2.
- Final composition session 22980: terminal 0, 12/12 native and 16/16 protocol
  drivers, including the full default protocol, transaction/savepoint/lock
  neighbours, the original enum matrix and the quoted-type follow-up. All 58
  production receipts, cache, source, repeat objects and frozen bytes match at
  start and terminal. No foreign objects or binaries are borrowed.

Final source/test seal:
`280da1dffcf542dbe9708fe9cbdea995609fc3363db8e01464f2198e6f969e72`.
Frozen SHA-256:
`0a24168fe54d61b1382448b4df58a983a978ad1c12733c37c959930ef1b5c80a`.
All original crash logs, fixture mistakes, reference results and final evidence
remain under `/tmp/dbms-having-grammar.MPk64yc0/`, including
`trigger-session-final-publication12-16.log` and
`session-lifetime-frontend-v3.log`. An early direct launch before its frozen file
existed failed with FileNotFoundError; it is not counted as a product result.

This private repair is not approval of Root's still-running full registered
suite or completion of the original 273 requirements. Catalog publication,
recovery, corruption, table-owner atomicity and remaining ordinary requirements
stay in scope. No push or Actions activation.
