# Checked plan results retain structured SQL errors

## Actual failure

ROOT 3bc45e3b's canonical full gate finishes its native portion with
515/517 passed. Both `group_collection_aggregate_test` and
`grouping_expression_metadata_test` abort with uncaught `DbError`/22012.
Their original contract requires checked execution to return `ok=false` and
the 22012 diagnostic. The structured arithmetic repair exposed a pre-existing
mixed contract: checked callers sometimes received failure results and
sometimes SQL exceptions. Other tests correctly require XX001 and released
locks when an index is corrupt; those guarantees must remain intact.

## Repair

`PlanExecutionResult` now retains original SQLSTATE, raw message, display
diagnostic and the original exception object. `executePlanChecked()` catches
SQL `DbError` into a failed result, discards all partial rows/NULL metadata,
and attempts cleanup without allowing its error to replace the first error.
The initial interrupt and structured-row capability check are inside the
same SQL error boundary. Unknown C++ exceptions retain their original type.
The result's `throwIfFailed()` restores the original structured exception,
including `StatementCommitError`'s phase identity; generic operator/null-plan
failures have an explicit XX000 fallback rather than text-derived codes.

All eight existing main SQL-dispatch consumers call `throwIfFailed()`,
including the formerly silent empty derived-query fallback. Aggregate
operators' own typed-error cleanup also cannot overwrite that SQL exception.
No alternate display-only EXPLAIN tree or text-SQLSTATE parsing is added.

The original GROUP assertions are retained and strengthened with exact
structured state and empty partial-row checks. The original corrupt-index
catch/state/lock assertions are retained: they now additionally assert the
checked failure result and then call `throwIfFailed()`. Direct StorageEngine
API throw assertions are unchanged.

## Matching private proof

Artifacts: `/tmp/dbms-checked-plan-error.tDFiKQMd`.

- V1 normal O2 build 80141 exited 0 with all 57 production CPPs freshly
  compiled. Original two GROUP regressions and the twenty existing native
  controls passed, and 14 protocol gates passed (28559/exit 0). The native
  group 66490 exited 1 because the newly added test mistakenly qualified the
  global `SessionInterruptState` with `dbms::`; that compiler failure remains
  in `checked-v1-native.log`, not relabeled as a runtime failure or pass.
- V2 adds original SQL-exception-object preservation, strengthens the
  open-false cleanup/capability check and fixes that test namespace. Because
  the public result layout changed again, build 76658 freshly recompiles all
  **57** production CPPs at normal O2 and exits 0, not just two changed CPPs.
- All 57 production object signatures and the binary configuration stamp
  match before each test group. Frozen V2 server SHA256 is
  `2c176e4c47d9c53f70ed23e3204e625b6045663769ed407be6c3bbf6497be40a`.
- **21 freshly linked native tests, 41097/exit 0**, include both original
  GROUP tests, real 300-row parallel aggregation, corrupt-index XX001/no-lock
  controls, math/CAST/type/aggregate/carrier/native-query adjacent tests.
  The new fault operator covers metadata/open/next/structured-row/close
  failures, partial-row discard, primary-vs-cleanup errors, message-embedded
  misleading SQLSTATE, early 57014/57P01, receiver demand and preservation of
  the original `StatementCommitError` subclass.
- **14 protocol/E2E gates, 65999/exit 0**, preserve exact original arithmetic,
  aggregate, describe, typed-plan EXPLAIN, routine binding/RAISE and UPDATE
  assertions on the exact V2 server.

No new production-header changes occurred while those groups ran. This is
independent private verification, not ROOT's full-gate result. The root full
75090 remains live/frozen and its original failures remain retained. Future
ROOT integration must rebuild every unit against this public ABI and must
also update the later private WHERE/ORDER consumer's error handling.

## Still open

Whole executor/query/type families, typed quantified ANY/ALL EXPLAIN, ordinary
scalar WHERE/ORDER, general operator error classification and the complete
total ledger remain partial. No full-suite, whole-engine sanitizer, TLS or
PostgreSQL 18 differential claim is made. No push or Actions enablement.
