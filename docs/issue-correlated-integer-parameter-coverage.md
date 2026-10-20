# Genuine INTEGER correlated-parameter coverage

## R187: observed exit 2 remains open; failure diagnostics added

The original canonical full run on frozen `2442e4c7` returned **2** for
`correlated_integer_parameter_origin_test`, without its complete footer or
a diagnostic. That exit can occur during setup, row insertion, missing bound
metadata, **or final database cleanup**. The old log does not establish which
operation failed, nor that the twelve execution cases were never reached.

An instrumented clone subsequently completed all 28 original assertions.
Two further runs of the exact original binary, one with file-operation tracing,
also completed all 28, as did the relative-data-directory diagnostic control.
These later passes do not explain or repair the original failure. No production
or INTEGER-demand defect has been established, and this item remains open.

The permanent fixture now identifies each failing operation and includes its
DBStatus/SQLSTATE, or the precise failed type/metadata prerequisite. Database
calls retain their original argument lists and short-circuit order. Every
original value, true four-byte INTEGER descriptor, NULL, cursor/fallback,
two-writer-effect and 28-assertion reach check remains unchanged. It still
returns 2 on operation failure; no assertion or timeout was removed.

Both accepted-only and workspace libraries completed all 28 assertions with
the diagnostic fixture. Each additionally ran in a privately owned read-only
directory and actually returned 2 with the exact diagnostic
`create database status=5 sqlstate=58030`. Directory permissions were restored.
Both fixtures were freshly compiled/linked against their own checked 57
production-library objects/stub and all 127 headers. The workspace library
retains pre-existing unaccepted changes; this is not approval of those changes.
Production code, headers, registered test counts and Actions were unchanged.

Evidence:

- Original full trace: `/tmp/dbms-accepted2442-full1492.9obmw81r/canonical-full-registered.log`.
- Exact original/trace controls: `/tmp/dbms-r187-original-trace.v3e75754/completed-observation-receipt.json`,
  SHA256 `4a400bdb61556fa725eb444ecafe3c288fa5184a80c22b0c2921312fb96d530d`.
- Positive/negative diagnostic gates: `/tmp/dbms-r187-permanent-diagnostics.ntd5s083/completed-focused-receipt.json`,
  SHA256 `6fbfbc54977957824de49b2f00a874c3e960d2a01c031b9da41d7a1b6a40bbd7`.

The owned canonical scheduler resumed its unchanged inputs after each idle-boundary
check. Its original failure remains recorded; this is not a new full-suite pass,
PostgreSQL differential, TLS/sanitizer result or closure of the 273 requirements.
Next recurrence must use the new operation diagnostic to investigate the actual
status and preserve the failing data before retrying. R185 remains skipped;
no push. The older evidence below is historical.

The original `parameter_origin_correlated_child_test.cpp` has 24 execution
cases plus a reach assertion. Its `makeIntColumn("i", true, 4)` factory makes
an eight-byte BIGINT. That test remains required and unchanged; describing its
physical input as INTEGER was inaccurate. The separate existing 12-case wire
fixture does use genuine SQL INTEGER.

`correlated_integer_parameter_origin_test.cpp` adds independent coverage:

- Actual registry resolution and persisted four-byte INTEGER schema.
- Actual bound INTEGER source descriptor and Boolean output metadata, with
  no writer effects during preparation.
- All twelve NULL/zero/one, BETWEEN/NOT BETWEEN, typed-cursor/native-fallback
  cases, genuine descriptor-ordered input cells and exactly two writer effects.
- Reach assertions: 28 complete controls, without removing the original 25.

## Actual current-root evidence

Artifacts: `/tmp/dbms-root-parameter-origins.jChCYiKO`.
The unchanged new fixture compiled against actual old Root `8505b94e` headers
and its receipt-verified normal project objects passes all 28 controls, exit 0
(session 50034, `true-integer-old-root.log`). The actual old Root binary is
`c7abd647dad7e633de9f4e09f387640f100cbdae121ac8afdd1fb03c3eec568b`.

The four independently composed demand/origin/carrier/context commits at
`2585d80d` genuinely compile all 58 production units from scratch, exit 0
(session 5065). Its own normal binary is
`0d4d368bde7db00a4bcbc2f69892ec4df7e589b1ec91cf3e338b77f93bcb8bec`.
All 15 complete native fixtures pass on its own current normal objects,
including original 25 and new 28 (session 5407, `current-core-native15.log`).
All source/header/compiler/flag receipts, build stamp, frozen binary and input
seal are checked before and after that gate. This is a coverage/documentation
correction, not evidence of a new INTEGER production defect or completion of
the full INTEGER family. The independent 9,649-control integer fixture still
fails 4,116 controls on this four-commit composition (session 30030,
`current-core-integer9649-baseline.log`); its separate integer repairs remain
necessary.
