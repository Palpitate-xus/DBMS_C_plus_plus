# Scalar fixes: current public-API integration proof

This is a validation record, not a claim that the full protocol or all scalar
and planning families are complete. No production source is changed here.

## Frozen inputs and build

The private source history is `043ffa7c` plus the original independent fixes
`8770ba41` (cardinality detection), `0709d889` (static output descriptor),
`7965d970` (retained runtime), and the exact ROOT formatter `a7d460c7`.
`git diff a7d460c7 HEAD -- src scripts tests` was empty: production sources,
headers, tests, registry, and build inputs are byte-identical to ROOT `a7d460c7`.
This is an independent **normal O0**, not ROOT's normal O2 binary.

Artifact directory: `/tmp/dbms-scalar-current-planning-integration.KjjAKRHU`.
The initial new-cache build compiled all 58 production units, session `39173`,
terminal 0. The subsequent formatter-only rebuild `55409`, normal repeat,
all 58 signatures, and binary configuration stamp were all terminal 0.

Frozen binary:
`final-immutable.vJV6H9rA/dbms_main.frozen`, SHA256
`7bad59eb7b5c671b090ebfff8ca49b3e3a63e04b03e2dfb8c895932cd96abb56`.
The immutable directory retains production source/header manifests, flags,
objects, and test/build-input hashes. No old public-layout objects were reused
for the initial fresh build.

## Actual results

| Gate | Terminal result |
| --- | --- |
| 17 matching native tests, session `55632` | 0; includes unchanged original WHERE/ORDER host counters, mixed scalar/quantified host ownership, typed source/context/cursor, constant planning, unary, and interval formatting |
| Unsplit scalar projection protocol matrix | 0; original 21000 assertions, OID 23, BIGINT/array/BOOL, NULL/empty text, TEMP/quoted/alias/VIEW/JOIN/CTE/correlation, no partial results, recovery, and missing child column before writer retained |
| Complete builtin unary protocol matrix | 0; all original SQLSTATE, widths, dead-input and effect assertions retained |
| PL query destination protocol | 0; both original sequence observations retained |
| Both scalar and unary strict reference matrices, session `49401` | 0 against the actual XML-enabled PostgreSQL `180006` endpoint, matched en_US/libc database |
| 11 serial adjacent protocol scripts, session `97624` | 0, no failures; quantified demand 84 plus TEXT/JSON instrumentation, ordinary WHERE/ORDER, sort slots, WITH, EXPLAIN, PL binder/INTO/atomicity, ARRAY and CASE |
| Original fromless demand matrix, part of session `95535` | 1; only two assertions for `SELECT (SELECT 1/0) WHERE false` failed: actual successful `SELECT 0`, expected 22012 and no completion |
| Unchanged complete original protocol, session `74497` | 1; original scalar 21000 and quantified assertions passed, next failure remains the original joined-view UPDATE at line 2764 |

The four-script wrapper `95535` as a whole returned 1, not 0. Its passing
components must not be relabelled as a passing entire group. The constant-child
planning boundary is tracked independently; its expectations were not lowered.

## Original full-protocol failure retained

The full test kept the original SQL, assertions, 10-second socket deadline,
startup/shutdown bounds, and compatibility mode. An external read-only wrapper
recorded wire error fields and each owned server's stdout/stderr; it did not
change repository test code or results. `TMPDIR=/dev/shm` is a bounded semantic
test placement, not evidence that disk I/O or durability problems are fixed.

`original-full-current-a7.log` records 345 `simple_query` calls before the
unchanged assertion at line 2764:

```sql
UPDATE jt_view SET val = 'v2' WHERE bid = 10
```

Actual wire SQLSTATE is 42703, message `column "v2" does not exist`, no command
completion, and ReadyForQuery `I`. The complete test returned 1; later assertions
were not reached. The two owned server logs are `original-full-server-1.log` and
`original-full-server-2.log`. Its finally block stopped the test-owned process.

A separate new worktree `/tmp/dbms-joined-view-trigger.Cbhks6Kd` reproduced the
same exact SQL and unchanged base value `v1`; the corresponding real PostgreSQL
18.6 joined view with a legitimate trigger function updated to `v2` and deleted
the row. The DBMS-specific inline trigger SQL was not substituted in the DBMS
baseline. The legacy SET helper strips `'v2'` to raw `v2`, while the view trigger
substitution expects SQL representation. That independent transport repair is
not claimed complete by this record.
