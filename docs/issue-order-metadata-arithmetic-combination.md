# Stored ORDER, routine metadata and typed table arithmetic

Date: 2026-10-06. Three independent local source/test commits are integrated.
Their optimized ROOT combination passed the 44 focused native and protocol
entry points below, but its full-default protocol failed during startup. This
does not complete the overall audit or any of the broader mapped families.

| Actual defect | Independent ROOT commit | Retained repair evidence |
| --- | --- | --- |
| Direct stored-function ORDER BY ignored errors and writes; equivalent target/key spellings executed twice; text keys were compared numerically | `98b70e2f` | Shared scalar binding, typed comparison and source/routine/operand identities. Actual candidate red on qualified/unqualified target/key sharing and a separate NULL versus text `null` key collision are preserved. Final private expanded ORDER wire, metadata native, eight adjacent natives and nine adjacent wires passed. |
| Stored-routine return-type annotations collided with legal quoted column hints | `3e7ee5f4` | Actual pure-metadata native red inferred INTEGER instead of BIGINT, with zero function calls/writes. An AST-node-keyed routine map is separate from read-only column hints; no merely renamed guessable namespace. Final ten distinct private natives and four wire scripts passed. |
| Ordinary table arithmetic bypassed typed execution, losing REAL rounding, BIGINT exactness, small/int overflow and division errors | `a00d147a` | Actual baseline matrix: REAL sum 16777217 instead of 16777216, INT/SMALL sums succeeded rather than 22003, division by zero returned an empty value rather than 22012, BIGINT MAX+0 rounded to 9223372036854775808. Typed AST evaluation retains real types, explicit NULLs, structured errors and the actual engine owner. Final dedicated native/wire plus nine adjacent natives/ten adjacent wires passed. |

ORDER details are in `docs/issue-stored-function-order-execution.md`.
Namespace details are in `docs/issue-stored-function-metadata-namespace.md`.
The private optimized-helper namespace binary SHA-256 is
`3f72db901fd77eb136a29c49d77a13b54d40108a469a7767492299f17060c9ec`,
retained under `/tmp/dbms-function-metadata-namespace.QpsuV6`.
The private arithmetic binary SHA-256 is
`a77b3d398b62543b0153027aad8c73a15943d66e0e76c52581b3c9f787aeff72`,
retained under `/tmp/dbms-null-safe-join.0MiJvD`. It reused 50 byte-matching
development core objects with fresh main/network/helper/evaluator/storage
and stubs; main/network/core were O0 and selected changed sources were O2.
Arithmetic sanitizer checks instrumented storage and its test only. These are
not all-ROOT O2, full-suite or whole-engine sanitizer claims.

The header/layout changes include structured `ExprEvalResult.sqlState` and
`OrderBySpec.expressionIdentity`. Metadata accessors do not invoke routines.
The merged helper retains the earlier shared arithmetic type rules and exact
quoted-row positional binding. The merged projection-demand adapter retains
its explicit actual-engine argument; namespace maps retain all recursive
routine annotations. No source merge conflicts occurred.

## New ROOT checkpoint

Production source is frozen at `a00d147a`. Fresh formal-O2 production build
`3991` exited 0; the log contains all 55 compilation entries. Normal repeat
`5e48a1` reported up-to-date and `dc98c1` verified 55/55 object signatures
and the production binary stamp against the new header set.
Log and planned verifier are under
`/tmp/dbms-order-metadata-arithmetic-combination.zJLGTGV3`.
`verify.sh` checks object signatures and the binary stamp before compiling
44 matching native tests or running 44 focused protocol scripts. Native
`16836` and protocol `33559` both exited 0: all 44 entries in each group
passed. Logs are `native.log` and `protocol.log`. Unchanged diagnostic
`24208` exited 1 with exactly five remaining genuine failures, preserving
the original SQLSTATE/rows/writes expectations. The two direct ORDER failures
are repaired, not removed from that diagnostic. Frozen binary SHA-256 is
`f30d15ff4ae72e1ab2bfbf66c8cf5a3f069d4640151861d29ed76378eb0e2ad5`.
This revision's full-default protocol `56926` reached terminal exit 1 with its
original 10-second socket timeout. It failed at the initial socket connect
(`tests/postgres_protocol_test.py:1330`, ConnectionAbortedError/errno 103),
before negotiation or any SQL assertion. The fixture discarded server output,
so the startup cause is not established. This is neither a full pass nor an
observed SQL semantic failure; the original log is retained as
`full-default-protocol.log`. Captured startup diagnosis is separate work.
The previous frozen `666bf0d1` production binary's 41 native/38 focused/full
default passes are recorded separately and are not reused as this revision's
proof. The previous unchanged diagnostic still had seven genuine failures;
the private ORDER candidate independently left five, not zero.

## Remaining actual failures and scope

Scalar-subquery WHERE/ORDER propagation, actual EXPLAIN ANALYZE execution,
computed-predicate arithmetic UPDATE and procedural whole-query metadata
preparation remain independent work. Plain table EXTRACT still has an actual
retained empty-value failure; CAST(EXTRACT)'s success does not replace it.
A further actual prepared-protocol diagnostic returned OID23 instead of20
for quoted BIGINT `"F"+f`, despite correct Simple Query value/OID20. This
Describe identity loss remains scheduled separately with its original type
expectation, not counted as passing by the arithmetic bridge.
An aggregate-arithmetic compatibility probe compared 26 queries on the old
immutable API2 binary and the new arithmetic candidate (`43114`, exit 0).
They agreed, so it did not establish a new bridge regression. It did retain
four existing actual failures: empty aggregate arithmetic returned zero rows
instead of a singleton 0/NULL; all-NULL SUM+1 returned 1 instead of NULL;
filtered SUM+1 ignored the filter; CAST(SUM(...) AS BIGINT)+1 returned 42883.
These are not supported green aggregate-scope claims and remain independent
work with their original singleton/NULL/filter/cast expectations.

A separate unchanged registered `typed_group_key` gate actually passed on
the old formal `666bf0d1`/b418 binary (`25836`, exit 0) and failed on the new
formal `a00d147a`/f30 binary (`35286`, exit 1): ORDER BY count(*) reported
42883. Unlike the 26-query comparison above, this establishes an introduced
aggregate-role regression. Independent repair is now ROOT `3983dccb`; its
private fresh-56 development build passed the original gate and expanded
controls. New ROOT optimized combination verification is recorded separately
in `docs/issue-prepared-and-projection-combination.md`, not attributed to f30.

Further actual quoted-range diagnostic `79860` on the private metadata
binary rejected legal `AS "M" ... fn("M".id)` and `AS "m" ... fn(m.id)`
with 42P01, but accepted illegal `AS m ... fn("M".id)`. PostgreSQL 17.2
transactional controls confirmed all three original expectations. Canonical
source/range binding is independently being repaired; exact row datum binding
does not imply correct range namespace lookup.

These changes do not supply all query scopes, operator/type overloads,
correlated/subplan execution, aggregate/window arithmetic, ARRAY semantics,
DDL dependencies, protocol or transactional behavior in the original audit.
No all-family or full-suite completion is inferred from the dedicated tests.
Totals remain 273: 22 complete, 166 partial, 70 unverified and 15 deferred.
No push; Actions remain disabled; skipped security/TDE work remains deferred.
