# Integer constant widths in prepared CASE

Prepared CASE and VALUES now retain the static integer constant type selected by its range. Decimal constants use integer, bigint, or numeric rather than always integer. Lexical plus and minus signs are absorbed before choosing the type, including the minimum signed values and nested negation. Explicit casts, parameters, routines, and nonliteral unary expressions remain executable expressions.

## Actual failures

The native baseline fails the descriptor assertion for `2147483648`, which was integer instead of bigint. The expanded protocol baseline reports 92 assertion failures from one underlying binding defect: comparisons report 22003 for valid wide operands, and successful result expressions report OID 23 instead of 20 or 1700. For example, `CASE 2147483648 WHEN 2147483648 THEN 1 ELSE 2 END` should return 1, while a result constant `9223372036854775808` requires numeric metadata.

PostgreSQL 18.6 with the strict server version 180006 gate passes the unchanged queries. Its grammar absorbs the sign of an untyped constant before type commitment; this is distinct from folding an executable cast or arithmetic expression. See the official [operator type resolution documentation](https://www.postgresql.org/docs/18/typeconv-oper.html).

## Verification

Artifacts are under `/tmp/dbms-case-literal-width.RUPqi12c`.

| Control | Retained result |
| --- | --- |
| Native baseline | `baseline.native.log`, assertion failure for integer versus bigint |
| Expanded protocol baseline | `baseline.expanded.log`, 92 failed assertions |
| Strict PostgreSQL 18.6 reference | `reference18.final.log`, complete strengthened matrix passes |
| Candidate protocol | `candidate.final.log`, complete strengthened matrix passes |
| Seven fresh optimized native tests | `build.retry.log`, all pass |
| Ordinary CASE and genuine source demand | `ordinary.protocol.log`, full original source matrix passes |
| Adjacent CASE equality, constant demand, VALUES | `adjacent.protocol.log`, all pass |
| Scoped address and undefined behavior checks | `asan.log` and repeated execution, both native tests pass |

The protocol matrix retains SELECT, VALUES, zero-row WHERE, signed boundaries, double negation, result OIDs, incompatible TEXT, explicit CAST overflow, discarded overflow branches, and a writing CTE whose invalid operator must fail before a sequence effect.

The candidate reuses only the previously verified 58-object private source contract, with every header and other 57 source files compared against the immutable donor and identical flags. The binder and test stubs are fresh. Its SHA256 is `14eecde3b642d2e1346ef79abf4b8cef425e01ed514942f88300c72c5677888c`. Sanitizer instrumentation covers the binder, prepared expression execution, and execution plan translation units; the other matching objects are not instrumented and leak detection is disabled. This is not a fully sanitized production build or a full repository gate.

## Remaining scope

Executable unary overflow still has a legacy native `std::runtime_error` creation site containing SQLSTATE 22003. The first strengthened native run exposed that separate structured-error boundary; `build.log` retains the failure, and the native width control checks the exact existing overflow diagnostic while the protocol still requires 22003. This commit does not change that error class, geometric equality, custom operator lookup, or the complete CASE family. Later production integrations with changed public headers require a fresh matching 58-object build.
