# CASE projection labels inherited from ELSE

CASE inherits a strong column or function name from its ELSE expression.
The pure projection helper previously returned `case` unconditionally, so
prepared RowDescriptions and CTE output names disagreed with PostgreSQL even
when rows and types were correct. The correction follows the original ELSE
AST recursively, before binding or execution.

## Reference and actual failures

The isolated reference is PostgreSQL 18.6 with the strict `180006` version
gate. Its parser gives CASE a strong ELSE-derived name; when ELSE has only
a weak type name or no name, the fallback remains `case`.
[PostgreSQL 18.6 target label implementation](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_target.c).

| Expression shape | Correct label | Original label |
| --- | --- | --- |
| ELSE `t.e` | `e` | `case` |
| ELSE quoted `t."E"` | `E` | `case` |
| ELSE `abs(2)` | `abs` | `case` |
| CAST of CASE whose ELSE is `abs(2)` | `abs` | `int8` |
| ELSE CAST of a source column | Original column name | `case` |
| ELSE nested CASE with a strong ELSE column | Original column name | `case` |

The dedicated native baseline failed six assertions with exit 134. Its wire
baseline independently failed six label assertions, while values and OIDs
were already correct. The weak ELSE CAST and explicit quoted alias controls
passed on both sides and remain unchanged.

## Pure correction and verification

`projectionLabel` now preserves ELSE label strength when it exceeds one.
Nested CASE, CAST and qualified/quoted column roles retain their original
identities. No ELSE value, function or source row is executed to choose the
name. The result is independent of which branch planning subsequently prunes.

Artifacts are under `/tmp/dbms-case-else-label.UELchklx`:

- `candidate.protocol.log` and the strict reference
  `/tmp/dbms-simple-case-binding.KtyWx31T/case-else-label.reference18.log`
  are terminal successes for all eight permanent controls.
- `build.log` contains six successful fresh O2 native tests, including the
  new label control, original projection labels and genuine source contexts.
- `ordinary.protocol.log` retains the complete ordinary CASE/source/demand
  matrix and now reports the correct `e` label for the NULL/empty view case.
  `adjacent-label.protocol.log` retains the original CTE label controls.
- `asan.log` contains two successful native controls with scoped query
  binding, prepared execution and execution-plan ASan/UBSan instrumentation.

The immutable all-58 header/layout donor is
`/tmp/dbms-simple-case-binding.KtyWx31T/sources`; current source signatures
match the verified `source-final-v3` artifact except for the freshly compiled
query-binding correction. Include paths and compiler flags match the frozen
donor exactly, fresh stubs are used, and all source/header audits pass.
Final binary SHA256 is
`0c94d566c27e2d34f79ac91daff9b23f9f7d689c81487ab4b9f3ce1e98f83f88`.

This is a projection-name correction, not closure of all prepared metadata,
CASE or operator families. The existing wide literal and geometric comparison
diagnostics remain independent follow-ups.
