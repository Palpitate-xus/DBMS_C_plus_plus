# Issue 959: parenthesized query expressions in the wire path

Status: partial. Scope: `QRY-06`, with `P0-02`, `P0-16`, and `PROTO-04` evidence.

## Reproduction

PostgreSQL 18.6 accepts query expressions that begin with parentheses, including a completely parenthesized SELECT and parenthesized set-operation operands. The old Simple Query path treated an opening parenthesis as an empty statement. A PG 18.6 oracle returned row `2` for:

```sql
(SELECT 1 UNION SELECT 2) INTERSECT (SELECT 2 UNION SELECT 3);
```

The old local wire path returned no row/command result. Parenthesized operands with local `ORDER BY`/`LIMIT` had the same routing problem. During regression expansion, `(SELECT 1)` also reproduced `42601`, and a trailing comment after a parenthesized operand reproduced `42601`. After routing was fixed, the first standalone-query implementation returned text OID 25 instead of PostgreSQL's int4 OID 23; that mismatch was preserved and corrected before acceptance.

## Change

Commit `3df0fef7` adds parenthesized-query handling across Simple Query statement splitting, parser command classification and snapshot detection, and the main query path. It strips only parentheses that enclose the complete query, recognizes trailing SQL trivia, and uses the existing protected-byte scanner so quotes and comments do not affect balance. Recursive execution captures typed result metadata at the right depth; parenthesized locking SELECTs retain the top-level statement transaction boundary. Set-operation operands now use the same unwrapping helper.

The structured wire regression covers precedence and associativity, nested parenthesized operands, a standalone parenthesized SELECT with int4 metadata, trailing operand comments, `UNION ALL` multiplicity, branch-local `ORDER BY`/`LIMIT`, and Extended Parse/Bind/Execute. The PG compatibility case is `tests/compat/cases/set_operation_precedence.sql`.

## Verification

- `bash scripts/build.sh`: passed on the committed source.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py`: passed after the final change.
- Full `tests/postgres_protocol_test.py` with 120-second socket/startup/shutdown budgets: passed after the final change.
- PG 18.6 `pg_diff_runner.py --only set_operation_precedence`: final seven-statement case passed, `cases=1 failed=0`; captured at `/tmp/dbms-setop-precedence-final-959.log`.
- Before the final shared-unwrapper helper and last two regression statements were added, the integration tree passed `scripts/build_tests.sh` (460 C++ tests and 197 E2E/protocol entries) and the full PG 18.6 actual suite (`cases=462 failed=0`). After the helper, the production build, focused E2E, full protocol test, and all seven statements in the PG 18.6 case were rerun. This does not claim a second full-suite run on the final source.

## Remaining boundary

`QRY-06` remains partial: arbitrary set-operation type resolution and collation rules, general expression-based final ordering/pagination, and complete Extended-protocol Describe metadata are not closed by this fix. This commit is local only; nothing was pushed, and GitHub Actions were not changed.
