# Issue 963 — FETCH WITH TIES on set-operation results

## Finding

The outer-query rewrite rejected valid `FETCH FIRST/NEXT ... WITH TIES` on `UNION`, `INTERSECT`, and `EXCEPT` query expressions. Applying the row limit to the right operand would also be semantically wrong: the clause belongs to the complete set-operation result.

## Change

Commit `258bd554` preserves a valid set-operation `WITH TIES` tail through SQL preprocessing and applies it after combining and sorting all operand rows. `OFFSET n ROW(S)` is parsed before the fetch limit. Once the first `n` rows are selected, additional rows are retained while every sort key is equal to the boundary row; peer comparison respects SQL NULLs, numeric equality, and text collation. The command tag reports the final number of returned rows.

Parenthesized set expressions are routed through the same result-tail handling. The compatibility and wire regressions cover ordinary ties, parenthesized ties, offset-before-fetch, numerically equal `1`/`1.00` values, and NULL peers using `NULLS FIRST`.

## Verification

- `bash scripts/build.sh` — passed after the final source change.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py` — passed.
- PostgreSQL 18.6 direct oracle: `server_version_num=180006`; each added query returned the same rows as the wire regression, and `WITH TIES` without `ORDER BY` returned SQLSTATE `42601`.
- The generic compatibility runner's Docker reference is PostgreSQL 17.2 and fails its strict 18.6 preflight; it was not bypassed or counted as a PG18 differential run.
- The full C++/E2E/actual suite was not rerun after this change.

QRY-06 remains `partial`. Valid `FETCH WITH TIES` for non-set SELECTs is still unsupported in this legacy outer-query path, along with other broader type-resolution and protocol metadata gaps. No push was performed.
