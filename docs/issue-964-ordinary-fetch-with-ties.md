# Issue 964 — bounded ordinary SELECT FETCH WITH TIES

## Finding

After issue 963 enabled `FETCH ... WITH TIES` for set-operation results, valid ordinary table SELECTs still failed with `0A000`. The old SELECT executor also sliced ordered rows at the FETCH boundary without retaining peer rows.

## Change

Commit `bec759db` supports a deliberately bounded ordinary-query subset: a single non-catalog table, plain-column projections, plain projected-column `ORDER BY` keys, and an integer-literal `FETCH FIRST/NEXT n ROW(S) WITH TIES` count. `OFFSET` is applied before the fetch count. Rows tied at the boundary are retained using typed numeric, NULL, and collation-aware comparisons. `ASC` is now recognized explicitly by the legacy SELECT order parser (previously it could be included in the column name).

Queries whose shape cannot be evaluated faithfully by the structured-row path fail closed with `0A000`; missing `ORDER BY` remains `42601`. Joins, CTEs, aggregates, DISTINCT, windows, catalog relations, expression or hidden sort keys, parameterized/nonliteral counts, and broader query shapes remain unsupported.

## Verification

- `bash scripts/build.sh` — passed after the final source change.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py` — passed, including simple ties, numeric equivalence (`20`, `20.00`, `20.0`), aliases, OFFSET, multi-key ordering, NULL peers, fail-closed cases, and subsequent-query recovery.
- `python3 -m py_compile tests/set_operation_structured_protocol_e2e_test.py` and `git diff --check` — passed.
- PostgreSQL 18.6 direct oracle (`server_version_num=180006`) returned the same rows for the supported table-query shapes.
- The initial full `scripts/build_tests.sh` run on `bec759db` exposed one stale E2E assertion that expected ordinary SELECT `WITH TIES` to return `0A000`. Issue 965 updates that assertion and fixes typed peer equality; the final run on `aa27818d` passed all 460 C++ tests and 197 E2E/protocol entries.
- PostgreSQL 18.6 differential for `set_operation_precedence.sql` — `cases=1 failed=0`, using a fresh `en_US.utf8` database. A broader differential attempt was interrupted by a 120-second timeout on an unrelated DROP TABLE case; see issue 965 for the exact case and focused follow-up. No full PG18 differential pass is claimed.

QRY-06 remains `partial`; this implements only the subset above and does not close the broader SELECT/query-expression compatibility gap. No push was performed.
