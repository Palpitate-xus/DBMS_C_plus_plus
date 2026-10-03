# Issue 962 — FETCH WITH TIES requires ORDER BY

## Finding

The outer-query compatibility rewrite rejected every `FETCH ... WITH TIES` as unsupported (`0A000`). PostgreSQL instead reports syntax error `42601` when `WITH TIES` is used without a query-level `ORDER BY`.

## Change

Commit `36b73cd5` checks for a top-level `ORDER BY` before treating `WITH TIES` as a supported-but-unimplemented case. Without one, it now returns `42601`; with one, this execution path remains explicitly unsupported and returns `0A000` instead of producing incorrect rows.

The focused wire test covers both error boundaries, confirms the connection remains usable after either error, and exercises neighboring set-operation `FETCH ONLY` plus `OFFSET ... FETCH` cases. The no-ORDER-BY invalid query is included in the actual-compatibility case file.

## Verification

- `bash scripts/build.sh` — passed.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py` — passed.
- PostgreSQL 18.6 direct oracle: `server_version_num=180006`; `SELECT 1 UNION ALL SELECT 1 FETCH FIRST 1 ROW WITH TIES` returns SQLSTATE `42601`.
- The generic compatibility runner still cannot run against PostgreSQL 18.6 because its configured Docker `pgref` is 17.2; the strict preflight was not bypassed.
- Full C++/E2E/actual suites were not rerun after this change.

QRY-06 remains `partial`. Correct execution of valid `FETCH ... WITH TIES` on set operations is still unsupported. No push was performed.
