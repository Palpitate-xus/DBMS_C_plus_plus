# Issue 961 — Ambiguous output names in set-operation ORDER BY

## Finding

When the first SELECT of a set operation exposed duplicate output names, an `ORDER BY` reference to that name was silently bound to the first column. PostgreSQL rejects this as ambiguous (`42702`); positional ordinals remain unambiguous.

## Change

Commit `9d095496` distinguishes quoted names from folded unquoted names when resolving a set-operation sort key, unescapes doubled quotes for quoted names, and counts matching output columns. More than one match now returns SQLSTATE `42702`; a unique name and a valid ordinal continue through the multi-key ordering path.

The structured wire regression covers `SELECT 1 AS x, 2 AS x UNION ALL SELECT 3, 4 ORDER BY x`, checks `42702`, then verifies that a following statement on the same connection still succeeds. The same SQL is in the PostgreSQL actual-compatibility case.

## Verification

- `bash scripts/build.sh` — passed.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py` — passed.
- PostgreSQL 18.6 direct oracle returned `server_version_num=180006` and SQLSTATE `42702` for the same query.
- The generic compatibility runner's configured Docker reference is PostgreSQL 17.2 and fails its strict 18.6 preflight; it is not counted as an actual-differential pass.
- Full C++/E2E/actual suites were not rerun for this change.

QRY-06 remains `partial`; this closes only duplicate-name ambiguity in the set-operation sort-key lookup. No push was performed.
