# Issue 966 — signed FETCH counts and SQLSTATE

## Finding

PostgreSQL 18 accepts an explicit positive sign for `LIMIT`, `OFFSET`, and `FETCH` counts, and treats `-0` as zero. A negative nonzero `FETCH ... WITH TIES` count reports SQLSTATE `2201W`. The `WITH TIES` rewrite accepted only unsigned digits, so it returned a syntax error for signed values; the parser also tokenizes unary `+` and `-` separately and did not consume those tokens for row counts.

## Change

The ordinary `WITH TIES` path now validates the complete signed decimal literal, reports `2201W` for a negative nonzero count, and canonicalizes `+n` and `-0` before the parser sees the rewritten query. `parseNonNegativeInteger` now consumes a separated sign token for `LIMIT`, `OFFSET`, and `FETCH`, accepts `+n` and negative zero, and continues to reject negative nonzero counts. Regression coverage includes wire queries, parser cases, and the PostgreSQL differential case.

## Verification

- Source commit: `9d07b4e7` (`fix(query): honor signed FETCH counts`).
- `bash scripts/build.sh` — passed after the final parser change.
- `tests/parser_phase1_test.cpp` — the first full-suite run exposed that the lexer splits `+5` into `+` and `5`; after adding sign-token consumption, the test was rebuilt and passed directly.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/fetch_clause_boundary_e2e_test.py` — passed after the final parser change.
- PostgreSQL 18.6 focused differential, `set_operation_precedence`: `cases=1 failed=0`.
- The full regression script ran all 460 C++ tests and all 197 E2E/protocol entries. Its only failure was `parser_phase1_test` against the initial sign-token implementation; that test passed when rebuilt and rerun after the correction. The full script was not rerun to a single all-green exit after that correction.
- Full PostgreSQL 18.6 differential completed 462 cases with one unrelated existing difference: `to_char_numeric`, where `to_char(482, 'L9999')` returned `$  482` in PostgreSQL and `   482` here. The FETCH case passed. Log: `/tmp/dbms-fetch-ties-966-full-pg18-en-us-prepared.log`.
- Python compilation and `git diff --check` — passed.

QRY-06 remains `partial`; this fixes signed row-count boundaries, not the whole query-compatibility family. No push was performed.
