# Issue 960 — Multiple ORDER BY keys after set operations

## Finding

The query-expression tail handler accepted one sort key after `UNION`, `INTERSECT`, or `EXCEPT`. PostgreSQL permits a comma-separated list of output column names or ordinals, so valid multi-key ordering was rejected or incompletely handled. This is one narrow part of QRY-06, not a claim that set-operation semantics are complete.

## Change

Commit `439a8ef7` parses a comma-separated set-operation ORDER BY list without treating commas inside quoted/commented bytes or nested delimiters as separators. Each supported key resolves to an output name or ordinal, and the stable comparator evaluates keys in sequence with each output column's numeric/string comparison behavior, explicit collation, direction, and NULL ordering. ASC defaults to NULLS LAST and DESC to NULLS FIRST. Direction and NULLS keywords are case-insensitive.

Regression coverage orders by two output names, DESC/ASC direction, and explicit NULLS FIRST, and checks returned headers, OIDs, rows, and command tag over the PostgreSQL wire protocol. The PostgreSQL compatibility case contains the same query.

## Verification

- `bash scripts/build.sh` — passed; this environment uses the TLS stub because OpenSSL was not detected.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py` — passed.
- PostgreSQL 18.6 direct oracle: `SHOW server_version_num` returned `180006`; the exact regression query returned `(1,NULL)`, `(1,'a')`, `(1,'b')`, `(0,'z')`, matching the local wire regression.
- The generic `pg_diff_runner.py --only set_operation_precedence` preflight was attempted but correctly rejected the configured Docker `pgref` because it is PostgreSQL 17.2. That attempt is not counted as a passing PG18 differential run.
- The complete C++/E2E/actual suite was not rerun after this change.

QRY-06 remains `partial`: set-operation type/collation resolution, more of the query-expression grammar, and Extended Describe metadata still need separate work. No push was performed.
