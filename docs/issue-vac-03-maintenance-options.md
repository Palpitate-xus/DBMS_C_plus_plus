# VAC-03 — VACUUM option handling and remaining rewrite semantics

Status: partial. Review and focused repair recorded on 2026-10-05.

## Reproduced behavior

- With automatic analyze disabled, a populated table initially retained the default planner estimate. `VACUUM (ANALYZE)` was accepted but followed the ordinary vacuum path, leaving both persisted row statistics and the EXPLAIN estimate unchanged. The regression was first run against the old production binary and failed because no statistics file was produced.
- Source inspection showed that the parenthesized `FULL` option was accepted but not routed to the table rewrite, and `FREEZE` was accepted without freezing tuples.
- VACUUM from an explicit transaction block was not rejected at the SQL command boundary; a failed `VACUUM FULL` also lacked a success result distinct from a valid rewrite of zero rows.

PostgreSQL 18 documents `ANALYZE` as collecting planner statistics, `FULL` as rewriting the table, and `FREEZE` as an aggressive freezing operation. The project does not implement all of those semantics; see the [PostgreSQL 18 VACUUM reference](https://www.postgresql.org/docs/18/sql-vacuum.html).

## Change

Source/test commit `09611ff0`:

- Parse parenthesized and legacy `FULL`, `ANALYZE`, `VERBOSE`, `FREEZE`, `CONCURRENTLY`, and `PARALLEL` options rather than silently discarding recognized words. Boolean options accept explicit values, including whitespace around `=`.
- Run the actual table analyze path when requested; route `FULL` to the rewrite path and distinguish rewrite failure from a successful zero-row rewrite.
- Reject unsupported `FREEZE` with SQLSTATE `0A000`, `VACUUM` inside explicit transaction blocks with `25001`, and a nonexistent named relation with `42P01`.
- Add a protocol regression for both ANALYZE spellings, persisted statistics and planner estimates, `ANALYZE = true`, explicit unsupported/transaction errors, and FULL heap shrink while preserving the remaining row.

## Verification

- `scripts/build.sh` — passed.
- `python3 tests/legacy_forced_analyze_protocol_e2e_test.py` — passed, including the new regression.
- `scripts/build_one_test.sh vacuum_full_test` — passed: heap/NULL/TOAST/index preservation, partition rewrite, backup failure preservation, and mid-rewrite restoration.
- `git diff --check` — passed.
- Full registered test suite and PostgreSQL 18.6 differential were not run for this change.

## Remaining work

- `VACUUM FULL` still uses the existing backup-based rewrite; it is not a proven crash-atomic transactional relation swap. Crash windows and concurrent-reader behavior remain open.
- Tuple freezing, visibility/freeze map semantics, failsafe/wraparound handling, and `VACUUM FREEZE` are not implemented. `FREEZE` now fails explicitly instead of returning false success.
- VACUUM progress views and PostgreSQL-compatible VERBOSE counters are not implemented.

Therefore VAC-03 remains partial and unchecked in the overview rather than being marked complete. Security/TDE review remains deferred as requested; no GitHub Actions were enabled and no push was performed.
