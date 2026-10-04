# Issue 994 — Typed `pg_settings` subset

## Finding

The legacy virtual `pg_settings` renderer always printed the same three text
columns, regardless of the SQL projection or predicate. It therefore made a
partial view look complete and could return rows that did not satisfy `WHERE`.
PostgreSQL 18 documents 17 columns for this view; the implementation has
reliable values only for `name`, `setting`, and `unit`.

## Change

Commit `d4ab5852` replaces the legacy renderer with a typed three-column
execution path. It evaluates supported `WHERE` expressions and applies
`LIMIT`/`OFFSET`; empty units are transmitted as SQL `NULL`. `SELECT *`, known
but unimplemented PostgreSQL 18 columns, and unsupported query shapes fail
closed with `0A000`; genuinely unknown columns retain `42703`. It does not
claim to implement all settings rows or the remaining view schema, so CAT-03
remains partial.

Queries for `pg_catalog.pg_settings` use the virtual view. An unqualified
`pg_settings` resolves to the virtual view only when there is no same-named
physical table or view. Extended-protocol `Describe` now publishes the same
supported column names and text OIDs as execution; it does not advertise the
partial schema for `SELECT *`. Existing settings-value checks now request the
three supported columns explicitly; the manual example was updated as well.

## Verification

- `bash scripts/build.sh` — passed.
- `python3 tests/pg_settings_query_protocol_e2e_test.py` — passed in Simple
  and Extended Query modes; covers row descriptions/types, filtering, SQL
  NULL, unsupported and unknown columns, and same-name table/view resolution.
- `python3 tests/postgres_protocol_test.py` — passed.
- `python3 tests/div14_feature_gate_test.py` — passed.
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py` — passed.
- `python3 -m py_compile tests/pg_settings_query_protocol_e2e_test.py tests/postgres_protocol_test.py tests/div14_feature_gate_test.py` and `git diff --check` — passed.

The schema boundary was checked against the
[PostgreSQL 18 `pg_settings` documentation](https://www.postgresql.org/docs/18/view-pg-settings.html).
No PostgreSQL 18.6 runtime oracle/differential or full registered test suite is
claimed for this focused change. The change is committed locally; nothing was
pushed.
