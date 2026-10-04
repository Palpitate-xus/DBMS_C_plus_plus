# Issue 995: typed `pg_stat_activity` subset and query-text privacy

## Finding

The old virtual-view renderer printed five whitespace-separated values for
every backend, regardless of the caller's projection or `WHERE` clause. The
wire result therefore did not have trustworthy column types, and ordinary
users could read other users' active SQL. `ProcessInfo` also kept no last
statement for idle sessions, while the Extended Query Describe path did not
describe this view's columns.

The privacy defect was reproduced through PostgreSQL wire connections: Alice
held a row lock, `act_victim` blocked on
`UPDATE /*PRIVATE_ACTIVITY_MARKER*/ activity_secret ...`, and unrelated
ordinary user `act_monitor` queried `pg_catalog.pg_stat_activity`. The old
implementation exposed the marker in the victim's SQL text.

PostgreSQL 18 documents 22 `pg_stat_activity` columns. Its monitoring-view
rules allow ordinary users to see session metadata, while query text and other
statistics are restricted to the same user or roles with elevated monitoring
privileges. See the [PostgreSQL 18 monitoring statistics documentation](https://www.postgresql.org/docs/18/monitoring-stats.html).

## Change

The virtual view now offers a deliberately limited typed subset:

| Column | PostgreSQL type | Wire OID |
| --- | --- | ---: |
| `pid` | `integer` | 23 |
| `datname` | `name` | 19 |
| `usename` | `name` | 19 |
| `state` | `text` | 25 |
| `query` | `text` | 25 |

Supported direct-column projections, simple boolean predicates, `LIMIT`, and
`OFFSET` return rows from the live process list. Explicit `pg_catalog` lookup
uses the virtual view; an unqualified same-named physical table or view keeps
ordinary relation resolution. `SELECT *` and recognized PostgreSQL 18 columns
outside this subset fail with `0A000`; genuinely unknown columns remain
`42703`.

Query text is SQL NULL to unrelated ordinary users and visible to the session's
own user, a superuser, or a member (including inherited membership) of
`pg_read_all_stats`. Session state now reports `active`, `idle`, `idle in
transaction`, or `idle in transaction (aborted)` where available, and retains
the last query for idle backends. Extended Describe reports the same typed
column metadata as execution.

## Verification

- `bash scripts/build.sh` — passed.
- `python3 tests/pg_stat_activity_protocol_e2e_test.py` — passed. Covers Simple
  and Extended metadata, projection/filtering, unsupported/unknown columns,
  concurrent lock-wait privacy, self/superuser/`pg_read_all_stats` visibility,
  and same-name table resolution.
- `python3 tests/pg_settings_query_protocol_e2e_test.py` — passed.
- `python3 tests/postgres_protocol_test.py` — passed.
- `python3 tests/div14_feature_gate_test.py` — passed.
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py` — passed.
- `python3 -m py_compile tests/pg_stat_activity_protocol_e2e_test.py`,
  `bash -n scripts/build_common.sh`, and `git diff --check` — passed.

The production/test change is local commit `9b3a7272`; follow-up own-query
visibility coverage is `fb01e1ec`. No full registered test-suite run or
PostgreSQL 18.6 runtime differential is claimed.

## Remaining scope

This does not complete MON-04 or the full 22-column view. Fields such as
`query_id`, transaction/query timestamps, wait events, client address/port,
leader PID, backend type, statistics snapshot behavior, and the full PostgreSQL
expression/order semantics remain unimplemented. MON-04 is therefore partial,
not complete. Nothing was pushed; GitHub Actions remain disabled, and the
user-deferred security/TDE items remain deferred.
