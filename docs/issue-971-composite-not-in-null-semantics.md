# Issue 971: composite `NOT IN` row NULL semantics

Date: 2026-10-03  
Source/test commit: `f0317ce6`  
Status: locally committed; not pushed. `QRY-04` remains partial.

The simple semi/anti-join path accepted only one selected column and treated a
composite key as an ordinary hash key. That made row-valued `NOT IN` wrong in
the presence of NULL: it could keep rows whose comparison to an inner row was
UNKNOWN, even when a different non-NULL field proved the row unequal.

The parser now accepts matching simple-column row lists on both sides of a
subquery and records every field pair. The multi-column semi-join executor
compares each row pair with SQL three-valued row equality: any definite
inequality makes that pair FALSE; otherwise a NULL makes it UNKNOWN; only a
fully equal pair is TRUE. `NOT IN` excludes rows for either a TRUE match or an
`UNKNOWN` comparison, while an empty right-hand side keeps every outer row.
EXISTS correlation still treats UNKNOWN as a non-match in its WHERE semantics.
Child scan errors are propagated rather than reported as a successful result.

The previous binary was reproduced returning all four outer rows for
`(a,b) NOT IN (SELECT a,b ...)` with an inner `(3,NULL)` row. The PostgreSQL
18.6 local reference returns `TRUE`, `UNKNOWN`, `UNKNOWN`, and `TRUE` for the
corresponding row comparisons, including the empty-subquery case.

Verification:

- `bash scripts/build.sh`: passed after the executor/parser changes.
- `tests/composite_not_in_null_semantics_protocol_e2e_test.py`: passed. It
  covers `IN`, `NOT IN`, a definite match plus UNKNOWN pairs, and empty input.
- `tests/foreign_key_statement_visibility_protocol_e2e_test.py` and
  `tests/subquery_sqlstate_e2e_test.py`: passed.
- `tests/postgres_protocol_test.py`: passed.
- `git diff --check`: passed before commit.
- The full registered test suite has not yet been rerun after this change.
  The standard differential runner is also not yet verified against the local
  PG18.6 instance; its default `pgref` endpoint is PostgreSQL 17.2.

This closes only the reproduced multi-column `IN`/`NOT IN` row-comparison bug.
Other correlated subquery forms, expressions, coercions, and full SQL NULL
truth propagation remain in the broader `QRY-04` scope. The ledger remains 273
items: 22 complete, 140 partial, 96 unverified, and 15 deferred by user. The
user-skipped security/TDE audit remains deferred.
