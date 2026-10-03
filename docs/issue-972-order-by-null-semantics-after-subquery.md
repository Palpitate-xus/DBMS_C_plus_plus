# Issue 972: preserve ORDER BY NULL placement after a subquery filter

Date: 2026-10-03  
Source/test commit: `e5cc492b`  
Status: locally committed; not pushed. `QRY-10` remains partial.

Single-table plans retained only the physical sort column and direction in
`PlanContext`. When an uncorrelated semi/anti-join was lowered into that plan,
the explicit `NULLS FIRST/LAST` request was lost before `SortOp`. The sorter
then applied PostgreSQL's default placement. A separate ambiguity in
`SortOp` inferred NULL from an empty rendered value, conflating SQL NULL with
an empty text value.

`OrderBySpec` and `PlanContext` now carry whether null ordering was explicit
and which direction was requested. `SortOp` applies that policy and captures
the sort key's actual NULL state while the child row metadata is available.
Plans with explicit NULL placement skip the parallel GatherMerge branch,
which does not yet carry explicit NULL ordering; they use the correct serial
sort instead.

The new protocol test was run against the pre-fix binary and failed: the query
requested `ORDER BY a NULLS FIRST` but returned the SQL NULL row last.

Verification:

- Stable `bash scripts/build.sh`: passed.
- `tests/composite_not_in_order_by_nulls_protocol_e2e_test.py`: passed after
  initially failing against the prior binary. It covers explicit NULLS FIRST,
  explicit DESC NULLS LAST, default DESC NULLS FIRST, and empty text versus
  SQL NULL through subquery-filtered results.
- `tests/order_by_terminator_protocol_e2e_test.py` and
  `tests/table_structured_protocol_e2e_test.py`: passed.
- Direct read-only PostgreSQL 18.6 queries confirmed the expected row order.
- Full registered suite and the full differential runner have not yet been
  rerun after this fix.

This fixes the explicit NULL ordering path for the covered single-column
physical sort. Arbitrary ORDER BY expressions, complete multi-key planner
propagation, collations, stable tie behavior, and parallel/external sorting
remain in the broader `QRY-10` scope. The ledger remains 273 items: 22
complete, 140 partial, 96 unverified, and 15 deferred by user. The user-skipped
security/TDE audit remains deferred.
