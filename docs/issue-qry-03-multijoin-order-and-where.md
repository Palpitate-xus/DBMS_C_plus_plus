# QRY-03 — Multi-table join chain order and WHERE filtering

Status: partial. The source/test fixes are local commits `4534b971` and
`b87d4a19` (2026-10-05).

## Reproduced behavior

- The multi-table branch greedily reordered every join and passed `"inner"` to
  the executor, regardless of the authored `JoinLink.type`. This discarded
  rows preserved by a later LEFT/RIGHT/FULL join. A three-table
  `LEFT JOIN ... LEFT JOIN ...` case omitted the unmatched first-table row.
- A chain containing only CROSS JOINs was rejected as requiring ON clauses.
- After assembling a multi-table join, the branch printed and returned before
  evaluating the query's WHERE clause. On the outer-join fixture,
  `WHERE a.id = 2` returned both rows instead of the single matching row.

## Fixes

- If a chain contains an outer join, execute it left-associatively in authored
  order and use each link's actual join type. Inner-only chains retain the
  existing greedy choice. CROSS-only chains can execute without ON predicates.
- Preserve structured cells, SQL NULL bitmaps, and source column types through
  intermediate materialization. Bind WHERE references against the participating
  relation aliases/schema, then evaluate the predicate on the completed join
  result so outer-join NULL extension happens first.

## Verification

- `scripts/build.sh` passed.
- `tests/join_type_protocol_e2e_test.py` passed. Added LEFT/RIGHT/FULL chain,
  three-way CROSS JOIN, post-join `WHERE`, and `IS NULL` over a NULL-extended
  row. The new WHERE assertions failed before the fix by returning both rows.
- `tests/multijoin_e2e_test.py` passed (three-table chain, reordered inner join,
  and four-table chain).
- The full registered suite and PostgreSQL 18.6 differential were not run.

This does not complete QRY-03 or OPT-02. The multi-table branch still does not
implement the general SELECT target list and ordering/grouping clauses; its
protocol row-description metadata is also not yet proven type-correct. OPT-02's
DP/exhaustive join search, GEQO threshold, semi/anti constraints, and bushy plans
remain unimplemented.
