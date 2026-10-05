# QRY-03 — Multi-table join chain order and WHERE filtering

Status: partial. The source/test fixes are local commits `4534b971`,
`b87d4a19`, `858d6da9`, `0a57fee1`, and `e042360b` (2026-10-05).

## Reproduced behavior

- The multi-table branch greedily reordered every join and passed `"inner"` to
  the executor, regardless of the authored `JoinLink.type`. This discarded
  rows preserved by a later LEFT/RIGHT/FULL join. A three-table
  `LEFT JOIN ... LEFT JOIN ...` case omitted the unmatched first-table row.
- A chain containing only CROSS JOINs was rejected as requiring ON clauses.
- After assembling a multi-table join, the branch printed and returned before
  evaluating the query's WHERE clause. On the outer-join fixture,
  `WHERE a.id = 2` returned both rows instead of the single matching row.
- The branch printed every intermediate column regardless of the SELECT target
  list and published no typed structured result. `SELECT a.id ...` returned all
  joined columns, and integer/text field metadata was not available to the
  protocol caller.
- It also returned rows in join/input order while silently ignoring ORDER BY,
  LIMIT, and OFFSET. `ORDER BY a.id DESC LIMIT 1` returned both rows.
- The FROM-chain parser stopped after the 12th JOIN without reporting an
  error. A 14-relation SELECT referencing the 14th alias incorrectly returned
  SQLSTATE `42P01` as though that relation were absent.

## Fixes

- If a chain contains an outer join, execute it left-associatively in authored
  order and use each link's actual join type. Inner-only chains retain the
  existing greedy choice. CROSS-only chains can execute without ON predicates.
- Preserve structured cells, SQL NULL bitmaps, and source column types through
  intermediate materialization. Bind WHERE references against the participating
  relation aliases/schema, then evaluate the predicate on the completed join
  result so outer-join NULL extension happens first.
- Project `*` in FROM order or supported simple column references (including
  aliases) in target-list order. Publish projected values, the NULL bitmap,
  output names/types, and command tag as a structured protocol result.
- Sort supported column-reference or output-position keys with ASC/DESC and
  NULLS FIRST/LAST, then apply LIMIT/OFFSET. Unsupported projections, sort
  expressions, DISTINCT/GROUP/WINDOW forms fail explicitly rather than being
  silently presented as completed queries.
- Remove the arbitrary 12-JOIN early exit so the parser and executor see the
  full authored relation chain.

## Verification

- `scripts/build.sh` passed.
- `tests/join_type_protocol_e2e_test.py` passed. Added LEFT/RIGHT/FULL chain,
  three-way CROSS JOIN, post-join `WHERE`, and `IS NULL` over a NULL-extended
  row; simple/reordered projections, aliases, duplicate output names, integer
  and text OIDs, DESC/LIMIT, NULLS FIRST, and output-position/OFFSET checks also
  pass. The new WHERE, projection, and ordering assertions each failed before
  their respective fixes.
- The same E2E now creates 14 one-row tables and joins all 14; it failed before
  the cap removal with `42P01`, then passed with the last relation projected
  and the correct integer OID/command tag.
- `tests/multijoin_e2e_test.py` passed (three-table chain, reordered inner join,
  and four-table chain).
- `tests/compat/cases/multijoin_projection_filter.sql` was added, but the
  differential runner refused preflight because the configured reference
  server reports PostgreSQL 17.2 (`170002`) while the runner requires 18.6
  (`180006`); the case did not execute against that reference.
- The full registered suite and PostgreSQL 18.6 differential were not run.

This does not complete QRY-03 or OPT-02. General target-list expressions,
qualified star expansion, DISTINCT/GROUP/HAVING/WINDOW, collation-aware and
arbitrary-expression ordering, and arbitrary nested/lateral join semantics
remain open. OPT-02's DP/exhaustive join search, GEQO threshold, semi/anti
constraints, and bushy plans remain unimplemented.
