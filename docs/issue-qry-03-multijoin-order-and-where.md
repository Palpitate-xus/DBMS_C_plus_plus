# QRY-03 — Multi-table join chain order and WHERE filtering

Status: partial. The source/test fixes are local commits `4534b971`,
`b87d4a19`, `858d6da9`, `0a57fee1`, `e042360b`, `856fe079`
(ON-conjunction fix, 2026-10-05), `65718c2b` (RIGHT/FULL residual coverage,
2026-10-05), `fd79875a` (USING/NATURAL output semantics, 2026-10-05), and
`3fef4f7d` (typed canonical JOIN keys, 2026-10-05), `124bc278`
(simple scalar expression projections, 2026-10-05), `1ef35962`
(evaluator-supported scalar functions, 2026-10-05), `de98eddb`
(simple CASE projections, 2026-10-05), `cd53df9d`
(ordering by a projected expression, 2026-10-05), `d7389c77`
(alias-qualified star expansion, 2026-10-05), and `93951ac3`
(multi-column star protocol coverage, 2026-10-05), plus `c7447f26`
(FROM-less LATERAL direct outer-column targets, 2026-10-05; QRY-02), and
`18fc748b` (bounded INNER/LEFT LATERAL `ON` conditions, 2026-10-05;
QRY-02/QRY-03), and `40ef739a` (FROM-less bare outer references in scalar
targets and WHERE, 2026-10-05; QRY-02/QRY-03), and `e63e4e1e`
(bare outer references beside a simple local FROM, 2026-10-05; QRY-02/QRY-03),
and `2080a994` (quoted outer relation-alias binding, 2026-10-05; QRY-02/QRY-03).
The follow-up regression commit `1a2e42b3` also verifies one mixed-case
quoted-column path without changing source.

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
- Multi-table target lists rejected even simple scalar expressions such as
  `a.id + b.val AS total` with SQLSTATE `0A000` instead of projecting one
  evaluated result per joined row.
- The same target-list path rejected evaluator-supported scalar calls such as
  `abs(a.id - b.val)` with SQLSTATE `0A000`.
- The multi-table projection binder also rejected CASE expressions even when
  every WHEN/THEN/ELSE reference belonged to the joined inputs.
- A multi-table target `b.*` was parsed as an ordinary column reference and
  failed with SQLSTATE `42703` instead of expanding the qualified relation's
  columns.
- It also returned rows in join/input order while silently ignoring ORDER BY,
  LIMIT, and OFFSET. `ORDER BY a.id DESC LIMIT 1` returned both rows.
- The FROM-chain parser stopped after the 12th JOIN without reporting an
  error. A 14-relation SELECT referencing the 14th alias incorrectly returned
  SQLSTATE `42P01` as though that relation were absent.
- In a multi-table chain, the parser split an `ON` expression at its first `=`
  and treated everything after it as a column name. A three-table query with
  `ON a.id = b.a_id AND b.val = 1` returned no rows instead of applying the
  second term as part of the join condition.
- A single JOIN with `USING (id)` reached the legacy path and failed with
  “missing ON clause”; a multi-link USING chain failed parsing with SQLSTATE
  `42601`. NATURAL joins had no merged SQL output schema and were rejected.
- Join execution used raw display strings as hash keys. A `NUMERIC` pair
  containing `1.0` and `1.00` therefore produced no match in both `USING` and
  explicit `ON`, even though the numeric values compare equal.
- `LEFT JOIN LATERAL (...) x ON ...` was not recognized by the lateral
  materializer and fell through to a missing-relation error; it also had no
  per-left-row ON filtering or SQL NULL-extension behavior.
- In a FROM-less lateral subquery, a bare outer column used inside a scalar
  expression or WHERE predicate was not bound; `id + 10` and `WHERE id < 3`
  both failed with SQLSTATE `42P01` instead of being evaluated for each left
  row.
- In a LATERAL subquery with a local base-table FROM, a bare outer column not
  present in the local schema was also unresolved. A correlated `payload`
  reference in the target expression/WHERE failed with SQLSTATE `42703`,
  despite qualified references such as `l.id` being supported.
- A quoted outer relation alias in a qualified LATERAL reference, such as
  `"OuterAlias".id`, was skipped by the previous text replacer and failed
  with SQLSTATE `42P01` even when the matching relation was in scope.

## Fixes

- If a chain contains an outer join, execute it left-associatively in authored
  order and use each link's actual join type. Inner-only chains retain the
  existing greedy choice. CROSS-only chains can execute without ON predicates.
- Preserve structured cells, SQL NULL bitmaps, and source column types through
  intermediate materialization. Bind WHERE references against the participating
  relation aliases/schema, then evaluate the predicate on the completed join
  result so outer-join NULL extension happens first.
- Project `*` in FROM order or supported simple column references and
  unary/binary/literal/cast scalar expressions (including aliases) in
  target-list order. Evaluate expressions after joins and WHERE against typed
  qualified/unambiguous row values with the SQL NULL bitmap. Publish projected
  values, the NULL bitmap, output names/types, and command tag as a structured
  protocol result.
- Evaluate evaluator-supported scalar function calls recursively against the
  same row context. Known aggregates, window calls, FILTER, ordered/named
  function arguments remain explicitly unsupported and fail closed rather
  than being evaluated independently for each row.
- Bind and evaluate simple CASE expressions by validating every switch,
  condition, result, and ELSE reference against the same joined-row context;
  result type inference and SQL NULL evaluation remain delegated to the
  expression evaluator.
- Expand a simple relation-alias-qualified `alias.*` in target-list order to
  the original schema columns from that relation. Keep its duplicate names,
  type metadata, and per-column NULL bitmap rather than applying unqualified
  JOIN USING merge rules.
- In a FROM-less LATERAL subquery, bind a direct bare-column target to the
  unique visible left input column, and bind unique bare outer columns inside
  evaluator-supported scalar target expressions and WHERE predicates by
  per-row, type-aware literals. Direct targets preserve their source type and
  alias; SQL NULL and quoted text are retained. This applies only when the
  lateral SELECT has no own FROM or has a simple local base-table scope whose
  columns are known. Local bare names retain precedence over outer names; a
  bare name absent locally may bind to a unique visible left input column.
  Unknown local scopes (CTE, derived subquery, or table function) are left
  untouched rather than guessed. The visible left input may be one base table
  or a simple comma/CROSS chain of base tables. A simple quoted relation alias
  followed by an unquoted or mixed-case quoted column is token-aware and
  regression-tested. More complex quoted/schema-qualified names and other join
  trees or arbitrary nested correlations remain unsupported or unverified.
- For one simple left base relation or a comma/CROSS chain of base relations,
  materialize `JOIN`/`INNER JOIN LATERAL` and `LEFT [OUTER] JOIN LATERAL` per
  combined left row, evaluate a supported boolean `ON` expression against
  typed, NULL-aware values from all left inputs and the lateral result, and
  apply LEFT NULL extension when no right row passes the condition. The left
  input chain must consist only of base tables joined with CROSS/comma and
  have no `ON`/`USING`; this path requires aligned structured rows from the
  lateral subquery and does not implement arbitrary join trees, complex or
  nested lateral chains, or table functions. A two-step FROM-less scalar
  LATERAL chain is covered by regression.
- Sort supported column-reference or output-position keys with ASC/DESC and
  NULLS FIRST/LAST, then apply LIMIT/OFFSET. Unsupported projections, sort
  expressions, DISTINCT/GROUP/WINDOW forms fail explicitly rather than being
  silently presented as completed queries.
- If an ORDER BY scalar expression textually matches a projected expression,
  reuse its precomputed values and inferred result type for sorting. General
  unprojected expressions and AST-equivalent-but-differently-written forms
  are still rejected rather than evaluated a second time.
- Remove the arbitrary 12-JOIN early exit so the parser and executor see the
  full authored relation chain.
- Split multi-table `ON` conjunctions, use one qualified cross-relation
  equality as the hash key, and carry supported remaining comparisons as
  storage-engine `ON` filters. Inner-chain residuals are applied only once
  their referenced relations are present; outer-chain residuals stay on their
  authored join edge and are evaluated before NULL extension. Unsupported
  residual expressions now fail explicitly instead of being silently ignored.
- For simple identifier lists, parse single and chained `USING`/`NATURAL`
  joins, build a logical output schema with merged columns first in SQL order,
  and retain original qualified base columns alongside hidden intermediate
  outputs. FULL joins coalesce the merged key from both sides; RIGHT joins use
  the right key; composite USING keys add equality filters after the hash key.
  NATURAL with no common columns uses Cartesian semantics, including outer
  preservation. The chain is kept in authored order when merged-key outputs
  are present. Quoted USING identifiers, arbitrary expressions, lateral or
  parameterized inputs, and general nested join trees are not implemented.
- Build inner/left/right join hash keys from the column type's canonical key
  representation rather than the display cell. This keeps values with equal
  typed representations (including differing NUMERIC display scales) in the
  same candidate bucket while preserving their original output cells. The
  INNER, LEFT, RIGHT, and FULL storage join paths use the same encoding.

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
- After the conjunction fix, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. The protocol E2E covers reordered inner joins with additional `ON`
  filters, a residual that cannot run until an earlier relation joins, and
  two-edge LEFT/RIGHT/FULL chains whose filtered edges must preserve
  NULL-extended rows on the correct side. The conjunction case returned no
  rows before the fix; the multi-join E2E also retains its three-table,
  reordered, and four-table cases.
- After `fd79875a`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. The protocol E2E covers single-pair and chained USING/NATURAL,
  merged `SELECT *` ordering, bare merged-key WHERE/projection, qualified
  `a.id`/`b.id` on FULL USING, composite keys with reordered USING lists,
  LEFT/RIGHT/FULL unmatched rows, and NATURAL outer joins with no common
  columns. Before the fix, single USING failed with missing ON and the chained
  USING case failed parsing with `42601`.
- After `3fef4f7d`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. A NUMERIC `USING` join of `1.0` to `1.00` returned zero rows before
  the fix and one row after it. Protocol assertions now cover that case for
  INNER/LEFT/RIGHT/FULL USING and explicit ON, including retained display
  values and numeric OIDs. This does not establish complete cross-type,
  timestamp, or collation-aware join equality.
- After `124bc278`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. A three-table arithmetic target list returned computed integer
  values with their alias and OID, and `ORDER BY 1` sorted those computed
  values; a second case confirmed arithmetic over a NULL-extended input remains
  NULL and can be sorted with `NULLS FIRST`. The former query failed with
  `0A000`. Function calls, CASE, SRFs, aggregates, and arbitrary expressions
  remain unsupported in this path.
- After `1ef35962`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. A three-table `abs(a.id - b.val)` projection returned the expected
  per-row integer values, alias, OID, and ordering; the `count(*)` negative
  control returned explicit `0A000` rather than a scalar result. CASE, SRFs,
  aggregates, and window expressions remain unsupported.
- After `de98eddb`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. A three-table CASE expression selected its THEN/ELSE branch per
  joined row, retained the alias and integer OID, and sorted by the computed
  output position; the query previously failed with `0A000`. This does not
  complete arbitrary CASE, SRF, aggregate, or window target-list semantics.
- After `cd53df9d`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. A three-table query ordered by the same arithmetic expression it
  projected now returns the expected descending order by the computed value;
  the previous behavior rejected it with `0A000`. This is not general
  ORDER-BY-expression or collation-aware ordering support.
- After `d7389c77`, `scripts/build.sh`,
  `tests/join_type_protocol_e2e_test.py`, and `tests/multijoin_e2e_test.py`
  passed. `SELECT b.*` on a LEFT JOIN chain now expands the right relation's
  column and returns NULL for the unmatched row; before the fix it failed with
  `42703`. Follow-up `93951ac3` adds a three-column expansion with INT/TEXT
  OIDs and a real NULL in the final field. General row expansion and
  schema-qualified star remain incomplete.
- After `18fc748b`, `scripts/build.sh`,
  `tests/derived_type_protocol_e2e_test.py`, and
  `tests/sql_literal_preservation_e2e_test.py` passed. The protocol test covers
  LEFT LATERAL `ON true`, LEFT per-row filtering where false/NULL conditions
  must preserve the left row with a typed SQL NULL, and INNER LATERAL filtering;
  the original LEFT LATERAL query failed before the fix with `42P01`. This is
  bounded to one simple left relation and evaluator-supported boolean `ON`
  expressions; the full suite and PostgreSQL 18.6 differential were not run.
- After `40ef739a`, `scripts/build.sh`,
  `tests/derived_type_protocol_e2e_test.py`, and
  `tests/sql_literal_preservation_e2e_test.py` passed. The regression covers
  `id + 10`, a text CAST/concatenation over empty, quoted, literal-`NULL`, and
  SQL-NULL values, `WHERE id < 3` true/false/NULL outcomes, and an all-filtered
  result that still reports typed columns. The former expression and WHERE
  cases failed with `42P01`. Correlation is limited to a single left relation
  and evaluator-supported expressions in a LATERAL SELECT with no own FROM;
  the full suite and PostgreSQL 18.6 differential were not run.
- After `e63e4e1e`, `scripts/build.sh`,
  `tests/derived_type_protocol_e2e_test.py`, and
  `tests/sql_literal_preservation_e2e_test.py` passed. A local `right` table
  keeps `r.id` in local scope while bare outer `payload` is evaluated in both
  `r.label || payload` and `WHERE payload <> 'skip'`; the previous query failed
  with `42703`. The helper only infers local columns from simple existing base
  tables and preserves local-name precedence; the full suite and PostgreSQL
  18.6 differential were not run.
- After `e7a9d086`, a comma/CROSS chain of simple base-table inputs can feed a
  LATERAL subquery. Qualified references from each input are bound per
  combined row, including evaluator-supported `INNER/LEFT JOIN LATERAL ON`
  predicates. Before the change, a two-table `CROSS JOIN ... CROSS JOIN
  LATERAL` query failed with `42601`. The protocol regression covers explicit
  CROSS, comma input, and an `ON` predicate referencing both left relations;
  `bash scripts/build.sh`, `tests/derived_type_protocol_e2e_test.py`, and
  `tests/sql_literal_preservation_e2e_test.py` passed. Non-CROSS left join
  trees, table functions, lateral chains, the full suite, and PostgreSQL 18.6
  differential remain unverified or unsupported.
- After `6123c5b3`, a two-step FROM-less scalar LATERAL chain no longer spins
  while `processDerivedTables` scans consecutive LATERAL subqueries, and the
  previous step's synthetic output column is carried into the next temp schema
  instead of being confused with that step's right column. Before the fix the
  query consumed a CPU core until protocol timeout; after cursor advancement
  alone it returned the wrong first-step value (`3` instead of `2`). The
  two-row regression now returns `1→2→3` and `2→3→4` with integer OIDs.
  `bash scripts/build.sh`, `tests/derived_type_protocol_e2e_test.py`, and
  `tests/sql_literal_preservation_e2e_test.py` passed. Longer/complex correlated
  chains, the full suite, and PostgreSQL 18.6 differential remain unverified.
- After `2080a994`, a simple quoted outer relation alias such as
  `"OuterAlias".id` is recognized while binding a LATERAL reference. The
  pre-fix query failed with `42P01`; the token-aware scanner now skips SQL
  strings and comments while matching identifier paths. `bash scripts/build.sh`,
  `tests/derived_type_protocol_e2e_test.py`, and
  `tests/sql_literal_preservation_e2e_test.py` passed. The regression verifies
  the quoted alias with an unquoted column and typed protocol output; quoted
  column names, broader schema-qualified forms, full suite, and PostgreSQL
  18.6 differential remain unverified.
- Follow-up test commit `1a2e42b3` adds a mixed-case quoted outer column,
  `"OuterAlias"."MixedId"`, in both a FROM-less LATERAL scalar target and the
  outer WHERE. The existing production binary built from `2080a994` returns the
  expected rows, `MixedId`/`next_id` headers, and integer OIDs;
  `python3 -m py_compile tests/derived_type_protocol_e2e_test.py` and
  `tests/derived_type_protocol_e2e_test.py` passed. This is bounded test
  coverage, not general quoted-name or schema-qualified correlation support.
- `tests/compat/cases/multijoin_projection_filter.sql` was added, but the
  differential runner refused preflight because the configured reference
  server reports PostgreSQL 17.2 (`170002`) while the runner requires 18.6
  (`180006`); the case did not execute against that reference.
- The full registered suite and PostgreSQL 18.6 differential were not run.

This does not complete QRY-03 or OPT-02. General target-list expressions
(including SRFs, aggregates, and window expressions; only simple CASE and
evaluator-supported scalar calls are handled), general row expansion beyond
simple `alias.*`, DISTINCT/GROUP/HAVING/WINDOW, collation-aware and arbitrary-
expression ordering, and arbitrary nested/lateral join semantics remain open.
The FROM-less LATERAL direct-column target case is only a bounded QRY-02 fix;
simple INNER/LEFT LATERAL `ON` evaluation works with either one simple left
relation or a simple base-table comma/CROSS chain before the LATERAL item.
The regression-tested two-step FROM-less scalar LATERAL chain also works;
longer or more complex correlated chains remain unsupported/unverified.
Bare outer references in evaluator-supported scalar targets and WHERE are now
also bound when that LATERAL SELECT has no own FROM or uses a local base-table
scope with known columns. Other left join trees remain unsupported. Unknown
CTE/derived/function FROM scopes, complex quoted/schema-qualified references, arbitrary correlated
expressions/subqueries, complex lateral chains, table functions, and general
parameterized or nested join semantics remain unsupported or unverified. A
simple quoted outer relation alias with an unquoted or one mixed-case quoted
column is covered by regression tests.
OPT-02's DP/exhaustive join search, GEQO threshold, semi/anti constraints, and
bushy plans remain unimplemented.
