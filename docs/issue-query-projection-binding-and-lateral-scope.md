# Query projection binding and LATERAL output scope

Status: partial. This records individual reproduced database-use bugs, not
completion of the PostgreSQL query/binding families. No push was performed;
GitHub Actions remain disabled and user-deferred security/TDE work is unchanged.

## Work ledger

| Issue | Observed failure | Source/test commit | Verification state |
| --- | --- | --- | --- |
| Quoted JOIN projection | `SELECT l."id", r."id"` returned two SQL NULLs and quoted header tokens instead of the stored keys | `bac2595a` | Final optimized binary: focused protocol test and adjacent JOIN tests passed |
| Scalar function ORDER BY | Sorting a scalar projection by `coalesce(id,fallback)` split `a b` into `a` and changed the text `NULL` to SQL NULL | `38d6b32c` | Final optimized binary: focused protocol and table-structured tests passed |
| Table CASE projection | LATERAL output CASE was mistaken for a function; a generic CASE result also lost the text `NULL`, and an untyped NULL arm advertised text for an integer result | `f933bf38` | Final optimized binary: focused protocol plus five related native tests passed |
| LATERAL output namespace | Bare outer `id` failed with 42703; star exposed synthetic leaf names/duplicate USING keys; chain and following JOIN binding were incomplete | `3fa5c71c` | Final optimized binary: expanded regression and full derived-type recheck passed |

## Mechanisms

Quoted JOIN projection references are now parsed into canonical identifier
components before matching physical relation/column names. Header derivation
uses the decoded AST column name. Quoting is not removed from literals or from
the evaluation SQL of general expressions.

Scalar function ORDER BY now uses the structured projected-row sorter. An
exact token match reuses a target cell; an input sort expression not projected
by SELECT is evaluated into a hidden typed-context cell. Hidden cells are
removed before publishing the result. Cells, SQL NULL flags, duplicate input
rows and row ordering no longer depend on parsing legacy display strings.

CASE work retains the table-backed projection AST and binds all branches. The
storage expression path evaluates a CASE as a complete expression before its
legacy boolean shortcuts inspect IS NULL/AND/OR. Type inference selects the
CASE value-arm types before applying standalone-NULL/text heuristics.

The LATERAL candidate distinguishes physical leaf cells from the logical
visible schema, retaining original qualified aliases across materialization.
FULL USING/NATURAL keys coalesce their source leaves while qualified leaf keys
retain independent NULLs. Final expression/star binding occurs after the
LATERAL chain, not by replacing every similarly spelled identifier globally.
Ordinary JOINs between/after LATERAL items must retain the same namespace.

## Verification boundaries

Focused tests inspect decoded values, SQL NULL bitmaps, column names/type OIDs,
command tags and expected binding error states in isolated protocol databases:

- `tests/join_quoted_projection_protocol_e2e_test.py`
- `tests/scalar_function_order_structured_protocol_e2e_test.py`
- `tests/table_case_ast_protocol_e2e_test.py`
- `tests/lateral_scope_protocol_e2e_test.py`
- CASE type assertions in `tests/constraint_expr_test.cpp`

Development probes reproduced the quoted-JOIN and scalar-order failures on the
preceding production binary. CASE probes reproduced both a function-dispatch
error in LATERAL output and loss of text `NULL` in a standalone table CASE.
Intermediate candidate regressions are not hidden: uppercase generated AS
tokens and ORDER BY spacing initially broke binding; converting every original
USING tree to ON also rejected a previously supported chained FULL USING input.
The latter regression was corrected by retaining original table USING/NATURAL
trees and lowering references only when an input is a cached materialization.
The complete derived-type development-binary recheck then passed, including
the previously supported chained FULL USING input.

An optimized-build attempt also caught a compile error in a CASE-prefix
optimization (`toLower` was not available in the storage translation unit).
It was changed to `SQLParser::toLower`; a fresh development build and the
table-CASE/LATERAL protocol tests then passed. The final optimized rebuild
subsequently passed; the failed attempt is not counted as a successful build.

The native development-object checks passed: `constraint_expr_test`,
`case_when_projection_default_test`, `expression_null_predicate_test`,
`null_predicate_projection_test`, and `coalesce_predicate_null_test`. They used
the matching development storage/expression objects and unchanged production
objects for the other sources, not a new whole-project native-cache rebuild.

## Final combined-source check

`bash scripts/build.sh` passed with TLS stub/plain TCP, zlib and ICU. A second
invocation verified the production binary was up to date. After the final
source fixes, all four independent source/test commits were present together;
the following tests passed against that final optimized binary:

- All four new focused protocol tests above.
- `derived_type_protocol_e2e_test.py` (complete script, including the original
  chained FULL USING regression), `join_type_protocol_e2e_test.py`.
- `sql_literal_preservation_e2e_test.py`, `table_structured_protocol_e2e_test.py`,
  `review_sql_e2e_test.py`, `multijoin_e2e_test.py`.
- `update_delete_from_protocol_e2e_test.py`, `dml_cte_protocol_e2e_test.py`.

The five native tests listed above were also recompiled/linked against the
matching optimized production objects (excluding main, with freshly compiled
test stubs) and all passed in isolated test directories. This checks the final
combined source; it is not a claim that each intermediate commit independently
passed a complete rebuild or full registered suite.

The complete default-configuration `postgres_protocol_test.py` is still running
at this documentation checkpoint. No result is inferred from its process being
alive. Full registered-suite and PG18.6 differential gates remain unverified.

The configured `pgref` container was checked on 2026-10-06 and reports
`server_version_num=170002`; it cannot prove PostgreSQL 18.6 compatibility. No
full registered-suite or PG18.6 differential result is claimed here.

General nested/correlated scopes, grouped FROM atoms, arbitrary expression and
operator binding, full LATERAL syntax (functions/VALUES/column alias lists),
parameterized planning, and all aggregate/window/locking combinations remain
outside the verified coverage. QRY-01/QRY-02/QRY-03 and SQL-04 remain partial.

The next open scope issue was reproduced separately on the final binary:
`WITH c AS (SELECT 1 AS id) SELECT c.id,x.n FROM c CROSS JOIN LATERAL
(SELECT c.id+1 AS n) x` returned 42P01, as did the equivalent derived-table
left input. Ordinary CTE/derived-table projections with the same qualified
`c.id` succeed and return integer 1, so this is a LATERAL composition failure,
not proof that CTEs themselves are unsupported. This failure is not closed by
the four commits above; its cause and fix remain the next work item.
