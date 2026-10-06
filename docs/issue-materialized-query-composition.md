# CTE, derived-table and LATERAL materialization composition

Status: partial. This is an issue-level record, not completion of SQL-04 or
QRY-01/02/03. No push; Actions remain disabled; user-deferred security/TDE
items remain deferred.

## Independently reproduced failures

| Failure | Cause | Source/test commit |
| --- | --- | --- |
| A constant CTE or derived source followed by LATERAL returned 42P01 | Independent counters all tried to create the live `__cte_0` relation | `98f9c978` |
| A filtered-empty CTE/derived source followed by LATERAL returned `function lateral(unknown) does not exist` | Protocol precheck scanned from an inner WHERE through the remainder of the outer statement | `514b4caf` |
| Qualified derived `d.id` became ambiguous when a right source also exposed id; `d.*` included the right source and duplicated output | Derived rewriting removed every alias qualifier; CTE rewriting also stripped qualifiers before stars/quoted columns | `f2feaaea` |
| An unrelated composition query failed when `__cte_0` was an unpopulated materialized view | Allocator lookup incorrectly applied the view scan-access gate | `4b2ff4f6` |
| Unknown function analysis on an unpopulated materialized view disconnected the client | Type lookup threw outside the protocol exception boundary and applied scan gating during analysis | `5928ff7a` |

The first regression fails on the preceding development binary with 42P01.
After the allocator fix, all non-empty composition cases and a physically empty
input passed; the filtered-empty regression still failed with 42883. That
failure was retained in a separately registered protocol test rather than
removed. The alias regression failed on the preceding optimized binary with
42702; the candidate passed it and ordinary derived/CTE WHERE, GROUP BY, JOIN
and qualified-star controls.

## Implemented mechanisms

The shared materializer advances past active internal relations, session-owned
temporary relations and visible user tables/views. This prevents collision
between nested/sibling materializers and prevents internal names from shadowing
the user's objects. Existing row values, NULL flags and type metadata stay
structured; cleanup retains parent-owned relations.

The WHERE precheck uses the shared SQL tokenizer to delimit each predicate by
its query depth and following clauses, then visits actual function-call AST
nodes. Strings, dollar bodies and comments are not function-call tokens. The
existing bounded built-in list remains; this is not a complete PostgreSQL
function resolver. Unknown functions are still rejected on empty input and in
an unreachable CASE arm, and failed UPDATE/DELETE leave all source rows intact.

Derived factors retain `AS alias` instead of erasing qualified column references
throughout the statement. CTE stars and quoted column references retain their
qualifiers when the CTE name is mapped to its materialized source. Duplicate
column names from independent qualified sources remain independently bound;
the same bare name is still an ambiguity error.

## Verification at the three-commit combination

`bash scripts/build.sh` passed (optimized TLS-stub/plain-TCP, zlib and ICU).
All three new focused protocol scripts passed on that optimized binary:

- `materialized_lateral_composition_protocol_e2e_test.py`
- `where_function_scope_protocol_e2e_test.py`
- `materialized_alias_scope_protocol_e2e_test.py`

The composition test covers sibling CTEs, consecutive LATERAL items, LEFT NULL
extension, a physically empty source, and preservation of a user's colliding
temporary/public table. The WHERE test covers filtered-empty and filtered
non-empty composition, literal call-shaped text, empty-table/unreachable-arm
function errors and unchanged rows after rejected UPDATE/DELETE. The alias
test covers independent same-name qualified columns, repeated/reordered stars,
ambiguous bare columns, exact spaces/empty text/text NULL/SQL NULL, mixed-case
quoted columns, ordinary projection/WHERE/GROUP/JOIN and typed empty output.

At this combination, production CTE-boundary, subquery-SQLSTATE and nested
comment E2E passed. Matching development builds passed the complete derived-type,
JOIN-type, LATERAL-scope, SQL-literal, boolean-literal, source UPDATE/DELETE and
DML-CTE scripts. The complete default `postgres_protocol_test.py` also passed
on the development combination. These development checks are not described as
optimized-build or full registered-suite checks.

## Open boundaries and follow-up evidence

Extended probes found an unpopulated materialized view named `__cte_0` could
make an unrelated CTE/LATERAL query fail with 55000 during name allocation.
They also found WHERE function type analysis could throw the same metadata
error outside the protocol exception boundary and disconnect the client.
These are closed by the two separately committed follow-ups above, not by the
initial three commits. Their expanded regressions failed on the preceding
optimized binary (55000 for the unrelated query; connection closed while
reading a PostgreSQL message for the function query). Both expanded scripts,
the materialized-view refresh E2E and alias-scope script passed on the matching
development combination. Final optimized build passed and a repeat build
reported up to date. All sixteen focused/adjacent/default protocol entry points
below have passed on that final binary.

Allocation treats the resolver's unpopulated-view error as an occupied name,
without suppressing the actual view scan gate. Tests preserve populated and
unpopulated views in public and a non-public search path. WHERE type analysis
uses the unpopulated view's backing schema without opening a scan. Qualified
relation tokens retain their complete spelling, and precheck exceptions now
pass through the normal structured protocol error boundary. Regression checks
require 42883 with integer argument metadata, retain 55000 for actual scans,
and verify the same connection can still execute SELECT 1. A diagnostic probe
in the local PostgreSQL 17.2 reference also produced 42883 for the unknown
function on an unpopulated view; its test DDL was rolled back. This is explicitly
not a PostgreSQL 18.6 differential result.

## Final follow-up optimized verification

The five source/test commits are present together in the optimized TLS-stub,
zlib and ICU build. Final binary checks passed:

- All three new focused protocol scripts (including expanded view controls).
- Materialized-view refresh, CTE boundary, subquery SQLSTATE, nested comments
  and extended-protocol error-abort E2E.
- Complete derived-type protocol E2E.
- Complete JOIN-type and LATERAL-scope protocol E2E, SQL-literal and
  boolean-literal boundary E2E, source UPDATE/DELETE and DML-CTE protocol E2E.
- Complete default `postgres_protocol_test.py`: plaintext SSLRequest
  negotiation, startup/auth/simple/extended query and extended-query error
  recovery/ReadyForQuery assertions. This is not TLS validation.

These are sixteen protocol/E2E entry points, not the full registered suite.
No new native C++ regression or full registered-suite result is claimed for
this main/protocol-only change. The six gap-ledger checker unit tests and
documentation-status, version-consistency and compatibility-contract scripts
passed.

An extra DIV-14 run and a separate repeat both failed on extended ALTER TABLE
with 57014. The fixture set statement_timeout=1234 for a plain-SET syntax case
and never restored it, leaving unrelated maintenance/restore/DDL cases under
that deadline. A separate fixture change now checks the SET value and restores
the original timeout before continuing; its complete recheck passed. The
earlier failures are not counted as successful runs, nor attributed to a proven
production timeout defect. Fixture-only commit: `ff693582`; all capability,
maintenance and DDL success/error assertions were retained. The real timeout behavior remains covered by the
default protocol test and unchanged timeout-specific tests.

Additional reproduced open issues: a compact `FROM(SELECT ...) d` factor can
fuse the generated table name with FROM; ordinary `WHERE id=1 /* comment */`
returned both stored rows, and a dollar-quoted RHS comparison also returned
both rows instead of filtering. CTE binding still globally rewrites matching
identifiers rather than maintaining a general nested CTE namespace, and
`AS(` grammar needs further review. None is recorded as fixed by these commits.

The global-name defect was verified independently in the final development
combination: `WITH id AS (SELECT 1 AS id) SELECT id FROM id` and
`WITH c AS (SELECT 1 AS c) SELECT c FROM c` both report 42703 for column
`__cte_0`, although a CTE with a distinct name and qualified id works. An
unrelated output alias `AS c` was also renamed to `__cte_0`. This needs
relation/scope binding, not another blanket identifier replacement.

General nested/correlated binding, parameterized planning, all function/VALUES
LATERAL syntax, arbitrary JOIN conditions, aggregate/window/locking combinations
and a complete function/type resolver remain open. Full registered suite and
PostgreSQL 18.6 runtime differential have not been run. The installed reference
container is PostgreSQL 17.2 (`170002`), not 18.6.
