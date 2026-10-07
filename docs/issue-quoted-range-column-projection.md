# Qualified quoted physical columns are misinterpreted as scalar expressions

## Reproduced cause and finite fix

Actual source predicates and ORDER BY accepted quoted aliases, while
`SELECT "R".id FROM table AS "R"` and the space/embedded-quote variants
failed with 42P01. Separately, real field names `"f(x)"` and `"x+y"`
were reinterpreted as function/arithmetic targets, losing labels or values.
Textual removal of an unquoted relation prefix did not establish a physical
column's identity.

The single-physical-source target receiver now recognizes an actual parser
ColumnRef in the exact source range and physical column ordinal. It consumes
that field through the existing ordinary projection contract: source column,
output alias, projection order and aggregate placeholder remain physical.
This is not global string rewriting or scalar/routine metadata discovery.
Hidden relation qualifiers, unknown aliases and unknown columns still fail
before false-WHERE/LIMIT-0 execution.

## Permanent complete protocol test

`tests/quoted_range_column_projection_protocol_e2e_test.py` creates an owned
real table and covers unquoted/quoted/case-sensitive/space/embedded-quote
aliases; ordinary, space, embedded-quote, function-looking, arithmetic-looking
and numeric-looking field names; empty and populated sources; NULL versus
stored text `NULL`; duplicate field targets and output aliases. It checks
actual physical table origin, column OIDs, widths, typmods, formats and labels.

| Complete invocation | Actual result |
| --- | --- |
| Strict owned PostgreSQL 18.6, version 180006, C/libc; `quoted-range-column-final-strict-reference18-whole.log` | 0; 84 controls, no differences |
| Unmodified Root Source80 frozen donor; `quoted-range-column-root80-baseline-whole.log`, session 71451 | 1; all controls collected, quoted aliases and scalar-looking field names reproduced |
| Final frozen candidate; `candidate-final-complete-12-whole-wire.log`, session 37162 | 0; all 65 candidate controls and all eleven complete neighbouring wrappers passed |
| Final fresh native/stub set; `candidate-final-complete-11-native.log`, session 20405 | 0; all eleven neighbours passed |

All artifacts are in `/tmp/dbms-empty-integer-preparation.megX23IB`. The
candidate SHA256 is
`fd92f1210c5114c175005180067f6ec79dbeef9bca746d7ccfc5519671eb4972`.
Different reference/candidate control counts are the reference transaction
and error-savepoint checks, not omitted SQL. Baseline reds were not edited or
relaxed. See the [integer issue](issue-integer-unknown-comparison-preparation.md)
for the private Source80 build's exact ABI limits. This is not a proof for
all JOIN/CTE/subquery projection families or the later Root header epoch.
No push or Actions were run.
