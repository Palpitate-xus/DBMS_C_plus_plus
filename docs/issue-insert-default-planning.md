# Retained INSERT default planning and row execution

The resolved target's required ordinary defaults are retained in
`InsertStmt::preparedDefaults` as live, independently bound expressions plus
their true physical column ordinals. The same `PreparedQueryExecution`
indexes, copies, plans, prepares and evaluates these sites. Preparing a
default never calls its routines or opens a source. Constant planning reaches
required defaults even for a zero-row INSERT SELECT; actual evaluation is
once per missing/DEFAULT column of each demanded row. Explicit NULL or a
supplied value does not demand that column's default. IDENTITY/generated
mechanisms retain the storage engine's existing role and override handling.

The metadata callback is the existing UPDATE default lookup: copied current
logical schema, proven default origin, actual target namespace and actual
session/engine. Stored definitions are parsed once in an empty value
namespace, not rendered from rows or reparsed per tuple. Existing prepared
INSERT consumers that do not use BoundDmlExecution are not claimed repaired.

Evidence is under `/tmp/dbms-insert-default-plan.oBQfCVWC`:

- Strict PostgreSQL 18.6 (`180006`) initial complete 27 by 4 EXPLAIN formats
  and ANALYZE controls: `reference18.initial-whole.log`, terminal 0.
- Immutable ordinary EXPLAIN consumer `40470eea...` complete baseline:
  `baseline.consumer-28-whole.log`, terminal 1, preserving all assertions.
  Pure DEFAULT `1/0` incorrectly succeeds; demanded execution returns 22023,
  and an empty source omits the required constant error.
- Original-header native baseline independently verifies exact source and
  object manifests, then fails the required planning SQLSTATE assertion:
  `baseline-native.log`, terminal 134.
- New InsertStmt layout has all 58 production translation units freshly
  compiled in `candidate-v1`; initial build 1132 links successfully, then the
  new native fixture mistakenly checks the ambient session's currval.
  Corrected fixture explicitly checks the actual passed session without
  changing SQL or expected values: 36229 terminal 0. This is O0, not an O2 or
  whole-program sanitizer claim. Sources and headers audit exactly.
- Complete V1 wire 33594 is terminal 1. All retained INSERT default controls
  pass except the four dead-CASE CREATE-default controls: main's legacy CASE
  rewrite persisted `case_when(...)` instead of the original expression.
  That independent CREATE input bug remains visible; no expected result,
  timeout or counter is weakened to label this complete matrix green.

Broader EXPLAIN descriptor/Parse/Describe phases, WITH primary envelopes,
trigger/view mutation plans and full identity/generated planner lowering
remain separate work. This implementation does not claim exact PostgreSQL
costs, estimates or all physical child instrumentation.

## Independent CREATE input preservation

CREATE TABLE now uses the original DDL input, not main's rewritten CASE or
array evaluator syntax. Its default collector respects true CASE boundaries
and balanced parentheses/brackets, and preserves the original expression
bytes using lexer token provenance. The DDL native entry and bridge both use
the same provenance-aware parser. This does not make arbitrary subqueries,
set-returning functions or all CREATE defaults legal.

The original CREATE-source native fails against the first default carrier's
matching objects: 67367 terminal 134 at the required source-span assertion.
V2's values/NULL/array positives succeed but a new mismatched bracket negative
fails (23589 terminal 134); V3 checks actual delimiter kinds and preserves the
negative. The two changed CPPs and final parser correction are freshly
compiled on the same new InsertStmt all-58 header epoch, with unchanged other
56 source/object/flag and complete header audits.

Final frozen normal binary SHA-256:
`4cd7e162e261212bafd2eec9b33795bb4b2c04a1065c3f21c1329eb27a8704d5`.
The expanded strict `180006` 29 by 4 whole matrix is terminal 0. Final serial
98385 runs that full matrix plus twelve complete adjacent scripts, all 0
(DML EXPLAIN, typed EXPLAIN, domain origin/ancestry/transaction, UPDATE DEFAULT,
long defaults, bound children, original WITH source, MV target and Q84).
Twelve native 87556 are all 0. Five changed production CPPs, test drivers and
stubs are scoped ASan/UBSan, with the other 53 matching normal objects;
41938's three native tests and 99951's entire 29 by 4 wire matrix are 0.
Leak detection is disabled. This is not all-58 sanitizer or O2 evidence.

The original native/wire failures remain intact. Additional strict18 stored
default phase diagnostics prove Parse/Describe has no default calls and a
named EXPLAIN ANALYZE uses the changed `nextval`, constant 22 and NULL defaults
correctly. The unchanged next `ALTER ... SET DEFAULT 1/0` instead fails XX000
because the existing compound expression root has no original source span;
`default-phase-candidate.log` remains terminal 1 vs strict18 terminal 0.
That independent ALTER-span bug and JSON/other protocol descriptor phases
remain open; the successful core matrix is not a whole phase-family claim.

## Independent compound-expression provenance

The expression parser now gives an untagged composite root its genuine lexer
token interval. Already tagged inner/child sites keep their original span.
This fixes storing actual arithmetic/unary defaults without a toString
fallback or an invented span. Native baseline 95473 is 134 at the missing
span; 33805 and scoped parser sanitizer 34011 are 0. V4's twelve natives 8742
are 0, including binding/constant planner/ordinary WHERE and ORDER carriers.

Stronger permanent protocol phase assertions exposed a separate existing
frontend omission before the ALTER check: Describe EXPLAIN returns NoData
instead of the actual QUERY PLAN/text descriptor. V4 final 25452 is terminal
2: eleven original whole scripts pass, but the new phase whole fails both
normal and scoped sanitizer. The frozen previous 588 phase also fails. All
descriptor assertions are retained; the phase test is not registered until
that independent descriptor/analysis producer is genuinely fixed. Strict
`180006` entire permanent phase is terminal 0. No completed protocol-phase
claim follows from the native span fix.
