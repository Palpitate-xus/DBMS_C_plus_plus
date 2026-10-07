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
