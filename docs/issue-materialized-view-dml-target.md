# Materialized-view mutation targets

Ordinary INSERT/DELETE could mutate a materialized view's backing heap, while
WITH primary INSERT/UPDATE/DELETE reported `0A000`. Unpopulated targets could
incorrectly report the source-only `55000` error. A DELETE predicate containing
a volatile routine could actually run before the eventual failure.

The private DML target resolver now distinguishes actual materialized-view
metadata from ordinary VIEW/table metadata. Target I/U/D goes through complete
whole-query binding and explicit execution-owned pure constant planning, then
reports `42809` before physical target schema lowering, source/child opening,
routine calls, CTE writes or row mutation. Names, input conversion and reached
constant errors retain their legitimate earlier SQLSTATE. UPDATE duplicate
assignment analysis remains before numeric narrowing/division planning. The
prepared consumer uses the actual physical target occurrence, excluding
logical OLD/NEW channels and source ranges, rather than a name prefix.

No SQL is reconstructed, no temporary surrogate is created, and default
planner activation is unchanged. Source reads of unpopulated materialized
views still report `55000`. Same-named TEMP tables remain writable ordinary
targets. MERGE, general VIEW mutation and complete catalog/type/privilege or
query-family coverage are not claimed by this I/U/D repair.

## Actual evidence

Evidence is retained in `/tmp/dbms-materialized-readonly.wP7CV3tN`:

- `baseline-build.log`: all 58 production TUs and test stubs freshly compiled
  at O0 from ROOT `a7d460c7` plus the two independent geometric input/descriptor
  fixes. Source/header audits pass; the original native target test then exits
  134. Its 17 controls report 13 incorrect SQLSTATEs, retaining child-open and
  unchanged backing-row/no-data-state assertions.
- `mv-baseline-wire.log`: the original unsplit 56-target/two-source matrix
  exits 1, preserving all original SQL, expected states, no-partial-result/tag
  checks and monotonic sequence sentinels. A real DELETE predicate writer
  advances the sequence; later sentinels keep exposing that effect rather
  than resetting it or assuming rollback undoes `nextval`.
- `mv-target-reference18.log`: the original matrix passes on strict
  PostgreSQL `180006`. `mv-target-reference18-final.log` additionally passes
  all 71 target controls, source guards, six ordinary table I/U/D positives,
  search_path/logical CTE shadowing and physical TEMP shadowing. Reference
  objects live in an isolated transaction which is rolled back.
- `candidate-build.log`: the first DML-only candidate freshly compiles the
  changed TU against 57 matching donor sources/all headers; eight native
  tests pass. `mv-candidate-wire.log` passes all original 56 controls but
  overlapped a peer's unrelated server, so it is diagnostic rather than the
  final serial proof.
- `delete-priority-red/native.log` preserves the adjacent genuine DELETE
  boolean-context binding failure. Its independent repair is commit
  `a0b3825b`; the MV guard does not hide that predicate error behind `42809`.
- `final-build.log`: both changed TUs freshly compiled against the other 56
  exact source objects and all matching headers. Nine native tests pass.
- `final-wire.log`: authoritative terminal 0 for eight serial scripts:
  the complete expanded MV matrix, WITH primary DML, WITH OLD/NEW RETURNING,
  materialized-view refresh, UPDATE duplicate-target priority, primitive
  assignment, typed UPDATE matrix and ordinary UPDATE FROM/DELETE USING.
  Every script finally stops its owned server. No deadline or assertion was
  relaxed and no reference expectation was removed.

Final candidate SHA256:
`795bd2058a1ab5e76c6553b7d0d173d61b9ff0ccfc7b2518b69b9cfa35aa04f3`.
`more-native-build.log` is also authoritative terminal 0: the expanded 23-case
MV native repeat, VIEW target namespace/caller restoration, materialized-view
storage, WITH multisource DML and foreign-key action DML all pass. Together
with the earlier group these are 13 distinct native tests plus the expanded
MV repeat; the repeat is not counted as another distinct test. All old strong
MV assertions remain unchanged. This is private O0 evidence, not a claim that
ROOT's later normal O2/full-suite integration has already passed.

There are no new public APIs, existing header/layout changes or production TUs.
The protocol regression is registered for the normal E2E gate. No push or
Actions ran, and the overall DML/database review goal remains partial.
