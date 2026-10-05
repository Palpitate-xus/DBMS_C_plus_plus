# OPT-01 — Relation/path planner framework audit

Status: partial. Source audit recorded on 2026-10-05; no production code changed.

## Evidence

- `src/executor/ExecutionPlan.h` defines a broad `PlanContext`, but `QueryPlanner::buildSelectPlan()` returns one `OpPtr`; there is no relation/path collection representing alternative access paths or parameterized paths.
- `EquivalenceClass` and `PathKey` are small data structs, not planner-owned equivalence/pathkey search structures.
- The `buildSelectPlan(... requiredPathkeys, eqClasses)` overload explicitly discards `eqClasses`, calls the basic single-plan builder, and—when an index appears to satisfy ordering—contains a safe-fallback `break` instead of removing a `SortOp`. Thus the advertised pathkey optimization does not currently change the plan.
- `tests/eq_class_pathkey_test.cpp` runs the ordering case over an empty table and asserts only that the plan exists and returns no rows. Its other checks validate struct defaults and assignment; they do not validate path enumeration, index ordering, sort removal, or equivalence-class propagation.

## User impact and scope

This is an optimizer capability gap, not a demonstrated wrong-result regression: the fallback keeps sorting, which avoids relying on an unordered index path. The present implementation does not explore/compare candidate paths or provide general parameterized paths, so expected PostgreSQL planning choices and performance are unavailable. Removing the sort without a genuinely ordered scan would risk wrong query results and is not a safe local workaround.

No behavior test or PostgreSQL 18.6 differential was run for this source-only audit. The issue remains unchecked in the top-level checklist and `partial` in the progress ledger; a future implementation needs path alternatives, costs, required/provided pathkeys, equivalence propagation, parameterization, and non-empty planner/execution regressions.
