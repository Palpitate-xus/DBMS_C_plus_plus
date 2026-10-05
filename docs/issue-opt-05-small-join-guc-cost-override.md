# OPT-05 — Small-join shortcut overrode disabled Nested Loop

Status: partial. This defect was fixed in source/test commit `61c8bc57` on 2026-10-05.

## Reproduction

For two small (4-row and 3-row) relations with ANALYZE statistics and no join-key index, set the planner cost model's `enableNestloop` to false and build an equality join. The pre-fix `planner_runtime_stats_test` returned `NestedLoopJoinOp` instead of the still-available Hash Join.

Two source paths caused the regression:

1. `costJoinAlgorithm()` correctly priced a disabled Nested Loop at the planner's `1e18` sentinel.
2. The stats-based join-selectivity adjustment then overwrote that sentinel with a finite cost, and the `<50` rows shortcut selected Nested Loop without checking whether it remained a candidate.

## Fix

The selectivity adjustment now applies only when Nested Loop has not been priced out, and the small-table shortcut has the same candidate check. When NLJ is disabled and Hash Join is enabled, the planner now chooses Hash Join; if no alternative is available, the existing fallback behavior remains.

## Verification

- The new 4×3 ANALYZE-backed planner assertion failed before the fix and passed after it: `scripts/build_one_test.sh planner_runtime_stats_test`.
- Existing planner-cost/GUC coverage passed: `scripts/build_one_test.sh cost_model_test`.
- Production build passed: `scripts/build.sh`.
- Full registered suite and PostgreSQL 18.6 differential were not run.

This closes one incorrect cost/enable interaction, not OPT-05 as a whole. Type/operator/collation-aware selectivity and costs, broader statistics and invalidation semantics, and other planner requirements remain open.
