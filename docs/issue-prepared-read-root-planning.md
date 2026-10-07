# Plan constants on the actual prepared read root

The shared read runtime now opts its actual primary SELECT/VALUES and quantified execution/EXPLAIN roots into pure constant planning. The existing borrowed execution carrier retains the planned AST, parameter/source identities, callback owner and child graph; there is no discarded temporary preflight or SQL rewrite.

The local `selectPlan` option defaults to false. Scalar, VIEW, derived and CTE producer builders keep that default so constructing a child cannot reacquire a target pruned by the root. The VALUES branch plans on the same `PreparedQueryExecution` instance that later evaluates its typed cells. The root `queryPlan()` and `runRead()` calls explicitly opt in. No public header, storage layout or API changes are introduced.

## Actual failures

Exact ROOT `a7d460c7` plus the independent interval-value correction still skipped three interval-overflow constants under `WHERE false` and executed a sequence-writing CTE before its constant error. The original FROM-less 24-control test additionally has the two real scalar-child-constant failures (`SELECT (SELECT 1/0) WHERE false` returned successful zero rows rather than `22012`).

The dedicated strict-18 fixture has ten failed assertions on that immutable candidate. It covers scalar FALSE/LIMIT 0, irreversible producer effects, plain and ANALYZE quantified EXPLAIN, dead CASE children and a successful zero-demand writing CTE. The reference suppresses no errors and expects a successful CTE to execute once despite LIMIT 0. An additional VALUES scalar-child/writer control retains the same expectation and pure-planning contract.

## Evidence

Artifacts are under `/tmp/dbms-unary-interval-value.0cBXVx8l`.

| Artifact | Actual result |
| --- | --- |
| `root-planning.baseline.log` | Initial ten real assertion failures, exit 1 |
| `root-planning.reference18.log` | Complete initial strict official PostgreSQL 18.6 fixture, version 180006, exit 0 |
| `root-planning.reference18.values.log` | Expanded fixture with VALUES writer/child control, exit 0 |
| `root-planning.baseline.values.log` | Expanded fixture: thirteen retained assertion failures, exit 1 |
| `fromless_select_execution_demand_protocol_e2e_test.final.log` | Original two scalar-constant failures retained |
| `unary_interval_value_protocol_e2e_test.final.log` | Original four root-planning failures retained |
| `build.root-planning.log` | Fresh main against the exact matching other 57 production objects and all header bytes, exit 0 |
| `root-planning.wire.log` | Nine complete focused protocol scripts, all terminal 0 |
| `root-adjacent.log` | Five complete CASE/source/Q84/EXPLAIN46×2 and extended-protocol scripts, all terminal 0 |
| `build.root-optimized.log` | Fresh optimized main, other 57 matching objects and header/source audits, exit 0 |
| `root-optimized.wire.log` | Five complete optimized-main repeats, including the expanded VALUES fixture and original 24 FROM-less controls, all terminal 0 |

The matching development binary SHA256 is `7c479e3ef690459f89ba4f8a2d118d2db92ad422414fd0beeec7604ee4f2fba5`. The optimized-main binary SHA256 is `bef15f3f105c4cfedde2ee1aebad0346bf0d92747c2e87d9ee4f68efe0766a9f`; main is freshly `-O2`, with the other matching 57 production objects retaining their recorded `-O0` flags. Its full 58-object predecessor was rebuilt after the public borrowed-plan root option changed; no objects from the earlier ABI were reused. Source/header audits pass. The matching nine native controls and separately scoped optimized/sanitizer interval/error units remain unchanged by this main-only flag correction. Read-only peer review also checked each child/VIEW/CTE/validation call retains the false default; that is source review, not additional runtime approval.

This is not a claim that every EXPLAIN, ProjectSet, custom type or planner shape is complete. The separate actual probes are retained in `explain-projectset.reference18.log` and `explain-projectset.candidate.log`: ordinary non-quantified EXPLAIN of `1/0` or minimum-interval negation succeeds instead of the strict reference's `22012`/`22008`. That convenience builder still has the false root default. ProjectSet combined with a quantified predicate returns existing `0A000` instead of the reference's planning-time `22008`; the legal shape and its true execution-root contract remain open. These are follow-ups, not permission to narrow the existing CASE, unary, source or effect assertions.
