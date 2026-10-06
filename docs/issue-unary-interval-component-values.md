# Unary interval component values

Unary minus now negates finite INTERVAL months, days and microseconds independently. It uses the shared datum parser and canonical formatter, retaining mixed signs, fractional input, width boundaries and typed SQL NULL. INT32 minimum months/days and INT64 minimum microseconds cannot be negated; these arithmetic errors are `22008`, not interval-input `22015`. A finite result may not manufacture PostgreSQL's reserved positive-infinity triple.

This follows the checked field operations in the official PostgreSQL 18.6 `timestamp.c` `interval_um_internal`. Unary TIME reaches the INTERVAL operator through the independently selected genuine implicit CAST. The three independent structured integer/MONEY low-level overflow creation sites are unchanged.

## Actual failures

The immutable baseline merely removes or adds the first text sign. This changes only the first component of a mixed-sign interval and can silently emit out-of-range values for all three signed minima. The original wire matrix has 22 failed assertions, including wrong values, wrong TIME result OID, ignored `WHERE false` constant overflow, writing-CTE sequence effects and runtime negation after a demanded sequence input.

The initial strict 18.6 run revealed two incorrect fixture spellings for explicit plus signs in mixed-sign output; its log is retained. The corrected fixture follows the actual reference outputs and passes in full. No query SQL, width/error/NULL assertion, row-count or effect expectation was removed. The independent shared formatter correction precedes this value change.

## Evidence

Artifacts are in `/tmp/dbms-unary-interval-value.0cBXVx8l`.

| Artifact | Actual result |
| --- | --- |
| `native.baseline.log` | Real raw evaluator/bound source/parameter failures retained; exit 134 |
| `baseline.log` | Original wire failures retained; exit 1 |
| `reference18.log` | Initial two mixed-sign spelling expectations disagree with PostgreSQL; retained |
| `reference18.corrected.log` | Strict official PostgreSQL 18.6, version 180006; full matrix passes |
| `build.candidate.log` | Eleven fresh native drivers pass against the original compatible 58-object basis, with optimized evaluator |
| `candidate.protocol.log` | Values/NULL/types/runtime effects corrected; four constant-planning entry-point failures remain |
| `builtin.stage1.protocol.log` | Complete unchanged unary-signature wire matrix passes with the independently retained-AST consumer |
| `build.final.log` | All 58 production units and stubs freshly compiled from exact ROOT `a7d460c7` plus the interval value change; exit 0 |
| `native.final.log` | Nine fresh native drivers pass against that new public-header layout |
| `unary_interval_value_protocol_e2e_test.final.log` | Four root-consumer failures remain; no all-green/family claim |
| `unary_builtin_binding_protocol_e2e_test.final.log` | Complete unchanged signature wire matrix passes on the new 58-unit basis |
| `optimized.log` | Fresh optimized evaluator and stubs; interval values and original structured overflow units pass |
| `asan.log` | The same two native controls pass under scoped ASan+UBSan, exit 0 |

The complete new-header candidate SHA256 is `d310e5c2e9598cf933ed770c4ddaa77b7b786b8dac42f61dee97280f63f648fc`. Full production flags use the shared configuration with final `-O0`; optimized and sanitizer checks replace only the changed evaluator plus stubs/drivers with their recorded flags. The other sanitizer-linked units are uninstrumented and leak detection is disabled. Header/source audits pass; no different public-layout objects or ROOT cache are reused.

## Separate active consumer issue

The exact new ROOT-based wire test still reports three `WHERE false` overflow errors being skipped and one writing-CTE sequence effect before the error. The local read-root producer passes the existing borrowed-plan root flag as false. A separate commit must enable pure planning on the actual owned root execution carrier before opening any producer, leaving scalar/VIEW/CTE child builders false. This value commit does not pretend to fix that entry-point cause.

Interval infinity input/storage, custom types/operators, other IntervalStyle renderers and the whole temporal family remain separately scoped. An additional strict-18 diagnostic found REAL/DOUBLE unary NaN displays `-NaN` instead of `NaN`; its two logs are retained under `floating-unary.*.log` and that existing rendering cause is not folded into this interval commit.
