# GROUPS unbounded frame ends must use partition boundaries

## Actual failure and repair

The structured WindowOp initialized GROUPS frame bounds to the current peer
group and adjusted them only for nonnegative offsets. The existing negative
sentinel means UNBOUNDED; leaving those bounds unchanged made unbounded
frames aggregate only one peer group. This affects result values, counts and
EXCLUDE behavior, not merely output order.

For a negative start offset, use the actual partition start; for a negative
end offset, use the actual exclusive partition end. Keep the existing bounded
peer-group stepping and exclusion receiver unchanged. The change is confined
to ExecutionPlan.cpp; there is no public header, storage format, parser or
Main change. The earlier default/explicit window NULL policy remains intact.

## Permanent coverage and exact evidence

Artifacts: `/tmp/dbms-window-nth-frame.ul6nHapu/`.

`window_groups_unbounded_test.cpp` exercises the real public planner and
WindowOp across 72 shapes: ASC/DESC, independently unbounded/current/one-group
frame starts and ends, and all four exclusions. Each shape checks SUM,
COUNT(*), COUNT(value), MIN and MAX, actual structured NULL bits, three
partitions including a NULL partition, peer ties, NULL keys/values and an
empty input. An independent group-membership oracle supplies expectations.

`window_groups_unbounded_protocol_e2e_test.py` executes the same whole matrix
through the actual server: 147 statements locally, 150 on the reference
because of its three isolated schema/transaction setup statements. It checks
every populated/empty result, SQL NULL, type OID, header and SELECT row tag;
failures are collected without stopping before the matrix completes.

| Evidence | Actual result |
| --- | --- |
| Original Source81 native 40501 / full wire 58947 | 1/native134; 147 statements, 52 differences (including the independent NULL policy defect) |
| Exact Source82 native 39274 / full wire 41844 | 1/native134; 147 statements, 40 differences after the NULL policy repair |
| Strict PostgreSQL 18.6, version 180006, identical permanent matrix | Complete150, zero failures, actual0; both original and final reference logs retained |
| Candidate normal build 24221 and immediate repeat | Actual0; sole ExecutionPlan freshly compiled, 57 matching current Source82 normal objects individually proved; repeat does not recompile |
| Complete16 matching native drivers 66161, default disk | Actual0: new72 GROUPS and previous144 NULL-policy controls, original windows/metadata/Volcano51/binding/prepared/group/CASE, current enum/BIT/BETWEEN, original DDL19 and all four TRUNCATE sections |
| Complete seven whole protocol files 1484, default disk/deadlines | Actual0: new GROUPS147, NULL-policy195, original window-type/window-e2e, enum comparison, Boolean BETWEEN and BIT array constructor |

The normal donor is the exact clean `10989d96` fresh58 tree. The external
immutable `verify-current82-groups.sh` checks all production sources, all
public headers, manifest, actual flags, all58 original donor receipts,
production stamp and frozen binary before using unchanged object bytes.
Drivers/stubs are fresh. This is not another fresh58 build or sanitizer run.
Candidate production SHA-256:
`0054bcc5f94840604c916280eeee5495c318aaa08ff8a9a9e1c292a63cd8c8c1`.

The complete source/script/test/manifest hash remains unchanged after both
terminal gates. A fresh complete target postcommit repeat is required before
master integration and is recorded in the canonical integration checkpoint.
No prefix of a gate is treated as a complete pass.

## Scope and remaining requirements

This repairs the actual unbounded GROUPS boundary defect. It does not close
QRY-08/QRY-10 or the original273 audit. Other frame directions/validation,
complex ordering and window expressions, type-aware ordering/collation,
spill and all original unverified/partial requirements remain open.
Source80 original full suite retains its frozen original inputs and is not
restarted, relabeled as Source82/current, or declared passing. No push,
Actions activation or user-deferred security/TDE work.
