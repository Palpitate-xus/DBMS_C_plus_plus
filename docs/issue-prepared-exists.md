# Prepared EXISTS: verified repair and remaining scope

| Master commit | Change |
| --- | --- |
| `bcaf593c` | Real retained-child row-presence cursor, early close, multiple columns, correlation and per-execution memoization; ignored target/sort handling after whole binding; registered native/protocol tests. |
| `5dbb7c83` | Correct omitted-sort FETCH lowering, genuine UNKNOWN-to-TEXT child descriptor resolution, and grammar-owned boolean identity rather than routine lookup. |
| `8972cf58` | Preserve original binder fixture SQL, require boolean despite its ordinary mock routine callback deliberately returning bigint. |

These are one scoped source repair with independently retained corrections.
Total170 scoped repairs; inventory762 automatic native +2 actual Main drivers,
425 registered protocol/E2E entries,58 production units. No whole-suite pass.

PostgreSQL's [EXISTS simplification implementation](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/optimizer/plan/subselect.c)
distinguishes presence-only queries from OFFSET/aggregate/set/SRF and other
barriers. Full binding and source descriptors precede the eligible-child flag;
ignored output cells never determine boolean presence or a type. Unsupported
graph families remain open, not claimed implemented by these tests.

## Actual evidence

Artifacts: `/tmp/dbms-root-catalog-followup.PAz6zkPp`.

| Invocation | Terminal result |
| --- | --- |
| Original native6 probe43824 | 1;all6 incorrectly0A000. Same6 actualPG18.6 SQL returns correct booleans/OID16. |
| New protocol on previous frozen Root | 1;27 statement checks reached,9 failed assertions retained. |
| Initial reference fixture | 1;unnamed statement invalidated by Simple Query,26000 retained. Named-statement correction preserves SQL/rows/OIDs/effect assertions. |
| Final full updated PG18.6 reference | 0;41 statement checks plus explicit Extended assertions, including multi-UNKNOWN child. |
| Genuine Root fresh57-unit78138 +Main45435 | Both0;57+1 actual compile entries. Normal link45128=0, all58 current own-path receipts/cache verified; initial frozen generation retained. |
| Initial native20528 | 1;18 reached,2 actual lost-Sort/UNKNOWN-descriptor failures retained. |
| Corrected units97990 | 0;only binder/evaluator/plan rebuilt; other55 current Root units verified, not another fresh58 or foreign-object reuse. |
| Final normal/repeat/frozen proof28286 | Current58 receipts/cache, no-recompile repeat and source/frozen fences pass.19 native all0;15 wire invocation1 with4 invalid-argv invocations and1 genuine CTE fixture failure. |
| Correct-argument complete same15 wire48749 | 1;14 passes,1 retained original CTE fixture failure; final current58/cache/frozen/source fences pass. All4 rejected invocations rerun in full with valid arguments. |
| Unchanged original BIT38442056 | 0;384 reached,zero differences against actualPG18.6. |

Final frozen SHA256:
`cf8145b8e2ee2f96426eb4ebee396bc5d4f62762ab7e37c2c39a81e601b2089b`.
Source seal:
`6de2260ed291904d3e74c49b4bcb6ad9554468c08a0511474aef9e544837566d`.

New native19 and protocol28 statement controls pass, including explicit
Extended boolean OID/width/origin and sequence-effect assertions. Coverage:
multiple columns, NULL, empty/source-free children, ignored error/volatile
targets and ordering, OFFSET, NOT/CASE, correlation, name errors, unchanged
scalar cardinality. Original full scalar FETCH92 now has zero failures,
including all original demand/counter and Simple/Extended assertions. Default
full protocol passed twice. Adjacent SRF/set metadata/type/parameter fixtures
passed after valid-argument rerun. No expected SQL/row/OID/count/deadline weakened.

## Remaining work

The original CTE owner fixture retains4 stored-routine/effect assertions:
recursive writer42883, its missing three rows/effects, two SQL readers22023.
Broader EXISTS aggregate/set/SRF/locking graphs and other preceding whole-run
failures remain open. No new completed762+2/425 whole run, SAN/TLS runtime or
original273 completion claim.

Original273:22complete/166partial/70unverified/15deferred_by_user. Item hash:
`c4e3eb56b88632c440e58a4b0139d302aefdd61d90002499493e7a7041a5b250`.
No push, Actions activation or user-deferred branch restart. README remains
evergreen in `db35e522`.
