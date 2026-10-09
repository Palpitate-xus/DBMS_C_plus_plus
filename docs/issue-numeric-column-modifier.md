# Durable numeric declaration modifiers

SQL NUMERIC declarations previously lost precision and scale at the physical
column factory. Native writers validated an unbounded decimal without applying
its declared rounding/overflow rules. Four strict global-aggregate controls
therefore read 40/30/20/80 instead of 40.00/30.00/20.00/80.00.

The repair stores PostgreSQL's packed precision/signed-11-bit scale modifier in
the real scalar/array column descriptor and its existing C-format type-aware
footer. Old unmodified schemas retain modifier -1; bare NUMERIC is not assigned
the legacy factory's nominal default 18,2. Physical width is not a precision.
The registry accepts PostgreSQL's negative scale and scale larger than precision.

Native INSERT/UPDATE convert actual runtime datums through the existing numeric
cast implementation before final row checks. Numeric array elements use the
same declaration. SQL input analysis converts only genuine contextual unknown
literal inputs, retaining typed expressions' runtime evaluation order. Unknown
overflow raises 22003 before unrelated sequence effects and even for zero rows.
ALTER retains the modifier when rewriting values and persisting the new schema.

This is a scoped SQL declaration/storage repair. The older native factory API
still does not itself attach precision/scale; native callers must use a resolved
column or actual typeMod. That independent API contract remains open. Numeric
physical capacity and the whole numeric compatibility family are not complete.
The independent ROW constructor dependency is not repaired here.

## Actual evidence, including failed candidates

Artifacts: /tmp/dbms-having-grammar.MPk64yc0.

| Run | Actual result |
| --- | --- |
| PostgreSQL 18.6 reference | New numeric protocol: 23 controls including BEGIN, zero failures, exit 0; actual server 180006 verified. |
| Previous frozen baseline | Session 35276 exits 1: 22 controls, 12 failures. Original binary/log preserved. |
| Own fresh production objects | Sessions 10934/3311 exit 0: 57 non-Main plus actual Main. |
| First direct native group | Session 52907 exits 0: all 5 entries pass, including numeric cold-load/native assignment/rewrite controls. |
| First complete focused group | Session 51084 exits 1. All 13 native entries and 4/7 protocol entries pass, including the entire original default protocol. Numeric wire retains three modifier-descriptor failures; original multidimensional array fixture retains 42804. Strict global aggregate entry retains only its one ROW failure. All 58 current own receipts/cache/repeat/source/frozen terminal fences pass. |
| Original BIT differential | Session 71537 exits 0: 384 controls, zero differences against 180006. |
| Corrected own translation units | Sessions 81179/43506/10068 exit 0 for network, binding and DDL respectively. Public headers/Main are unchanged from the first verified generation. |
| Strengthened PostgreSQL reference | 26 controls including BEGIN, zero failures, exit 0. |
| Corrected complete focused group | Session 78431 exits 1: all 13 native and 6/7 protocol entries pass, including all 25 new numeric controls, every original adjacent entry and the entire default protocol. Only the independent strict ROW control remains failed. |
| Corrected original BIT differential | Session 59453 exits 0: 384 controls, zero differences on actual 180006. |

Final numeric declaration/storage controls pass; this does not relabel the
one remaining global aggregate ROW failure. Its SQL, expected row and OID
remain unchanged. The final gate verifies all 58 current own receipts, cache,
no-recompile repeat, source seal and frozen binary equality at start and
terminal. No copied objects or inherited private/master proof.

Corrected source seal:
bd4f2e198acfd8670d7894c7fa9c8a83cbce25736f4aa53871b2ecd3bb82827d.
Corrected frozen binary: dbms_main.numeric-column-corrected.frozen.
SHA256: d639c6bf83ea324c3b785ee29e98826448b3b6c44ce36bf8f2760a82e4405f98.

Initial source seal:
bc159956a9aac9d9bbe48f54e8f0e55d0cfa6831b88e358774d21c69fc51a3ab.
Initial frozen binary: dbms_main.numeric-column-initial.frozen.
SHA256: e40b9dc89e4a67bbe8ff4bd4e7b8cb5207d50883776c00dc3be7fa72c1d6ad92.
Its failed generation remains intact; no deadline or assertion is weakened.

The failed generation's scalar protocol adapter excluded NUMERIC from physical
descriptor admission. Its multidimensional ARRAY binder passed a scalar target
to the assignment callback for an actual array-valued child. After terminal,
the correction admits matching actual numeric column descriptors and preserves
the structural nested-array target. Current declaration metadata takes priority
over historical catalog-only numeric modifiers. Added strict controls check a
multidimensional numeric assignment and zero-row wire descriptors.

The entire preceding master full driver 84918 remains active and source-frozen.
No private source import, whole-suite completion claim, push or Actions activation.
