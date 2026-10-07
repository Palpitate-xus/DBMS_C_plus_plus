# Genuine UNKNOWN integer comparison input is validated only after row demand

## Actual ordinary SQL regression

On an empty real table the original `SELECT id FROM own.bits WHERE i=''`
(also `i<>''` and `i<''`, including original ORDER BY) returned SELECT 0.
Owned PostgreSQL 18.6/version 180006/C/libc rejects these with 22P02 during
preparation. Source descriptors must not depend on sampling or executing rows.

The ordinary comparison binder now transforms only a genuine untyped UNKNOWN
Literal paired with a metadata-proven SMALLINT/INTEGER/BIGINT expression.
The actual integer input function validates/normalizes its bytes during pure
binding, yielding a typed Literal/Const with original source coordinates.
UNKNOWN NULL stays SQL NULL. Enum comparison ownership is preserved.
No row scan, nextval/UDF, volatile evaluation, parameter-value inference or
text replacement is used to discover types. Actual Parameters, explicit
typed casts and prepared children are not converted into constants.

Using a runtime implicit Cast instead was rejected after its actual EXPLAIN
changed forward IndexScan to TypedSource. The final typed Const retains the
existing public planner/index receiver and exact BIGINT values above 2^53.

The protocol Parse path now invokes this same pure whole-query binder for a
finite candidate: an actual integer physical-column/UNKNOWN-literal comparison
in a single physical-source SELECT. The existing physical schema probe is
metadata-only. Real declared parameter positions/OIDs remain parameters;
custom parameter-type ownership and unsupported query shapes are unchanged.
Malformed literal errors are sent at Parse, before ParseComplete/Bind/Execute.

## Permanent controls and final actual outcomes

Native `integer_unknown_comparison_preparation_test.cpp` exercises real DDL,
PK/secondary/non-indexed INTEGER/BIGINT/quoted fields, empty/populated/NULL,
all six operators and both directions, projection/FALSE/LIMIT-0 boundaries,
22P02 versus 22003, source coordinates and descriptor OIDs, typed Const shape,
actual public prepared planner execution, true UNKNOWN parameters, retained
explicit TEXT casts, volatile expression sites and >2^53 exact BIGINT values.
Its 275 counted checks are accompanied by uncounted parameter/volatile/cast
and exact-bigint assertions; the counter is not a total assertion count.

The complete wire test also covers actual Parse/Bind/Describe/Execute,
declared integer/bigint parameters with valid/NULL/invalid binds, physical
RowDescription origin/OID/width/typmod/format, ordinary INTEGER/TEXT quoted
names/case/space/embedded quotes, and actual sequence/stored-writer side effects.
Invalid preparation writes nothing; two actual writer occurrences each run
once. Real stored NULL, empty text and text `NULL` remain distinct.

| Entire invocation | Actual terminal result |
| --- | --- |
| Final strict owned PG18.6/version180006; `reference18-final-whole881-wire.log` | 0; 881 controls, zero differences |
| Root Source80 baseline `baseline-root80-complete-native.log`, session 95796 | 1 (body 134), first missing preparation error reproduced |
| Root Source80 original new protocol baseline, session 53361 | 1; original 183 controls fully collected, 290 failure records including partial publication |
| Final fresh drivers/shared stub; `candidate-final-complete-11-native.log`, session 20405 | 0; all eleven entire native tests passed |
| Final `candidate-final-complete-12-whole-wire.log`, session 37162 | 0; input348, reverse121, quoted-projection65, and all nine original entire neighbouring wrappers passed |

Reference transaction/savepoint controls explain the different check counts.
No original compatibility fixture, default timeout or complete wrapper was
shortened. The original BIT-specific quoted26 test is separately retained:
session36431 exits1 on its first B'01' predicate with this Root's held BIT
capabilities absent. Later controls in that failing wrapper are not claimed
executed. Its quoted-name dependency is not a claim that the BIT matrix passed.

## Exact private build scope and unsuccessful evidence

Artifacts are `/tmp/dbms-empty-integer-preparation.megX23IB`, private initial
Root99a647e8 plus exact quoted-name dependency2760ab1e. Donor normal Source80
is `/tmp/dbms-canonical-between-type.zwBrZgUc/repo`, exact production3ffbb516,
clean source/header/manifest/flags/TLS, all58 original object receipts and
cache stamp proven; immutable donor binary SHA256
`701db03fc9abbaad25136c8fd95d355dea08a8481c604d0745ea14e2bfb48a2d`.
No private production header changed. Final normal session66364 built sole
Main fresh plus three current-input receipt-proven changed objects and54
exact donor objects, then repeated build and all58 current receipt/cache/frozen
checks: actual0. Earlier session59438 freshly built Main/TableManage/binder
with the fourth Network object receipt-proven. This is NOT a new fresh58.
Every final native invocation rebuilt the shared stub and each entire driver.
Final frozen production SHA256:
`fd92f1210c5114c175005180067f6ec79dbeef9bca746d7ccfc5519671eb4972`.

Earlier whole logs remain unsuccessful evidence: v1/v2 native author fixtures
incorrectly represented NULL or created a sequence without real DDL/catalog;
those versions and SHA256s remain frozen, while the newly authored controls
were corrected to genuine APIs. Whole v3/v4/v6 runs exposed real quoted-name,
space delimiter, reverse-result and quoted-projection consumers; they remain
failures. One v4-labelled expanded run raced an earlier frozen binary and is
not evidence for the later candidate. No failure is reclassified as a pass.
The final source/test set, not those obsolete author versions, has the complete
terminal-zero evidence above.

This is private old Source80 ABI evidence, NOT the root's later10989d96 Window
header epoch. Root integration requires exact finite hunks, current headers,
fresh appropriate builds and its own complete tests. General typed INT/TEXT
operator resolution, domains, every set/CTE/join query shape and the complete
273-item root goal are not claimed closed. No push or Actions were run.
