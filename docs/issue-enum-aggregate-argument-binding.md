# Enum operators in aggregate argument and FILTER expression roots

## Root and bounded fix

The generic aggregate argument dispatcher is a separate, already repaired
issue. It supplies real argument trees and correct SUM/BOOL reduction overloads.
These trees still lacked source catalog comparison bindings: their columns were
lowered to parameter slots using only the physical schema's rendered type name.
That discarded the actual type OID and declaration-order enum operator graph.

With declaration `('zeta','','alpha','aa','NULL','it''s','longlonglong')` plus
one SQL NULL row, `SUM(CASE WHEN r<'alpha' THEN 1 ELSE 0 END)` returned 3 rather
than 2, and `bool_and(r<'alpha') WHERE id<=2` returned false rather than true.
Writer CASE/FILTER arguments executed for IDs 2,4,5 (total 11), not the required
IDs 1,2 (total 3). Invalid labels in empty and LIMIT 0 sources were not rejected.
The complete new strict PostgreSQL 18.6 matrix proves these exact statements,
values, NULL flags, expected rows and builtin result OIDs.

Production changes are limited to Main's actual visible-range handoff,
`TableManage.h::QueryExprExecutionOptions::aggregateSourceAlias`, and the typed
aggregate expression graph in `TableManage.cpp`. Before column-to-parameter
lowering, the actual argument/FILTER root is prepared as a metadata-only scalar
projection over its physical catalog source. It retains the original argument
bytes and copies genuine column/operator/simple CASE bindings and coercions.
The ordinary engine binder owns catalog OIDs, qualified enum identity, ordered
labels, domains and UNKNOWN-literal validation. It never infers identity from
datum strings, row order, a global enum registry or raw numeric OID differences.

No whole aggregate query is bound to invent a SUM/BOOL result descriptor.
The existing eight reducer overload/result-type checks remain. Star COUNT,
ambiguous SUM/AVG UNKNOWN inputs and nested-aggregate error ownership retain
their established checks. Argument/FILTER preparation is pure and never calls
a stored function. Qualification, OR RID union/deduplication, lazy CASE arms,
FILTER demand, empty input and LIMIT 0 continue through the existing dispatcher.

The optional alias is filled only from Main's already decoded actual source
range: a genuine explicit alias or an inherited materialized CTE source role.
It is not guessed from a column prefix. Native callers default to the physical
relation's normal identity or explicitly supply their real visible alias.
Identifiers in metadata source SQL are quoted once. Quoted physical-name/
sidecar ambiguity remains a separate OPEN issue, not a claim of broad namespace
repair.

## Source1 exact evidence and scope

Private tree `/tmp/dbms-enum-rank-reductions.zCaoMHUM/repo` is based on frozen
`c6bdc0a08c459542b02883583032498c3230552c`, not current Root's ABI. The parent
tree, helper/tests and frozen c6 binary are unchanged. Bootstrap verified all
58 source/header/compiler/flags/manifest/real donor receipt and object bytes
before producing a normal baseline. Baseline frozen SHA-256 is
`7427273f3cb72cda39bdc521cb98ec584be94e613ba5f5442c3dd6df36ffa261`.

The public option adds a `std::string` to the execution options ABI. Therefore
Source1 rebuilt **all 58 production TUs** with the normal O2 toolchain; no old
objects or reissued donor receipts stand in for this compile. Normal build
session 50390, normal repeat and formal audit 96274 completed with exit 0.
The audit verifies each original production TU compiled exactly once, current
header/compile signatures, all 58 genuine receipts, final build stamp and binary
seal. Private inventory is 690 automatic native inputs plus the separately
supplied actual frontend input, 354 registered protocol drivers and 58 TUs.

Frozen `dbms_main.enum-argument-source1.frozen` SHA-256:
`1c4b862232d99c9f8fba956343f8a96329298a85a6d73697fe9dc1ce8420b60e`.
Future Root integration requires independent current-source/header/ABI proof
and must preserve its newer Window NULLS flags, BOOL syntax metadata, BIT
constructor and hash NULL follow-up; this private proof is not a Root build.

| Gate | Actual Source1 result |
| --- | --- |
| Full new argument operator wire matrix, exact c6 baseline | Exit 1, 53 strong failures |
| Same full matrix on owned strict reference | Exit 0, verified actual 180006 |
| Same full matrix on Source1, default disk | Exit 1, one independent existing result-descriptor failure; every value/effect/argument validation control now matches |
| New native plus original argument/expression/enum native neighbours | All four complete drivers exit 0, default disk |
| Complete 50-driver native argument/enum/CASE/index/provider/CTE neighbours | Exit 0, explicit TMPDIR=/dev/shm diagnostic scope, independent of default-disk results |
| Original full protocol neighbours plus inherited-owner regression | All 25 complete drivers exit 0, default disk |

All engine wire controls preserve the original 15-second timeout. References
use only the existing owned en_US 18.6 database at port 15487, unique owned
schemas inside BEGIN/ROLLBACK and `verify_reference_version` 180006. No shared
public writes, database reconfiguration, push or Actions changes occur.

Artifacts are in the private tree's parent directory:

- `baseline-complete-enum-aggregate-arguments-v1-default-disk.log` (75532=1)
- `strict18-en-us-complete-enum-aggregate-arguments-v1.log` (actual strict exit 0)
- `source1-fresh58-normal-build.log` (50390=0), normal repeat and formal audit
- `source1-complete-enum-aggregate-arguments-default-disk.log` (43781=1)
- `source1-complete4-native-author-v2-default-disk.log` (16133=0)
- `source1-complete25-original-whole-default-disk.log` (56292=0)
- `source1-complete50-native-explicit-tmpfs.log` (64882=0)

The first authored native build used a nonexistent `insertNullable` API and
genuinely failed (22156=1). It was corrected to the existing nullable `insertRow`
overload; the failed log remains. This is a test-author correction, not a
production fix, alternate expected result or removal of a strong control.

The new native driver checks two databases with opposite declaration order
despite the ambient session selecting B, true SQL NULL/empty/text-NULL, actual
alias binding, FILTER, OR union, an actual A-side index, pure invalid-label
errors at zero demand and cold engine catalog consumers. Cross-database numeric
OIDs may coincide; only actual type identity/relative semantics matter.
The protocol matrix checks all seven labels against all seven comparison
operators in SUM CASE/AND/OR roots, EVERY, searched/simple/derived CASE, quoted
aliases, qualified source, enum CAST, FILTER and WHERE agreement, empty/NULL/
LIMIT 0, indexed input, actual Parse/Describe without execution, real writer
effects, overlapping OR once semantics and source OID versus changed search_path.

## Required independent follow-ups, not whole readiness

The original strong combined EVERY/SUM control still requires builtin OIDs
`[16,20]` but Source1 returns `[16,16]` for:

```sql
SELECT every(r<'alpha'),sum(CASE WHEN r IS DISTINCT FROM '' THEN 1 ELSE 0 END)
FROM ranks WHERE id<=2;
```

Rows are now correctly `['t','1']`; SUM's descriptor is still wrong. The exact
baseline already returned the same wrong OIDs, masked in its failure report by
the earlier false EVERY value. `ExprHelper::inferResultType` scans predicate
text inside a non-predicate outer result and mistakes that result for BOOLEAN.
This is a distinct result-inference owner. The exact SQL/OID assertion remains;
it is not changed to 16 or marked successful. A separate issue commit must
repair and prove it before the combined full wire matrix can be called green.

MIN/MAX reduction creates a fresh comparison AST without the argument's enum
ordering binding; that distinct reducer consumer needs a separate follow-up.
No explicit TEXT cast or raw custom OID comparison hides it in this argument
gate. Custom result OIDs, ALTER/new-label transaction rules, quoted physical/
sidecar identity, earlier CTE CASE/SQL reader roots, scalar COUNT/UNKNOWN
lowering and the broader TYPE-08 checklist remain OPEN. This change does not
revisit filtered namespace/WAL/trigger/EXPLAIN branches or deferred security/TDE.
