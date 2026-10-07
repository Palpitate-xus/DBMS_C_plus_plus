# Inherited materialized CTE source preparation ownership

## ROOT current-source composition

Root applies only this issue's79-line Main owner guard and additive fixtures
on current84 source. The earlier generic aggregate dispatch, Boolean range
metadata, enum AST/copy bindings, BIT constructor, Hash NULL follow-up and
public window NULL flags/GROUPS bounds remain. No public header changes.

Current-root artifacts: `/tmp/dbms-root-cte-owner-current.pYaBHYvo/`.

- Exact current84 frozen production baseline62109 actually ends1: all six
  complete file invocations fail. The unchanged original21 driver reaches its
  genuine recursive lookup failure; factor and inherited-target fail their
  original recursive SQL. Exhaustive original derived72 has two strong errors,
  clause-boundary one, and expanded owner fifteen. These are current-root
  results, not substituted private baseline13 or older full-suite receipts.
- The identical inherited-target, original derived72, original factor and
  complete expanded matrices each pass on the owned matched en_US strict
  PostgreSQL18.6 profile, with actual180006 version checks. All original SQL,
  NULL/type/header/tag/error/effect assertions stay; no shared public writes.
- Normal85939 and immediate repeat actually end0: fresh sole Main plus57
  individually source/header/actual-flags/manifest/original58-receipt/stamp/
  object-byte-proved currentfd46 normal donors, not fresh58. All58 current
  receipts match. Frozen SHA:
  `ec7114882be31c02af16f91f33fab94b636d7f5554a7128f008712538153be3f`.
- Complete25 native97349 and31 whole protocol61761 actually end0 on default
  disk/deadlines. All original ten CTE/native and fourteen CTE/protocol files
  are included, as are current aggregate, enum, window, BIT/BETWEEN and all
  original DDL19/TRUNCATE4 neighbours. Inherited target checks actual nextval
  three calls, independent stored SPI scope and physical-name conflicts.
- The separate exhaustive OPEN gate84431 actually ends1, all three complete
  files executed: original derived reaches all72 and keeps only older COUNT
  child0A000; clause keeps its older UNKNOWN childXX000; expanded keeps nine
  strong older CASE/writer/SQL-reader failures. No final assertion is waived,
  no failure is hidden inside the31 passing-file group, and these are not
  reported as three additional PASS files.
- All frozen source/script/test/manifest inputs remain unchanged after the
  terminal gates. A postcommit full original21/inherited/factor repeat must
  pass before master import; its result is recorded in the canonical ledger.

Root review also queried omitted FILTER/ORDER/OVER/group/FROM-function roles
against both older donors, actual c6 and strict180006. The complete11-SQL
diagnostic has identical eleven older failures across all three engine
binaries, while strict passes. Actual admission/parser probes show these
omitted fields are rejected earlier or are an older raw-SQL-child gap; no new
introduced counterexample is established. This does not prove all AST roles,
CTE support or original273 requirements complete.

Root inventory with this issue:693 auto-native plus one actual frontend,
357 registered,58 production TUs. The expanded OPEN test is registered and
still genuinely fails. Source80 original full is terminal1/all1043 receipts;
it is not this source's full verdict. No push/Actions/deferred-security work.

## Earlier private evidence

## Closed root issue, not complete CTE or TYPE-08 support

The enum projection receiver introduced an eager whole-query catalog binding
probe for ordinary comparison candidates. A recursive child already has a real
Main `QueryCteFrame` mapping its logical source to the current work table. That
source is not a catalog relation. Preparation incorrectly looked up the logical
name in the catalog before the existing CTE source receiver could execute it.

The unchanged original regression is:

```sql
WITH RECURSIVE id(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM id WHERE id<3)
SELECT id FROM id ORDER BY id;
```

Its required rows are `1,2,3`, header `id`, INT4 OID 23 and tag `SELECT 3`.
The exact private baseline returned 42P01. Both actual earlier production
binaries returned those required rows: `7504b801` and `82bf3739` had also
passed the original 21-case driver in their full-suite logs. Creating a real
table named `id` first masks this regression: the speculative binder finds the
unrelated catalog table and the legacy execution path subsequently succeeds.
The new fixture therefore tests the original query before creating that table,
and also tests the conflict afterward.

The sole production change is 79 added lines in `src/main.cpp`. The CASE/
comparison receiver declines catalog ownership when an actual FROM relation
role resolves to a visible inherited materialized CTE. The probe uses the real
session/database frame stack and stops at `inheritPrevious == false`, matching
the existing `resolveTableName` boundary without opening storage, registering
temporary access or evaluating a function. It walks actual source/query-child
roles, distinguishes qualified relations, and respects decoded local WITH
declarations, sequential visibility, recursive self visibility and shadowing.
It does not match a column, function or string containing a CTE name; it does
not exclude every WITH/RECURSIVE statement. Local SQL-owned enum CTEs continue
through the existing whole binding graph and declaration-order operators.

## Evidence and exact build scope

Private tree: `/tmp/dbms-enum-aggregate-binding.xtq3IHB6/repo`, initially clean
at `012fdb36169a2cf023c0a2c3ef1a53ba17f190aa`. Frozen baseline normal binary
SHA-256: `bce160bf0467b80507e257ef2b1c2f71956bee064c0e706ba2bc10ab1cb617b5`.
Artifacts below are in that tree's parent directory.

| Gate | Actual outcome |
| --- | --- |
| Unchanged original 21-case relation-scope driver, baseline | Exit 1, exact recursive case 17 returns 42P01; subsequent controls not falsely claimed reached |
| Same entire unchanged original driver, candidate | Exit 0; all 21 queries, post-query physical-table checks and four original SQLSTATE controls |
| Full original 14-driver protocol group | Exit 0; all 11 CTE/DML protocol neighbours plus complete enum comparison/cold restart, generic aggregate argument and ordinary CASE drivers |
| Entire original materialized-factor-boundary driver | Candidate exit 0, ten original statements; exact012f baseline exit 1 on the original recursive lookup, both earlier donors exit 0 |
| Entire original derived-type driver, additive exhaustive collector | All 27 setup statements and 72 original query controls reached; candidate exit 1 only on the older scalar COUNT child lowering, baseline exit 1 on that error plus recursive edges 42P01, both earlier donors exit 1 only on the older COUNT error |
| Entire original factor and derived-type drivers on strict reference | Both exit 0, actual PostgreSQL 18.6/180006, unchanged SQL/rows/header/type/tag assertions in uniquely owned rolled-back schemas |
| Full original ten native neighbours | Exit 0, default disk; eight WITH/CTE native drivers plus complete enum and query-binding drivers |
| New inherited-comparison ownership regression driver | Exit 0, default disk; original recursive SQL both without/with physical-name conflict, quoted recursion, recursive CASE, true NULL/empty/text-NULL, nested shadowing, qualified source, actual nextval once, independent stored SPI scope and original scope errors |
| Same complete new regression driver on strict reference | Exit 0, actual PostgreSQL 18.6/180006 |
| Unchanged full enum comparison reference driver | Exit 0, strict 180006, all original quoted/type/path/error/CTE controls retained |
| Complete expanded owner matrix | Exit 1, nine retained strong failures for separate older consumers; strict 180006 reference exit 0 |
| Original clause-boundary neighbour | Exit 1 on baseline, candidate and old7504, same scalar UNKNOWN child XX000; complete sequential collector confirms only that strong failure and reaches every remaining original clause/descriptor control |

Normal candidate build `source1-normal-build.log` (session 16934) genuinely
completed with exit 0. Only Main was freshly compiled by the unchanged normal
O2 toolchain. The other 57 actual objects match the frozen, clean exact012f
donor, with matching production source/header/compiler/flags/source-manifest
bytes and genuine verified donor/current object receipts. Normal repeat and
final repeat exited 0 without compiling any TU; the final repeat relinked after
test registration changed the build stamp. Formal `source1-formal-audit.log`
(77710) exited 0 and verified all 58 receipts, final stamp, binary/source seals
and unchanged original 21-case test bytes. This is not a fresh58 or current
Root source proof. Future integration must preserve Root's BIT constructor,
hash NULL follow-up, BOOL metadata and newer public window NULLS ABI/flags.

Frozen candidate `dbms_main.cte-owner-source1.frozen` SHA-256:
`7427273f3cb72cda39bdc521cb98ec584be94e613ba5f5442c3dd6df36ffa261`.
Private inventory is 689 automatic native inputs plus the separately supplied
actual frontend input, 353 registered protocol drivers and 58 production TUs.
The expanded OPEN gate is registered and still fails; that is not an overall
green suite claim.

Key logs are `baseline-original-full-cte-scope-default-disk.log` (6956=1),
`source1-original-full21-cte-scope-default-disk.log` (89840=0),
`original10-native-default-disk.log` (43805=0),
`source1-complete14-original-whole-default-disk.log` (31913=0),
`source1-complete-inherited-regression-default-disk.log` (25049=0),
`strict18-en-us-complete-inherited-regression.log`,
`strict18-en-us-complete-original-enum.log` and
`strict18-en-us-v3-complete-cte-owner.log` (each strict reference exit 0).
The two additional complete original gates are
`source1-original-complete-materialized-factor-boundary-default-disk.log`
(89782=0) and
`source1-sequential-collect-original-complete-derived-type-default-disk.log`
(75961=1). Derived contains 69 main cases, including the true five-column
spaced/empty/text-NULL/SQL-NULL recursive edges, recursive JOIN UNION ALL and
UNION, plus three original scalar cases. All 72 are genuinely executed in
candidate, baseline, both earlier donors and strict reference. The complete
diagnostic wrapper `classify-original-factor-derived.log` (66152=0) only
records each actual child status; its own exit 0 is not semantic success.
The unchanged original factor returns baseline 1, old7504 0, old82bf 0.
The exhaustive original derived driver returns baseline 1 (two strong errors),
old7504 1 and old82bf 1 (one strong error each).
`strict18-en-us-original-complete-materialized-factor-boundary.log` and
`strict18-en-us-original-complete-derived-type.log` are each genuine exit 0.
All these engine gates use the original default disk and 15-second wire
deadline; no tmpfs result substitutes for a failed default-disk gate.

References use only the already owned matching en_US database at port 15487,
require `verify_reference_version` 180006 and create unique owned schemas
inside BEGIN/ROLLBACK. No shared public relation or database configuration is
changed. The original 21-case driver itself contains explicit `public.c`; its
complete engine run is preserved, not misrepresented as a complete reference
run that would require shared-public writes. The reference regression uses
the original recursive statements unchanged and additional explicitly owned
qualified controls.

Original factor/derived tests gain only additive strict reference mode with
unique owned BEGIN/schema/search_path/ROLLBACK setup. The derived test also
gains an exhaustive collector. `original-cte-control-ast-audit-v2.log` verifies
the original setup/case/scalar fixtures and every original strong assertion:
12 derived, three factor and eight clause-boundary assertions. Default modes
keep failing on the same original assertion; neither collector suppresses
the final failure or changes the wire deadline. The first audit attempt also
compared newly added reference bootstrap loops as if they were old fixtures
and failed; v2 compares all actual original fixture keys and preserves that
failed attempt instead of silently relabelling it successful.

## Retained OPEN gates and historical failures

`tests/cte_prepared_owner_protocol_e2e_test.py` is a larger, explicitly OPEN
gate. Its full original v2/v3 SQL and strong assertions remain. Neither its
name nor a known-gap exception turns its failed assertions into success.
Both old production binaries completed the same v2 matrix with 11 strong
failures; the exact012f baseline had 13, adding the two introduced ordinary/
quoted recursive lookup failures. Candidate Source1 has nine failures in
both v2 and the additively expanded v3:

| Older independent consumer | Retained candidate failure |
| --- | --- |
| Three ordinary/nested WITH CASE bodies | 42703, column `=` does not exist |
| Recursive stored scalar writer | 42883 unknown input overload; expected writer/result effects fail too |
| Nested WITH CASE writer | 42703; expected single writer effect fails too |
| Two SQL-language query-bodied reader controls | 22023 stored-function evaluation failed |

Candidate repairs the recursive CASE/typed-text argument controls as well as
the introduced comparison probe, but does not fabricate a scalar writer
argument type or fix the distinct nested WITH/routine-body consumers.
`source1-full-v2-cte-owner-default-disk.log` (2209=1) and
`source1-full-v3-cte-owner-default-disk.log` (28368=1) retain all nine failures.
Old donor whole logs are `old7504-full-v2-cte-owner-default-disk.log` (79717=1)
and `old82bf-full-v2-cte-owner-default-disk.log` (62049=1). Their actual binary
hashes are respectively
`29f59b89b4ff3a0ea7de901285891da6d37a90e0fa667f3aa6da15348d989675`
and `d1e0e3d442957f661d12414e589048ddd3d49fe8bc7ed8b0306db4515b63c5e1`.

The authored v1 fixture and actual failed run are also frozen externally.
It created the conflicting real `id` table too early; v2 fixes that setup
ordering rather than accepting the resulting accidental pass. Its original
query/assertions and all other older gap controls remain. V3 only adds real
nextval demand and stored PL/pgSQL independent-scope controls.

The original derived-type scalar control
`SELECT (SELECT count(*) FROM typed_src) AS c;` still returns 0A000, additional
prepared plan lowering. Both earlier donors and the exact012f baseline return
the same error. The candidate repairs every original recursive case and keeps
the subsequent two scalar cases passing; it does not repair this distinct
aggregate child lowering owner. The full strict18 original driver genuinely
passes all 72 query controls, so this remains a real OPEN ordinary SQL issue.
One early candidate exhaustive default-disk attempt timed out during original
setup and is retained; the later sequential default-disk run completed all
72 controls with exactly the documented older lowering failure, not tmpfs.

`cte_clause_boundary_e2e_test.py` gains only additive `--collect-errors` and
original-SQL logging, retaining every default SQL/assert/deadline. Three
concurrent default-disk diagnostic attempts timed out at the first setup
CREATE TABLE, before any scalar error; these failed logs remain. No connection
poisoning or new production cause is inferred from them. The subsequent
sequential candidate collector reached all original statements and final
descriptor controls, returning exactly the older scalar UNKNOWN XX000 failure.

Enum aggregate argument rank binding, custom result OID metadata, ALTER/new
label transactions, quoted physical-name/sidecar identity and the older CTE
consumers above remain separate OPEN issues. This change neither revisits the
filtered namespace/WAL/trigger/EXPLAIN scope nor security/TDE, and performs no
push or Actions enablement. It closes the introduced inherited source-owner
regression only, not the whole checklist or TYPE-08.
