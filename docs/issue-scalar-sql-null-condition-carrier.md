# SQL NULL scalar qualification: datum identity and execution demand

## Scope and independent root

This is a private repair based on `b3622209`, retaining Root104 public ABI.
It is independent of Main literal-left commutation and VARBIT TypeName/catalog
ownership. Neither this issue nor its green gates claim that those other
issues, Root107/current ABI, or the entire project checklist are complete.

The compact condition representation retained bare SQL NULL as display bytes.
BIT/TEXT inequality and range consumers therefore returned rows for predicates
which can never be TRUE. Index and streaming receivers also evaluated sibling
or target routines before discovering the nonmatching NULL branch. Stored
routines use a different, prepared filter graph: its ordinary builtin operator
strictness was not retained by the binder, and the execution carrier initially
registered only the complete WHERE root, not independently consumed OR children.

## Bounded production changes (two CPP files, no public headers)

* `parseConditions` recognizes an actual bare LiteralExpr NULL, not quoted
  `'NULL'`, native API data or an empty string. The existing nullable carrier
  `patternIsNull` and real input type retain this distinction. BIT UNKNOWN
  input conversion now receives the nullable datum instead of empty BIT data.
* Compact/filter/public-planner consumers admit every signature and input
  before empty demand, validate the physical column owner, and avoid scans or
  runtime routines for a known nonqualifying AND branch. OR retains genuine
  non-NULL branches; a surviving single indexed branch gets its actual plan.
  Empty aggregate input still produces COUNT0, not an absent result row.
* Typed predicates analyze actual AST roles and positive AND/OR only. Their
  remaining expression source intervals are consumed without global SQL
  replacement; NOT/CASE/function value roles retain their NULL semantics.
* The prepared filter uses the retained strict comparison implementation, or
  the existing pure builtin resolver with a real bound column descriptor.
  It never discovers a type or constant from a row, function output or actual
  ParameterExpr value. Three-valued truth possibilities decide WHERE demand,
  not projected values. Remaining genuine child ASTs are registered with the
  same execution carrier before child cursors are prepared. Shared query AST,
  parameters, metadata/source intervals and once-per-occurrence identities
  are not rewritten or replaced by invented SQL.

## Complete evidence and preserved author diagnostics

Artifacts: `/tmp/dbms-scalar-null-carrier.BbGopEMM`.
Owned reference: PostgreSQL18.6, `server_version_num=180006`. All actual SQL,
rows, NULL masks/OIDs, SQLSTATE, headers, tags and effects are recorded. Wire15,
startup20 and normal disk-backed directories were unchanged.

| Complete matrix | Unchanged b362 baseline | Candidate/reference |
| --- | --- | --- |
| native SQL/API/public planner | 376 controls / 223 failures, exit1 | 376 / 0, exit0 |
| Simple SQL, empty/populated/NULL/data/index/OR/error/effects | 442 / 32 failures, exit1 | 442 / 0; strict443 / 0 |
| real Parse/Describe statement, Bind/Describe portal, Execute/Close | 117 / 11 failures, exit1 | 117 / 0; strict118 / 0 |
| full native neighbors plus new native fixture | — | 34 suites / 0 failures, exit0 |
| full protocol neighbors plus both new fixtures | — | 30 suites / 0 failures, exit0 |

The Simple fixture retains the original 374 assertions and adds NOT/CASE/
COALESCE, missing-column42703 at empty/cap0, missing-routine42883 before cap0,
and genuine writer demand. It distinguishes SQL NULL, `'NULL'`, empty data and
physical NULL across all seven scalar operations and both literal directions.
The native fixture uses the actual SQL structured overload and genuine
TypeRegistry INTEGER/dsize4; the default native data overload explicitly keeps
unquoted NULL as real TEXT data. Primary, secondary and heap consumers, real
public OR plans and caller-owned nullable/type carriers are checked.

Extended protocol retains declared OID23 in ParameterDescription and INTEGER
RowDescription, uses actual NULL/non-NULL Bind cells, verifies zero effects at
P/B/D/E independently, and preserves a NOT(UNKNOWN AND runtime boolean) control
which genuinely demands three runtime calls (sequence1 ->4). This is not
permission to treat a real parameter or nullable source cell as a constant.

The initial native drafts accidentally called the native data overload for
SQL comparisons. Their complete results (350/203 baseline, 349/20 candidate)
and corrected whole versions remain recorded; neither is green evidence.
Strict draftv2 incorrectly expected runtime calls for NOT(UNKNOWN OR writer):
PostgreSQL skipped that WHERE branch. Its actual one-failure log is retained;
the corrected strict whole matrix passes. Candidatev1 retained three writer
effect errors, v2/v3 retained four, and v4 exposed the real OR child registration
XX000 plus three consequent25P02 errors. All whole red logs remain available;
none was filtered, silently retried, or represented as terminal success.

Final targeted logs: `scalar-null-native-baseline-v4.log`,
`scalar-null-native-full-v5.log`, `scalar-null-baseline-b362-v4.log`,
`scalar-null-strict18-v4.log`, `scalar-null-candidate-v5.log`,
`scalar-null-preparation-baseline-b362-v2.log`,
`scalar-null-preparation-strict18-v2.log`,
`scalar-null-preparation-candidate-v2.log`, `scalar-null-whole-v5.log`.
The full30 protocol run used the original115-control preparation fixture;
its two final negative-role controls passed in the complete117/118 targeted
run on the identical production binary. Post-registration whole gates are
recorded separately; no pre-registration result is called a later run.

`verify-current.sh` proves all58 original normal compile receipts, manifest,
all header/source/flags/signature and copied object bytes before using only
matching donor objects. TableManage and ExecutionPlan were actually compiled
normally for this issue; the other56 objects match `b3622209` exactly. This is
not a new fresh58 build and not proof for a different public-header epoch.
Fresh native drivers/stubs and exact34-native/30-protocol lists are preserved
in `gates-current.sh`, with current cache/frozen signature checks before/after.
Final normal logs are `scalar-null-normal-v5.log` and
`scalar-null-normal-post-registration.log`; repeat is genuinely up-to-date.
Frozen `dbms_main.scalar-null.frozen` SHA256:
`9265855475d8052aaf07101b477d19a6eb174d5beae2afa2d02f873b37ab58fc`.
No Root/master, old fixture/donor, filtered branch, push or Actions was changed.
