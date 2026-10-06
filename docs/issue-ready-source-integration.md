# Independent source integrations after canonical 75090

## Latest ROOT checkpoint: typed ARRAY, sources and row images

The following are independent local commits, not one combined repair:

| ROOT commit | Scope |
| --- | --- |
| 24e3a5fd | Shared declared common types for JOIN USING. |
| 9e0076a9 | Real ARRAY AST, typed concatenation, execution and metadata. |
| 0f7bf12e | Keep original invalid ARRAY SQL as precise error controls; add valid positive cases. |
| 7ecb6de4 | Nullable multi-source UPDATE/DELETE through real target RIDs and logical source channels. |
| 4975a81f | Versioned target-only UPDATE preserves quoted typed OLD-row bindings and zero-row effect demand. |
| 4cf76fdd | RETURNING expression copies, stored-array stars and array operators preserve declared types. |

Current inventory is549 native fixtures,285 registered Python entry points,
and58 production translation units. Fresh normal optimized build97256 and
normal repeat/all58 signature/binary-stamp audit29030 both actually exit0.
Frozen `/tmp/dbms-array-source-integration.XsuuhG42/dbms_main.array-source.frozen`
has SHA256 `385b18577f94f449f8a4d4bc5af72f2ca40c43f05f990411b6aa5b36585b06c0`.
Matching65 fresh native fixtures75787 and31 complete protocol scripts40643
are live; no combined runtime success is claimed yet. Logs are
`array-source-combination-native.log` and `array-source-combination-wire.log`
in that artifact directory. Source/header/test/registry remain frozen.
The harness preflight initially named a nonexistent deferred-query wire file;
that nonexecuted preflight exit2 is retained. The corrected inventory names
the actual `extended_literal_transaction_demand_protocol_e2e_test.py`;
all65/31 files exist and are distinct. No SQL/assertion was removed.

The separate immutable4897 CASE/comma snapshot completed all58 normal
optimized compilation/signature checks and45 fresh native fixtures with exit0.
Its21-wire gate2051 actually exits1:20 pass, original full protocol fails at
line2332 because `ON CONFLICT ... WHERE name` is ambiguous, SQLSTATE42702.
Strict180006 accepts the target-qualified positive statement but rejects the
exact original unqualified SQL without partial mutation. The private fixture
keeps both strong controls. Its complete original protocol21174 still exits1
at quantified EXPLAIN line2444; later assertions were not reached. Neither
result is relabeled as a full protocol pass.

The original immutablefd/23a canonical540/275 gate23633 is still live. Its
three native failures and original full protocol failure remain retained.
The original465-case XML18 C-profile gate2071 exits1 after332 OK and13 DIFF,
then a15-second SERIAL NULL ALTER timeout;119 subsequent cases are unexecuted.
The unchanged full matched en_US/libc gate86503 exits1 after38 OK and1 DIFF,
then a15-second valid multi-ADD COLUMN timeout;425 subsequent cases are
unexecuted. Each timed-out case itself is also incomplete. Ten matched
locale-focused files pass independently52999; that is not a full465 proof.

Ready simple-CASE equality, constant-NULL planning and pure AddColumn dirty
repairs remain queued until this ROOT gate ends; sequence allocation and
quantified execution have separate actual red controls under repair. No
family is marked complete from these focused fixes. Audit counts remain
273:22 complete/166 partial/70 unverified/15 user-deferred. No push, active
Actions workflow or resumption of deferred security/TDE work occurred.

Everything below records historical checkpoints, including their then-live
handles; the latest terminal/live results above take precedence.

## Authoritative terminal full gate

The original unchanged `scripts/build_tests.sh` session **75090 exited 1**.
Its retained log is
`/tmp/dbms-native-explain-prepared-combination.64xbTwJu/full-registered.log`.
It actually completed all **517 native tests (515 passed / 2 failed)** and
**257 registered Python entry points (254 PASSED labels / 3 failed)**.
The intentional TLS-stub skip is one PASSED label, not TLS runtime evidence.
Frozen server source is 3bc45e3b and SHA256 is
`09121ee818a71dc1f34309ce4a0a827dcf257885f80964f59f7733b60700b5a6`.
The runner freshly compiled its 56 no-main production units; the separate
warm helper's successful completion did not change that actual cache path.

| Original terminal failure | Actual retained result / next repair |
| --- | --- |
| group_collection_aggregate_test | Uncaught DbError/22012 instead of the original checked-result false/error contract; checked API repair integrated below. |
| grouping_expression_metadata_test | Same checked API contract failure; original precise 22012 assertion remains. |
| postgres_protocol_test.py | Original quantified EXPLAIN at line 2433 returns no rows; same frozen binary diagnostic reports 42703 column select. Spaced SELECT itself returns 3/4; compact SELECT also fails. Parser/binder/lowering work continues. |
| table_lock_timeout_protocol_e2e_test.py | Locked statement-start status/recovery failure; independent lock-status and proven-constant owner repairs integrated below. |
| statement_timeout_cli_e2e_test.py | Original 100ms cancellation also times out following SELECT 1. Exact original test on independent a933 frozen SHA6738292c completed with session29791/exit0; its log is /tmp/dbms-query-begin-lock-state.TNJZ551R/statement-timeout-cli-root-check.log. This does not replace the failed original or close all read I/O. |

Later pg_settings assertions in the original postgres_protocol script were
not reached. Their earlier 57014 failures remain retained, not declared fixed.

## Actual local ROOT commits

After the full handle's terminal result, ROOT source freeze ended and these
independently committed, independently verified private changes were
integrated as separate local commits. No push or history rewrite occurred.

| ROOT commit | Independent issue / private donor |
| --- | --- |
| 35594bc0 | Checked-plan SQLSTATE/message/original exception and cleanup/result contract; 81d9dd4f. |
| 33bfda17 | Canonical quoted UPDATE/RETURNING identities; 3fbdb4e9. |
| 87a8fbaa | Shared precise interval storage/expression input parser; 4afbdb02. |
| 06b3794f | Complete INSERT VALUES grammar/separators; 6d6f57c6. |
| cae1f9bf | Interval target input classification before row effects; 3b2fdf9b. |
| bf1e53c7 | Permanent WITH-final-DML interval input and pre-effect red controls; 2995c6bd. This is a test commit, not a runtime repair. |
| cc2a7abf | Native scalar routines bound to the actual owning engine; 1d569746. |
| 3200fc6e | Prepared scalar WHERE child execution/type/NULL/error demand; 7cbd2c0d. |
| 28a621af | Prepared scalar ORDER typed slots and child execution; d4179397. Sort-site identity remains a separately reproduced gap. |
| 7e10a85c | Failed statement-start SQLSTATE/depth restoration; 2f85de1f. |
| 2090fdc7 | Pure AST proof avoids database owners only for proven literal results; a93312f2. |
| bafb163c | Describe origins from real parsed FROM sources rather than literal/comment bytes; 54e79094. |
| 8b220342 | Extended literal queries use a real logically active deferred owner, with physical promotion and interrupt fences; 8ae58eb5. |
| dc30853e | Pure interval assignment input adapter; 9b248ebf. This alone is not INSERT SELECT consumer closure. |
| 4a8aa1a3 | Actual WITH-primary-DML AST/binding and typed logical source provider foundation; f92acb26. This alone is not WITH runtime closure. |
| 75f586b8 | New prepared scalar consumer uses checked result throwIfFailed, preserving original SQLSTATE/exception instead of reconstructing XX000. |

Conflicts were resolved by preserving both real EXPLAIN result publication
and proven-constant owner demand, both checked-result metadata and borrowed
typed-source interfaces, and each distinct E2E registration. No old SQLSTATE,
effect/rollback, corruption, or empty-input assertion was deleted to merge.

## First combined verification: actual terminal results

Source revision **75f586b8** has **58 production translation units**,
**530 native tests** and **267 registered Python entry points**. Changed
public checked-result, transaction, AST, binding and source-provider headers
require a complete fresh optimized object group, not private O0 objects or
old 57-unit ABI objects. Actual normal `scripts/build.sh` session **27360**
exited0 with exactly58 production compile entries. Normal repeat and all58
object signatures/binary configuration stamp audit **96036 exited0**.
The combined frozen server SHA256 is
`9bc92255dae633976ab0fd289a1e8478db4a539fba54f14d3bee2b06ce00fed3`.
Focused native **9480 exited1:94/96 passed**, with exactly two new fixture
wrapper failures in ordinary_scalar_where_plan and ordinary_scalar_order_plan.
Focused wire **35806 exited1:79/80 passed**, with exactly the permanent
interval_with_dml_input_sqlstate gate's four original assertions still red:
three bad inputs give42703 instead22015, and writable CTE advances its
sequence before error instead of leaving currval55000. Artifacts:
`/tmp/dbms-ready-integration.n9mTFFAn/build-full-O2.log`.
The matching verifier audited every production signature and binary stamp
before beginning to freshly compile/link96 selected native tests and run80
selected wire entry points, including the real remaining WITH red gate.
Both focused handles are terminal failures, not still live or fully passing.
Original GROUP22012, corrupt-indexXX001, table-lock55P03/recovery and the exact
CLI100ms/15sec cancellation/recovery controls all passed in this combination.
No complete canonical/full-suite green claim follows from focused coverage.

ROOT06ef197d (private726affc6) independently adapts the newer WHERE/ORDER test
run wrappers to false-result metadata plus original throwIfFailed. All old
P0001/22P02/21000, NULL/ordering/demand/site assertions remain unchanged;
false-result SQLSTATE and cleared partial outputs are additionally asserted.
Both fresh matching ROOT58 natives77069 exit0. This does not relabel the
original96-native failure as a terminal success or require new production ABI.

## Next independent integrations and actual optimized combination

After both focused handles finished, these five source issues were integrated
as five independent local ROOT commits:

| ROOT commit | Independent issue / donor |
| --- | --- |
| a0940bb4 | Standard window metadata/signatures/canonical quoted identity;0314ce63. New13positive/10error native55227, ten adjacent53916 and three wire9337 actually exit0 on matching ROOT58 basis plus one changed normal-O2 CPP. |
| 926f8856 | Ordered original duplicate UPDATE assignment sites and pre-effect semantic validation;c657e9f8. Private26native passes,12wire11pass/1separatewindowmetadata red retained; its combined32case final proof is still required. |
| c9a16ed9 | INSERT SELECT interval target input analysis before source demand;2e07810c. |
| 207ddbd4 | Direct INSERT SELECT WHERE diagnostics delegated to whole analysis, preserving other statements and transaction/privilege phases;14685eec. |
| d7c1f0a8 | Actual prepared SQL-child sort value slots and structural identity;20e69063. |

INSERT SELECT donors preserve all26static-error/four-sequence/no-effect,
multi-star/NULL/type/OID/quote/transaction controls; final5native/5wire plus
24native/11wire and limited instrumented3native actual passes are recorded
with O0/partial instrumentation boundaries in their own issue document.
Sort donor actual56case PG17reference and candidate matrix, nine natives,
seven adjacent wires and unchanged1/2/1-site controls pass on its fresh58O0
basis; this is not optimized ROOT proof. All intermediate candidate/fixture
failures remain retained. Window, sort, INSERT SELECT and duplicate repairs
do not imply complete respective requirement-family closure.

Current source **d7c1f0a8** inventory is **534 native tests /271 registered
Python entry points /58 production TUs**. Public UPDATE vector AST and
ExprHelper callback headers changed, so actual new whole58 normal-O2 build
**69854 exited0**, with exactly58 compile entries. Repeat and all58 object
signatures/binary stamp audit **22267 exited0**. Artifact:
`/tmp/dbms-next-query-integration.Hv7QYyMM/build-full-O2.log`.
Frozen server SHA256 is
`a8130de0f4eec4fcc48674415a9cb80ab1c5bec4a2dcbee1a264232c6dbb32da`.
Matching freshly compiled **101 native tests96252 all passed/exit0**;
**86 wire entry points42926 exited1:85 passed/1 failed**, exactly the original
four WITH input/sequence assertions above. The strong duplicate32case gate,
sort56case gate, both INSERT SELECT gates and both window gates now actually
pass together. This resolves the private duplicate matrix's window dependency
in this combination, not the whole UPDATE/window families. No new complete
canonical/full-suite green or all-sanitizer claim is made.

Private checked-result donor final V2 had a true fresh57 normal-O2 build
76658/exit0, 21 natives41097/exit0 and 14 wire65999/exit0, retaining the original
GROUP22012 and corrupt-indexXX001 contracts. Quoted donor final V2 had
25 natives88195/exit0, 12 wire46194/exit0 and exact final PG17.2 reference/exit0.
These prove their recorded private scopes, not this new ROOT combination.
Other private scopes and intermediate failures are retained in the issue
documents added by their independent commits.

The stronger duplicate-UPDATE private matrix contains all original 24 cases
plus eight nested-source/window controls. Actual PG17.2 reference/exit0
requires unknown input CAST errors before duplicate-target errors but does
not execute 1/0 or volatile writers first. Frozen V3 fails seven new controls;
matching changed-DML V5 fixes six and retains the real row_number metadata
42883 instead of required22P02. The failed V4 build recipe omitted main-source
initialization, failed linking, then copied an old binary; its artifact is
not candidate proof. V5 initializes the complete manifest and uses a unique
frozen artifact. Its remaining window failure must not be removed or called
passed. Source/runtime binding, INSERT SELECT input, WITH execution,
SubLink sort identity, quantified lowering and broader type/query/transaction
families remain ongoing.

## Further independent integrations and current verification

After86-wire42926 actually finished, these changes were committed separately:

| ROOT commit | Independent change / donor |
| --- | --- |
| 86779990 | Original VALUES expression transformation precedes contextual width errors;25882473. |
| 6b161f76 | Qualify the intended target in three old ON CONFLICT predicates proven ambiguous in PostgreSQL;7d4e4611. All old values/NULL/tags/effects remain, with new42702 negative controls in the next independent source issue. |
| 706806ae | Actual typed WITH-primary-DML source/cache/consumer and one atomic fixed command view;ed5ac648. Original WITH4red plus47controls,14native and12adjacent wires pass privately on matching fresh58O0; actual failures/intermediate dependency profiles retained. |
| 7d159891 | Demand-driven typed child cursors, primary error/close contract and typed DISTINCT collation cache;dfa20dea. Eleven native and seven adjacent wires pass on audited fresh58O0 private group. Original quantified parser/EXPLAIN is not switched or declared fixed. |
| f6cbc8d6 | Pure ON CONFLICT EXCLUDED logical namespace, unqualified ambiguity42702 and RETURNING scope isolation;a5c9e6fc. Source/header audits, native, wire and partial ASan pass privately; original seven wire failures retained. |
| 6d39bf0f | Explicit strict180006 reference mode and genuine PG18.6 reference results, separately from retained strict170002 diagnostic mode. |

The WITH donor's older two checked-native wrapper hunks conflicted with
ROOT06ef197d. ROOT retained its stronger false-result SQLSTATE/cleared-output
assertions and original throwIfFailed. No original demand/error/site/NULL
assertion was removed. Main's existing precise checked consumer also remains.

Source combination f6cbc8d6 plus test-only6d39bf0f had539 native tests /274
registered Python entry points /58 production TUs. Public DML/prepared cursor/
operator-vtable headers changed, so actual **complete normal-O2 build96836
exited0**, exactly58 production compile entries. Artifacts:
`/tmp/dbms-with-cursor-integration.xOfpugBC/build-full-O2.log`.
After this build finished, two further independent commits were integrated:
**ea857141** (7f51f576) explicitly verifies the conflict matrix against18.6;
**fd183ec3** (30c1ccfa) validates primitive unknown string input during whole
preparation. Sixteen literal/lexical native errors plus typed parameter,
unused arithmetic/narrowing/typmod/routine controls preserve actual analysis
versus runtime phases. The two old static22P02 fixtures are explicitly tested
at preparation, with dynamic typed-parameter22P02 controls retained. Cursor's
data-dependent CASE/late22P02/close/cardinality fixture is unchanged.

This issue changes only TableManage.cpp, no public header or layout. Exact
other57 original optimized object signatures were audited/exit0; normal build
**75194 exited0 with exactly one CPP compile entry**. Normal repeat and
all58 signatures/binary stamp audit **21543 exited0**. Current inventory is
**540 native tests /275 registered Python entry points /58 production TUs**.
Frozen combined SHA256 is
`23a0f0c631a1aa837e02396726a426e0f4faf628e6a2a89b1714af3b4eff0acc`.
**107 fresh matching native97739 exited0/all passed**; **91 wire19966 exited1:
90 passed/1 failed**, exactly the original complete postgres_protocol test's
line2433 QuantifiedSubqueryFilter empty result. Original WITH4, duplicate32,
window/sort, EXCLUDED and unknown-input gates all pass in the combination.
Later pg_settings assertions were not reached and are not declared fixed.
Private O0 results were not substituted for this optimized proof; no old ABI
reuse or full canonical green is asserted.

After both handles finished, ROOT independently integrated **84e988d2**
(258f41ca): plain PL query results require a destination after SPI execution;
PERFORM explicitly discards them, INTO and original execution errors remain.
One utility CPP normal-O2 build89636 exits0, repeat/all58 signatures39969 exits0.
Frozen SHA256 is
`1e6aa2a53d42eb78cbcb4bfa1e102089b7aee01f495975e1ce313a59e1b78420`.
Eleven freshly compiled matching native83658 and twelve wire25611 both exit0.
Strict actual18.6 reference also passes. Current master has541 native/276
registered scripts. After this gate's actual terminal result, the source
freeze ended. Independent range namespace donor c3ff0ac8 is integrated as
**95b89c25**: canonical same-alias/same-physical occurrence errors42712,
while different-schema unaliased physical same-basename ranges remain distinct
and legal. Its fresh58O0 private eight native/ten wire/actual18 reference
results are retained in `docs/issue-with-dml-range-namespace.md`. No public
header changes. Normal changed-binder build16341 exits0/exactly1CPP; repeat/
all58 signatures91250 exit0. Frozen SHA is
`deeac91d837c4f13d6574f69f3f67fa58f8ce70cfe919c3dbdc915d2bdf16fe7`.
Fresh eight native98410 and ten wire65605 both exit0; each expected entry
and final summary was actually verified. Strict18 en_US reference also exits0.
This source revision has542native/277registered.

The prior fd source/tests/registry are independently frozen in detached
worktree `/tmp/dbms-canonical-input-cursor.4U3gkfUf/repo`, revision b353fe8b.
Its original `scripts/build_tests.sh` full540native/275registered23633 is
actually live with fresh production objects, not borrowed private ABI.
Its complete465-case strict18.6 differential2071 is live against frozen23a.
Neither full gate is terminal or evidence for the newer master. Initial
reference uses C.UTF-8; true default-collation differences must be distinguished
from the actual array-concat OID1007/1009 versus25 defect. A separate owned
en_US.utf8/libc reference database was created and verified; matched-profile
reverification remains pending the original run's terminal result.

The RETURNING namespace donor was withheld until its independently repaired
newer WITH consumer actually preserved the original47 controls and genuine
bound transition images. The activation baseline native43960 aborts134 and
unchanged original47wire48076 fails with multiple-target XX000; this regression
is not dismissed as optional scope. Final private normal matching58 ROOT84
basis plus3changedCPP15639,8native3854, four wire19610 (new19/old47/old4/legacy)
and selected3CPP ASan8811 actually pass. Scope and retained originals are in
`docs/issue-returning-transition-namespaces.md`.

ROOT then separately commits **d3f54e97** (6b268130) namespace+legacy consumer,
**bab862de** (ed02ea32) valid default-alias masking grammar fixture, and
**b4bb3962** (d0cb36ca) genuine typed WITH transition channels. The range
namespace guards and new returning namespaces both remain in the merge.
Physical target selection excludes logical rows, and actual UpdateRowImage,
sourceOrdinal/descriptor/NULL channels distinguish INSERT OLD NULL, DELETE
NEW NULL, quoted aliases and qualified stars without string-key rebuilding.

No public header/layout changes: normal3CPP build63840 and repeat/all58
signatures58730 actually exit0. Combined frozen SHA256 is
`1db6b18d51701efb91a9757e18d797188b016822d11686e1c9ca65fb97859b82`.
This checkpoint's inventory is544native/279registered/58TUs. Eighteen fresh
matching native49693 and all twelve wire60365 actually exit0; both retained
logs contain their final full selected-set summaries. Both strict18 en_US
transition matrices repeat/exit0. The source freeze ended after60365's real
terminal result. No latest full-suite/all-ASan green is claimed. Full captured
fd gates remain separate; new CASE, ARRAY/concat and FROM/USING public headers
require complete fresh58 combination builds when their independent fixes
are verified and integrated, not splicing these old-layout objects.

## CASE and comma-source integrations after the transition gate

| ROOT commit | Independent repair / private donor |
| --- | --- |
| b9a3c1ce | Pure builtin CASE common-type selection, static UNKNOWN input/boolean-condition checks, actual implicit coercion nodes and public CastExpr flag; 8e2c2076. |
| 9ca12bef | Direct INSERT VALUES invokes preparation; VALUES and supported SELECT execute retained prepared coercions and typed source occurrences; bb447a38. |
| 306facb4 | UPDATE FROM / DELETE USING comma lists become genuine CROSS AST nodes with explicit JOIN precedence; e1194fa7. |

The first two causes have an exact strict PostgreSQL18.6 reference matrix.
Their optimized immutable prior84 baseline62587 has36 failed assertions;
the matching new-layout common-type-only matrix76462 still has20 failures,
not four or zero. Final private fresh all58 development objects46617,
32native executions across30distinct fixtures and11protocol entry points,
plus four freshly instrumented native tests using six instrumented production
units, actually pass. Source/header audits and the uninstrumented remainder
are explicit in `docs/issue-case-common-types-and-insert-consumers.md`.

The separate comma grammar fixture preserves the actual previous parser
assertion failure134 and the PostgreSQL47-control matrix, including its
original comma-source late-cast SQL. It is not FROM/USING runtime closure;
that candidate has independently reproduced WHERE-false/NULL ON side effects
and FULL-USING INTEGER/BIGINT merged-key overflow under further repair.
See `docs/issue-dml-comma-source-grammar.md`.

Current ROOT inventory after these commits is546native/280registered/58TUs.
CastExpr's public layout changed. No normal fresh ROOT combination build or
current full-suite pass is claimed yet; the preceding1db6 frozen evidence
cannot certify this new layout. ARRAY/concat and source-runtime public
contract integration will likewise require a complete fresh normal58 build.

Actual locale-focused handle52999 exited0: ten original C-profile differing
case files all givecases=1/failed=0 under explicit en_US.utf8/libc, using the
same immutablefd runner and frozen23a binary. The original C-profile full
handle2071 and original canonical23633 remain live and separate. This
focused replay does not repair or hide the locale-independent ARRAY OID
defect and is not current-master or full matched differential evidence.

The isolated official PostgreSQL18.6 build and actual180006 wire reference
are recorded in `docs/issue-pg18-reference-bootstrap.md`. Six focused original
matrices plus the original56 sort controls actually pass on18.6; the six old
17.2 modes also remain passing. Missing XML/TLS/LZ4/ZSTD reference capabilities
are explicit, not treated as broad18 compatibility proof. Original quantifier
EXPLAIN/compact syntax, the remaining input/type families, general CASE
operator/catalog rules, expanded OLD/NEW RETURNING coverage, genuine WITH
UPDATE FROM/DELETE USING and wider families remain open. EXCLUDED and
unknown-input matrices also actually repeat/exit0 on18.6 from ROOT; see
reference-excluded-pg18.log and reference-unknown-input-pg18.log.

All PostgreSQL17.2 references here remain diagnostics, not a PostgreSQL18
oracle. Only the explicitly version-checked18.6 results above are18 evidence.
273 audit requirements remain **22 complete / 166 partial / 70 unverified /
15 deferred by user**. No requirement-family closure is inferred from these
individual changes or green narrow checks. Actions remain disabled; no push
or resumption of the user-deferred security/TDE work occurred.
