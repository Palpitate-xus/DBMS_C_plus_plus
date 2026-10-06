# EXPLAIN typed execution and statement effects

Status: the original two EXPLAIN execution failures and the retained typed
tree/publication controls are repaired and verified. Newly found SubLink
sort-slot sharing and broader lowering/type boundaries remain open.

## Actual baseline and reference

Source parent is `dbf78446ce39b985daa31665f253d781321a83fa`; the private branch
first includes the independent reached-query binder `ad75b27c` as `ee524433`,
scalar context `8eea4e71` as `ca7b7a4e`, original-AST result inference
`088d489e` as `74f590ee`, and shared typed execution carrier `3e98e86d` as
`f860c22f`. The verified typmod prerequisite `c333533d` is mapped privately
as `73949187`. The dependency mapping is private, not a claim about ROOT HEAD.
The old runtime is the exact frozen ROOT a00 production group, not a newly
compiled dbf source binary:

- `/tmp/dbms-order-metadata-arithmetic-combination.zJLGTGV3/dbms_main.frozen`
- SHA256 `f30d15ff4ae72e1ab2bfbf66c8cf5a3f069d4640151861d29ed76378eb0e2ad5`

The two original controls retain their PostgreSQL expectations: FROM-less
`EXPLAIN ANALYZE SELECT explain_typed_strict(93)` must raise `P0002`, and
table-backed `explain_typed_cast(id)` must raise `22P02`. Both must publish no
plan rows and leave no inserted rows. The frozen binary instead returns
`42601` for the first and successfully scans without calling the second.

The expanded, corrected 29-case TEXT/JSON wire baseline finished with exit 1
(handle 77959); artifact
`/tmp/dbms-explain-typed-execution.fGV8WYCQ/baseline-a00-v2-red.log` retains the
actual states, plans, surviving writes and sequence observations. Its 65
failed assertions are not 65 independent bugs. Earlier handle 9100 also
finished with exit 1; that v1 test mistakenly required a final newline which
the wire row splitter removes. Its 77 failed assertions are not used as the
final baseline count.

PostgreSQL reference is 17.2 / 170002, not an 18.6 differential. Its fixtures
use temporary tables/sequence/functions, one outer transaction, per-case
savepoints and final `ROLLBACK` (`00000`). The expanded v7 reference retains
all earlier controls, correlated children and four NULL/constant controls:
all 45 non-deferred
sequence next-values match the expected call counts, including zero calls
after metadata errors. Artifact:
`/tmp/dbms-explain-typed-execution.fGV8WYCQ/pg17-reference-v7.log`.

A separate autocommit reference for deferred FK failure returns `23503`, no
plan rows and no surviving child row, while its sequence next-value remains 2.
Artifact: `/tmp/dbms-explain-typed-execution.fGV8WYCQ/pg17-reference-deferred.log`.

## Intermediate evidence, not final proof

The initial independent 56-unit O2 build (handle 62263) completed, repeated
up-to-date, and matched all object signatures and the binary stamp. Its exact
frozen intermediate binary is `dbms_main.explain-v1.o2`, SHA256
`5a88cac9d337da61ecc7f3aef48caf27da7c6ec0eb7bb5670fae05528a0230a3`.
The 40-case TEXT/JSON run (98944) terminated with exit 1 and 24 failed
assertions (`candidate-explain-v1.log`). The original two failures were fixed,
but the expanded controls exposed genuine execution gaps: preprocessing
EXPLAIN's raw CASE/null grammar before parsing, OFFSET-before-LIMIT left
unparsed and falling back to the old projection, and scalar child envelopes
being mistaken for physical columns before metadata binding.

The native run also retained a real VIRTUAL-generated-column NULL failure
(69118, exit 134, `native-explain-v1-fixture3.log`). Earlier native failures
were test-fixture mistakes: string `"NULL"` instead of a typed null cell, then
a schema without its real table identity. The repaired test uses `nullopt`,
the engine's real schema and an independent stored-NULL assertion; its caller
NULL-binding restoration assertions remain intact.

These intermediate failures are retained. The second candidate preserves raw
EXPLAIN grammar until pure binding, accepts either legal LIMIT/OFFSET order,
chooses scalar lowering from the bound AST, and distinguishes a VIRTUAL
computed NULL from the physically stored placeholder. It switches to the
shared carrier's actual source-ordinal cells and execution-owned AST copies,
not the initial private column-name adapter. New shared headers require a
fresh 57-unit build; the initial 56-unit binary is not proof for that ABI.

Further intermediate results are retained, not reclassified as passes:

- V2 wire 13174 exited 1. Two array failures were real; two unknown-NULL
  alias/DISTINCT failures were real; one extended test incorrectly reused a
  portal after Sync and was repaired by a new Bind, not by weakening its
  execution/effects assertion.
- V3 wire 71613 exited 1 with only the two array assertions remaining.
  V4 wire 42199 also exited 1: copied catalog arrays were fixed but physical
  `int[]` cells still disagreed with `integer[]`. Both native failures and
  their exact descriptors are retained in the separate array issue report.
- V5 wire 9411 and 15-native batch 52584 exited 0. V5 adjacent batch 55553
  exited 1 with 15/17 passing. The CLI fixture's CREATE FUNCTION line with an
  outer terminating semicolon failed before any EXPLAIN; its repaired line
  omits only that optional terminator, retaining all body semicolons and FK
  failure/publication assertions. This does not claim a CLI terminator fix.
  The second failure was a VARCHAR typmod regression from this old private
  base missing the independently verified `c333` prerequisite; that commit
  is now included as `73949187`, not treated as an EXPLAIN source repair.

The reference establishes two distinct sort demands: a volatile non-key
target with physical ORDER BY and LIMIT 1 runs once; ordering by that same
target, its alias or ordinal evaluates three keys, not six. Immutable CAST
targets run below Sort and can fail on a later input row before any volatile
non-key target runs. OFFSET consumes projected rows; LIMIT 0 must not even
start a sort or offset child.

[PostgreSQL EXPLAIN documentation](https://www.postgresql.org/docs/17/sql-explain.html)
states that ANALYZE executes the statement and records actual execution
information. SELECT results are discarded, but normal statement side effects
remain. Plain EXPLAIN does not execute the statement.

## Implementation contract

The prepared scalar path owns a whole, metadata-bound query AST, typed
parameter cells and canonical source descriptors. It uses Result (one empty
source row) or a physical scan, typed qualification, typed sort-key slots,
typed projection, typed DISTINCT, OFFSET and explicit optional LIMIT.

Source columns use exact bound range occurrences/ordinals and typed
`RowContext` cells rather than a second case-folding lookup. Parameters remain
genuine `ParameterExpr` nodes. `PreparedQueryExecution` consumes retained raw
spans, source metadata and typed cells, and creates private execution copies
without mutating the original AST. Lazy scalar children share the caller's
snapshot/CID; their memo is per actual execution and original expression site.
Function resolution during preparation
only reads metadata and binds actual-engine callbacks; evaluation belongs to
the operators that need the value. Shared target/sort expression identities
reuse cached typed values; raw spelling or private callback names are not
the expression identity.

The same plan instance is executed and rendered. TEXT and JSON are assembled
after successful execution; no display-only SELECT or second plan produces
the reported counters. ANALYZE is never a cached execution result. Plain
cache hits still follow preparation and retain resolved relation identities.
The caller's existing statement transaction, read view, CID and CTE scope
remain the execution owner; expression `DbError` states propagate without
rewrapping as a generic error. Failure closes the tree before outer rollback.

Publication has a depth-scoped pending sink: statement execution alone is
not enough to expose a plan. Immediate FK checks, statement finishing and
outer implicit commit must also succeed. SPI/nested EXECUTE captures remain
separate, and errors discard the pending output. The CLI TEXT/JSON deferred
FK control verifies `23503`, no partial plan and no surviving inserted row.

The existing physical/index/bitmap and bounded JOIN/group paths are retained;
the typed path adds scalar expressions, aliases and demand handling rather
than deleting formerly working paths. Legal relation-alias tests change from
the old deliberate `0A000` boundary to successful execution, while retaining
hidden-relation `42P01` and unknown-column `42703` controls.

Startup materialization/sort/offset timing is included without inventing an
emitted row or next-call. Existing `loops` counters still count `next()` calls,
not PostgreSQL's plan-rescan cycles; this change does not claim complete
PostgreSQL instrumentation or cost-model compatibility.

## Explicit remaining boundaries

Generic root CTE/derived/LATERAL/set-operation and aggregate/window expression
lowering are not all provided by this scalar plan. Its single physical-source
and FROM-less paths can consume correlated scalar children through the shared
carrier; this does not imply complete multi-range lowering.
No full type/operator/coercion family or complete optimizer compatibility is
claimed. Plain preparation still inherits the shared binder's documented
static-type and parser boundaries.

A separate post-V5 actual diagnostic found two SubLink sort-slot gaps:
identical scalar-child target/ORDER expressions called nextval twice instead
of once, and duplicate ORDER scalar children also called twice instead of
once. Two independent identical SELECT targets correctly called twice.
PostgreSQL 17.2 establishes 1/1/2 respectively. Artifacts are
`/tmp/dbms-plpgsql-raise-state.0N05Nq7X/candidate-sort-sites.log` and
`reference-sort-sites.log` (68407 exited 1). The current per-expression-site
child identity intentionally preserves separate target sites; fixing sort
sharing requires a separate prepared-query structural-equivalence change,
not merging all identical targets or using raw SQL/evaluated values. These
new controls are open, not included in the 46-case closure below.

## Final matching proof

The production group uses the fresh 57-unit header/carrier ABI. Changed-source
O2 rebuilds and relinks, a normal repeated build, all 57 object signatures and
the binary stamp were checked. Final build 51314 exited 0 and reported
up-to-date on repeat. Frozen V6:
`/tmp/dbms-explain-typed-execution.fGV8WYCQ/dbms_main.explain-v6.o2`, SHA256
`f39e8891871f141aedad08aa0e5bf4282d108490bb594d37ce2040d662d2140d`.
This is the private dependency/fix group, not a claim that another ROOT HEAD
or the initial 56-unit candidate produced the results.

Wire 78440 exited 0: 46 retained cases in both TEXT and JSON, plain-repeat
no-effect checks, repeated ANALYZE execution, and three extended-protocol
preparation/execution/error controls (`candidate-explain-v6.log`). Its owned
PID 3862482 was stopped and confirmed gone. Native 32194 exited 0 with all
15 matching tests (`native-explain-v6-final.log`): array metadata, typed
EXPLAIN, binder/carrier, demand, resolver/actual owner/atomicity/namespace,
ORDER metadata/parsed result types, and four existing instrumented-plan tests.
CLI publication 28436 exited 0 in both TEXT and JSON
(`cli-publication-v6.log`). A stronger final test also asserts the empty child
output and successful SELECT 42 recovery; repeat 74735 exited 0
(`cli-publication-v6-strong-final.log`). Captured full CLI output is retained
in `cli-publication-v6-output.log` (inspection 90170 exited 0).

Final serial adjacent batch 58244 exited 0 with 18/18 passing and no failure
labels (`adjacent-explain-v6-final.log`). It covers CLI publication, TEXT
ANALYZE/timing, JSON options/cache/actuals, bitmap, relation alias, JOIN,
BETWEEN/Boolean, stored-function atomicity, PL INTO row demand, PL namespace
binding, stored-function ORDER/WHERE, quoted arithmetic Describe and table
character-cast typmods. Each test-owned process was waited/stopped by its
own runner; the final PID 3917671 is gone. This is focused closure, not a
full 501-native/244-E2E or default-protocol result.
