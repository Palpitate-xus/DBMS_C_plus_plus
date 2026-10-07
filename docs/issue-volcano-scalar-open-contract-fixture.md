# Volcano scalar open: stale bool failure oracle

## Cause and original baseline

The production repair `ab9794709da37de679fa5f077a19e4c8769b8db8` already
creates `DbError("21000", ...)` at `ScalarSubqueryProjectOp::open()`'s second
inner tuple. Its once-only cleanup preserves that primary error. See
`issue-legacy-scalar-cardinality-sqlstate.md` and the independent
`scalar_subquery_operator_sqlstate_test.cpp`.

The final scalar-multirow control in `volcano_select_phase51_test.cpp` still
expected `!multiScalar->open()` and a string-only operator error. The direct
open correctly throws before that old bool assertion can complete. Returning
false/string again would regress the production repair to XX000; the fixture
must expect the established structured API contract instead.

The exact original complete source was compiled and run against normal O2
ROOT `8f69ffa8ca493392fe16407232c025110205c953`. Session **83554** actually
terminated **134**, with uncaught `dbms::DbError` and the exact cardinality
message plus SQLSTATE 21000. Every preceding original control passed, including
the 300-row parallel scan, window/group and empty/NULL/text scalar controls.
The original scalar context, enabled predicate, planner-built physical plan
and `multiScalar->open()` call have not been substituted.

Evidence directory: `/tmp/dbms-volcano-scalar-open.D6NTuOCI`.

- `baseline-volcano.log` retains the failure.
- `baseline-volcano-source.cpp` is the complete unchanged original source,
  SHA-256 `a25cbc945a0d3550be2ec97f7186d4972366d34d44f76797cad62bd419050fc0`.
- `volcano_select_phase51.baseline.O2` retains its executable, SHA-256
  `b5c5403e6c0fa258eacb81700aad6001d49a722efad79913a0e1b3bec65c9512`.

## Test-only correction and strengthened proof

The original physical planner/open control now catches only `DbError`, checks
the exact 21000 metadata and exact message, and requires no structured row,
no scalar row and no outer next/read. Explicit graph cleanup follows. A fresh
planner-built checked execution verifies 21000 and all empty result carriers.

An additional physical graph uses the same outer/inner tables, schemas,
enabled predicate, projection targets and scalar column. A counting subclass
delegates every call directly to the real `TableScanOp`; the inner still uses
the actual `FilterOp`. It does not supply synthetic rows, alter NULL metadata,
or mask a repeated close with an idempotence guard. The measured inner calls
are open 1, next 2, close 1; the outer never opens or reads. Closing the scalar
graph afterwards leaves the same inner at close 1 and closes the unopened
outer once. The original four outer id/payload rows remain unchanged.

No original positive or negative control, SQL, setup row, output expectation,
production source, public header, or build registry is removed or relaxed.

## Exact build and terminal results

This is **not** a new all-58-source build. The donor was ROOT's already-fresh
normal-O2 `8f69ffa8` group in
`/tmp/dbms-canonical-mutation-plan-close.NWHhcrcB/repo`. Before reuse,
`audit-and-copy.sh` verified all 58 production CPP bytes and object receipts,
all 109 relative public/internal headers, manifest, shared build flags/script,
the actual driver stub source, binary configuration stamp and frozen binary.
`donor-audit.log` ends `MATCH_ALL58_CPP_HEADERS_FLAGS_STUB_SOURCE_STAMP`.
The private copy is immutable; each native linked its 57 non-main objects with
fresh normal-O2 driver stubs and fresh test source.

| Whole retained control | Actual terminal |
| --- | --- |
| Corrected complete `volcano_select_phase51`, 31136, `volcano-final.log` | 0, all original Phase 5.1 controls and strengthened assertions |
| Original independent `scalar_subquery_operator_sqlstate` and `scalar_subquery_projection_cardinality`, 36188, `native-adjacent.log` | 0, both complete natives |
| Original expanded `scalar_projection_cardinality_sqlstate_protocol_e2e_test.py --reference18`, `scalar-reference18.log` | 0, strict actual server_version_num 180006 |
| Original expanded candidate whole, 63588, `scalar-whole.log` | 1, unchanged disk/default-15 timeout in setup before scalar controls |
| Independent unchanged serial candidate whole repeat, 78507, `scalar-whole-repeat.log` | 0, complete original expanded matrix |

The whole reference/candidate script remains unchanged: original multirow SQL,
NULL tuple cardinality, empty/NULL/empty-text/literal-NULL, exact OIDs including
23/20/1007/16, TEMP aliases/quoted names, VIEW/JOIN/CTE/correlation,
unknown-before-writer effects, savepoint and autocommit recovery all remain.
The first timeout is retained, not attributed to a new code cause or erased
by a later success. No deadline or TMPDIR is changed. The candidate used the
exact unchanged frozen production binary
`/tmp/dbms-volcano-scalar-open.D6NTuOCI/dbms_main.frozen`, SHA-256
`559f6446bdfe7c2bfa5ecf6879985b9dbb1a536d9fa3772cc13ca95efd692326`.

This fixes only the stale native API oracle. It does not claim all scalar
query shapes, unrelated protocol latency or the overall project checklist
are complete.

The serial whole repeat ran only after the complete original native finished
and ended `[SCALAR PROJECTION CARDINALITY SQLSTATE] passed`. All owned native
and wire processes reached actual terminal states; the original runner finally
closed the candidate server. The earlier real timeout remains in the proof.
