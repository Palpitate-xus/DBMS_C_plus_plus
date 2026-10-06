# Prepared SQL-child sort slots

This fixes the independently reproduced duplicate execution at target/ORDER
and duplicate ORDER SQL-child sites, in both ordinary SELECT and the actual
EXPLAIN ANALYZE tree. It does not merge independent SELECT evaluation sites or
claim all SQL-child, aggregate/window, type preparation or quantified-query
families complete.

## Actual old and reference results

The original frozen EXPLAIN V5 diagnostic retained two failures: identical
target/ORDER child and duplicate ORDER children each advanced a private
sequence twice instead of once. Its two genuine SELECT sites correctly
advanced twice. The later checked-in ordinary known-gap test retained the
same 1/2/1 expected versus 2/2/2 actual results.

The new isolated matching baseline is peer foundation `f92acb26`'s completed
58-TU O0 binary `/tmp/dbms-with-final-dml.C9f0t0DH/foundation/dbms_main`, SHA-256
`036030dd7a3d96c29c26a0f37e4b16a60500d7b741c14946fe6c2efb6e4b23a7`.
It includes the independent ordinary WHERE/ORDER consumers and borrowed-SELECT
factory. Baseline `30841` reproduced the unchanged ordinary three controls.
The first expanded gate also retained an incorrect JSON-fixture assumption
about PostgreSQL's plan document layout; it is not attributed to SQL execution.
After using this DBMS's established joined-line JSON/actualRows contract,
matching baseline `51525` is terminal 1 with 24 count failures: eight each in
ordinary SELECT, TEXT ANALYZE and JSON ANALYZE. All 14 plain EXPLAIN no-effect
controls passed. Logs `baseline-f92.log` / `baseline-f92-v2.log` retain both.

PostgreSQL 17.2 (`server_version_num=170002`) passed all 56 matching semantic
controls in `reference-matrix.log`. The reference used owned TEMP relations and
sequence, a pg_temp writer, BEGIN/savepoints and final ROLLBACK; its sequence
counter is intentionally nontransactional. Seven additional peer reference
controls establish that whitespace, implicit/qualified access to the same
bound column, and INT/INTEGER casts share a slot, whereas changing/removing a
child's source alias does not. Two genuine SELECT targets remain two sites.
There were no role/authentication changes and no PG18.6 suite claim.

## Execution contract

Sort equivalence reads the retained prepared child AST, not raw SQL or AST
rendering and not evaluated values. It encodes canonical routine metadata from
the actual engine, operators/literals/casts, typed parameter slot identities,
output descriptors and query clauses. Source occurrences inside that child
are normalized by their ordered metadata occurrence; true caller occurrences,
scope depth, physical column ordinal and declared type remain unchanged.
Visible range/From aliases, physical relation identities and local versus
caller CTE identities are retained. Bound output aliases use their actual
prepared descriptor position. SQL NULL and parameter declaration types remain
typed cells, never substituted into the equivalence key.

The key is used only to assign sort-input slots. Equivalent target/ORDER child
expressions reuse the target value; equivalent ORDER-only expressions reuse
the previously computed key value. Explicit references to two distinct target
ordinals remain distinct even when those targets have equal structural keys.
The shared execution carrier still memoizes each original Expr site: it does
not globally memoize raw SQL or merge two genuine target sites. Every plan
execution resets its carrier, retaining ordinary lazy evaluation, caller
snapshot/CID and demanded rows. Plain EXPLAIN performs no child execution.

Opaque unsupported child grammar (for example an aggregate's raw ORDER text or
unstructured table-function source) conservatively keeps unique site identity.
This is no-sharing, not rejection of an otherwise supported query and not a
claim that those remaining representations now have complete equivalence.
Structured preparation errors other than 0A000 are not swallowed.

## Matching candidate proof

New ExprHelper API/header signatures require a complete private rebuild. Fresh
58-TU O0 build, repeated no-change build and all object/source/header signature
plus binary-stamp checks (`13020`) are terminal 0. Frozen
`/tmp/dbms-sublink-sort-identity.hbGoYyn5/dbms_main.v1.o0` has SHA-256
`b08abf5ca40ed188dcc7320541feffb7b9af77f9630c271b6c47b1f0f59bd05e`.

New 56-control protocol matrix and the unchanged ordinary known-gap gate
(`43392`) are terminal 0. They retain alias/nonalias negative controls, genuine
target ordinals, structural casts/literals, true ancestry, different child
literals, repeated order directions, plain no-effects, real TEXT/JSON actual
rows, and the original 1/2/1 sequence expectations. PIDs 343667/345507 stopped
in finally and are confirmed gone.

Corrected nine-native group `33507` is terminal 0: new pure-metadata identity
(including typed NULL/different datum slots, nested CTE/ancestor provenance and
zero writer effects), borrowed logical-source plan, prepared-query execution,
binding, typed EXPLAIN, stored-function atomicity/actual owner, scalar resolver
and routine ORDER metadata. The initial group `79066` is retained terminal 1
after seven actual passes because the harness requested a nonexistent test
filename; it was corrected without changing source or assertions. The final
stronger metadata-only COUNT/SUM controls (`1567`) are also terminal 0.

Seven matching adjacent protocol gates (`1399`) are terminal 0: ordinary WHERE,
ordinary ORDER, typed EXPLAIN's 46 cases in both formats, PL query binding,
SELECT INTO execution demand, stored-function atomicity and WITH scalar child.
No full default suite was launched. ROOT's newer checked-result and deferred
owner header combinations still require their own complete fresh build;
these private objects must not be linked against those different layouts.

After registering the new gate, final unchanged-header source/signature audit
and rebuild/repeat are complete with the identical frozen binary SHA. Only the
registration file changed, so production sources required no recompilation;
the linker publication and all 58 cache signatures were rechecked. Peer read-only
review found no directly demonstrable issue in the range/alias/CTE key or true
sort value slots; this is source review, not an extra runtime pass.
