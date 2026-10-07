# Signed FETCH input must retain its actual runtime demand

Status: finite signed INTEGER literal descriptor/typed Limit consumers fixed;
ordinary legacy aggregate/window consumers and the complete FETCH family
remain open. This is private Root84 evidence, not current-master completion.

## Genuine input, grammar and demand

`SelectStmt` previously retained only a non-negative `size_t` cap. Actual
binding rejected negative FETCH with42601, although PG18.6 accepts signed
integer grammar and reports2201W when that query's Limit is demanded.
The new appended `signedFetchCount` retains the real signed64 input, including
INT64_MIN. Both genuine FETCH grammar receivers consume signed tokens without
absolute-value overflow. Only non-negative input produces the existing cap;
negative input has no fake size_t zero, sentinel or executable row limit.
All existing public AST/enum/Window fields are retained.

An actual `PreparedNegativeFetchOp` owns the existing graph at its real Limit
site. Graph construction, binding and static descriptors are pure. Opening
the Limit reports2201W before opening its physical source, qualification,
sort, projection or set-returning arguments. A parent false/zero/dead CASE does
not demand its child. It forwards genuine graph/structured/outer-row contracts
and preserves ordinary SELECT, UNION ALL and ProjectSet consumers. Scalar
child cardinality is not substituted for the count error. The two existing
legacy scalar/EXISTS demand sites also check the genuine signed descriptor.

The Main frontend propagates the parser's known no-ORDER WITH TIES grammar
error42601 before its legacy negative-count fallback. Its signed admission
uses a valid retained FETCH AST plus the actual quote/comment/depth-aware
top-level FETCH token owner. It admits explicit+/- spellings, including+1 and
-0. Ordinary unsigned FETCH retains its original physical/index consumer.
No SQL literal, comment, quoted range name, actual parameter or condition is
rewritten or treated as a constant to discover this metadata.

The new descriptor has two additional pure consumers: sort-child identity
must distinguish-1/-2 and INT64_MIN/INT64_MIN+1 while preserving canonical
-01/-1, +01/1 and-0/+0 equivalence; a stored default's expression-only envelope
must not silently accept a negative FETCH clause when its cap is absent.
The identity/default native test uses the public binder's declarative,
metadata-only relation/default callbacks. It neither edits a catalog nor
executes a query/routine/source row to guess metadata.

## Complete actual evidence, with earlier reds retained

Artifacts `/tmp/dbms-fetch-signed-count.zzUfQaxe`; immutable driver/helper
`verify-signed-current.sh`. Actual owned PG18.6 is version180006, C/libc.
Original SQL/assertions and default protocol disk/deadlines are preserved.

| Entire invocation | Actual terminal result |
| --- | --- |
| Root84 + positive dependency baseline normal68808 | 0; ExecutionPlan/parser fresh plus56 exactly proved fd46 donors; all58 receipts/cache/repeat/freeze verified |
| Signed native baseline58167 | 1/body134; expected2201W, actual42601 |
| Complete corrected signed baseline52288 | 1;134 controls fully collected,106 difference records |
| First actual new-header normal30717 | 0; all58 truly freshly compiled, every old receipt invalid under the new public AST signature, no donor migration |
| Signed-v1 complete16native87325 /16whole5344 | 0 /1; seven signed frontend differences plus old strong18 and unchanged DISTINCT assertion retained |
| Pure descriptor-consumer baseline21515 | 1; all12 controls collected, four identity/default failures |
| Signed-v2 normal71647 | 0; genuinely fresh Main/Binder/ExprHelper only, current remaining receipts retained, all58 source/header/flags/cache/repeat/freeze proofs verified |
| Signed-v2 complete18native96919 /16whole58159 | 0 /1; signed142 fully pass, original strong18 and unchanged DISTINCT assertion remain |
| Actual SRF native baseline71448 | 1/body134; negative ProjectSet returns rows instead of2201W |
| Complete extended SRF baseline10455 | 1; all152 controls collected,12 records including two real writer calls and consequent sequence differences |
| Final signed-v3 normal46063 | 0; fresh sole ExecutionPlan, unchanged new-header epoch; all58 receipts/cache/repeat/freeze proofs verified |
| Final complete18native54107 | 0; all whole native tests pass, including actual public graph,43 signed controls,12 pure identity/default controls and unchanged sort-child identities |
| Final complete16whole38980 | 1; fourteen entire neighbours/new signed matrix pass; original strong FETCH and old DISTINCT assertion remain red |
| Final strict signed wire | 0;349 complete controls, including error savepoints; candidate all152 controls pass |
| Original complete strong FETCH on strict18 | 0;142 controls; same candidate all83 controls collect the unchanged18 remaining records |

Final binary SHA256:
`d3d54e73f7f6b3eac1cacf1dfb301931f613a9910ada38433b08577627e4c3bb`.
Previous actual58-fresh epoch is frozen independently at
`dbms_main.signed-fetch.v1-genuine58.frozen`, SHA256
`409d5d44773d5bb7de002262b45da240b290e5379b5eb8c0cade1d879c96ff67`.
Signed-v2 immutable binary is `dbms_main.signed-fetch.v2-threeconsumers.frozen`,
SHA256 `a47456a5baa557a12e3715aed872690e1144cd54191391c52caf8842d980d870`.

The initial donor is exact fd46f1a7
`/tmp/dbms-root-aggregate-current.Yfb196kC/repo`; all58 actual normal receipts,
source/header/flags/TLS/manifest, cache stamp and byte-identical frozen binary
are proved before reuse. Frozen SHA256
`d0c81357d73eea52f52388bea0cb684b82ff9408f5f811ebc343fa8a6d864da3`.
That baseline migration does NOT count as another genuine fresh58; the new
AST build30717 really compiles all58. Later source-only rebuilds are not58
fresh either. Each native invocation compiles a fresh driver and stub and
uses a new owned cwd. Root85/current composition remains separately required.

The first protocol collector assumed RowDescription existed after a rejected
negative Parse. Its earlier actual1 log and frozen source/hash remain; the
correct collector records missing static metadata and continues all strong
assertions, without weakening expected types, SQLstates, values or effects.
Every subsequent baseline collects the entire matrix before reporting red.

## Explicitly open consumers and scope

The old strong FETCH matrix still has source-free EXISTS incorrectly false
(one record), five ordinary main queries with unprojected ORDER keys rejected
0A000, and twelve extended INTEGER/BIGINT/stored-writer scalar OID/width
records (wrong TEXT metadata at both Describe and Execute). Original
`subquery_sqlstate_e2e_test.py` still expects DISTINCT0A000 while both strict18
and the candidate actually21000; that fixture is unchanged, not silently
relabeled green. COUNT/UNKNOWN scalar-child lowering is independently owned.

An additional unchanged20-statement diagnostic compares empty/non-empty
ordinary SRF/aggregate/window/VALUES/set consumers against strict18. The SRF
negative/demand controls are now fixed and promoted into permanent tests.
Legacy Main COUNT/SUM/ROW_NUMBER still ignore the negative cap; VALUES FETCH
is rejected42601 by its existing grammar. SUM(1/0) on an empty relation must
raise PG22012 during pure planning before2201W, but that legacy host currently
returns NULL. These are independent actual consumers, not an excuse for a
universal early count throw which would steal planning-error priority.
The public pure whole-query planning API was independently exercised using
actual metadata: COUNT/SUM/ROW_NUMBER bind without executing rows and SUM(1/0)
and ordinary1/0 both correctly report22012. This supports a subsequent
finite genuine root-demand integration, not a present completion claim.

This finite descriptor accepts signed64 integer FETCH literals. Quoted,
NULL/parameter/expression FETCH counts and wider input/range behaviour are
not claimed complete. All unresolved original/new strong assertions and
diagnostic SQL stay retained. No master mutation, push, Actions, filtered
EXPLAIN/security/TDE/background task or catalog/fault injection was performed.
