# Prepared scalar FETCH WITH TIES has no actual relational lowering

Status: finite positive-count implementation committed; complete FETCH/wire
composition remains held. No overall-family or current-master success claim.

## Root cause and finite implementation

Current83/ec1b1a02 (production479675f1) reproduces all three original whole
failures: ordered scalar child FETCH WITH TIES is rejected with0A000 by
`supportsPreparedSelectShape`/borrowed plan construction. An unordered child
incorrectly gets the same0A000 instead of PostgreSQL42601.

The actual typed sort now exposes its already evaluated key slots and compares
peers using their NULL state, declared type, collation and bound enum order.
An actual prepared LimitWithTies consumes the existing Project/Offset stream
and retains its real sort owner. All keys participate; NULL keys are peers.
No sort key, target or child expression is recomputed for the peer test.
Zero demand does not open the child. EOF and typed errors remain distinct;
structured cells, NULL bitmap, typed values and outer-context restart survive.

Strict PG18 evidence demonstrates that FETCH WITH TIES actually executes the
projection's first untied boundary probe: a volatile non-key target gets two
calls for one returned row. OFFSET1 adds its own projected row, giving three.
Two genuine scalar child sites get four total calls, not one memoized call.
The new limiter intentionally preserves those existing projection sites.
The new strong tests retain all original SQL and prove these actual counts.

The ordinary SELECT parser checks the actual WITH TIES grammar before analysis.
A finite scalar/EXISTS envelope syntax preflight recognizes real FETCH tokens
and the parsed child, not quoted data/identifiers or a rewritten condition.
It preserves source bytes/AST envelopes, performs no catalog lookup/execution,
propagates the no-ORDER syntax failure across nested binding-parse contexts,
and retains the existing128-level query nesting boundary.42601 takes priority
over missing child/outer relations, bad names and bad typed inputs. COUNT,
UNKNOWN child outputs and aggregate lowering are not changed.

## Complete actual evidence, including retained reds

Artifacts: `/tmp/dbms-scalar-fetch-ties.9fUZaolW`.

| Entire invocation | Actual terminal result |
| --- | --- |
| Current83 original three whole wrappers, session90091 | 1; all three fail at original scalar FETCH0A000/no-ORDER priority |
| Original three unchanged assertions on owned PG18.6/version180006/C/libc using only a reference connection/schema adapter | 1; review and fetch wholly pass; subquery SQLSTATE's final old DISTINCT assertion expects0A000 but PG actually21000 |
| Correct actual-public-API new native baseline, session15487 | 1/body134; first positive child plan rejects0A000 |
| Final strong complete new strict reference matrix | 0;142 controls, no differences |
| Candidate-v2 positive normal, session79462 | 0; fresh sole parser plus current changed ExecutionPlan receipt and56 exact479 normal donors; all58 current receipts/cache/repeat/freeze proved |
| Complete fresh drivers/stub15 native, session47210 | 0; all fifteen whole tests passed, including real public graph, typed NULL/empty/exact BIGINT, scalar21000 and no-ORDER preparation priority |
| Complete14 whole wrappers, session13241 | 1; twelve entire original neighbours pass, including original review/fetch; new strong matrix and original subquery SQLSTATE remain red |

Candidate SHA256:
`63ae121d2ec5354d7a77d910f758f9150ea5ef87c4f7bed67bdd6314b68d41fb`.
Donor is exact current479675f1
`/tmp/dbms-window-nth-frame.ul6nHapu/repo`, normal58 receipts/source/header/
actual flags/TLS/manifest/cache stamp proven before reuse, immutable SHA256
`0054bcc5f94840604c916280eeee5495c318aaa08ff8a9a9e1c292a63cd8c8c1`.
No public header changed and this invocation is not another genuine fresh58.
Both whole test layers use original default disk/deadlines, not trimmed cases.

The142 strict controls include transaction/error-savepoint operations; all83
candidate controls were collected. Its19 failure records remain exactly:
negative FETCH child42601 versus2201W; source-free EXISTS incorrectly false;
five main-query unprojected ORDER keys rejected0A000; twelve INTEGER/BIGINT/
stored-routine extended scalar OID/width mismatches (wrong TEXT metadata),
including both Parse/Describe and Execute. The finite positive scalar values,
peer cardinality, NULL/empty and real occurrence/effect checks match reference.
No new strong statement/assertion is removed to hide these consumers. Original
subquery SQLSTATE still exits1 at its unchanged DISTINCT0A000 assertion, while
the candidate now correctly gives PG21000. That fixture contract is separate.

Earlier author versions remain frozen: nativev1 used an incorrect public
factory signature (compile failure, not a DBMS regression); nativev2 assumed
the structured NULL payload was display text `NULL` (assert134, not a DBMS
failure). Corrected native assertions use actual cells plus a real typed-NULL
cursor check. New protocolv1 had incorrect peer/input-priority/call-count
expectations; complete strict log retains all12 failed records. Nativev1,
nativev2 and wirev1 source snapshots and their SHA256s are retained in the
artifact directory. New expectations were calibrated to actual strict180006,
not weakened old fixtures. Candidate-v1 complete15/14 logs also remain red.

The signed/negative FETCH descriptor and runtime demand error2201W require a
separate genuine AST issue and new header-consistent fresh58. Main/wire/EXISTS
consumers remain open and will be handled independently. This private Source83
proof is not Root84/current aggregate ABI evidence; precise current-root
composition is still required. No master edits, push, Actions or user-deferred
security/TDE tasks were performed.
