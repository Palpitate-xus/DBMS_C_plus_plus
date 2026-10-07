# Same-process heap extent publication and live marker ownership

This repairs a physical-file publication race, not complete SSI or coherent
multi-engine heap buffers. No public header/layout or existing assertion changes.

## Actual defects

Currenta7 normal-O2 unchanged `phase5_remaining_test` repeated ten times:
six pass/four fail at the disjoint indexed predicate's first commit. A separate
status-print diagnostic retains the assertion and reports two actual58030
failures, not40001. The original full4f failure is retained too.

The new deterministic native control pauses one allocator inside its WAL
barrier after creating `.extent_pending`. A second open allocator's flushPage
returnsfalse immediately (`waited=0`, `observer=0`); baseline81665 exits134.
Per-allocator flushMutex does not serialize this physical-file marker.

The first canonical-file mutex candidate passes eight scoped natives and ten
unchanged SSI repeats, but a new failed-owner control still exits134:
another allocator's open succeeds and consumes the failed live owner's marker
(`other_open=1`, `marker_retained=0`). That failure is not relabelled green.

## Repair and invariants

PageAllocator.cpp now serializes marker recovery, extent inspection, flush,
open and close on a canonical physical-file publication state. Relative and
absolute aliases converge. The state retains the actual live marker owner
through a failed flush; foreign live open/flush fails closed rather than
rolling back the owner's retryable cache. Ownership ends only after successful
cleanup, a confirmed absent marker, or owner retirement after buffer discard.
Atomic marker-write errors with an existing/unknown marker retain ownership.

The registry survives global-engine teardown and periodically prunes idle
unowned entries. Its mutex is never held while waiting on publication; the
public lock order is publication, allocator flush, allocation, buffer pool.
Independent source-only review found no concrete inverse lock edge; runtime
verification is separately recorded below. WAL barriers and actual I/O failures
are not suppressed. Closed-owner crash recovery still runs normally.

## Actual matching proof

Artifacts: `/tmp/dbms-root-native-rechecks.nqfnjb9y`.

| Gate | Actual result |
| --- | --- |
| `shared-extent-baseline.log`,81665 | 134; live foreign-marker failure |
| `ssi-normal-repeat.log`,91495 | 1; unchanged currenta7 O2 sixpass/fourfail |
| `ssi-status-repeat.log`,56764 | 1; two58030 failures, strong assertion kept |
| `shared-extent-candidate.log`,17131 | 0; first candidate eightnatives/tenSSI only |
| `failed-live-owner-v1.log`,95977 | 134; first candidate's failed live owner recovery remains wrong |
| `shared-extent-candidate-v2.log`,28444 | 0; complete stronger native, checkpoint, phase5, background worker, MVCC/update WAL/page/rollback guards; ten further unchanged SSI repeats allpass |
| `shared-extent-sanitized.log`,66019 | 0; same stronger native plus phase5, scoped ASan+UBSan |

Normal proof freshly compiles the changed PageAllocator with the formal shared
O2 flags. Every other production source byte, all headers, every58 donor object
signature and configuration stamp are audited against frozen ROOTa7. The native
drivers/stubs are fresh. It is not a second fresh all58 build or a full suite.
Sanitizer proof instruments PageAllocator, BufferPool, stubs and the two drivers;
other matching production units are uninstrumented, leak detection disabled.

## Separate data-loss defect remains open

An additional unchanged two-engine disjoint-insert probe adds a fresh reader's
two-row assertion. With this repair both commits return00000 but only id100
survives, threeofthree iterations (`two-engine-rows-v2.log`,92314 terminal1).
The publication mutex does not make independent heap caches or allocation
headers coherent. That silent lost-row defect is independently assigned and
must preserve both valid commits, both rows, restart data and dangerous SSI
abort controls. The old commit-only smoke is not sufficient proof.

Cross-process publication, hard-link aliases, general multi-engine cache/WAL
ownership and the complete storage/SSI families are not closed by this change.
No push, Actions enablement or user-deferred security/TDE work.
