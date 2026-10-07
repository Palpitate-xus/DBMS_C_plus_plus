# Prepared mutation plan foundation

`buildBoundDmlPlan` constructs an actual `ModifyTable` operator backed by the
same `BoundDmlExecution` carrier it executes. Construction and describing do
not open a source, call a routine, or modify a row. Its first `next()` drives
one atomic mutation; later calls emit the actual typed RETURNING rows.

The plan retains true target identity and source occurrence positions. A
source reader is driven by an instrumented typed context node and cached for
multiple target candidates. SQL NULL is a cell property, not display text.
Whole binding, privileges and duplicate assignments precede structural root
planning. Source/INSERT SELECT hooks receive a weak reference to that genuine
carrier. They must preserve provider lifetimes and never render SQL to infer
rows or descriptors. Explicit close releases source callbacks; failed
execution cannot be retried through the same mutation operator.

The native driver covers pure DEFAULT preparation, volatile defaults once
per matching row, one execution per plan, target alias identity, actual
source rows including nullable cells, one source scan, real runtime counters,
late division rollback, restoration of a distinct ambient native session,
constant error priority, and no residual transaction/table locks. The first
fresh-58 build's driver failed because the prior generic context source did
not instrument next(). The retained diagnostic recorded correct data and
reads (2 rows, 3 reads, 1 scan) but zero runtime rows. The mutation's actual
source node now instruments those reads. This was not an extra SQL execution.

This foundation does not itself activate the SELECT-only EXPLAIN frontend.
The unchanged whole DML protocol matrix remains an explicit consumer gap
until the separate frontend integration is actually tested. View triggers,
MERGE, conflict actions and cursor mutations retain their existing lowering
boundaries; this is not all-DML or all-planner completion.

Public DML hooks and Operator's static plan-attribute virtual require every
production TU and test stubs to be rebuilt with matching headers. No TU was
added. Initial verification used a fresh all-58 O0 epoch, followed by only the
changed DML CPP and native driver with all remaining source/header bytes
matched against that epoch; it is not an all-O2 or all-sanitizer claim.

Foundation evidence: all-58 build `10040` completed compilation but its first
native exited 134 as described above. Corrected native `23210`, all seven
adjacent native drivers `68223`, and all seven complete serial protocol scripts
`33677` finished with exit 0. The protocol group retained WITH range/source,
bound SQL children, domain DEFAULT and transaction lifecycle, MV target errors,
interval WITH input priority and both typed EXPLAIN formats. Source/header
audits matched 57 unchanged production sources, the freshly rebuilt DML source,
and all 108 current headers. Frozen foundation server SHA-256:
`0d283ba03cd6b3185e05e7f4605178be1d6b3d61fee37eb65471727cb92d0387`.

## Explicit terminal provider release

An independent close baseline (`68199`) against the frozen foundation exited
134: a weak reference to a real source plan remained live after explicit
close without open. The instrumented source itself could release its reader,
but the facade's close skipped never-demanded sources. Root callbacks also
retained their provider captures. This stronger control was not part of the
original green foundation driver and is not relabeled as having passed there.

Terminal close now closes even never-opened source facades, releases row and
cursor factory callbacks, and clears finish/extra-child providers even during
cleanup failure. `releaseQueryCallbacks()` is terminal cleanup, not correlated
restart: it preserves the actual closed cursor graphs and counters. Extra
children are owned `shared_ptr<Operator>` graphs, separately retained before
their callback is cleared; no dangling display-only pointer is substituted.

The strengthened native retains pure and executed-but-undemanded source
weak-owner release, pure callback release, late 22012 unwind, and checked
executor automatic close. The same real target/producer pointers and runtime
counters survive close for rendering. A second all-58/new-header O0 build
`76185`, strengthened native `37289`, seven adjacent natives `50274`, seven
complete serial protocol scripts `85299`, and scoped DML/PCE plus driver/stub
ASan/UBSan `37780` all finished with exit 0. This is two instrumented production
TUs with the other matching normal objects, not all-58 sanitizer coverage.
Frozen close epoch server SHA-256:
`76e4f1bd5388f18c1a1d6ed683336cc3de29f3f30b024ed7211b719cd379caec`.

## Ordinary I/U/D EXPLAIN consumer

The frontend now selects the actual bound INSERT/UPDATE/DELETE statement
before legacy SQL normalization. Plain EXPLAIN builds/describes without open;
ANALYZE drives the same retained `ModifyTable` once and renders only after
success. The operator's real RETURNING rows supply its actual row counter;
without RETURNING, affected rows are not fabricated as emitted plan rows.

Source/derived JOIN expression lowering borrows the root's compiled carrier.
INSERT SELECT retains its actual source operator and typed cells with that
same owner; its full-row input is not coerced through scalar/quantified cursor
metadata rules. The first whole candidate failed two INSERT SELECT shapes in
both ANALYZE formats because their legitimate unknown input literal was sent
through the strict value-child cursor. That failure and its cumulative counter
drift are preserved. Scalar and quantified value children still use genuine
paired typed cursors, and their real graph owners remain alive for rendering.
The instrumented mutation source facade measures its emitted contexts; these
counts do not independently prove every physical provider's internal metrics.

The permanent unfiltered matrix has 33 controls across TEXT/JSON and
plain/ANALYZE: real volatile/default row demand, zero/false predicates,
nullable FROM/USING, correlated scalar and multirow ANY/ALL, precise static
and runtime errors, late unique failure rollback, dead CASE, real RETURNING
counts, and cumulative nontransactional sequence sentinel 33. Strict PG
180006 `71542` and corrected consumer whole `83666` finished with exit 0;
initial consumer `44907` finished with exit 1. Nine complete serial scripts
`22756` passed, including that whole matrix and all retained EXPLAIN,
WITH/source, quantified, DEFAULT, MV and input-priority adjacent controls.

Four further complete originals `54204` (WITH multisource and its boundary,
ordinary quantified DML and original Quant84) finished with exit 0. All ten
matching native drivers `83583` finished with exit 0; initial native wrapper
`5524` exited 1 only for a nonexistent last filename after nine PASS results,
and that wrapper failure remains recorded. Scoped DML/PCE ASan/UBSan server
whole `21137` finished with exit 0; its other 56 production TUs (including
main) are matching normal O0 objects, not sanitizer coverage. Actual consumer
epoch started from the fresh-58 close build and rebuilt only changed main and
DML CPPs; all source/header/object/stub/flag imports were audited. Frozen
normal consumer SHA-256:
`40470eea598ce875b5878e2ffeba86f66e1bfc76f758ee8b88c80dc9d7635c42`.

This is not all-DML EXPLAIN completion. WITH-final-DML envelopes, view
mutation/trigger plans, conflict actions, cursor/inheritance lowering, and
MERGE are not claimed. Additional strict-18 phase evidence independently
retains known next roots: omitted/explicit INSERT DEFAULT constants must be
planned even for empty input; JSON descriptors need OID 114 in Simple/P-D;
Parse is analysis-only for 1/0 but rejects bad unknown CAST input. Read-only
plain EXPLAIN succeeds while ANALYZE rejects physical writes. Those stronger
phase/default controls were not removed or replaced by a known-gap green mode.
