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
