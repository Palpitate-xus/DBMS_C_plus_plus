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
