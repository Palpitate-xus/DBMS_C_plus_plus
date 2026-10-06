# Ordinary scalar subqueries in ORDER BY

After the separate WHERE fix, ordinary ORDER-only scalar children still
used the legacy clause collector: their functions were not executed, errors
were ignored and keys were not sorted. A lazy-branch unknown callee was not
prepared; the outer volatile projection actually wrote and committed three
rows instead of failing before effects.

The ordinary prepared consumer now also selects scalar ORDER expressions
and ordered scalar projections (including output aliases/ordinals). It
prepares the original whole query, executes the shared typed plan with real
nullable source cells and actual owner, preserves demand, and publishes only
successful structured output. Alias/ordinal sorts borrow the projection's
real target slot. A scalar child retains its per-site/per-execution memo;
genuine separate SELECT targets are never globally merged by SQL text or
datum values. Bare unknown WHERE literals acquire their SQL boolean context
by pure input conversion; explicitly typed integer/text NULL stays a 42804
error. This does not execute a routine during preparation.

## Evidence and limits

Private base `7cbd2c0d` depends on native routine fix `1d569746` and WHERE
consumer `7cbd2c0d`; their ROOT efc501b9 dependencies need no repeat cherry.
Frozen base binary SHA256:
`28238dd82ec024e7377f4661dbfd5f34f8e7433bc1d8d8a27b9c74777fb09188`.
New matrix baseline 10805 exited 1: cast/STRICT children returned SELECT 3
without errors, correlated/NULL/BIGINT keys were wrong, cardinality was
ignored, and unknown-callee pre-effect checks observed three surviving
projection writes. That actual baseline log is retained unchanged.

Main-only O0 rebuild 8052 exited 0. The other 56 objects use immutable
matching origins (native TableManage from 55337, other 55 from the full fresh
57-TU efc build); public headers/layout/flags are unchanged and audited.
Candidate SHA256:
`24f8933179e175c91e80728d531ab3f20d65b01ae4348d5b56ce0c2dc565cd54`.
New wire matrix 3306 exited 0: all 24 named controls, exact SQLSTATE,
rollback/pre-effect checks, real NULL/empty/text-null order, quoted BIGINT
metadata, one/two-depth correlation, LIMIT/OFFSET-demand semantics, CASE,
cardinality, separate target sites, alias/ordinal slots, and explicit
BEGIN/SAVEPOINT recovery. Native 92554 exited 0 for the new actual-owner
typed plan test (NULL/quoted keys, 22P02/21000, full-key demand and site counts).

Final serial group 37191 terminated 0 with four actual green scripts: new
WHERE matrix, new ORDER matrix, stored-function ORDER and the 46-case typed
EXPLAIN matrix. The same wrapper deliberately retains two nonzero diagnostic
runs, not green gates: unchanged original clause diagnostic now has exactly
one pre-DML UPDATE failure; the separately checked-in
`ordinary_scalar_sort_slot_known_gap.py` has two actual sort-slot failures.
All production source/header hashes were checked after validation. Artifacts
are under `/tmp/dbms-ordinary-scalar-consumer.I1bn6m0R`.

Isolated reference PostgreSQL **17.2** passed 25 controls using temporary
objects, BEGIN/savepoints and final ROLLBACK. This is not PostgreSQL 18.6
parity. The reference also confirms expected call counts 1/2/1 for equivalent
target/sort children, two genuine target sites, and equivalent ORDER keys.
The candidate currently gives 2/2/2. Those two canonical SubLink sort-slot
failures remain explicit separate work, with their real expectations retained
and not registered as a supported green E2E.

This commit repairs the staged ordinary scalar sort consumer, not every
top-level CTE/JOIN/view/catalog/group/window/multirow role, optimizer path,
static signature/type family or the total review checklist. ROOT's typed
UPDATE fix is independent and not present in this private baseline. The
shared query and CTE families remain partial.
