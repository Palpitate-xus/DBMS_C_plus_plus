# Bound DML owns paired cursors and compiled root expressions

`BoundDmlExecution` prepared genuine typed expressions but installed only a
full-row scalar reader. A reached SQL ANY/ALL requires an actual cursor;
its carrier therefore raised `0A000` rather than executing the bound child.
It also had no opt-in constant-planning phase on the carrier that mutated rows.
For example, `UPDATE target SET v=1/0 WHERE false` incorrectly succeeded.

Both public bound-DML APIs now append a default-empty
`PreparedChildCursorFactory` and a default-false `planRootConstants` flag.
`prepareBoundDml` retains its existing source-provider fourth argument;
`executeBoundDml` retains reader/source-provider fourth/fifth arguments.
Existing callers remain source-compatible, but the exported function ABI and
public header change require a wholly matching rebuild. There is no new TU.

The actual carrier owns the paired factory. It completes metadata, target
privilege and duplicate-target checks, plans its own execution-owned roots
when requested, then binds/prepares those same roots. It never plans an
already private-key-rewritten AST or discards a separate preflight clone.
INSERT SELECT retains its real full-source reader; scalar/quantified value
children use the paired typed cursor. Explicit normal/error cleanup closes
owned child cursors and preserves the original dynamic primary exception.
Legacy callers without a cursor provider retain their lazy default behaviour.

Main's WITH primary passes the paired provider and true root flag. Writing
CTE producer calls pass the paired provider with the false default. Child
SELECT/VIEW/CTE constant planning is not globally enabled.

## Retained evidence

Private basis: `/tmp/dbms-bound-dml-cursor.rTuMF7gk/repo` from ROOT
`d2e8c0ac`. Artifacts are under its parent directory.

- `baseline-build.log`: all 58 O0 production objects plus fresh stubs,
  source/header/flags audits pass; immutable binary SHA256
  `50908dc477a67b44f7eed95d42e06e9db07988e56a39794d536a41ee5a9e79c1`.
- `baseline/native.log`: the decisive old-API test exits 134 with seven
  failed controls, no cursor creates/reads/closes, and actual `0A000`/wrong
  constant-priority states. An earlier missing test-local `SqlCell` alias is
  retained as a compile/harness failure, not a production red.
- `candidate-api-build.log`: wholly fresh all-58 new-header O0 build and
  matching source/header/flags audits pass; seven distinct native tests pass.
- `candidate-api-v2-build.log`: one subsequent DML object rebuild with exact
  other-57/header guards, seven distinct natives pass again, including the
  explicit old/default lazy-compatibility control. Immutable SHA256
  `851c68fa6d9e0faeb88046a4a6c4ce14c12cc883071a84d87ef7a05c63c27e30`.
- Native controls use real query graphs and declared nullable cells, correlated
  OLD-row binding, SQL cardinality 21000, exact analysis/runtime SQLSTATE,
  distinct cursor sites and a forbidden full-row-reader fallback.
- `candidate-api-v2-adjacents.log`: nine serial, complete adjacent protocol
  scripts pass (`WITH` primary/multisource/boundary, MV target, RETURNING
  transition, quantified demand, ProjectSet, PL binding and atomicity).
  Each wrapper stops its owned server; this is the isolated O0 candidate,
  not ROOT's formal O2 combination or the still-red whole matrix below.

## The full protocol matrix is still a required gate

`bound_dml_query_children_protocol_e2e_test.py` preserves a whole 31-query
matrix with cumulative, never-reset sequence sentinels and full row rollback
checks. `reference18-corrected.log` passes strict PostgreSQL 18.6 / 180006.
The diagnostic is checked in but not registered as a passing supported gate
until its distinct consumer/source/planning roots are repaired.
The first reference log retains and explains the corrected initial expectation:
an outer-variable ANY qualification can be pulled into a semijoin before
false-qualification simplification; the constant-LHS and dead-CASE controls
must still skip that child. PostgreSQL's primary optimizer sources describe
that preprocessing order and the parent-variable prerequisite in
[prepjointree.c](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/optimizer/prep/prepjointree.c)
and [subselect.c](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/optimizer/plan/subselect.c).

`baseline-wire-corrected.log` and `candidate-api-wire.log` retain the whole
failures rather than reset counters or drop unsupported examples. The API
candidate fixes WITH scalar/quantified values, typed NULLs/RETURNING,
root SET/child division priority before an unused writer, fixed CTE/base
visibility, and late-error rollback. Remaining independent roots are ordinary
Q-DML dispatch, physical-source restart with captured WITH frames, UNION ALL
child lowering, and local-variable ANY qualification planning priority.
This API commit is not a claim of that whole gate, ROOT formal combination,
or complete DML/planner families passing. Those consumers remain required
follow-up work.

## Ordinary quantified-DML consumer

The next independent consumer routes genuine parsed I/U/D quantifier roles
through the existing retained-query runtime and its actual paired carrier.
It leaves ordinary non-quantified `executePreparedUpdate` unchanged. VIEW
targets are identified by their bound physical occurrence and `viewQuery`,
and delegated to the existing typed trigger boundary. ONLY/current-cursor,
UPDATE DEFAULT and INSERT conflict/override/default contracts remain on their
established paths, not a fail-and-retry fallback after execution.

`ordinary_quantified_dml_protocol_e2e_test.py` is a separate, whole 23-query
consumer matrix. Strict 180006 reference and matching O0 candidate pass;
`candidate-ordinary/ordinary.baseline.log` retains the original old-entry
failures. It covers all three commands, SQL and array ANY/ALL, empty/NULL
semantics, quoted physical correlation, nullable/empty/text-NULL RETURNING,
two genuine scalar sites, exact static/cardinality/late-runtime errors and
complete row rollback with non-reset sequence sentinels.

`candidate-ordinary-build.log` records a fresh main object with the same
all-58-new-API O0 epoch: other-56 source hashes, the V2 DML source hash and
all public headers remain exact. Immutable SHA256 is
`86a855ba6686951f5dd78ec644fb92d2b116e98699a571a308b53bc202c725e3`.
`candidate-ordinary-adjacents.log` is authoritative terminal 0: the whole
23-case matrix plus seven distinct complete adjacent scripts pass (typed
UPDATE, duplicate priority, WITH primary/transition, MV target, typed VIEW
trigger, quantified demand). This does not imply a whole formal O2 build.
Seven matching native dependency binaries were rerun successfully in fresh
isolated working directories; main is not part of those native binaries.

The unchanged original 31 queries remain in the expanded 34-query whole
diagnostic (`reference18-ordinary-expanded.log`, strict 180006 pass).
`baseline-ordinary-expanded-wire.log` and `candidate-ordinary-wire.log`
retain its remaining correlated WITH-frame, UNION ALL and local-Var ANY
preplanning failures, including every cumulative counter expectation.
No narrow consumer result closes those source/planner roots or the full
DML/query families.

## Physical child restart under ancestor WITH

A separate source-consumer fix removes the blanket `frames.empty()` guard.
Restart eligibility is now metadata-only: the child has no local CTE
definitions and each leaf is an actual physical occurrence belonging to that
exact child/source AST, with neither a CTE producer nor a VIEW query.
Each invocation rebuilds the real source node and its scan/join cursor with
the new nullable caller row. It does not change a project row while retaining
a stale parameterized producer. Derived/local CTE/VIEW producer restart
continues to require its own explicit lifetime contract.

The whole 13-query `with_physical_child_restart_protocol_e2e_test.py` passes
strict 180006 (`physical-restart-reference18.log`). The old ordinary binary
fails it (`candidate-correlated-v2/restart.baseline.log`); the source candidate
passes it and seven complete serial adjacents
(`candidate-correlated-v2-adjacents.log`, terminal 0). Controls include quoted
caller aliases, multiple physical join leaves, scalar/ANY/ALL/NULL children,
two different caller rows, an unused writer after successful primary,
rollback after late 22P02/21000, static errors before effects and RETURNING
reading the caller's unchanged command snapshot.

Fresh main plus exact other-56/DML/header guards pass; SHA256 is
`1d527d8bd7d745ee0980eb5d76429041009ea40fc8726de8211e733beebd585c`.
This is the same all-58-new-API O0 epoch, not ROOT's formal O2 combination.
The first source compile rejected a mistyped AST field (`isJoin`); its log is
retained under `candidate-correlated`, not counted as a runtime failure.
The corrected V2 source has no public-header change.

All original 31 and expanded 34 queries remain in the now-37-query diagnostic.
`reference18-restart-expanded.log` passes strict 180006; the complete source
candidate log still fails the genuine UNION ALL child and local-Var ANY
preplanning controls, including their original cumulative sequence values.
Those are the next independent roots, not exclusions from a passing full gate.
