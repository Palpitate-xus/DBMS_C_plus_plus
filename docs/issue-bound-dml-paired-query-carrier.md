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
