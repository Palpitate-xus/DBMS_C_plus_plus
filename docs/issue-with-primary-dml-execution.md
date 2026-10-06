# WITH primary DML execution

This fixes a primary `INSERT`, ordinary `UPDATE`, or ordinary `DELETE` in a
genuine prepared `WithStmt`. It is not a completion claim for the WITH/CTE,
query-binding, assignment-type, or routine families.

## Root cause and boundary

The former textual WITH executor treated final DML as a SELECT projection.
The unchanged `interval_with_dml_input_sqlstate_protocol_e2e_test.py` received
42703 for `into` instead of 22015. Worse, a writing CTE had already consumed a
sequence before the final invalid interval input was rejected.

The executor now retains the whole prepared AST. The primary target is its
physical relation identity, not a same-spelled CTE. Logical CTE producers use
their original statement identity and typed positional output descriptors;
they do not create temporary surrogate tables or run internal DDL. Duplicate
output labels, SQL NULL, empty text and the ordinary text `NULL` stay distinct.

One DML statement boundary surrounds the primary command and all CTE writes.
Referenced producers run on demand. After a successful primary command,
unconsumed writing producers run once to completion, including final LIMIT 0.
An immediate primary failure does not start an unused writer. This ordering
matters for irreversible sequence calls, not just row rollback.

The caller's command read view is not advanced between CTEs. A VOLATILE
function may obtain its own newer view and see prior CTE writes plus its own
writes; its return restores the fixed caller view without discarding the
transaction-wide command counter or combo-CID map.

## Evidence

Artifacts are retained under `/tmp/dbms-with-dml-runtime.aTCLEN`.

- `baseline-interval-with-f92.log`: frozen f92 binary
  `036030dd7a3d96c29c26a0f37e4b16a60500d7b741c14946fe6c2efb6e4b23a7`,
  unchanged registered test, four failed assertions, including a real
  irreversible sequence effect.
- `baseline-runtime-f92.log`: expanded final-DML controls against the same
  frozen binary; failed expectations were preserved.
- `reference-runtime-v1.log`: initial reference expectation incorrectly
  assumed an unused writer ran before an immediately failing primary INSERT.
  Actual PostgreSQL 17.2 returned currval 55000. This failed diagnostic was
  retained; v2/v3 expectations follow the actual reference result.
- `reference-runtime-v3.log`: full temporary-object PostgreSQL 17.2 matrix
  exited 0, including dependent versus unused writer errors, fixed sibling
  visibility, VOLATILE-function read-after-write and explicit savepoints.
  This is a 17.2 diagnostic, not PostgreSQL 18.6 parity evidence.
- `candidate/dbms_main.v1.frozen`, `wire-candidate-v1.log`: first fresh
  58-object candidate. The original interval test passed, but the expanded
  suite retained two failed assertions from the unused-writer scheduling bug.
- `unused-input-cast-reference.log`, `unused-input-cast-candidate-v1.log`:
  a separate retained gap. PostgreSQL rejects an unused CTE's
  `CAST('bad' AS INT)` at analysis (22P02), but does not evaluate unused `1/0`.
  The current preparation hook validates interval input, not every type's
  unknown-literal conversion; the latter candidate diagnostic remains red.

Intermediate V2 matching results (before the additional native/view and
duplicate-target controls):

- First full development build 22302 exited 0: 58 fresh objects and fresh
  test stubs, source/header audits 0. After the scheduling correction only
  main was rebuilt (82871, exit 0); the other 57 sources and all headers are
  unchanged. `candidate/sources.final.audit` and `headers.final.audit` pass.
  The build uses the existing TLS stub configuration, not real TLS evidence.
- Final binary SHA256:
  `7fc033049e77160cfed042a5c97072ca4d3d39e2756c7c55d3f2ea17a8e8dea8`.
- `wire-candidate-v2.log`, 23992 exit 0: original registered interval WITH
  test and the new 39-control execution matrix both pass.
- `native-final.log`, 65271 exit 1: eight native tests passed; the last
  requested test filename did not exist. This harness failure is retained,
  not counted as a ninth pass. Corrected `native-corrected-final.log`, 37477
  exit 0, runs all eleven distinct matching native tests successfully.
- `wire-adjacent-v2.log`, 18502 exit 0: eleven serial adjacent scripts pass,
  including existing DML CTE snapshot controls, scalar WHERE/order, PL binding,
  stored-function atomicity, typed UPDATE, both EXPLAIN formats and the
  unchanged original nine-control function-clause diagnostic.

These are isolated matching development-object results. ROOT's combined
optimized build and its complete canonical suite must be verified separately.

## Final V4 matching proof

The strengthened controls exposed two further candidate failures before
submission. `native-command-view-v2-red.log` / 85998 exited 134 with actual
base rows 1 instead of 0: an ownerless native unit had not established a fixed
command view. `native-edge-v2-red.log` / 99375 and `wire-edge-v2-red.log` /
14398 retained the duplicate-target/no-sequence-effect failures.

The unit now establishes a command boundary only when its caller has no
active one. Earlier native writes remain visible, while this unit's CTE writes
are hidden from its base scans. A cleanup guard runs after statement undo and
only touches the same still-active xid; normal server/SPI callers keep their
existing view. Both implicit and borrowed native transaction controls pass.
Duplicate assignment rejection follows whole static expression preparation
and precedes all execution; it does not fold numeric narrowing or division.

V4 explicitly depends on ROOT's ordered UPDATE AST 926f8856 (private mapping
bbdd1e56), and window metadata a0940bb4 (private ae648c40). A new complete
58-object build was required after the public AST change; no old-header object
is reused. These dependencies are already separate ROOT commits, not part of
this executor root-cause change.

- `build-candidate-v4.log`, 66705 exit 0: all 58 production objects and fresh
  stubs. `candidate-v4/sources.final.audit` and `headers.final.audit` exit 0.
- Final immutable binary `candidate-v4/dbms_main` SHA256:
  `b26b40ebe8be1d22664970df4f2fc2704fa078f8cea2ebc5f9bf43b4bf0d6d81`.
- `reference-runtime-v5.log`: expanded 47-control PostgreSQL 17.2 diagnostic
  exits 0. Only temporary tables/sequences/function namespace are used.
- `wire-candidate-v4.log`, 62007 exit 0: unchanged interval WITH gate and all
  47 execution controls pass, including identical assignment sites retained
  by the real ordered AST, quoted canonical duplicates, unknown lhs/rhs/callee
  priority and invalid boolean WHERE before duplicates.
- `native-final-v4.log`, 39047 exit 0: fourteen distinct matching native tests
  pass, including native command view and prior writes, new bound DML, original
  carrier/NULL/ordinal controls and the ordered-target/window dependencies.
- `wire-adjacent-v4.log`, 6651 exit 0: twelve serial adjacent scripts pass,
  including all original duplicate-target controls (the nested window input
  priority case remains 22P02), and the original nine function-clause controls.
- `asan-final-v4.log`, 53667 exit 0: ASan/UBSan on the new bound-DML test and
  DmlExecutor TU, linked to the matching unsanitized remaining objects. This
  is partial-TU instrumentation, not a whole-server sanitizer claim.

Every listed V4 handle is terminal. The runtime source/header/object group is
frozen after these checks; ROOT still needs its own combined optimized proof.

## Supported subset and remaining work

The execution path lowers FROM-less and single physical/logical range SELECT
children, VALUES CTEs, scalar correlated children, typed scalar WHERE/order,
ordinary RETURNING, final INSERT SELECT, and ordinary UPDATE/DELETE.
Whole supported expression/range preparation precedes opening a source or
executing any writer. SQL failures retain structured state and roll back the
entire unit; an explicit user's earlier successful command/savepoint survives.

Additional source lowering is still needed for joins, derived sources, GROUP,
window/set/recursive queries, UPDATE FROM/DEFAULT, DELETE USING, conflict
actions, MERGE and versioned RETURNING namespaces. Unsupported shapes fail
explicitly; they are not interpreted as a reduced query. General common-type
selection, integer-literal/operator descriptor width, all-type assignment
coercion and preparation-time input conversion remain separate work. The
permanent `with_unused_input_cast_known_gap.py` preserves the latter failure
without changing its expected state or labelling it a supported green gate.
