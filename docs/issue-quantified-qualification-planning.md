# Top-level local-reference ANY qualification planning

This repair concerns the pure planning demand of SQL ANY qualifications. It
does not claim a complete semi-join optimizer, set-operation runtime, or the
whole query/DML family.

## Reproduced boundary

Strict PostgreSQL 18.6 (`server_version_num=180006`, isolated core profile at
127.0.0.1:15486) rejects `DELETE ... WHERE false AND id=ANY(SELECT 1/0)` with
22012. The matching candidate at 7ea28fa6 instead returned `DELETE 0`.
The original 16-query protocol baseline has eight such wrong-success queries;
the other eight lazy/priority controls pass. The expanded 19-query reference
also includes volatile scalar-left, child-local-only, and genuinely correlated
scalar-left operands. All reference assertions pass, including zero sequence
effects and unchanged rows after rollback. An initial guessed scalar-left
reference expectation was corrected only after retaining its actual PG18
22012 result.

PostgreSQL pulls eligible ANY SubLinks into its qualification join tree before
boolean expression preprocessing. Eligibility requires a parent-local relation
reference and a nonvolatile combining/test expression, and the qualification
walker recurses through AND, not OR or CASE. Primary source:
[prepjointree.c](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/optimizer/prep/prepjointree.c),
[subselect.c](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/optimizer/plan/subselect.c).

## Implementation

The explicit `PreparedQueryExecution::planStatementConstants` phase recognizes
that eligibility from the retained AST and real bound source occurrences. It
plans the actual RHS statement in the same carrier before simplifying the
qualification's boolean demand. Scalar-left child references and expanded
projection bindings retain their true owner: a child-local or higher-ancestor
column is not substituted for a local parent reference. Routine volatility is
looked up through the actual engine metadata, including nested scalar-left
queries; no routine or cursor is executed or constructed to infer eligibility.

SELECT targets and UPDATE SET expressions retain their existing earlier
planning order. In particular, `SET id=CAST(2147483648 AS INT)` still raises
22003 before a qualification child's 22012. ALL, constant/parameter-left ANY,
volatile-left ANY, OR/CASE-nested ANY, and default non-planning preparation
retain their lazy behavior. Shared original AST sites are not modified.

This is a CPP-only change, with no public header/layout or production-TU
change. Native tests install reader/cursor factories that fail on any access
and prove zero planning effects. The independent protocol matrix remains
registered without a known-gap skip. Full source/binding matrices retain all
their assertions; UNION ALL child execution and ordinary set-operation
descriptor consumers are separate remaining problems.

## Retained artifacts

Private evidence root: `/tmp/dbms-bound-dml-cursor.rTuMF7gk`.

- `qual-planning-baseline.log`: original 16-case protocol red baseline.
- `qual-planning-reference18-initial.log`: original guessed reference failure.
- `qual-planning-reference18-corrected.log`: corrected original reference.
- `qual-planning-reference18-expanded.log`: complete 19-case strict reference.
- `candidate-qual-planning/native.baseline.log`: same native assertions against
  old matching objects, exit 134; cursor/sequence checks are not weakened.
- `candidate-qual-planning`: dedicated changed-PCE object, final source/header
  hashes, audited unchanged 57 objects from the wholly fresh 58-object
  set-operation-metadata epoch, and test/protocol logs.

Final candidate SHA256:
`b3c48ae0c5637b365f3f75c7d75e2125b7e64d1f6b89be3011011aa3e7caeebb`.
Build/native handle 60736 and serial protocol handle 47946 are both terminal
0. Eight matching native tests pass (qualification planning, constant
planning, prepared execution, quantified execution, bound-DML query children,
prepared cursor, bound WITH DML, set-operation binding). The full new 19-query
protocol and eight adjacent scripts also pass: ordinary quantified DML,
physical-child restart, primary WITH DML, multisource WITH DML, quantified query
demand, prepared read-root planning, EXPLAIN root planning, PL query binding.
The expanded old protocol has nine wrong-success queries (18 failed state/tag
assertions), retained in `expanded.baseline.wire.log`.

The retained whole-37 diagnostic must not be relabeled a supported green gate
while its UNION ALL child lowering or cumulative effect assertions remain red.
