# INTERVAL INSERT SELECT input analysis

Status: source consumer implemented against the independent WITH/DML metadata
callback (`f92acb26`). Matching 58-object validation passes five native tests;
the complete protocol matrix retains two frontend error-priority failures for
the independent Network preflight fix. No whole-family completion is claimed.

## Actual old failures

The immutable baseline includes the shared interval parser, strict VALUES
grammar, quoted UPDATE fix, and direct interval SQL input classification. It
does not have static INSERT SELECT target-input analysis.

- Native `INSERT INTO target_rows SELECT 10,'2147483648 months' WHERE false`
  succeeds with zero rows instead of structured `22015`.
- A declared TEXT source containing the otherwise valid `1 day` is accepted
  and written to INTERVAL instead of rejected with `42804`.
- TEXT SQL NULL, table/stared TEXT output, integer/array source types, and
  derived unknown output finalized as TEXT also lack the proper static check.
- Invalid typed literals/direct unknown-to-INTERVAL casts are hidden by a
  false WHERE clause. Volatile source expressions can run before input failure.

Evidence is private at `/tmp/dbms-assignment-input.gZVw19Xo/`:

| Evidence | Result |
| --- | --- |
| `native.baseline.log`, execution 78528 | Exit 134: first zero-input assertion |
| `wire.baseline.initial.log`, execution 50800 | Partial baseline; wrong TEXT writes later caused 23505 |
| `wire.baseline.complete.log`, execution 22107 | All original controls ran, 32 failed assertions |
| `wire.reference.complete.log` | PostgreSQL 17.2 complete reference, exit 0 |
| `wire.reference.priority.log` | PostgreSQL 17.2 plus typed-input priority controls, exit 0 |
| `wire.reference.star.log` | All original controls plus mixed multi-column star/literal, exit 0 |
| `wire.baseline.star.log` | Matching old source, 37 failed assertions |
| `verification.consumer.V1.log`, execution 64910 | Five natives pass, complete wire retains two frontend priority failures |

The accumulating protocol test records every unexpected write before cleanup
isolates the next case. These are assertion counts, not counts of independent
bugs. PostgreSQL 17.2 is the runtime diagnostic reference, not a PostgreSQL 18
differential claim.

## Analysis, planning, and runtime must stay distinct

| Expression at INTERVAL target | Analysis action |
| --- | --- |
| Direct unknown SQL string or bare NULL | Contextual input conversion after the SELECT source binds |
| Declared TEXT, integer, array, or typed TEXT NULL | Reject 42804 without reading a row |
| Typed INTERVAL literal or direct CAST/:: from unknown string | Convert input at that expression's transform site |
| Already typed CAST chain, arithmetic, function, parameter, or scalar child | Do not evaluate merely to get type/input metadata |

Actual PostgreSQL controls distinguish these orders. An unknown bad target
string with a missing WHERE function reports `42883`; a projection containing
`CAST('2147483648 months' AS INTERVAL)` with the same missing WHERE function
reports `22015`; a typed invalid INTERVAL literal reports `22007`. Numeric CAST
narrowing and division are not unknown string input conversion and must not be
folded earlier as a shortcut.

The provider must retain actual AST/source identity and descriptor ordinals.
Direct unknown projection leaves retain contextual type; a derived/CTE output
finalizes unknown to TEXT. SQL NULL must retain its declared source type. Source
rows, the first value, `toString()` rendering, and volatile evaluation cannot be
used as metadata providers.

## Required execution proof

The permanent native and registered protocol tests retain zero-input errors,
TEXT and typed-NULL mismatch, declared table/star/derived source descriptors,
source/WHERE/RETURNING error order, four irreversible sequence demand controls,
valid empty command tags, microseconds, SQL NULL, quoted target order, OID 1186,
and transaction abort/rollback checks. Fresh matching objects for the new public
metadata callback/AST layout are required before marking this consumer ready.

The separate registered `interval_with_dml_input_sqlstate_protocol_e2e_test.py`
retains the original WITH INSERT failure, writing-CTE side-effect control, and
transaction checks. It is not skipped or counted as fixed by this adapter.
Other target types, INTERVAL arrays/infinity/typmods, and general static type or
planning-time coercion are not closed by this scalar INTERVAL change.

The remaining two protocol failures occur before this consumer is entered:
Network's isolated WHERE-function scan reports 42883 before a typed projection
input error can be analyzed. The fix must defer only a strictly parsed direct
INSERT SELECT to its whole-statement DML preparation, without changing database,
permissions, transaction phases, or raw WITH handling. The full original matrix
and both typed-priority assertions remain mandatory for combined validation.
