# Primitive assignment input during pure preparation

## Fixed contract

`prepareBoundQuery` now uses a real INSERT/UPDATE target column's declared
builtin primitive type to validate a direct, untyped SQL string constant.
An invalid INTEGER input is `22P02`, including an UPDATE with no matching rows;
an out-of-range INTEGER input is `22003`. No routine, sequence, source, scalar
child or parameter is evaluated by this contextual conversion.

UPDATE first binds SET expressions, then transforms its WHERE expression to a
genuine boolean AST expression, then binds RETURNING, and only then applies
contextual SET input conversion. PostgreSQL 18.6 confirms that an unknown WHERE
or RETURNING function precedes the invalid bare SET input. INSERT VALUES retains
its distinct per-row transformation/assignment/RETURNING ordering.

The current primitive validator covers builtin integer widths, boolean,
numeric, real/double and UUID. It does not claim complete assignment coercion,
typmod, array, domain, custom-cast or all-type support. Numeric narrowing CAST
and arithmetic are not executed during this analysis phase. Planning remains
an explicit separate API phase.

## Retained evidence

All artifacts are under `/tmp/dbms-materialized-target.fBH1T6Yr`:

- `assignment.baseline.log`: unchanged foundation native baseline, terminal
  assertion failure with 10 mismatching controls; sequence/table no-effects
  checks passed. These are 10 assertions, not 10 distinct root causes.
- `assignment-reference18-v1.log`: strict server version `180006`, all 18
  analysis/planning reference controls pass, in an isolated rolled-back
  transaction. EXPLAIN does not ANALYZE or execute the tested statements.
- `assignment-final-build.log`: terminal 0; candidate compiles the changed
  TableManage and binder objects, reuses only the 56 source-hash-matching
  objects from the all-58 fresh O0 foundation, and audits all 101 unchanged
  headers. Both primitive assignment (20 controls plus no-effects) and the
  original pure constant planner native tests pass.
- `assignment-adjacent.log`: terminal 0; nine matching native regressions pass:
  prepared execution, prepared cursor, query binding, simple CASE constant
  demand/equality, WITH primary DML, interval assignment, DML RETURNING, and
  WITH transition RETURNING.

The immutable candidate is
`assignment-final/dbms_main`, SHA256
`7540428342de383b212022ad15920d480c79b9eaa64c8e0f227fb658f1471da1`.
There is no public header/layout change in this fix.

## Consumer gaps remain open

`tests/prepared_primitive_assignment_consumer_known_gap.py` keeps the complete
ordinary-wire matrix and PostgreSQL expectations. It is a diagnostic, not
registered as an already-supported green gate. Strict PG18.6 passes the entire
script (`assignment-wire-reference18-v2.log`). Candidate wire
`assignment-wire-v2.log` retains two genuine legacy consumer failures:

- `UPDATE ... SET id='bad' RETURNING missing_assignment_function(1)` publishes
  `XX000` instead of the binder's precise `42883`.
- Legal `INSERT ... SELECT '3' WHERE 'true' RETURNING id` gets `42601` instead of
  insertion success. Its subsequent SELECT correctly receives aborted-state
  `25P02`; those dependent assertions are not separate bugs.

The same two failures are present in the unchanged foundation binary
(`assignment-wire-foundation-baseline.log`). All other 17 analysis/input wire
controls, their monotonic sequence sentinels and table rollback checks pass on
the candidate; valid direct UPDATE string and NULL assignments also pass. The
first wire attempt's explicit VOLATILE-option setup was rejected by the old
CREATE parser (`assignment-wire-v1.log`); the second uses the same PostgreSQL
default-VOLATILE function without changing tested statements or expectations.

This commit closes the pure metadata assignment root cause, not the two consumer
gaps, materialized-view target denial, geometry CAST inputs, or the overall DML
and type families. No push or Actions were performed.
