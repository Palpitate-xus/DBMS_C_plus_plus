# Preserve ON CONFLICT WHERE ambiguity as an error control

The complete protocol fixture originally expected successful rows from
`ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE name = 'expression-x'`.
Both the target relation and EXCLUDED expose `name`. Strict PostgreSQL18.6
(verified180006) rejects that exact original statement with42702, including
the attempted second VALUES row; no partial update or insertion occurs.

The test now retains the exact original SQL and asserts42702, no DataRow,
no CommandComplete, and the original target row with no inserted id6.
The original positive assertions are unchanged; the positive SQL explicitly
uses `WHERE dml_conflict.name = 'expression-x'` and retains both returned
rows and `INSERT 0 2`. No source/binder behavior is weakened to satisfy an
invalid fixture.

Actual strict18 reference exits0 in
`/tmp/dbms-with-cursor-integration.xOfpugBC/full-conflict-fixture-reference18.log`.
The isolated complete original protocol against immutable4897 CASE binary
SHA256 `3426cc22291b403e817623d14f58d9f90dba73706839ac1890959ce046e7347a`
completes all assertions through the corrected conflict control, but exits1
at the separate quantified EXPLAIN assertion, line2444. Actual log:
`/tmp/dbms-full-protocol-fixture.NlE2QKOo/full-original-protocol-qualified.log`.
Original uncorrected CASE21-wire gate2051 remains20passed/1failed at2332.
This fixture correction is not a full protocol pass or quantified repair.

Independent local test/documentation commit; no push, source change, active
Actions workflow or deferred security/TDE work.
