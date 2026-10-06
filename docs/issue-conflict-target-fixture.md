# ON CONFLICT target-only fixture qualification

The old native RETURNING fixture intended to test the existing target row in
three ON CONFLICT WHERE predicates. Its bare `name` is ambiguous between the
target and EXCLUDED. PostgreSQL 17.2 returns `42702` for all three original
queries, even where SET has a constant RHS. Qualifying the target column
preserves the intended old-row filtering.

This test-only correction adds `conflict_t.` or `composite_conflict.` in those
three predicates. All expected values, NULLs, row counts, command tags,
RETURNING assertions and failure injection remain unchanged. Permanent
unqualified `42702` negative controls are supplied by the separate EXCLUDED
metadata-binding regression, not replaced with a successful expectation.

Exact original-versus-qualified PostgreSQL 17.2 SQL and rows are in
`/tmp/dbms-conflict-namespace.vRmowtQI/fixture.reference.final.log` (terminal 0,
temporary tables and final ROLLBACK). The corrected complete native fixture
also passes against the immutable pre-binder-fix 58-object development basis
with the existing width fix: handle `76141`, `fixture.old-native.log`, terminal
0. This does not claim a production binding fix. Initial mistaken new
reference assumptions and the original EXCLUDED `42P01` remain in the private
logs, rather than being presented as passing evidence.
