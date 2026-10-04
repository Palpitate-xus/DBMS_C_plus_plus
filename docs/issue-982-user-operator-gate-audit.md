# Issue 982 — user-defined operator capability gate

Protocol regression commit: `1944c64c` (local only; not pushed).

## Finding and scope

FUNC-03 asks for runtime-backed user-defined operators, including commutator /
negator links, selectivity procedures, hash/merge joinability metadata, and
dependency tracking. The current parser only captures an operator object's
name for the DDL AST; the generic compatibility-object fallback deliberately
returns SQLSTATE `0A000` and writes no fake catalog object. Built-in operators
are a separate capability and do not satisfy this gap.

## Verification

- `python3 tests/div14_feature_gate_test.py`: passed. In PostgreSQL mode,
  `CREATE OPERATOR`, `ALTER OPERATOR`, `DROP OPERATOR`, and operator
  class/family create/alter/drop requests are checked for SQLSTATE `0A000` and
  the feature-not-supported message.
- Extended/project-extension mode is now explicitly covered for
  `CREATE/ALTER/DROP OPERATOR`; all three remain rejected with `0A000`.
- The test checks that no `.pg_compat_objects` store is created, so a failed
  operator definition cannot be mistaken for a persisted usable operator.
- No production code changed in this audit, and no PostgreSQL 18.6 operator
  oracle/differential was run.

## Remaining work

FUNC-03 remains partial, not complete. No operator catalog/runtime exists for
user-defined execution or planner integration: commutator/negator resolution,
selectivity support, hash/merge flags, object dependencies, and drop/alter
invalidation are still unimplemented. The verified behavior is only the
fail-closed capability boundary that prevents false success.
