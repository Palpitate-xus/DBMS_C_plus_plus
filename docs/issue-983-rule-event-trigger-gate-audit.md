# Issue 983 — rule and event-trigger capability gates

Protocol regression commit: `64ab219f` (local only; not pushed).

## Finding and scope

CAT-19 requires a real rewrite-rule system and DDL event-trigger execution.
The current implementation has parser/AST shapes for these objects, but no
rule rewrite or event-trigger runtime. Their generic compatibility-object
handler is expected to reject requests with SQLSTATE `0A000` rather than
persisting an object that never affects queries or DDL.

## Verification

- `python3 tests/div14_feature_gate_test.py`: passed. PostgreSQL mode already
  checks CREATE/ALTER/DROP RULE and CREATE/ALTER/DROP EVENT TRIGGER for
  SQLSTATE `0A000`.
- Added extended-mode protocol cases for all six lifecycle paths; each still
  fails with `0A000` and the feature-not-supported message.
- The same E2E verifies that no `.pg_compat_objects` store is created.
- No production code changed in this audit, and no PostgreSQL 18.6 oracle or
  DDL/rewrite differential was run.

## Remaining work

CAT-19 remains partial, not complete. Rules do not rewrite statements, event
triggers do not receive real DDL command events, and event-trigger ordering,
transition state, transactional behavior, and catalog dependencies are not
implemented. This audit confirms only the fail-closed boundary.
