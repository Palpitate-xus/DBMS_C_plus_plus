# Prepared DELETE predicate context

The DELETE branch of the whole-query binder validated expression names but did
not supply the boolean WHERE context already used by SELECT and UPDATE.
`DELETE ... WHERE 1`, typed TEXT NULL and a TEXT column were accepted during
preparation instead of `42804`; an unknown `'bad'` string was not converted
and failed to produce analysis-time `22P02`. Invalid RETURNING routines could
also incorrectly take priority over these earlier predicate errors.

The binder now checks the static WHERE type and adds the existing genuine
unknown-to-boolean coercion before preparing RETURNING. It never invokes a
routine, query child or table reader to determine that type. This is an
independent metadata root cause, not a replacement of ordinary legacy DELETE
execution or a claim that every DELETE/USING predicate consumer is complete.

## Actual evidence

Evidence is retained under `/tmp/dbms-materialized-readonly.wP7CV3tN`:

- `delete-priority-red-build.log` preserves an initial test harness compile
  error (`tableName` instead of the real `tablename` member); it is not a
  production failure or a passed build.
- `delete-priority-red-build-corrected.log` and
  `delete-priority-red/native.log`: the matching old binder exits 134 with
  11 failed strong assertions across 15 controls. Eight SQLSTATE/priority
  assertions and three unknown-predicate descriptor assertions fail.
- An isolated strict PostgreSQL `180006` transaction verifies 13 ordinary
  equivalents, including RETURNING error priority, typed TEXT NULL, valid
  unknown true/false/NULL and the never-called sequence predicate. That
  read-only result is retained in the tool transcript. The expanded
  `mv-target-reference18-expanded.log` additionally verifies the same WHERE
  priority on materialized targets and WITH primary DELETE.
- `final-build.log` and `final/delete_where_boolean_context.log`: all 15
  controls pass with a freshly rebuilt binder, genuine boolean AST result
  types and a first `nextval` of 1. The same group verifies eight adjacent
  native tests. Its 58-TU matching basis was freshly compiled in
  `baseline-build.log`; the unchanged 56 source objects and every header are
  audited before linking the two changed implementation objects.

No public API/layout changes, push, Actions, routine execution or SQL text
substitution are part of this repair. Materialized target denial is a separate
consumer repair and the overall DML family remains partial.
