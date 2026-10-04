# Issue 981 — bounded correlated subquery execution evidence

## Finding and scope

OPT-06 calls for parameterized paths, nested-loop inner index scans,
subplan/initplan, memoize, and correlated execution. The codebase has real but
bounded correlated execution paths: scalar subquery expression evaluation and
Volcano semi/anti-join/existence operators. The source explicitly leaves
correlation, aggregate, ordering, and expression forms outside the simple
quantified-subquery planner path until parameterized expression plans exist.

## Verification

- `bash scripts/build_one_test.sh scalar_subquery_projection_cardinality_test`:
  passed; exercises correlated scalar values, empty-result NULL, multi-row
  cardinality error (`21000`), aliases, and simple inner predicates.
- `bash scripts/build_one_test.sh scalar_subquery_self_correlation_test`:
  passed; exercises multiple outer rows, alias binding, and NULL propagation
  for a relation correlated with itself.
- `bash scripts/build_one_test.sh volcano_select_phase51_test`: passed;
  exercises Volcano SemiJoin/AntiJoin, ExistenceFilter, and ANY/ALL behavior.
- The complete registered C++/protocol/E2E suite at the current source state
  passed in the preceding verification run (`All tests passed`).
- No PostgreSQL 18.6 direct oracle or differential was run specifically for
  this bounded audit.

## Remaining work

OPT-06 remains partial. These tests establish only currently supported
execution subsets; they do not establish a general parameterized-path planner,
inner index path selection, initplans, memoize, broad correlated aggregate or
expression support, or PostgreSQL-equivalent costing and invalidation.
