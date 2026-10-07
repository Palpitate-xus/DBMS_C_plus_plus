# Keep routine predicate preparation with its database owner

The new pre-scan type stage correctly checked a typed routine predicate in
the planner, then FilterOp reparsed it without a database owner. That second
guess classified the real VARBIT-returning routine as TEXT and introduced
42883 in the public QueryPlanner path. Ordinary Main routine queries took a
different path and passed, so wire-only validation did not detect this.

Typed-expression preparation now stays with the real database/engine owner
in the planner, including semi-join, EXISTS, quantified and scalar inner
relations. An owner-less filter still checks compact BIT operand metadata,
but does not guess routine signatures. No header, layout or deadline changes.

All evidence is under `/tmp/dbms-bit-literal-typing.zyVFLv/`:

- `literal-routine-native-baseline-v6.log`, shell 64532 exit 1: the permanent
  eight-control driver invokes actual createUDF/QueryPlanner and fails at
  `text = bit`, 42883. The exact combined baseline source is fde214aa;
  its frozen production SHA is 47c63bcf...9bc0a14. No copied evaluator or
  synthetic routine-return metadata substitutes for the real engine.
- `typed-routine-pre-scan-baseline-v3-whole16.log`, shell 91506 exit 1:
  complete strict PG18.6 versus the earlier candidate, four old BETWEEN
  Boolean-label 42804 errors. `typed-routine-pre-scan-candidate-v6-whole16.log`,
  shell 49631 exit 0: all 16 whole Main controls already pass. This does not
  relabel those Main queries as the native owner regression.
- `bit-routine-strict-reference18-whole16.log`: permanent complete wire
  fixture against actual strict PostgreSQL 18.6 version 180006, direct 0.
- `literal-typing-combined-normal-native-v7.log`, shell 35950 exit 0 and
  `literal-typing-combined-normal-native-v8.log`, shell 99441 exit 0: all
  thirteen complete native files pass, including the unchanged new eight.
- `literal-typing-combined-normal-native-v9.log`, shell 85393 exit 0: all
  fourteen complete native files, including actual four-child-path 16,
  actual UDF eight, unknown comparison 3387, predicate-pre-scan 272, typed
  literal78, original stored/API-data/comparison/ARRAY/bit/registry/quantified,
  binding and current project-module typed_group512. That native grouping
  driver links 57 project modules, not Main.
- `literal-typing-original-four-native-v9.log`, shell 62362 exit 0:
  the four remaining unchanged complete 90c6 controls (array concat, TEXT,
  NAME and INTERVAL comparison) also pass against the same production.
- `literal-combined-whole-wrapper-v9.log`, shell 49351 exit 0: twelve whole
  wire files (unknown33, original91, list56, indexed156, routine16, quoted26,
  original bit/stored21/comparison32/ARRAY6/bounds/quantified-demand84) plus
  the three-query CLI pass. CLI queries use separate processes sharing one
  directory, not one backend session.
- Strict permanent 33/91/56/156 files rerun directly against real version
  180006 all exit 0 in `combined-strict-reference18-whole*.log`.
- Production SHA256 is
  `f3df8e1bae16e0f86be351a11c4d4b0452787e260164db39b7fd841f12a95cd3`.
  Every current receipt, repeat and stamp is checked. Six changed CPPs have
  actual normal O2 objects; 52 unchanged donors are exact-source/header/
  flags/all-58-donor-receipt/object-byte-proved 90c6 inputs. No fresh58 claim.
- Original strict matrix `literal-typing-combined-original132-v9.log`,
  shell 38006 exit 1, still reports five independent differences: two BIT
  b/x text input and three INTEGER unknown empty-input errors.

The owner issue is fixed; TYPE-11/the original checklist are not complete.
A subsequent unknown-left mixed BIT/INTEGER list probe is retained as a
separate issue: 40 complete controls expose per-pair conversion differences
in `unknown-list-candidate-baseline-v9-whole40.log`; that follow-up is not
hidden behind the preceding green matrices or included in this owner patch.
All reference schemas/routines are unique owned objects dropped in cleanup;
shared PostgreSQL public objects and ROOT production are untouched.
