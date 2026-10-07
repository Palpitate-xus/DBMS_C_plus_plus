# Validate BIT predicate types before opening or choosing a scan

Empty tables could suppress invalid BIT BETWEEN operand errors because
the parser owns BETWEEN as a FunctionCall expression, while the BIT type
preparer only checked ordinary BinaryOp comparisons. The literal-list
follow-up therefore passed all 56 list controls but still failed the last
original 91-control empty-table INTEGER BETWEEN statement with SELECT 0
instead of PostgreSQL 42883.

The pure expression preparation and real query binder now resolve both
BETWEEN bounds when any operand is BIT/VARBIT. The parser-owned grammar
node remains Boolean. Executor predicate preparation occurs before opening
a filter and before the planner chooses or consumes any indexed condition;
disjunctive/bitmap candidates receive the same checks. Physical column
types come from the real table schema, and typed-expression preparation
does not run nonliteral operands. No public header/layout or deadline changes.

This BIT signature root is separate from the shared inferAstResultType
Boolean return-label repair owned by ROOT; it does not edit that function.

## Exact evidence and scope

Artifacts are under `/tmp/dbms-bit-literal-typing.zyVFLv/`:

- `literal-in-candidate-original-whole91-v2.log`: original permanent SQL fixture,
  shell 2398 exit 1 at empty INTEGER BETWEEN. V3 normal/native shell 11821
  exits 0, but the same original91 shell 82381 still exits 1. That compact
  metadata-only filter attempt did not cover the FunctionCall expression;
  neither failure is hidden or relabeled as a pass.
- `literal-in-candidate-normal-native-v4.log`: shell 97756 exit 0, ten
  complete native controls. Original91 first run shell 31715 and new56
  shell 90261 encounter the unchanged initialization timeout. Identical
  original91 repeat shell 86348 exits 0; no SQL/assertions/deadline changed.
- `literal-in-candidate-normal-native-v5.log`: shell 39498 exits 0, eleven
  complete native drivers. New `bit_predicate_pre_scan_type_test.cpp`
  performs 272 actual planner/filter/prepared-query checks across primary,
  secondary and nonindexed columns, populated/empty/NULL tables, comparisons,
  IN/BETWEEN and conjunctive/disjunctive/bitmap planning. It invokes the
  actual public prepareBoundQuery API, not a copied type evaluator.
- `index-literal-strict-reference18-156.log`: all 156 permanent indexed
  wire controls against actual strict PostgreSQL 18.6 version 180006,
  direct exit 0. `index-literal-baseline-v3-whole156.log`, shell 94390,
  also exits 0: the Main indexed SQL path already checked these types.
  This is a retained positive control, not an invented indexed SQL failure.
- `literal-final-whole-wrapper-v5.log`: shell 39642 exits 0, original91,
  new56, indexed156, quoted26, original bit, stored21, comparison32,
  ARRAY6, original bounds, quantified-demand84, and separate three-query
  group CLI all complete and pass. Native group512 is current project
  modules, not Main; CLI processes share a directory, not one backend.
- Actual normal candidate SHA256 is
  `d517cb97e0a7b3c1e1287af6198414efc3d9fb55721ee1d498d087b3fdb7cf34`.
  All 58 current receipts, cache stamp and immediate repeat are checked;
  six changed CPPs use genuine normal objects and 52 unchanged donors are
  individually source/header/flags/donor58-receipt/object-byte-proved.
  The clean frozen donor remains 90c6/SHA3513ca8f. No fresh58/SAN/full claim.
- Original strict 132-control matrix remains five red, shell 57024 exit 1:
  unknown BIT b/x text input and INTEGER unknown empty-input handling are
  independent. The unknown BIT comparison truncation dependency is handled
  separately by commit 2828fd56, not silently folded into this root.

All owned wire schemas use unique names and cleanup drops only those
schemas; shared PG public data is untouched. TYPE-11, numeric/operator/
descriptor work and the overall checklist are not declared complete.
