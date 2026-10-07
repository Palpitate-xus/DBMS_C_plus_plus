# BIT comparison identity must use ordered bits and their lengths

## Reproduced failures

Actual original production returns false for `B'001'<B'01'`, and true for
`B'1'=B'01'`: the generic comparator parses bit strings as decimal numbers.
The original capability resolver also rejects prepared BIT comparisons,
despite its genuine builtin equality signatures. Whole-query ANY/ALL,
simple CASE, stored column predicates, aggregate output ordering, and
DML qualification expose the same value/type error.

The strong protocol keeps all original controls and adds real stored
column-vs-column comparisons and UPDATE/DELETE RETURNING. The final strict
PostgreSQL 18.6 matrix passes all 32 controls, including actual OIDs, empty
and NULL cells, group/DISTINCT order, changed rows, and preserved rows.

## Repair and permanent regressions

ExprEvaluator compares typed BIT/VARBIT by ordered bits and then length;
the existing resolved builtin comparison gains its strict, exact BIT
equality/hash capability. Storage's typed and column-pair consumers use
the same semantics. Actual aggregate output sorts consult their retained
BIT type descriptor before the old numeric-content heuristic; they do
not infer a type from a display value. Other type behavior is unchanged.
No public header, physical layout, transaction/CID, or provider lifecycle
changes occur.

`bit_comparison_value_test.cpp` checks all ordinary ordering/equality
operators, leading zeros, empty/NULL, a value wider than int64, simple
CASE, typed mixed BIT/VARBIT identities and non-colliding hash keys,
the real storage comparator, and four complete actual PreparedQuery
ANY/ALL cursors over both ARRAY and SQL children.
`bit_comparison_protocol_e2e_test.py` is the registered complete 32-control
real frontend regression; `bit_group_comparison_cli_e2e_test.py` additionally
checks three actual CLI group/alias/direction orders, not a copied sorter.

## Actual verification and retained failures

Artifacts: `/tmp/dbms-bit-varbit-semantics.JYF0FS`.

- `comparison-post-array-baseline.log`: exact preceding array repair,
  still false for `B'001'<B'01'`, whole file terminal 1.
- `comparison-final-reference18.log`: all 32 strict 180006 controls, 0.
- `comparison-final-normal-native.log`: the earlier isolated comparison
  stage passes all eight complete natives, including unchanged original
  quantified, text/name/interval, and the full 512-row typed group test.
  Its protocol still fails the separately repaired stored-literal adapter;
  no complete whole pass is claimed for that stage.
- `group-comparison-cli-baseline.log`: actual pre-Main-fix SHA a39e2035...
  CLI stdout proves `1,01,001` numeric grouping order; unchanged strong
  row-order assertion fails, terminal 1.
- `combined-frontend-final-normal-native.log`: actual normal build/repeat,
  all 58 current source/header/flags/receipt/stamp checks, 11 complete
  natives all 0. Final Main was freshly compiled; current modified
  ExprEvaluator/TableManage and 55 proved normal donor objects are reused.
  It is not a fresh-58 build or an all-58 sanitizer claim. Production SHA
  `3513ca8f908815d1ac9599e69ab96d76e288d9e4ef3a2c5ba4e712862dc3bc5f`.
- `combined-frontend-final-whole-wrapper.log`: all four complete files 0,
  including all 32 comparison/21 literal controls and both original BIT
  and preceding array controls; `group-comparison-cli-candidate.log`
  additionally passes all three complete CLI orders, terminal 0.

The first combined wire run times out at CREATE SCHEMA on its original
deadline; its complete failure log `combined-comparison-wire.log` remains.
A subsequent run exposed the definite group-order failure, and its
wrapper `combined-repeat-whole-wrapper.log` remains terminal 1. Its
per-file log was inadvertently overwritten by a fixed-name repeat;
`combined-group-sort-wire.failure-tool-transcript.txt` is explicitly the
captured tool tail, not an original shell log. The independent complete
CLI failure remains untouched. Final source/type-correct verification is
separate; no deadline, SQL input, or required row/NULL assertion was weakened.

This comparison commit and the adjacent SQL-literal adapter commit are
verified together; the stored wire controls need both root causes fixed.
TYPE-11 as a whole remains open for unrelated scalar-cast, typmod,
operator-result, substring and binary-I/O differences found by the broader
150-statement/56-extended-message baseline matrix.
