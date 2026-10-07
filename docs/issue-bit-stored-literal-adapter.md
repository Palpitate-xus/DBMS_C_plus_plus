# Stored BIT predicates must decode SQL literals, not native API data

## Actual failures and contract

At original `d661ca4b`/`dbd4dcf7`, a stored VARBIT containing `01` does not
match SQL `WHERE v=B'01'`. The compact predicate adapter kept `B'01'` as
text rather than decoding the typed literal. The exact final complete
SQL-adapter native fails its first unchanged assertion (134), and the
complete protocol baseline fails the same predicate. PostgreSQL 18.6
(strict 180006) passes all 21 protocol statements.

Native `apicond` values are already data. A first unpublished intermediate
candidate incorrectly decoded literal-shaped TEXT data there. The new
permanent API guard proves original d661 passes, that intermediate fails
134, and the final candidate passes. Raw `B'01'`, `X'1'`, and even `B'02'`
in TEXT remain ordinary data, not SQL grammar or an invalid bit literal.

## Repair

The SQL compact adapter parses only a bit/hex literal envelope and evaluates
its real literal AST, before scanning. It preserves empty values, rejects
invalid digits, and marks a decoded scalar RHS as a literal rather than a
possible column name. SQL IN/BETWEEN literal lists use the same decoder.
The native data-only API disables that interpretation. No provider,
routine, SQL execution, privilege, public header, or storage format changes.

Permanent regressions are `bit_stored_literal_test.cpp` (the actual
SQL/structured public overload), `bit_stored_literal_api_data_test.cpp`
(unchanged native data contract), and the registered complete
`bit_stored_literal_protocol_e2e_test.py`.

## Exact evidence and independent comparison dependency

Artifacts: `/tmp/dbms-bit-varbit-semantics.JYF0FS`.

- `stored-literal-final-driver-baseline-native.log`: all original d661
  production inputs/58 receipts verified; final exact native assertion
  fails 134. The earlier API-shaped experimental baseline log is retained
  separately and is not substituted for this final-driver baseline.
- `stored-literal-baseline-wire.log`: original SQL predicate fails.
- `stored-literal-reference18.log`: complete 21-statement strict 180006
  value/NULL/hex/IN/BETWEEN/error-code/OID matrix passes, terminal 0.
- `stored-literal-api-data-original.log`: original native data guard passes,
  terminal 0; `stored-literal-api-data-intermediate-counterexample.log`
  proves the unpublished intermediate regression, terminal 1/body 134.
- `combined-frontend-final-normal-native.log`: actual 11 complete natives
  all pass, wrapper terminal 0; all 58 current normal inputs/receipts/stamp
  and repeat build verified. Final build freshly compiles Main and reuses
  the current independently compiled ExprEvaluator/TableManage objects
  plus 55 original source/header/flags/receipt/byte-proved normal donors.
  This is not fresh-58 or all-58 sanitizer evidence. Frozen production SHA
  `3513ca8f908815d1ac9599e69ab96d76e288d9e4ef3a2c5ba4e712862dc3bc5f`.
- `combined-frontend-final-whole-wrapper.log`: all four complete files
  (32 comparison controls, 21 stored-literal statements, six array controls,
  unchanged original BIT protocol) terminate 0; failed=0.

The final matrix includes the separately committed ordered-bit comparison
repair. The first literal-only whole matrix reached BETWEEN but still
matched `B'1'` numerically against `B'01'`; its terminal 1 is retained in
`stored-literal-candidate-wire-v1.log`. That independent comparison error
is not a literal-decoding failure and is not claimed green at that stage.
These two adjacent root-cause commits are handed off together with the
complete final evidence. TYPE-11 remains open for scalar casts, typmods,
operator result types, substring forms and binary wire contracts.
