# Original routine fixture must use canonical SQL type identity

The complete unchanged `function_procedure_test.cpp` fails at its first
`info.returnType == "int"` assertion against the current optimized production
epoch. The original full5ca also fails there. This is not evidence that the
original `CREATE FUNCTION inc(x int) RETURNS int` SQL or its result is broken.

The actual DdlExecutor producer explicitly uses `canonicalFunctionType` for
both SQL arguments and return types before storing them. TypeRegistry's
documented `int`/`int4`/`integer` identity is `integer`; the canonical field is
also used for CREATE OR REPLACE signature comparison. Reverting that producer
to literal alias spelling would weaken the existing replacement semantics.

Only the three stale SQL-metadata expectations change, to exact `integer`,
not an either-spelling conditional. The original SQL, body, native call and
5/42 result assertions remain. Added checks retain the exact canonical
multiargument and replacement identities and TypeRegistry aliases. Every
other original negative, raw native typmod/legacy sidecar, strict/volatility,
TVF, procedure, duplicate/storage-failure, transaction/rollback and replacement
assertion remains untouched.

The permanent strict PostgreSQL18.6 reference checks actual catalog argument
and result types, int4/int/INTEGER alias identity, calls, replacement and the
42P13 no-change error. Its real server is verified as 180006, and its own
unique schema lives inside one rolled-back transaction. The original native
expression-body syntax `AS 'x + 1' LANGUAGE sql` is explicitly retained as a
PG42601 negative; PostgreSQL's separate `SELECT x + 1` positive verifies SQL
value/type semantics, not purported identical native-extension grammar.

Evidence: `/tmp/dbms-function-procedure-type-oracle.PsvFOsPK`.

- Current unchanged whole baseline is the Root exact1d optimized58-consistent
  `original-full-native-current-3.log`, actual50319: function_procedure134.
  Its first failure and all original full5ca logs remain unchanged.
- `reference18.log` is terminal0 for the complete permanent strict reference.
- `current-native-full.log` is terminal0 for nine complete native fixtures:
  the entire original routine fixture, type registry, routine array signature,
  namespace declaration/cold creation/provider/bound slot, actual routine
  engine owner and original typed view-trigger parameters. Original routine
  execution reaches its real `[FUNCTION/PROCEDURE] all passed` endpoint.
- `verify-current-native.sh` checks every production source, all relative
  headers, manifest and actual flags against the Root completed exact90fe
  official O2 epoch, and all original58 object receipts before linking its57
  non-main objects. Only the test and stubs are fresh; this is not a fresh58
  production build or a sanitizer claim. Its donor frozen SHA256 is
  `af651bb8481396d2c098d6a5802b206c4bd6f174cca3ec64ee06747efcf59486`.

No production source, public API, input SQL or failure deadline is changed.
This corrects one fixture contract, not all routine/namespace/cast families.
