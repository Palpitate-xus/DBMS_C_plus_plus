# RETURNING arrays need declared metadata, not a TEXT fallback

The permanent returning_array_metadata protocol matrix passes strict real
PostgreSQL180006, including six exact INSERT/UPDATE/DELETE/empty-output
controls. The optimized prior b4/1db6 baseline has wrong TEXT OID25 for
array expressions, wrong INTEGER OIDs for stored arrays in stars, strict
NULL array concatenation and brace-shaped TEXT mistakenly treated as arrays.
The same original matrix also exposed the separately repaired quoted
versioned UPDATE assignment bug; changing array descriptors alone would
not correct that mutation.

Legacy RETURNING used its own scalar type whitelist, hardcoded ARRAY and
concatenation to TEXT, and ignored array flags in both star expansions.
The implementation now copies canonical columns into actual row-image
source/ordinal bindings and applies the same pure declared-type inference
as prepared SELECT. The actual session database and owning engine supply
routine metadata. UNKNOWN-only outputs finalize to TEXT; no output datum
or first returned row supplies the descriptor. Array columns retain their
array flags in unqualified and qualified star expansion.

Expression copies retain ArrayExpr.elementType and nestedElements,
BinaryOpExpr.arrayConcat and the existing CastExpr.implicit flag. A raw
legacy RETURNING AST can lack those metadata fields, so its execution-owned
ordinal-bound copy also receives pure prepareArrayTypes before evaluation.
This does not mutate the shared source AST or execute stored routines
while describing the result. Known source declarations, typed NULLs,
quoted V/v identities, OLD/NEW images, multidimensional rank and the CASE
array element common type are preserved by the new controls.

Artifacts under `/tmp/dbms-with-cursor-integration.xOfpugBC`:

- returning-array-reference18-v2.log: exact expanded six-control strict
  PostgreSQL18 reference, actual exit0.
- returning-array-baseline-v2.log, handle76880: actual exit1 against the
  unchanged optimized b4 binary. Earlier five-control baseline76347/exit1
  and reference exit0 remain separately retained.
- returning-array-syntax.log, handle13758: actual syntax-only production
  DML compilation exit0. This is not a runtime passing claim.

The new full public-layout ROOT build and matching native/protocol gates
are still required. All-58 normal optimized compilation is necessary for
the combined CastExpr, ARRAY, logical source visibility and mutation
callback interfaces. The earlier Cast-only frozen3426 combination and
private ARRAY/FROM fixtures are not current-master proof. General CASE
operator binding, expanded scalar child/RETURNING coverage, true array
lower-bound datums and the full compatibility families remain open.
