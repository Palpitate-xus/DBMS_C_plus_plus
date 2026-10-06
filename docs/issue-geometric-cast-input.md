# Geometric CAST input conversion

The original strict-NULL simple CASE controls accepted
`CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END`, returning 2, or
zero rows with `WHERE false`, instead of PostgreSQL 18.6 `22P02`. Runtime
geometric CAST also passed a dynamic malformed TEXT datum through unchanged.

CAST and `::` now transform only genuine direct unknown SQL string constants
with the existing GeometryValue codec during binding. Discarded CASE arms and
zero-row queries still reject invalid input before routine/query execution.
The engine's contextual unknown-string assignment hook uses the same builtin
geometric input contract. Runtime TEXT/UNKNOWN/character inputs and same-type
geometric datums use that codec too, so real TEXT parameters and the four-byte
value `NULL` are validated; actual SQL NULL remains a typed NULL.

`expression/geometric_input.h` resolves raw type-name components once, accepting
only the seven unqualified/pg_catalog canonical builtins. Quoted mixed-case or
custom-schema names are not treated as builtin aliases. It never executes a
function, prepared child, parameter, typed CAST chain or arithmetic expression
during analysis. Numeric-to-geometry/other geometry-to-geometry signatures,
domains, custom casts, precision and complete catalog type resolution are not
claimed by this input fix.

## Actual evidence

All files below are retained in `/tmp/dbms-geometric-cast-input.uD4ShMCZ`:

- `baseline-build.log`: 58 fresh O0 production TUs plus fresh test stubs; all
  source/102-header/native-test audits pass, then the strong native test exits
  134. The new shared helper is present but unused by production in this
  baseline. No ROOT or old-ABI object donor is used.
- `baseline/native.log`: all 60 malformed-input controls fail, including pure
  preparation without callbacks and genuine dynamic TEXT parameters; qualified
  runtime type/NULL positives also expose wrong old passthrough descriptors.
- `geometry-cast-baseline-wire.log`: original whole matrix fails, including
  both NULL PATH controls and a writing CTE. A sequence sentinel reaches 39
  instead of 38: this is a real analysis-before-effects defect, not a rollback
  generation algorithm issue. Failed-statement rows are rolled back.
- `geometry-cast-reference18-v2.log`: strict `180006`, all 38 negative and 28
  valid/NULL/OID controls pass in an isolated rolled-back transaction.
- `input-candidate-build.log`: terminal 0. The three affected TUs are freshly
  recompiled, all 55 donor sources and the same 102 headers are checked, and
  eight matching natives pass (new input, typed literal, equality, original
  geometry, primitive input, INSERT boolean context, UPDATE RETURNING, and
  the original pure constant planner). All 60 errors, 28 valid/NULL parameter
  values and engine metadata no-effects checks pass.
- `geometry-cast-input-candidate-wire.log`: the same unsplit whole matrix exits
  1 exclusively for seven qualified builtin geometry OID assertions, still 25
  instead of the actual geometry OIDs. Every malformed-input SQLSTATE,
  no-partial-result/tag assertion and monotonic sequence sentinel now passes.

Input candidate SHA256:
`bd9d3ad24d5c7952560c6bd23f3f5708b939f0f28276047139e7feff392d5983`.
The remaining qualified result-type root cause is independent and retains
`geometric_cast_result_type_test.cpp` and all original protocol expectations.
The whole script is not registered as a supported green gate until that root
is repaired; no assertions, effects checks or dynamic cases were removed.

The optional peer read-only review found no concrete defect in raw-name
protection/direct-literal-only input scope. It did not execute tests or approve
runtime semantics. No push or Actions ran. Materialized-view denial and the
overall type/CAST/DML families remain open.
