# Predicate lowering must retain quoted identifier identity

The real frontend's `lowerSourceComparison` verified a column reference,
then rendered its decoded physical name without identifier quotes. The
valid column `"01"` became numeric literal `01`. Queries consequently
matched all/no rows; after correct BIT operand checks they instead raised
an invalid INTEGER-versus-BIT error. Quoted identifiers must remain names,
not values, when lowered into the actual stored predicate pipeline.

The seven-line replacement emits a properly double-quoted physical column
name with doubled embedded quotes. It keeps the real parsed operator and
literal bytes, changes no binding namespace, routine evaluation, headers,
storage layout, server deadline or permission behavior.

## Actual complete evidence

Artifacts are under `/tmp/dbms-bit-literal-typing.zyVFLv/`:

- `quoted-column-strict-reference18-v1.log`: all 26 permanent SQL controls
  pass against actual strict PostgreSQL 18.6 version 180006, direct exit 0.
  These include all original 21 numeric-name controls from the unchanged
  broad matrix, an actual qualified range alias and stored NULL.
- `quoted-column-baseline-whole26.log`: the first original-SQL control
  receives `42883` instead of ID 2. Cleanup also hits the unchanged wire
  timeout; shell 4121 exit 1. This failure was retained unedited.
- `quoted-column-baseline-whole26-repeat.log`: exact same fixture/binary/
  deadline, shell 94532 exit 1 at that same value/error mismatch; no timeout
  adjustment or assertion change was used for the repeat.
- Baseline is frozen `dbms_main.literal-type-before-frontend.frozen`,
  SHA256 `1f0009c00c406de4b385cf25f2bc5beebd60d84b53baca730c3cd8d6bfc526f7`.
- `quoted-column-candidate-normal-input-v1.log`: shell 20050 exit 0. Main
  was freshly compiled normal O2; the four previously changed CPPs retained
  their exact matching current-input receipts, and the 53 remaining objects
  are source/header/flags/all-58-donor-receipt/byte-proved normal 90c6 donors.
  Current all-58 receipts, build stamp and immediate repeat passed. No
  native driver argument was run at this Main-only input-audit stage.
- `quoted-column-candidate-whole26.log`: shell 7340 exit 0, all 26 exact
  SQL/rows/NULL/errors/OIDs controls passed.
- `quoted-column-controls-whole-wrapper.log`: shell 54843 exit 0, six
  complete real frontend files passed: new quoted-column protocol, unchanged
  original bit, stored literal, comparison, ARRAY constructor and actual
  three-query group CLI. Independent per-file logs retain their full output.
- `quoted-column-candidate-whole132.log`: original strict 132-control
  matrix is unchanged, shell 81554 exit 1 with seven remaining differences,
  down from 27 before this root. All quoted-column controls are now exact.
- Candidate and frozen `dbms_main.quoted-column.frozen` compare exactly,
  SHA256 `8f6598e970253f7506cf717f85ecf63c832556583af3e6a263e926ec671be17b`.

The remaining broad-matrix errors are preserved: two BIT b/x text input
controls, three INTEGER empty-input errors, frontend literal-IN ordering
and NOT-IN NULL truth. The original 91-control BIT operand fixture still
requires the independent frontend IN root. Neither TYPE-11 nor the original
whole checklist is declared complete by this quoted-name repair.
