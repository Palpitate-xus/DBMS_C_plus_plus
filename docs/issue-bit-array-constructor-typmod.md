# BIT array constructor coercion must not impose bit(1)

## Reproduced defect

At `dbd4dcf7` (production equal to `d661ca4b`), the actual prepared/native
expression `ARRAY[B'01',B'001',B'',NULL]` produced `{0,0,0,NULL}` instead of
PostgreSQL 18.6's `{01,001,"",NULL}`. The new complete native regression
aborted at its original first assertion (134). An earlier strong comparison
probe additionally exposed `B'001'<ANY(ARRAY[B'01'])` returning false: the
right element had already been shortened before the operator saw it.

The constructor reused the scalar explicit `::bit` conversion, whose default
length is one. A constructor's common element type is implicit and has no
length modifier. Nested constructors repeated the same scalar conversion.

## Repair and permanent controls

`ExprEvaluator::evalArrayExpr` validates a scalar BIT constructor element
through the unconstrained bit-string conversion and keeps its declared BIT
identity. An already BIT-typed nested array keeps its complete element
values; the existing NULL, shape, width, and lower-bound checks still run.
There is no repeated evaluation of an element expression, changed header,
public layout, storage format, or explicit scalar-cast behavior.

`bit_array_constructor_value_test.cpp` covers varying lengths, a leading
zero, an empty non-NULL element, a real NULL, an unknown input coerced to BIT,
explicit bit(3)/bit(5) elements, two-dimensional values, VARBIT controls,
and the retained explicit `B'01'::bit = B'0'` default.
`bit_array_constructor_protocol_e2e_test.py` is registered and checks all
six complete values and their actual OIDs (1561/1563/1560), with a strict
`--reference18` mode that rejects any release except 180006.

## Actual verification

Artifacts: `/tmp/dbms-bit-varbit-semantics.JYF0FS`.

- `array-constructor-baseline.corrected-helper.log`: original production
  SHA `29f59b89b4ff3a0ea7de901285891da6d37a90e0fa667f3aa6da15348d989675`,
  new complete native baseline fails 134; wrapper terminal 1.
- `array-constructor-reference18.log`: actual PostgreSQL 18.6, all six
  complete protocol value/OID/NULL/explicit-typmod controls, terminal 0.
- `array-constructor-candidate-normal-native.log`: one fresh optimized
  ExprEvaluator CPP plus 57 exact source/header/flags/original-58-receipt/
  object-byte-proved normal donors. Both normal builds and all 58 current
  receipts/stamp were checked; this is **not** a fresh-58 build or an
  all-58 sanitizer build. Production SHA
  `cd01638250ce099dd611e61e01ad9486fe7b6eec1c7fed20b929cf0885dae174`.
  Complete new native plus the unchanged original `bit`, `type_registry`,
  and `array_concat_type` natives all pass; authoritative wrapper terminal 0.
- `array-constructor-candidate-wire.log` and
  `array-constructor-original-wire.log`: the complete new six-control
  protocol and unchanged original BIT protocol both pass, terminals 0.

The first baseline helper attempt rejected a stale generated candidate
object before running a test. The corrected helper re-proved and migrated
the exact donor bytes and reproduced the native failure. Its log is kept;
that helper rejection is not a database regression.

This closes this constructor root cause only. TYPE-11 remains open: the
broader 150-statement/56-extended-message PostgreSQL matrix has separate
comparison, scalar-cast, result-OID, descriptor, substring, and binary-input
differences. No family-wide completion, full-suite pass, or fix of those
independent errors is claimed here.
